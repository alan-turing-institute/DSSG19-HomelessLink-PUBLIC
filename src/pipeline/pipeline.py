import warnings
warnings.filterwarnings('always')
import importlib
import logging
logger = logging.getLogger(__name__)
import gc
import os
import json
import shutil
import time

import time
from contextlib import contextmanager
from typing import NewType, TypeVar
from pandas.plotting import register_matplotlib_converters
from tqdm import tqdm
from utils.utils import *
from utils.entity_lda import *
#from utils.feature_importance_scaler import *
from utils.plot_average_model_feature_importance import *
from utils.count_keywords import *


register_matplotlib_converters()
dt = NewType('ndt', datetime.date)
predictor = TypeVar('predictor')
estimator = TypeVar('estimator')


@contextmanager
def timeit_context(name):
    """
    UNUSED IN PRODUCTION
    Wrapper to time a function for comparison purposes
    """
    start_time = time.time()
    yield
    elapsed_time = time.time() - start_time
    print('[{}] finished in {} s'.format(name, elapsed_time))


def create_temporal_folds(exp):
    """
    Returns list of as_of_dates to be used in temporal cross validation
    """
    as_of_dates = []
    temporal = exp['temporal_deltas']
    data_starts = temporal['data_starts']
    data_ends = temporal['data_ends']
    test_label_span = temporal['test_label_span']
    test_span = temporal['test_span']
    test_frequency = temporal['test_frequency']
    train_label_span = temporal['train_label_span']
    train_span = temporal['train_span']
    train_frequency = temporal['train_frequency']
    model_update_frequency = temporal['model_update_frequency']
    exp['temporal_deltas']['cutoff'] = []

    model_cur = data_ends
    while model_cur > data_starts + train_label_span + test_span + test_label_span:
        """
        Loop starts at the end of the data (per the experiment yaml) and steps back by model_update_frequency each iteration, recording all as_of_dates according to the test / train frequencies
        If a date is strictly less than cutoff then it must be in the 'past' relative to this cohort and thus part of the training data
        """

        # Starting at the beginning of the test data, insert dates with frequency test_frequency stepping forwards, up until and including the end of the test data (model_cur - test_label_span)
        as_of_dates_test = []
        cur = model_cur - (test_label_span + test_span)
        exp['temporal_deltas']['cutoff'].insert(0, cur.date())
        while cur < (model_cur - test_label_span):
            as_of_dates_test.append(cur)
            cur += test_frequency
        as_of_dates_test.append(model_cur - test_label_span)

        # Starting at the end of the train data, insert dates with frequency train_frequncy stepping backwards, up until start of the train data
        as_of_dates_train = []
        cur = max(model_cur - (test_label_span + test_span + train_label_span), data_starts)
        while (cur >= (model_cur - (test_label_span + test_span + train_label_span + train_span))) and (cur >= data_starts):
            as_of_dates_train.append(cur)
            cur -= train_frequency
        model_cur -= model_update_frequency

        # Append all of the dates from the inner two while loops, reversing the train ones such that the dates begin at the earliest date
        as_of_dates.append(as_of_dates_train[::-1] + as_of_dates_test)

    return as_of_dates[::-1], exp


def create_cohort(i, environs, exp, as_of_dates):
    """
    Creates a table (named according to the passed experiment yaml) in the temp schema containing ids and the determining characteristic values for that cohort
    """
    _, c = connect_cursor(environs)
    c.execute('drop table if exists temp.{}{}'.format(exp['cohort']['name'], str(i)))
    c.execute('drop table if exists temp.fold{}'.format(str(i)))
    c.execute('create table temp.fold{} (datetime_opened date)'.format(str(i)))
    f = IterFile((as_of_date.strftime("%Y-%m-%d") for as_of_date in as_of_dates))
    c.copy_expert("copy temp.fold{} from STDIN".format(str(i)), f)
    c.execute(exp['cohort']['query'].format(exp['cohort']['name'], str(i), str(i)))
    c.close()


def create_labels(i, environs, exp, referrals):
    """
    Adds a label column based on label spans and outcomes to the cohort table, spans defined in the experiment yaml
    """
    _, c = connect_cursor(environs)
    c.execute('drop table if exists temp.{}{}_temp'.format(exp['cohort']['name'], str(i)))
    if referrals:
        c.execute(exp['label']['query'].format(exp['cohort']['name'], str(i), ','.join(exp['label']['positive_definition']), ','.join(exp['label']['negative_definition']), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
    else:
        c.execute(exp['label']['query'].format(exp['cohort']['name'], str(i), exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), ','.join(exp['label']['positive_definition']), exp['temporal']['test_label_span'], exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['temporal']['train_label_span'], exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), ','.join(exp['label']['negative_definition']), exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
    c.execute('drop table if exists temp.{}{}'.format(exp['cohort']['name'], str(i)))
    c.execute('alter table temp.{}{}_temp rename to {}{}'.format(exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
    c.close()


def create_features_sql(i, environs, exp):
    """
    Augment the cohort table with all of the features that can be extracted from the data, the result of all of the tables in the features schema being joined on the cohort, filtered via the yaml experiment
    """
    _, c = connect_cursor(environs)
    c.execute('drop table if exists temp.{}{}_temp'.format(exp['cohort']['name'], str(i)))
    features_string = ''
    for feature in exp['features']['feature_list']:
        features_string += ',"' + feature + '"'
    c.execute(exp['features']['query'].format(exp['cohort']['name'], str(i), features_string, exp['cohort']['name'], str(i)))
    c.execute('drop table if exists temp.{}{}'.format(exp['cohort']['name'], str(i)))
    c.execute('alter table temp.{}{}_temp rename to {}{}'.format(exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
    c.close()


def create_features_python(i, environs, exp, all_location_entities, all_verb_entities) -> pd.DataFrame:
    """
    Further augment the cohort table by pulling cohort + sql-generated features into a pandas dataframe and creating more features in python
    Creates LDA, dictionary counts and topic modelling features before returning the full dataframe, filtered via the yaml experiment
    """
    conn, _ = connect_cursor(environs)
    featured_cohort = pd.read_sql('select * from temp.{}{}'.format(exp['cohort']['name'], str(i)), conn)
    all_alerts = pd.read_sql('select alert, loc_app_details from semantic.alerts', conn)
    conn.close()

    lda_location = get_lda_score(featured_cohort, all_location_entities, 10, 'loc_entities')
    lda_verbs = get_lda_score(featured_cohort, all_verb_entities, 10,'verbs')

    # Need to filter for only the python features in python_feature_list
    topic_features = pd.merge(lda_location, lda_verbs,  on='alert', how='inner')

    # Merge with keyword counts
    loc_appearance = pd.merge(featured_cohort, all_alerts, on='alert', how='inner')
    count_keywords  = create_count_feature(loc_appearance, '../keyword_diction/keyword_dictionary.xlsx')
    topic_count = pd.merge(topic_features, count_keywords, on='alert', how='inner')

    python_fea = exp['features']['python_feature_list']
    selected_python_features = topic_count[python_fea]
    selected_python_features['alert'] = topic_count['alert']

    featured_cohort = pd.merge(featured_cohort, selected_python_features, on='alert', how='inner')
    featured_cohort['datetime_opened'] = featured_cohort['datetime_opened']
    return featured_cohort



def create_matrices(i, cohort, exp):
    """
    Returns train and test features and labels matrices
    """
    # Splits cohort according to cutoff (= data_ends - test_label_span - test_span), train: strictly before cutoff, test: after cutoff
    train = cohort.loc[cohort['datetime_opened'] < exp['temporal_deltas']['cutoff'][i]]
    test = cohort.loc[cohort['datetime_opened'] >= exp['temporal_deltas']['cutoff'][i]]
    return train.drop('label', axis=1), train[['alert', 'label']], test.drop('label', axis=1), test[['alert', 'label', 'datetime_opened']]


def count_test_referrals(i, environs, exp):
    """
    Counts the number of referrals in the current fold's TEST data
    """
    _, c = connect_cursor(environs)
    c.execute("""
    select count(*)
    from temp.{}{} t1
    inner join semantic.alerts t2
    on t1.alert = t2.alert
    where referral_flag = 1 and t1.datetime_opened::date >= '{}'::date
    """.format(
        exp['cohort']['name'], str(i),
        exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'))
    )
    num_referrals = c.fetchone()[0]
    c.close()
    return num_referrals


def calculate_baseline(i, environs, exp):
    """
    Outputs the percentage of persons found amongst referrals in a cohort, recall will always be 1 for baseline
    """
    _, c = connect_cursor(environs)
    c.execute("""
    select
    date(d.date_series) as datetime_opened,
    SUM(case when t1.label is not NULL then 1 else 0 end) AS referrals,
    SUM(case when t1.label = 1 then 1 else 0 end) AS person_found,
    COALESCE(
    NULLIF(SUM(case when t1.label = 1 then 1 else 0 end),0)::float / NULLIF(SUM(case when t1.label is not NULL then 1 else 0 end),0)::float,0
    ) as precision
    from (
    select generate_series(min(date(datetime_opened)), max(date(datetime_opened)), '{}') as date_series
    from temp.{}{}) d
    left join temp.{}{} t1 on t1.datetime_opened::date = d.date_series::date
    inner join semantic.alerts t2 on t1.alert = t2.alert
    where t1.datetime_opened::date >= '{}'::date
    group by d.date_series::date
    """.format(
        exp['temporal']['test_frequency'],
        exp['cohort']['name'], str(i),
        exp['cohort']['name'], str(i),
        exp['temporal_deltas']['cutoff'][i])
    )
    precision_at_ks = {}
    output = c.fetchall()
    for row in output:
        precision_at_ks[row[0].strftime('%Y-%m-%d')] = {str(row[1]): row[3]}
    recall_at_ks = -1
    c.close()
    return precision_at_ks, recall_at_ks


def get_classifier(name, params):
    """
    Returns an instance of a classifier based on a string of the package and parameters from the yaml experiment
    """
    module_name, class_name = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    clf = getattr(module, class_name)
    instance = clf(**params)
    return instance


def train(train_X:pd.DataFrame, train_y:pd.DataFrame, clf:estimator) -> predictor:
    """
    Returns a trained model by fitting with train_x and train_y
    """
    model = clf.fit(train_X, train_y['label'])
    return model


def evaluate(exp, i, test_X: pd.DataFrame, test_y: pd.DataFrame, model: predictor):
    """
    Makes predictions using the passed model
    Uses predictions to calculate precision and recall at all possible k
    Returns the alerts, predicted scores from the model, and metric values at all k, to be inserted in results.predictions / experiments
    """
    output = test_y.groupby(test_y['datetime_opened']).size().reset_index(name='Count')
    # returns model output and reorders it based on probability
    test_y = test_y.assign(prob=model.predict_proba(test_X)[:, 1])

    precision_at_ks = {}
    recall_at_ks = {}

    for row in output.iterrows():
        temp = test_y.loc[test_y['datetime_opened'] == row[1][0]]
        temp_cumsum = temp.sort_values(by=['prob'], ascending=False)['label'].cumsum()
        temp_cumcount = temp.sort_values(by=['prob'], ascending=False)['label'].notna().cumsum()

        temp_cumsum.reset_index(inplace=True, drop=True)
        temp_cumcount.reset_index(inplace=True, drop=True)
        # We set to -1 if it is ever undefined, i.e. when there are 0 people found / no true labels in temp
        precision_at_ks[row[1][0].strftime("%Y-%m-%d")] = [null_division(temp_cumsum[:k].max(), temp_cumcount[:k].max()) for k in range(1, len(temp_cumsum) + 1)]
        recall_at_ks[row[1][0].strftime("%Y-%m-%d")] = [null_division(temp_cumsum[:k].max(), temp_cumsum.max()) for k in range(1, len(temp_cumsum) + 1)]

    alerts, probs = test_y['alert'], test_y['prob']
    del test_X
    del test_y
    gc.collect()
    return alerts, probs, precision_at_ks, recall_at_ks


def create_cohort_outcome_graphs(train_x, train_y, environs):
    """
    Plotting to show composition of the data used in a particular experiment in terms of an alert outcome breakdown
    """
    train_count = count_classes_by_day(train_x, train_y, environs)
    test_count = count_classes_by_day(train_x, train_y, environs)
    plot_classes_percentage(train_count, str(i), 'train')
    plot_classes_percentage(test_count, str(i), 'test')
    plot_classes_count(train_count, str(i), 'train')
    plot_classes_count(test_count, str(i), 'test')


def setup_features(exp, environs):
    """
    Selects all possible features using the results.features table
    Generates a row of booleans to insert where included features are represented by True and the rest are False
    Returns the feature set used in current experiment
    """
    _, c = connect_cursor(environs)
    c.execute("""
        select column_name
        from information_schema.columns
        where table_schema='results' and table_name='features'
        """)
    all_features = ['"' + row[0] + '"' for row in c]
    included_features = exp['features']['feature_list'] + exp['features']['python_feature_list']
    included_features_string = ''
    check_string = ''
    for feature in all_features[1:]:
        if feature[1:-1] in included_features:
            included_features_string += "True, "
            check_string += feature + '=True and '
        else:
            included_features_string += "False, "
            check_string += feature + '=False and '
    try:
        c.execute("select * from results.features where {}".format(check_string[:-4]))
        feature_set = c.fetchone()[0]
    except:
        c.execute("""
            insert into results.features ({}) values ({}) returning feature_set
            """.format(",".join(all_features[1:]), included_features_string[:-2]))
        feature_set = c.fetchone()[0]
    c.close()
    return feature_set


def loop_the_grid(graph, environs, exp, all_location_entities, all_verb_entities, dir, referrals):
    """
    Starts by initiating logging for the loop, then generates temporal folds and iterates through at <model_update_frequency>
        Creates cohort, labels and features, then loads in dictionary of classifiers and parameter get_combinations
            Trains and evaluates with each classifier + param combination, stores results in database
    """
    _, c = connect_cursor(environs)
    logger.info('Running experiment ID {}'.format(exp['id']))
    logger.info(str(exp))
    feature_set = setup_features(exp, environs)
    c.execute("""
    select column_name
    from information_schema.columns
    where table_schema = 'results' and table_name = 'experiments'
    """)
    experiment_colnames = [row[0] for row in c]

    all_temporal_folds, exp = create_temporal_folds(exp)
    shutil.rmtree('../matrices/' + exp['id'], ignore_errors=True)
    os.mkdir('../matrices/' + exp['id'])

    t1 = tqdm(all_temporal_folds)
    for i, temporal_folds in enumerate(t1):
        t1.set_description("Cohort")
        logger.info("Creating cohort")
        create_cohort(i, environs, exp, temporal_folds)
        t1.set_description("Labels")
        logger.info("Creating labels")
        create_labels(i, environs, exp, referrals)
        t1.set_description("Features")
        logger.info("Creating sql features")
        create_features_sql(i, environs, exp)

        t1.set_description("Matrices")
        logger.info("Creating matrices with python features")
        train_x, train_y, test_x, test_y = create_matrices(
            i, create_features_python(i, environs, exp, all_location_entities, all_verb_entities), exp
        )
        train_x_fp = '../matrices/{}/train_x_{}.gz'.format(exp['id'], str(i))
        train_y_fp = '../matrices/{}/train_y_{}.gz'.format(exp['id'], str(i))
        test_x_fp = '../matrices/{}/test_x_{}.gz'.format(exp['id'], str(i))
        test_y_fp = '../matrices/{}/test_y_{}.gz'.format(exp['id'], str(i))
        train_x.to_csv(train_x_fp)
        train_y.to_csv(train_y_fp)
        test_x.to_csv(test_x_fp)
        test_y.to_csv(test_y_fp)

        num_nulls = test_y['label'].isna().sum()
        num_referrals = count_test_referrals(i, environs, exp)

        t1.set_description("Baseline")
        precisions_baseline, recalls = calculate_baseline(i, environs, exp)
        c.execute("""
        insert into results.experiments ({})
        values ({}, {}, {}, {}, '{}', NOW(), '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}') returning model
        """.format(
            ', '.join(experiment_colnames[1:]),
            exp['id'], str(i),
            'NULL', 'NULL', 'Baseline',
            train_x_fp, train_y_fp, test_x_fp, test_y_fp,
            str(num_nulls), str(num_referrals),
            json.dumps(precisions_baseline), json.dumps(recalls), 'NULL', 'NULL')
        )
        train_x.drop(['alert', 'datetime_opened'], inplace=True, axis=1)
        if graph:
            t1.set_description("Graphing")
            create_cohort_outcome_graphs(train_x, train_y, environs)

        t1.set_description("Modelling")
        t2 = tqdm(exp['classifiers'].items())
        for classifier, parameters in t2:
            t2.set_description(classifier.rsplit('.', 1)[-1])
            keys = parameters.keys()
            # Turns dict of {key1 : [list1], key2 : [list2]} into [{key1 : item11, key2 : item21}, {key1: item12, key2 : item21}, ...] to create individuals classifiers
            t3 = tqdm(list(product(*parameters.values())), desc='Train + Eval')
            for values in t3:
                logger.debug("Training a {} with {}".format(classifier.rsplit('.', 1)[-1], str(dict(zip(keys, values)))))
                clf = get_classifier(classifier, dict(zip(keys, values)))
                params = clf.get_params()
                params.pop('verbose', None)
                param_string = "param_config integer not null default nextval('param_config_seq'), "
                param_colnames = ''
                param_values_string = ''
                check_string = ''
                for key in params:
                    param_colnames += key + ', '
                    param_string += key + ' varchar, '
                    param_values_string += "'{}', ".format(params[key])
                    check_string += key + "='" + str(params[key]) + "' and "
                c.execute("""
                create table if not exists results.parameters{}({})
                """.format(classifier.rsplit('.', 1)[-1], param_string[:-2]))

                try:
                    c.execute("""
                    select *
                    from results.parameters{}
                    where {}
                    """.format(classifier.rsplit('.', 1)[-1], check_string[:-4]))
                    param_config = c.fetchone()[0]
                except:
                    c.execute("""
                    insert into results.parameters{} ({})
                    values ({})
                    returning param_config
                    """.format(classifier.rsplit('.', 1)[-1], param_colnames[:-2], param_values_string[:-2]))
                    param_config = c.fetchone()[0]

                model = train(train_x, train_y, clf)
                alerts, probs, precisions, recalls = evaluate(
                    exp, i, test_x.copy().drop(['alert', 'datetime_opened'], axis=1), test_y.copy(), model
                )

                if (classifier.rsplit('.', 1)[-1] == 'RandomForestClassifier') or (classifier.rsplit('.', 1)[-1] == 'ExtraTreesClassifier') or (classifier.rsplit('.', 1)[-1] == 'AdaBoostClassifier'):
                    importance_dict, std_importance_dict = get_feature_importance_dictionary(train_x,model,classifier)
                    c.execute("""
                    insert into results.experiments ({})
                    values ({}, {}, {}, {}, '{}', NOW(), '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}')
                    returning model
                    """.format(
                        ', '.join(experiment_colnames[1:]),
                        exp['id'], str(i),
                        str(feature_set), str(param_config), classifier,
                        train_x_fp, train_y_fp, test_x_fp, test_y_fp,
                        str(num_nulls), str(num_referrals),
                        json.dumps(precisions), json.dumps(recalls), json.dumps(importance_dict), json.dumps(std_importance_dict))
                    )
                else:
                    c.execute("""
                    insert into results.experiments ({})
                    values ({}, {}, {}, {}, '{}', NOW(), '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}')
                    returning model
                    """.format(
                        ', '.join(experiment_colnames[1:]),
                        exp['id'], str(i),
                        str(feature_set), str(param_config), classifier,
                        train_x_fp, train_y_fp, test_x_fp, test_y_fp,
                        str(num_nulls), str(num_referrals),
                        json.dumps(precisions), json.dumps(recalls), 'NULL', 'NULL')
                    )

                model_id = c.fetchone()[0]
                if (classifier.rsplit('.', 1)[-1] == 'RandomForestClassifier')  or (classifier.rsplit('.', 1)[-1] == 'ExtraTreesClassifier'):
                    plot_feature_importance_rf(exp, train_x, model, classifier, model_id)
                elif (classifier.rsplit('.', 1)[-1] == 'LogisticRegression')  or (classifier.rsplit('.', 1)[-1] == 'ExtraTreesClassifier'):
                    plot_feature_importance_logr(exp, train_x, model, classifier, model_id)
                # try strftime or whatever if date below doesnt work
                f = IterFile(
                    ("{}\t{}\t{}\t{}\t{}".format(
                        date, alert, int(label) if label in (0, 1) else '\\N', str(prob), str(model_id)
                    ) for date, alert, label, prob in zip(test_x['datetime_opened'], alerts, test_y['label'], probs))
                )
                c.copy_expert("copy results.predictions from STDIN", f)
    c.close()
