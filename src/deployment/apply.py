import gc

from utils.count_keywords import *


def apply_to_incoming_data(new_data, model_id):
    """
    WORK IN PROGRESS
    Applies a chosen model to incoming data and returns a list of prioitised alerts
    """
    new_data['datetime_opened'] = new_data['datetime_opened'].dt.tz_localize(None)

     _, c = connect_cursor(environs)
     c.execute(f'select train_x_fp, train_y_fp, algorithm, param_config, feature_set from results.experiments where model = {model_int}')
     exp = c.fetchone()
     #fetches necessary columns
    train_x_fp, train_y_fp, classifier, param_config, full_classifier, feature_set = exp[0], exp[1], exp[2].rsplit('.',1)[-1].lower(), exp[3], exp[2], exp[4]

    c.execute(
     f'''
     select *
     from results.parameters{classifier}
     where param_config = {param_config}
     ''')
     param_values = c.fetchone()

    c.execute(
    f"""
    select column_name
    from information_schema.columns
    where table_schema='results' and table_name='parameters{classifier}'
    """)
    param_names = [row[0] for row in c]
    #creates dictionary for param names and param values
    params = {param_names[i]: param_values[i] for i in range(1,len(param_values))}

    clf = get_classifier(full_classifier, params)
    new_train_x = pd.read_csv(train_x_fp,compression='gzip',header=0,sep=',')
    new_train_y = pd.read_csv(train_y_fp,compression='gzip',header=0,sep=',')

    model = train(new_train_x.drop(['alert', 'datetime_opened'], axis=1), new_train_y, clf)

    #INSERT NEW FEATURES FETCH FEATURE LIST FROM

    c.execute(f'select * from results.features where feature_set = {feature_set}')
    feature_values = c.fetchone()
    c.execute(
    f"""
    select column_name
    from information_schema.columns
    where table_schema='results' and table_name='features'
    """)
    feature_names = [row[0] for row in c]
    feature_list= {feature_names[i]: feature_values[i] for i in range(1,len(feature_values))}

    c.execute(f'drop table if exists temp.incoming_data{}')
    c.execute('drop table if exists temp.fold{}'.format(str(i)))
    c.execute('create table temp.fold{} (datetime_opened date)'.format(str(i)))
    f = IterFile((as_of_date.strftime("%Y-%m-%d") for as_of_date in as_of_dates))
    c.copy_expert("copy temp.fold{} from STDIN".format(str(i)), f)
    c.execute(exp['cohort']['query'].format(exp['cohort']['name'], str(i), str(i)))
    c.close()
    new_data = new_data.assign(prob=model.predict_proba(new_data)[:, 1])

     precision_at_ks = {}
     recall_at_ks = {}

     for row in output.iterrows():
         temp = test_y.loc[test_y['datetime_opened'].dt.date == row[1][0]]
         temp_cumsum = temp.sort_values(by=['prob'], ascending=False)['label'].cumsum()
         temp_cumsum.reset_index(inplace=True, drop=True)
         # We set to -1 if it is ever undefined, i.e. when there are 0 people found / no true labels in temp
         precision_at_ks[row[1][0].strftime("%Y-%m-%d")] = [null_division(temp_cumsum[k-1], k) for k in range(1, len(temp_cumsum) + 1)]
         recall_at_ks[row[1][0].strftime("%Y-%m-%d")] = [null_division(temp_cumsum[k-1], temp_cumsum[len(temp_cumsum) - 1]) for k in range(1, len(temp_cumsum) + 1)]

    test_y = test_y.sort_values(by=['prob'], ascending=False)

    alerts, probs = test_y['alert'], test_y['prob']
    del test_X
    del test_y
    gc.collect()
    return alerts, probs, precision_at_ks, recall_at_ks

     """
     Returns train, test features and labels matrices
     CHECK THE DIFFERENCES BETWEEN OLD WAY AND NEW
     """
     # Splits cohort according to cutoff (=data_ends - test_label_span - test_span), train: strictly before cutoff, test: after cutoff
     cohort['datetime_opened'] = cohort['datetime_opened'].dt.tz_localize(None)
     train = cohort.loc[cohort['datetime_opened'] < exp['temporal_deltas']['cutoff'][i]]
     test = cohort.loc[cohort['datetime_opened'] >= exp['temporal_deltas']['cutoff'][i]]
     return train.drop('label', axis=1), train[['alert', 'label']], test.drop('label', axis=1), test[['alert', 'label', 'datetime_opened']]
     #output = test_y.groupby(test_y['datetime_opened'].dt.date).size().reset_index(name='Count')
     # returns model output and reorders it based on probability

     _, c = connect_cursor(environs)
     c.execute('drop table if exists temp.{}{}_temp'.format(exp['cohort']['name'], str(i)))
     c.execute(exp['label']['query'].format(exp['cohort']['name'], str(i), exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), ','.join(exp['label']['definition']), exp['temporal']['test_label_span'], exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['temporal']['train_label_span'], exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
     c.execute('drop table if exists temp.{}{}'.format(exp['cohort']['name'], str(i)))
     c.execute('alter table temp.{}{}_temp rename to {}{}'.format(exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
     c.close()


# def apply_to_incoming_data(new_data: pd.DataFrame, model_id:int):
#     """
#     Applies a chosen model to incoming data and returns a list of prioitized alerts
#     """
#     new_data['datetime_opened'] = new_data['datetime_opened'].dt.tz_localize(None)
#
#      _, c = connect_cursor(environs)
#      c.execute(f'select train_x_fp, train_y_fp, algorithm, param_config, feature_set from results.experiments where model = {model_int}')
#      exp = c.fetchone()
#      #fetches necessary columns
#     train_x_fp, train_y_fp, classifier, param_config, full_classifier, feature_set = exp[0], exp[1], exp[2].rsplit('.',1)[-1].lower(), exp[3], exp[2], exp[4]
#
#     c.execute(
#      f'''
#      select *
#      from results.parameters{classifier}
#      where param_config = {param_config}
#      ''')
#      param_values = c.fetchone()
#
#     c.execute(
#     f"""
#     select column_name
#     from information_schema.columns
#     where table_schema='results' and table_name='parameters{classifier}'
#     """)
#     param_names = [row[0] for row in c]
#     #creates dictionary for param names and param values
#     params = {param_names[i]: param_values[i] for i in range(1,len(param_values))}
#
#     clf = get_classifier(full_classifier, params)
#     new_train_x = pd.read_csv(train_x_fp,compression='gzip',header=0,sep=',')
#     new_train_y = pd.read_csv(train_y_fp,compression='gzip',header=0,sep=',')
#
#     model = train(new_train_x.drop(['alert', 'datetime_opened'], axis=1), new_train_y, clf)
#
#     #INSERT NEW FEATURES FETCH FEATURE LIST FROM
#
#     c.execute(f'select * from results.features where feature_set = {feature_set}')
#     feature_values = c.fetchone()
#     c.execute(
#     f"""
#     select column_name
#     from information_schema.columns
#     where table_schema='results' and table_name='features'
#     """)
#     feature_names = [row[0] for row in c]
#     feature_list= {feature_names[i]: feature_values[i] for i in range(1,len(feature_values))}
#
#     c.execute(f'drop table if exists temp.incoming_data{}')
#     c.execute('drop table if exists temp.fold{}'.format(str(i)))
#     c.execute('create table temp.fold{} (datetime_opened date)'.format(str(i)))
#     f = IterFile((as_of_date.strftime("%Y-%m-%d") for as_of_date in as_of_dates))
#     c.copy_expert("copy temp.fold{} from STDIN".format(str(i)), f)
#     c.execute(exp['cohort']['query'].format(exp['cohort']['name'], str(i), str(i)))
#     c.close()
#     new_data = new_data.assign(prob=model.predict_proba(new_data)[:, 1])
#
#      precision_at_ks = {}
#      recall_at_ks = {}
#
#      for row in output.iterrows():
#          temp = test_y.loc[test_y['datetime_opened'].dt.date == row[1][0]]
#          temp_cumsum = temp.sort_values(by=['prob'], ascending=False)['label'].cumsum()
#          temp_cumsum.reset_index(inplace=True, drop=True)
#          # We set to -1 if it is ever undefined, i.e. when there are 0 people found / no true labels in temp
#          precision_at_ks[row[1][0].strftime("%Y-%m-%d")] = [null_division(temp_cumsum[k-1], k) for k in range(1, len(temp_cumsum) + 1)]
#          recall_at_ks[row[1][0].strftime("%Y-%m-%d")] = [null_division(temp_cumsum[k-1], temp_cumsum[len(temp_cumsum) - 1]) for k in range(1, len(temp_cumsum) + 1)]
#
#     test_y = test_y.sort_values(by=['prob'], ascending=False)
#
#     alerts, probs = test_y['alert'], test_y['prob']
#     del test_X
#     del test_y
#     gc.collect()
#     return alerts, probs, precision_at_ks, recall_at_ks
#
#      """
#      Returns train, test features and labels matrices
#      CHECK THE DIFFERENCES BETWEEN OLD WAY AND NEW
#      """
#      # Splits cohort according to cutoff (=data_ends - test_label_span - test_span), train: strictly before cutoff, test: after cutoff
#      cohort['datetime_opened'] = cohort['datetime_opened'].dt.tz_localize(None)
#      train = cohort.loc[cohort['datetime_opened'] < exp['temporal_deltas']['cutoff'][i]]
#      test = cohort.loc[cohort['datetime_opened'] >= exp['temporal_deltas']['cutoff'][i]]
#      return train.drop('label', axis=1), train[['alert', 'label']], test.drop('label', axis=1), test[['alert', 'label', 'datetime_opened']]
#      #output = test_y.groupby(test_y['datetime_opened'].dt.date).size().reset_index(name='Count')
#      # returns model output and reorders it based on probability
#
#      _, c = connect_cursor(environs)
#      c.execute('drop table if exists temp.{}{}_temp'.format(exp['cohort']['name'], str(i)))
#      c.execute(exp['label']['query'].format(exp['cohort']['name'], str(i), exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), ','.join(exp['label']['definition']), exp['temporal']['test_label_span'], exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['temporal']['train_label_span'], exp['temporal_deltas']['cutoff'][i].strftime('%Y-%m-%d'), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
#      c.execute('drop table if exists temp.{}{}'.format(exp['cohort']['name'], str(i)))
#      c.execute('alter table temp.{}{}_temp rename to {}{}'.format(exp['cohort']['name'], str(i), exp['cohort']['name'], str(i)))
#      c.close()