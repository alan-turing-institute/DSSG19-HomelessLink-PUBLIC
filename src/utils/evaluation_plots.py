from datetime import datetime
import json
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.font_manager import FontProperties
import time
from utils.utils import *
fm = FontProperties()


def plot_model_hist(dir, dpi, models, env_path, export):
    """
    Plots a score density histogram (from 0 to 1) for a specific set of passed models
    """
    _, c = connect_cursor(load_environment(env_path))
    for model in tqdm(models):
        c.execute("select score from results.predictions where model = {}".format(model))
        scores = [x[0] for x in c.fetchall()]
        plt.clf()
        fig, ax = plt.subplots()
        if export:
            out = pd.DataFrame({'scores' : scores})
            out.to_csv('scoresmodel{}.csv'.format(model), sep=',')
        ax.hist(scores, range=[0, 1], align='mid', bins=100, histtype='step')
        ax.set_xlabel('Output Probability')
        ax.set_ylabel('Frequency')
        ax.set_title('Score Density for model ' + model)
        fig.savefig(dir + 'score_density{}.png'.format(model), dpi=dpi, bbox_inches='tight')
        fig.savefig(dir + 'score_density{}.svg'.format(model), bbox_inches='tight')
        plt.close(fig)
    c.close()


def plot_all_k(dir, experiment, dates, models, dpi, metrics, env_path):
    """
    Plots recall and / or precision at every k-value for specific models and dates
    """
    _, c = connect_cursor(load_environment(env_path))
    for date in tqdm(dates):
        plt.clf()
        figp, axp = plt.subplots()
        figr, axr = plt.subplots()
        figb, axb1 = plt.subplots()
        axb2 = axb1.twinx()
        for fold in tqdm(models.values()):
            for model in tqdm(fold):
                c.execute("select algorithm, precision_at_k, recall_at_k from results.experiments where model = {}".format(model))
                (algorithm, precisions, recalls) = c.fetchone()
                if metrics[0] and metrics[1]:
                    axb1.plot(list(range(1, len(precisions[date]) + 1)), precisions[date], label=algorithm.rsplit('.', 1)[-1] + model)
                    axb2.plot(list(range(1, len(recalls[date]) + 1)), recalls[date], label=algorithm.rsplit('.', 1)[-1] + model, linewidth=0.75)
                elif metrics[0]:
                    axp.plot(list(range(1, len(precisions[date]) + 1)), precisions[date], label=algorithm.rsplit('.', 1)[-1] + model)
                elif metrics[1]:
                    axr.plot(list(range(1, len(recalls[date]) + 1)), recalls[date], label=algorithm.rsplit('.', 1)[-1] + model)
        if metrics[0] and metrics[1]:
            axb1.set_xlabel('k')
            axb1.set_ylabel('Precision')
            axb2.set_ylabel('Recall')
            axb1.set_ylim([0,1.05])
            axb2.set_ylim([0,1.05])
            axb1.set_title('Precision and recall at different k values on ' + date)
            axb1.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
            figb.savefig(dir + 'manykboth{}_{}.png'.format(
                experiment, date), dpi=dpi, bbox_inches='tight'
            )
            figb.savefig(dir + 'manykboth{}_{}.svg'.format(
                experiment, date), bbox_inches='tight'
            )
        plt.close(figb)
        if metrics[0] and not metrics[1]:
            axp.set_xlabel('k')
            axp.set_ylabel('Precision')
            axp.set_title('Precision at different k values on ' + date)
            axp.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
            figp.savefig(dir + 'manykprecision{}_{}.png'.format(
                experiment, date), dpi=dpi, bbox_inches='tight'
            )
            figp.savefig(dir + 'manykprecision{}_{}.svg'.format(
                experiment, date), bbox_inches='tight'
            )
        plt.close(figp)
        if metrics[1] and not metrics[0]:
            axr.set_xlabel('k')
            axr.set_ylabel('Recall')
            axr.set_title('Recall at different k values on ' + date)
            axr.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
            figr.savefig(dir + 'manykrecall{}_{}.png'.format(
                experiment, date), dpi=dpi, bbox_inches='tight'
            )
            figr.savefig(dir + 'manykrecall{}_{}.svg'.format(
                experiment, date), bbox_inches='tight'
            )
        plt.close(figr)
    c.close()


def plot_over_time(dir, experiment, k_values, baseline, models, dpi, metrics, env_path):
    """
    Plots specific models' precision and / or recall at specific fixed k values at a daily frequency or at the same k as the baseline each day
    """
    _, c = connect_cursor(load_environment(env_path))
    for fold, fold_models in tqdm(models.items()):
        plt.clf()
        if baseline:
            figp, axp = plt.subplots()
            figr, axr = plt.subplots()
            c.execute("""
            select precision_at_k
            from results.experiments
            where experiment = {} and fold = {} and algorithm = 'Baseline'
            """.format(experiment, fold))
            precisions_baseline = c.fetchone()[0]
            axp.plot([datetime.strptime(key, "%Y-%m-%d") for key in precisions_baseline.keys()], [list(precision.values())[0] for precision in precisions_baseline.values()], label='Baseline')
            for model in tqdm(fold_models):
                c.execute("""
                select algorithm, param_config, precision_at_k, recall_at_k
                from results.experiments
                where experiment = {} and model = {} and fold = {}
                """.format(experiment, model, fold))
                (algorithm, param_config, precisions, recalls) = c.fetchone()
                if metrics[0]:
                    precisions_to_plot = {datetime.strptime(date, "%Y-%m-%d"): precisions[date][int(list(precisions_baseline[date].keys())[0]) - 1] for date in precisions_baseline.keys()}
                    axp.plot(list(precisions_to_plot.keys()), list(precisions_to_plot.values()), label=str(param_config) + algorithm.rsplit('.', 1)[-1] + model)
                if metrics[1]:
                    recalls_to_plot = {datetime.strptime(date, "%Y-%m-%d"): recalls[date][int(list(precisions_baseline[date].keys())[0]) - 1] for date in precisions_baseline.keys()}
                    axr.plot(list(recalls_to_plot.keys()), list(recalls_to_plot.values()), label=str(param_config) + algorithm.rsplit('.', 1)[-1] + model)
            if metrics[0]:
                axp.set_xlabel('Date')
                axp.set_ylabel('Precision')
                axp.set_title('Precision over time compared against the baseline')
                axp.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
                figp.autofmt_xdate()
                axp.fmt_xdata = mdates.DateFormatter("%Y-%m-%d")
                figp.savefig(dir + 'precisiontimebaseline{}_{}.png'.format(
                    experiment, fold), dpi=dpi, bbox_inches='tight'
                )
                figp.savefig(dir + 'precisiontimebaseline{}_{}.svg'.format(
                    experiment, fold), bbox_inches='tight'
                )
            plt.close(figp)
            if metrics[1]:
                axr.set_xlabel('Date')
                axr.set_ylabel('Recall')
                axr.set_title('Recall over time at baseline k')
                axr.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
                figr.autofmt_xdate()
                axr.fmt_xdata = mdates.DateFormatter("%Y-%m-%d")
                figr.savefig(dir + 'recalltimebaseline{}_{}.png'.format(
                    experiment, fold), dpi=dpi, bbox_inches='tight'
                )
                figr.savefig(dir + 'recalltimebaseline{}_{}.svg'.format(
                    experiment, fold), bbox_inches='tight'
                )
            plt.close(figr)
        if k_values is not None:
            for k in tqdm(k_values):
                figp, axp = plt.subplots()
                figr, axr = plt.subplots()
                for model in tqdm(fold_models):
                    c.execute("""
                    select algorithm, param_config, precision_at_k, recall_at_k
                    from results.experiments
                    where experiment = {} and model = {}""".format(experiment, model))
                    (algorithm, param_config, precisions, recalls) = c.fetchone()
                    if metrics[0]:
                        precisions_to_plot = {datetime.strptime(date, "%Y-%m-%d"): precisions[date][int(k) - 1] for date in precisions.keys() if int(k) <= len(recalls[date])}
                        axp.plot(list(precisions_to_plot.keys()), list(precisions_to_plot.values()), label=str(param_config) + algorithm.rsplit('.', 1)[-1] + model)
                    if metrics[1]:
                        recalls_to_plot = {datetime.strptime(date, "%Y-%m-%d"): recalls[date][int(k) - 1] for date in recalls.keys() if int(k) <= len(recalls[date])}
                        axr.plot(list(recalls_to_plot.keys()), list(recalls_to_plot.values()), label=str(param_config) + algorithm.rsplit('.', 1)[-1] + model)
                if metrics[0]:
                    axp.set_xlabel('Date')
                    axp.set_ylabel('Precision')
                    axp.set_title('Precision over time at k = ' + k)
                    axp.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
                    figp.autofmt_xdate()
                    axp.fmt_xdata = mdates.DateFormatter("%Y-%m-%d")
                    figp.savefig(dir + 'precisiontime{}_{}_{}.png'.format(
                        experiment, fold, k), dpi=dpi, bbox_inches='tight'
                    )
                    figp.savefig(dir + 'precisiontime{}_{}_{}.svg'.format(
                        experiment, fold, k), bbox_inches='tight'
                    )
                plt.close(figp)
                if metrics[1]:
                    axr.set_xlabel('Date')
                    axr.set_ylabel('Recall')
                    axr.set_title('Recall over time at k = ' + k)
                    axr.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
                    figr.autofmt_xdate()
                    axr.fmt_xdata = mdates.DateFormatter("%Y-%m-%d")
                    figr.savefig(dir + 'recalltime{}_{}_{}.png'.format(
                        experiment, fold, k), dpi=dpi, bbox_inches='tight'
                    )
                    figr.savefig(dir + 'recalltime{}_{}_{}.svg'.format(
                        experiment, fold, k), bbox_inches='tight'
                    )
                plt.close(figr)
    c.close()


def plot_over_fold(dir, experiment, k_values, baseline, param_configs, folds, dpi, metrics, env_path, export):
    """ Function to plot by-fold average precision or recall at baseline k or
    provided k values, param_config serves as link between models across folds.
    Note that most args are strings for purposes of SQL querying, would logically be ints.

    Parameters:
        dir : string                    (path of directory to save plots)
        experiment : string             (id of experiment to plot)
        k_values : list(string)         (values of k at which to plot)
        baseline : bool                 (whether to plot at baseline)
        param_configs : list(string)    (determines which model groups to plot)
        folds : list(string)            (number of folds at which to plot)
        dpi : ints                      (resolution of exported PNGs)
        metrics : (bool, bool)          (true / false tuple for precision and recall plotting)
        env_path : string               (path of environment file for db connection)

    """
    _, c = connect_cursor(load_environment(env_path))
    plt.clf()
    if baseline:
        figp, axp = plt.subplots()
        figr, axr = plt.subplots()
        baseline_k_values = {}
        precisions = {}
        for fold in folds:
            c.execute("""
            select sum(case when a.label in (1, 0) then 1 else 0 end), sum(case when a.label = 1 then 1 else 0 end) from (
                select distinct alert, label from results.predictions
                where model in (
                    select model from results.experiments
                    where experiment = {} and fold = {} and algorithm != 'Baseline' order by model desc limit 1
                )
            ) a
            """.format(experiment, str(fold)))
            (pn, p) = c.fetchone()
            baseline_k_values[fold] = pn
            precisions[fold] = p / pn

        axp.plot(list(precisions.keys()), list(precisions.values()), label='Baseline')
        for param_config in tqdm(param_configs):
            precisions = {}
            recalls = {}
            for fold in folds:
                c.execute("""
                select distinct alert, score, label from results.predictions
                where model in (
                    select model from results.experiments
                    where experiment = {} and fold = {} and param_config = {} order by model desc limit 1
                ) order by score desc
                """.format(experiment, str(fold), param_config))
                _, _, labels = (item for item in zip(*c.fetchall()))
                precisions[fold] = sum(filter(None, labels[:baseline_k_values[fold]])) / baseline_k_values[fold]
                recalls[fold] = sum(filter(None, labels[:baseline_k_values[fold]])) / sum(filter(None, labels))

            axp.plot(list(precisions.keys()), list(precisions.values()), label=param_config)
            if metrics[1]:
                axr.plot(list(recalls.keys()), list(recalls.values()), label=param_config)

        if metrics[0]:
            axp.set_xlabel('Fold')
            axp.set_ylabel('Precision')
            axp.set_title('Precision over fold compared to baseline')
            axp.set_ylim([0,1.05])
            axp.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
            figp.savefig(dir + 'baselineprecisionbyfold{}.png'.format(
                experiment), dpi=dpi, bbox_inches='tight'
            )
            figp.savefig(dir + 'baselineprecisionbyfold{}.svg'.format(
                experiment), bbox_inches='tight'
            )
        plt.close(figp)
        if metrics[1]:
            axr.set_xlabel('Date')
            axr.set_ylabel('Recall')
            axr.set_title('Recall over fold compared to baseline ')
            axr.set_ylim([0,1.05])
            axr.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
            figr.savefig(dir + 'baselinerecallbyfold{}.png'.format(
                experiment), dpi=dpi, bbox_inches='tight'
            )
            figr.savefig(dir + 'baselinerecallbyfold{}.svg'.format(
                experiment), bbox_inches='tight'
            )
        plt.close(figr)
    if k_values is not None:
        for k in tqdm(k_values):
            figp, axp = plt.subplots()
            figr, axr = plt.subplots()
            for param_config in tqdm(param_configs):
                precisions = {}
                recalls = {}
                for fold in folds:
                    c.execute("""
                    select distinct alert, score, label from results.predictions
                    where model in (
                        select model from results.experiments
                        where experiment = {} and fold = {} and param_config = {} order by model desc limit 1
                    ) order by score desc
                    """.format(experiment, str(fold), param_config))
                    _, _, labels = (item for item in zip(*c.fetchall()))
                    precisions[fold] = sum(filter(None, labels[:k])) / sum([0 if x is None else 1 for x in labels[:k]])
                    recalls[fold] = sum(filter(None, labels[:k])) / sum(filter(None, labels))
                if export:
                    out = pd.DataFrame({'fold' : list(precisions.keys()), 'precision' : list(precisions.values()), 'recall' : list(recalls.values())})
                    out.to_csv('overfoldk{}experiment{}param_config{}.csv'.format(str(k), experiment, param_config), sep=',')
                if metrics[0]:
                    axp.plot(list(precisions.keys()), list(precisions.values()), label=param_config)
                if metrics[1]:
                    axr.plot(list(recalls.keys()), list(recalls.values()), label=param_config)

            k = str(k)
            if metrics[0]:
                axp.set_xlabel('Fold')
                axp.set_ylabel('Precision')
                axp.set_title('Precision over fold at k = ' + k)
                axp.set_ylim([0,1.05])
                axp.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
                figp.savefig(dir + 'precisionbyfold{}_{}.png'.format(
                    experiment, k), dpi=dpi, bbox_inches='tight'
                )
                figp.savefig(dir + 'precisionbyfold{}_{}.svg'.format(
                    experiment, k), bbox_inches='tight'
                )
            plt.close(figp)
            if metrics[1]:
                axr.set_xlabel('Date')
                axr.set_ylabel('Recall')
                axr.set_title('Recall over fold at k = ' + k)
                axr.set_ylim([0,1.05])
                axr.legend(prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
                figr.savefig(dir + 'recallbyfold{}_{}.png'.format(
                    experiment, k), dpi=dpi, bbox_inches='tight'
                )
                figr.savefig(dir + 'recallbyfold{}_{}.svg'.format(
                    experiment, k), bbox_inches='tight'
                )
            plt.close(figr)
    c.close()


def plot_all_k_over_fold(dir, experiment, param_configs, folds, dpi, metrics, env_path, export):
    """
    Plots precision and / or recall across number of referrals potentially made by specific param configs
    on a by-fold level, also shows baseline precision at their 'k' (true number of referrals)
    """
    _, c = connect_cursor(load_environment(env_path))
    for param_config in param_configs:
        for fold in tqdm(folds):
            plt.clf()
            figp, axp = plt.subplots()
            figr, axr = plt.subplots()
            figb, axb1 = plt.subplots()
            axb2 = axb1.twinx()
            c.execute("""select case when label = 1 then 1 else 0 end, case when label in (0, 1) then 1 else 0 end, case when label is NULL then 1 else 0 end, row_number() over (order by score desc) as rank from results.predictions
            where model = (
                select model from results.experiments
                where fold = {} and experiment = {} and param_config = {} limit 1)
            order by score desc
            """.format(fold, experiment, param_config))
            try:
                (top, bot, nulls, all) = zip(*c.fetchall())
                null_proportions = np.array(bot).cumsum() / np.array(all)
                precisions = np.array(top).cumsum() / np.array(bot).cumsum()
                recalls = np.array(top).cumsum() / np.array([np.array(top).cumsum().max() for x in top])
                if export:
                    out = pd.DataFrame({'k' : list(range(1, len(precisions) + 1)), 'null_proportions' : null_proportions, 'precision' : precisions, 'recall' : recalls, 'x' : [np.array(bot).cumsum().max() for i in range(len(precisions))], 'y' : [np.array(top).cumsum().max() / np.array(bot).cumsum().max() for i in range(len(precisions))]})
                    out.to_csv('allkfold{}param_config{}experiment{}.csv'.format(fold, param_config, experiment), sep=',')
            except:
                (top, bot) = [], []
            if metrics[0] and metrics[1]:
                axb1.plot(list(range(1, len(precisions) + 1)), precisions, label=param_config)
                axb2.plot(list(range(1, len(recalls) + 1)), recalls, label=param_config, linewidth=0.75)
                axb2.plot([0, len(recalls) + 1], [0, 1], '--')
                axb1.plot([np.array(bot).cumsum().max()], [np.array(top).cumsum().max() / np.array(bot).cumsum().max()], 'X')
                axb1.set_xlabel('k')
                axb1.set_ylabel('Precision')
                axb2.set_ylabel('Recall')
                axb1.set_ylim([0,1.05])
                axb2.set_ylim([0,1.05])
                axb1.set_title('Precision and recall at different k values in fold ' + fold)
                figb.savefig(dir + 'byfoldmanykboth{}_{}_{}.png'.format(
                    experiment, fold, param_config), dpi=dpi, bbox_inches='tight'
                )
                figb.savefig(dir + 'byfoldmanykboth{}_{}_{}.svg'.format(
                    experiment, fold, param_config), bbox_inches='tight'
                )
            elif metrics[0]:
                axp.plot(list(range(1, len(precisions) + 1)), precisions, label=param_config)
                axp.set_xlabel('k')
                axp.set_ylabel('Precision')
                axp.set_title('Precision at different k values in fold ' + fold)
                figp.savefig(dir + 'byfoldmanykprecision{}_{}_{}.png'.format(
                    experiment, fold, param_config), dpi=dpi, bbox_inches='tight'
                )
                figp.savefig(dir + 'byfoldmanykprecision{}_{}_{}.svg'.format(
                    experiment, fold, param_config), bbox_inches='tight'
                )
            elif metrics[1]:
                axr.plot(list(range(1, len(recalls) + 1)), recalls, label=param_config)
                axr.set_xlabel('k')
                axr.set_ylabel('Recall')
                axr.set_title('Recall at different k values in fold ' + fold)
                figr.savefig(dir + 'byfoldmanykrecall{}_{}_{}.png'.format(
                    experiment, fold, param_config), dpi=dpi, bbox_inches='tight'
                )
                figr.savefig(dir + 'byfoldmanykrecall{}_{}_{}.svg'.format(
                    experiment, fold, param_config), bbox_inches='tight'
                )
            plt.close(figb)
            plt.close(figp)
            plt.close(figr)
    c.close()


def plot_quadrants(dir, dpi, experiment_ids, folds, param_configs, k_values, env_path, zoom, export):
    """
    Meant to be used with 2 experiment, param_config and k arguments to plot a referral label model on the y-axis and a
    person found label model on the x-axis, exposes distributions of both co-varying to break down predictions into quadrants
    """
    _, c = connect_cursor(load_environment(env_path))
    for fold in tqdm(folds):
        plt.clf()
        fig, ax = plt.subplots()
        c.execute("""
        with referrals as (
            select * from results.predictions
            where model = (
                select model from results.experiments
                where experiment = {} and fold = {} and param_config = {}
                order by model desc limit 1
            )
        ),
        person_found as (
            select * from results.predictions
            where model = (
                select model from results.experiments
                where experiment = {} and fold = {} and param_config = {}
                order by model desc limit 1
            )
        )
        select a.label, a.score, case when b.label = 0 then 0 when b.label = 1 then 1 else 2 end, b.score from referrals a
        inner join person_found b using(alert)
        """.format(experiment_ids[0], fold, param_configs[0], experiment_ids[1], fold, param_configs[1]))
        (ref_labels, ref_scores, pf_labels, pf_scores) = zip(*c.fetchall())
        c.execute("""
        select score from (
        	select *, row_number() over (order by score desc) as rank from results.predictions
        	where model = (
        		select model from results.experiments
        		where experiment = {} and fold = {} and param_config = {}
        		order by model desc limit 1
        	)
        ) a where rank = {}
        """.format(experiment_ids[1], fold, param_configs[1], k_values[1]))
        x = c.fetchone()[0]
        c.execute("""
        select score from (
        select *, row_number() over (order by score desc) as rank from results.predictions
            where model = (
                select model from results.experiments
                where experiment = {} and fold = {} and param_config = {}
                order by model desc limit 1
            )
        ) a where rank = {}
        """.format(experiment_ids[0], fold, param_configs[0], k_values[0]))
        y = c.fetchone()[0]
        scatter = ax.scatter(pf_scores, ref_scores, s=1, alpha=0.9, c=pf_labels)
        if export:
            out = pd.DataFrame({'pf_scores' : pf_scores, 'ref_scores' : ref_scores, 'pf_labels' : pf_labels, 'ref_labels' : ref_labels, 'x' : [x for i in range(len(ref_labels))], 'y' : [y for i in range(len(ref_labels))]})
            out.to_csv('quadrantexperiments{}_{}pcs{}_{}ks{}_{}fold{}.csv'.format(experiment_ids[0], experiment_ids[1], param_configs[0], param_configs[1], k_values[0], k_values[1], fold), sep=',')
        if zoom:
            ax.plot([x,x], [min(ref_scores), max(ref_scores)], c='black', linewidth=0.25)
            ax.plot([min(pf_scores), max(pf_scores)], [y,y], c='black', linewidth=0.25)
        else:
            ax.plot([x,x], [0,1], c='black', linewidth=0.25)
            ax.plot([0,1], [y,y], c='black', linewidth=0.25)
            ax.set_xlim([0,1])
            ax.set_ylim([0,1])
        ax.set_xlabel('score(Person Found)')
        ax.set_ylabel('score(Referral)')
        ax.set_title('Person found (pc = {}, k = {}) vs Referral scores (pc = {}, k = {}) from fold {}'.format(param_configs[1], k_values[1], param_configs[0], k_values[0], fold))
        legend = ax.legend(*scatter.legend_elements(), prop=fm.set_size("small"), loc="center left", bbox_to_anchor=(1, 0.5))
        legend.get_texts()[0].set_text('Not Found')
        legend.get_texts()[1].set_text('Found')
        legend.get_texts()[2].set_text('N/A')
        fig.savefig(dir + 'quadrant{}_{}_{}-{}_{}_{}-{}.png'.format(
            experiment_ids[0], param_configs[0], k_values[0], experiment_ids[1], param_configs[1], k_values[1], fold), dpi=dpi, bbox_inches='tight'
        )
        fig.savefig(dir + 'quadrant{}_{}_{}-{}_{}_{}-{}.svg'.format(
            experiment_ids[0], param_configs[0], k_values[0], experiment_ids[1], param_configs[1], k_values[1], fold), dpi=dpi, bbox_inches='tight'
        )
        plt.close(fig)
    c.close()
