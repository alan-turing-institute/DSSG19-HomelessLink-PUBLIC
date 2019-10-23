import argparse
import shutil
import os
from utils.evaluation_plots import *
from utils.utils import *
from utils.plot_average_model_feature_importance import *


parser = argparse.ArgumentParser(description='Plot functions to evaluate and compare the models.')
parser.add_argument('-e', '--experiment_id', default='0', type=str, help='ID of experiment.')
parser.add_argument('-ep', '--env_path', default='../.env', type=str, help='Path of the environment file.')
parser = argparse.ArgumentParser(description="""
Plot functions to evaluate and compare the models. Need to give at minimum an
experiment id + a plot type (time / histogram / dates) and also pass k_values
/ baseline flag if plot type is time.
If neither -p or -r are provided the default behaviour is to plot both.
""")
parser.add_argument('-e', '--experiment_id', nargs='+', type=str, help='REQUIRED ARG: ID of experiment(s).')
parser.add_argument('-t', '--time', action='store_true', help='Flag to pick the daily / over time plot type.')
parser.add_argument('-k', '--k_values', nargs='+', type=int, help='The value of k at which to plot precision and/or recall.')
parser.add_argument('-b', '--baseline_k', action='store_true', help='Flag to use the same k as the baseline at each point.')
parser.add_argument('-hi', '--histogram', action='store_true', help='Flag to pick the score density plot type.')
parser.add_argument('-d', '--dates', nargs='+', type=str, help='The date(s) at which to plot k.')
parser.add_argument('-bf', '--by_fold', action='store_true', help='Flag to pick the by-fold plot type.')
parser.add_argument('-ak', '--all_k', action='store_true', help='Flag to pick the by-fold all k type.')
parser.add_argument('-p', '--precision', action='store_true', help='Show precision in relevant plots.')
parser.add_argument('-r', '--recall', action='store_true', help='Show recall in relevant plots.')
parser.add_argument('-m', '--models', nargs='+', type=str, help='ID of the model(s) to plot (unique in experiments table).')
parser.add_argument('-pc', '--param_configs', nargs='+', type=str, help='ID of the parameter config(s) to plot.')
parser.add_argument('-c', '--classifiers', nargs='+', type=str, help='Algorithm type(s) to plot, e.g. RandomForestClassifier.')
parser.add_argument('-f', '--folds', nargs='+', type=str, help='Fold number(s) to plot within an experiment.')
parser.add_argument('-ep', '--env_path', default='../.env', type=str, help='Path of the environment file.')
parser.add_argument('-dpi', default=1000, type=int, help='Resolution of exported graph .PNGs.')
parser.add_argument('-fea', '--feature_importance', action='store_true', help='Show averaged feature importance across models with same set of param configs.')
parser.add_argument('-q', '--quadrants', action='store_true', help='FIRST INDEX IS REFERRALS. Plot quadrants of referral model vs. person found model, supply two experiments and a parameter config to compare.')
parser.add_argument('-z', '--zoom', action='store_true', help='Zoom to limits of plotted points rather than showing full 0 to 1 ranges.')
parser.add_argument('-exp', '--export', action='store_true', help='Export outputs of queries to .csv for pretty plottying in R.')
args = parser.parse_args()

# Behaviour is different for histograms; we are only ever interested in specific models rather than by fold etc.
if args.histogram:
    dir = '../plots/' + 'noexp' + '/' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '/'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)
    plot_model_hist(dir, args.dpi, args.models, args.env_path, args.export)
else:
    dir = '../plots/' + args.experiment_id[0] + '/' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '/'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)

_, c = connect_cursor(load_environment(args.env_path))

# Pull down all the folds in an experiment if none were explicitly provided
if args.folds is None:
    c.execute("select distinct fold from results.experiments where experiment = {}".format(args.experiment_id[0]))
    folds = [str(x[0]) for x in sorted(c.fetchall())]
else:
    folds = sorted(args.folds)

models = {}
# Extract models from passed param_configs / classifiers or get all from an experiment if nothing was specified
for fold in folds:
    if args.classifiers is None and args.models is None and args.param_configs is None:
        c.execute("""
        select model
        from results.experiments
        where experiment = {} and algorithm != 'Baseline' and fold = {}
        """.format(args.experiment_id[0], fold))
    elif args.models is None and args.param_configs is None:
        c.execute("""
        select model
        from results.experiments
        where experiment = {} and reverse(split_part(reverse(algorithm),'.',1)) in ('{}') and fold = {}
        """.format(args.experiment_id[0], "','".join(args.classifiers), fold))
    elif args.models is None:
        c.execute("""
        select model
        from results.experiments
        where experiment = {} and param_config in ({}) and fold = {}
        """.format(args.experiment_id[0], ",".join(args.param_configs), fold))
    else:
        c.execute("""
        select model
        from results.experiments
        where experiment = {} and model in ({}) and algorithm != 'Baseline' and fold = {}
        """.format(args.experiment_id[0], ",".join(args.models), fold))
    models[fold] = [str(x[0]) for x in c.fetchall()]

# Extract param_configs for use in by-fold plotting from passed models / classifiers or get all from an experiment if nothing was specified
if args.classifiers is None and args.models is None and args.param_configs is None:
    c.execute("""
    select param_config
    from results.experiments
    where experiment = {} and algorithm != 'Baseline'
    """.format(args.experiment_id[0]))
    param_configs = [str(x[0]) for x in c.fetchall()]
elif args.param_configs is None and args.models is None:
    c.execute("""
    select param_config
    from results.experiments
    where experiment = {} and reverse(split_part(reverse(algorithm),'.',1)) in ('{}') and algorithm != 'Baseline'
    """.format(args.experiment_id[0], "','".join(args.classifiers)))
    param_configs = [str(x[0]) for x in c.fetchall()]
elif args.param_configs is None:
    c.execute("""
    select param_config
    from results.experiments
    where experiment = {} and model in ({}) and algorithm != 'Baseline'
    """.format(args.experiment_id[0], ",".join(args.models)))
    param_configs = [str(x[0]) for x in c.fetchall()]
else:
    param_configs = args.param_configs
c.close()

# Set both metrics to true if neither flags were provided
metrics = (args.precision, args.recall)
if metrics == (False, False):
    metrics = (True, True)

# Plot type flags determine which graphs to actually plot
if args.time:
    plot_over_time(dir, args.experiment_id[0], args.k_values, args.baseline_k, models, args.dpi, metrics, args.env_path)
if args.by_fold:
    plot_over_fold(dir, args.experiment_id[0], args.k_values, args.baseline_k, param_configs, folds, args.dpi, metrics, args.env_path, args.export)
if args.all_k:
    plot_all_k_over_fold(dir, args.experiment_id[0], param_configs, folds, args.dpi, metrics, args.env_path, args.export)
if args.dates is not None:
    plot_all_k(dir, args.experiment_id[0], args.dates, models, args.dpi, metrics, args.env_path)
if args.quadrants:
    plot_quadrants(dir, args.dpi, args.experiment_id, folds, args.param_configs, args.k_values, args.env_path, args.zoom, args.export)
