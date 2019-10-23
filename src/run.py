import warnings
warnings.filterwarnings('always')
import argparse
import logging
logger = logging.getLogger(__name__)
from pipeline.pipeline import loop_the_grid
from utils.utils import load_environment, load_experiment, load_nlp_environment
from utils.get_location_entities import *
from utils.get_verb_entities import *
from post_modelling.bias_matrix import *


LEC_PATH = 'utils/entities/location_entities_cleaned.pickle'
AAV_PATH = 'utils/entities/all_alerts_verbs.pickle'

logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('../logging/pipeline_debug.log')
formatter = logging.Formatter('%(levelname)s ~ %(asctime)s | %(message)s | %(lineno)d :: %(funcName)s :: %(name)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

parser = argparse.ArgumentParser(description='Pipeline for Homeless Link modelling tasks, can also provide the -l or -v flags to extract location and verb entities from location details field.')
parser.add_argument('-r', '--run', action='store_true', help='Choose whether to run the loop grid function.')
parser.add_argument('-e', '--experiment', default='test', type=str, help='Name of experiment.')
parser.add_argument('-ep', '--env_path', default='../.env', type=str, help='Path of environment file containing database credentials.')
parser.add_argument('-l', '--location', action='store_true', help='Extract location entities from location details.')
parser.add_argument('-v', '--verb', action='store_true', help='Extract verb entities from location details.')
parser.add_argument('-ref', '--referrals', action='store_true', help='Required when working with the referrals label definition.')
parser.add_argument('-bm', '--bias_matrix', action='store_true', help='')
parser.add_argument('-k', '--k_values', nargs='+', type=str)
parser.add_argument('-m', '--models', nargs='+', type=str)
parser.add_argument('-a', '--age', action='store_true')
parser.add_argument('-ge', '--gender', action='store_true')
parser.add_argument('-lo', '--london', action='store_true')
parser.add_argument('-n', '--notifier', action='store_true')
args = parser.parse_args()

if args.location:
    from utils.get_location_entities import *
    environs = load_environment(args.env_path)
    le_path = './src/utils/entities/location_entities_cleaned.pickle'
    get_location_from_all_data(le_path, environs)

if args.verb:
    from utils.get_verb_entities import *
    environs = load_environment(args.env_path)
    ve_path = './src/utils/entities/verb_entities_cleaned.pickle'
    get_verb_from_all_data(ve_path, environs)

if args.run:
    logger.info("Loading experiment '{}.yaml' and setting up environment...".format(args.experiment))
    environs = load_environment(args.env_path)
    exp = load_experiment(args.experiment)
    all_location_entities, all_verb_entities = load_nlp_environment(LEC_PATH, AAV_PATH)
    loop_the_grid(environs, exp, all_location_entities, all_verb_entities, dir, args.referrals)

if args.bias_matrix:
    environs = load_environment(args.env_path)
    if args.age:
        for model in args.models:
            for k in args.k_values:
                run_query_age(k, model, environs)
    if args.gender:
        for model in args.models:
            for k in args.k_values:
                run_query_gender(k, model, environs)
    if args.london:
        for model in args.models:
            for k in args.k_values:
                run_query_london(k, model, environs)
    if args.notifier:
        for model in args.models:
            for k in args.k_values:
                run_query_notifier(k, model, environs)
