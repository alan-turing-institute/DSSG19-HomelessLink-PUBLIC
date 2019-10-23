import io
import pickle
import psycopg2
import re
import sys
import traceback
import yaml
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from itertools import product


class IterFile(io.TextIOBase):
    """
    Accepts an iterator of strings and returns an object to read them as if it were a file
    """
    def __init__(self, iter):
        self.iter = iter
        self.file = io.StringIO()

    def read(self, length=sys.maxsize):
        try:
            while self.file.tell() < length:
                self.file.write(next(self.iter) + "\n")

        except StopIteration as e:
            pass

        except Exception as e:
            traceback.print_exc()

        finally:
            self.file.seek(0)
            data = self.file.read(length)
            remainder = self.file.read()
            self.file.seek(0)
            self.file.truncate(0)
            self.file.write(remainder)
            return data

    def readline(self):
        return next(self.iter)


def get_combinations(n:int):
    """
    UNUSED IN PRODUCTION
    Returns all possible combinations of booleans given n
    """
    return list(product([False, True], repeat=n))


def load_environment(env_path):
    """
    Loads database credentials (and other environs) from .env
    """
    environs = {}
    with open(env_path) as env:
        for line in env:
            if line.startswith('#'):
                continue
            key, value = line.strip().split('=', 1)
            # os.environ[key] = value # Load to local environ
            environs[key] = value
    return environs


def load_nlp_environment(lec_path, aav_path):
    """
    Uses paths defined in run.py to load pickle objects containing entity data
    """
    with open(lec_path, 'rb') as obj:
        all_location_entities = pickle.load(obj)
    with open(aav_path, 'rb') as obj:
        all_verb_entities = pickle.load(obj)
    return all_location_entities, all_verb_entities


def connect_cursor(environs):
    """
    Returns connection to database and cursor for executing queries
    """
    conn = psycopg2.connect(
        dbname=environs['POSTGRES_DBNAME'],
        user=environs['POSTGRES_USER'],
        password=environs['POSTGRES_PASSWORD'],
        host=environs['POSTGRES_HOST'],
        port=environs['POSTGRES_PORT']
    )
    conn.autocommit = True
    return conn, conn.cursor()


def load_experiment(experiment_name):
    """
    Loads an experiment from a yaml file by passed name (located in the experiments directory)
    Creates datetime objects out of strings where possible
    """
    with open('../experiments/' + experiment_name + '.yaml', 'r') as exp:
        exp = yaml.full_load(exp)
    exp['temporal_deltas'] = {k : datetime.strptime(v, '%Y-%m-%d') if k.startswith('data') else get_delta(v) for k, v in exp.get('temporal').items()}
    return exp


def preprocess(sent):
    """
    Clean text: remove punctuation, digits, stemming, stopwords can't be removed because no nltk in secure environment
    """
    ps = PorterStemmer()

    words = str(sent).split()
    new_words = []
    for w in words:
        w = re.sub(r'[^\w\s]', '', w)
        w = re.sub(r'[0-9]+', '', w)
        w = ps.stem(w)
        #if w not in set(stopwords.words('english')):
        new_words.append(w)
    return ' '.join(new_words)


def null_division(x, y):
    """
    Protects against division by zero or null
    """
    if y == 0 or y is np.nan or x is np.nan:
        return -1
    else:
        return x / y


def get_delta(delta:str):
    """
    Transforms yaml formatted interval into relativedelta object
    """
    if delta[-1].lower() == 'h':
        return relativedelta(hours=int(delta[:-1]))
    elif delta[-1].lower() == 'd':
        return relativedelta(days=int(delta[:-1]))
    elif delta[-1].lower() == 'w':
        return relativedelta(weeks=int(delta[:-1]))
    elif delta[-1].lower() == 'm':
        return relativedelta(months=int(delta[:-1]))
    elif delta[-1].lower() == 'q':
        return relativedelta(months=int(delta[:-1] * 3))
    elif delta[-1].lower() == 'y':
        return relativedelta(years=int(delta[:-1]))
    else:
        warnings.warn("Defaulting to 1 month", UnicodeWarning)
        return relativedelta(months=1)
