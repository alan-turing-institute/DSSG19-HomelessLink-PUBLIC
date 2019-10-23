import numpy as np
import pandas as pd
import re
from nltk.stem import SnowballStemmer
from multiprocessing import Pool
import multiprocessing
from pprint import pprint
import psycopg2
import spacy
import pickle
from gensim.parsing.preprocessing import remove_stopwords
from utils.utils import connect_cursor
from sys import argv
import warnings
warnings.filterwarnings('ignore', message='numpy.dtype size changed')

"""
this script returns verb entities from the text and dump it in a pickle object
database needs to join location details with appearance description and name the column as 'loc_app_details',
"""

nlp = spacy.load('en_core_web_sm')
ps = SnowballStemmer('english')
def get_verbs(sent):
    print('getting nouns')
    """
    part-of-speech tagging nouns in the text

    Parameters:
    argument 1 (str): sentences

    Returns
    A list of verbs from each alert
    """
    modal_verbs = ['can', 'is','am','were','was','have','has', 'may','must','shall','will','should']
    sent = re.sub(r'[^\w\s]', '', sent)
    verb_list =[]
    if sent is not None:
        doc = nlp(sent)
        for token in doc:
            if token.pos_ == 'VERB':
                w = ps.stem(token.text)
                if w not in modal_verbs:
                    verb_list.append(w)
    return verb_list

def get_verbs_frame(file):
    """
    apply verb entity function on parallelize dataframe

    Parameters:
    argument 1 (pd.DataFrame): data frame for parallelize processing

    Returns
    dataframe processes by the applied function
    """
    file['verbs'] = file['loc_app_details'].apply(lambda x: get_verbs(x))
    return file

def parallelize_dataframe(df, func):
    """
    parallelize processing pandas dataframe using multithread

    Parameters:
    argument 1 (pd.DataFrame): data frame for parallelize processing

    argument 1 (function): input function you will run on this dataframe

    Returns
    dataframe processes by input function
    """
    #df = sample_alerts_location_mall
    num_partitions = multiprocessing.cpu_count()
    num_cores = multiprocessing.cpu_count()
    df_split = np.array_split(df,num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def pickle_object(file_to_pickle, path_to_file):
    """
    pickle objects

    Parameters:
    argument 1 (str): variable for pickle_object

    argument 2 (str): path where you save the file
    """
    with open(path_to_file, 'wb') as handle:
        pickle.dump(file_to_pickle, handle, protocol = pickle.HIGHEST_PROTOCOL)

def get_data_from_sql(environ):
    """
    query alert data from database

    argument 1: environment object

    Return
    date frame with alerts
    """
    conn, cursor = connect_cursor(environ)
    all_alerts_loc_app = pd.read_sql('select alert, region, location_details, appearance_details, loc_app_details, outcome_internal from semantic.alerts', conn)
    conn.close()
    return all_alerts_loc_app

def get_verb_from_all_data(path_to_saveFile:str, environ):
    """
    read data from the database
    extract verb entities and pickle the result

    parameter 1(str): path to save the location loc_entities

    parameter 2:  environment object
    """

    all_alerts_loc_app = get_data_from_sql(environ)
    all_alerts_verbs = parallelize_dataframe(all_alerts_loc_app, get_verbs_frame)
    pickle_object(all_alerts_verbs, path_to_saveFile)
