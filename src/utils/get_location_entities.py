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
import time
from sys import argv
from utils.utils import connect_cursor
import warnings
warnings.filterwarnings('ignore', message='numpy.dtype size changed')


"""
this script returns location entities from the text and dump it in a pickle object
"""

nlp = spacy.load('en_core_web_sm')
ps = SnowballStemmer('english')
def get_nouns(sent):

    """
    part-of-speech tagging nouns in the text

    Parameters:
    argument 1 (str): sentences

    Returns
    A list of nouns from each alert
    """
    loc_list = ['street','avenue','bank','road','park','school','hospital','cafe','supermarket','river','hill','building']
    noun_list =[]
    if sent is not None:
        doc = nlp(sent)
        for token in doc:
            if token.pos_ != 'NOUN':
                noun_list.append(token.text)
            elif token.pos_ == 'NOUN' and token.text in loc_list:
                noun_list.append(',' + token.text.title())
            elif token.pos_ == 'NOUN':
                noun_list.append(token.text.title())
    return ' '.join(noun_list)

def get_entity_lda(noun_list):
    """
    extract location entities from nouns(Capitalizd frist letter)
    Nouns are uppercased for entity detection

    Parameters:
    argument 1 (str): A list of nouns from each document

    Returns
    A list of locations for each alert

    """
    doc2 = nlp(noun_list)
    location_list = []
    for ent in doc2.ents:
        if ent.label_ in ['FAC']:
            w = ent.text
            location_list.append(w)
    return ' '.join(location_list)

def process_words(sent):
    """
    remove digits, stem words in sentences
    we stem words to reduce the number of words in the corpus in the later process

    Parameters:
    argument 1 (str): sentence

    Returns
    list of processed sentence
    """

    sent = re.sub(r'[^\w\s]', '', sent)
    words = str(sent).split()
    new_words =[]
    for w in words:
        if not w.isdigit():
            w = ps.stem(w)
            new_words.append(w)
    return ' '.join(new_words)

num_partitions = multiprocessing.cpu_count()
num_cores = multiprocessing.cpu_count()
def parallelize_dataframe(df, func, num_partitions, num_cores):
    """
    parallelize processing pandas dataframe using multithread

    Parameters:
    argument 1 (pd.DataFrame): data frame for parallelize processing

    argument 1 (function): input function you will run on this dataframe

    Returns
    dataframe processes by input function
    """
    df_split = np.array_split(df,num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def get_nouns_frame(file):
    """
    apply noun entity function on parallelize dataframe

    Parameters:
    argument 1 (pd.DataFrame): data frame for parallelize processing

    Returns
    dataframe processes by the applied function
    """
    file['loc_noun'] = file['location_details'].apply(lambda x: get_nouns(x))
    return file

def get_entity_frame_lda(file):
    """
    apply location entity function on parallelize dataframe

    Parameters:
    argument 1 (pd.DataFrame): data frame for parallelize processing

    Returns
    dataframe processes by the applied function
    """
    file['loc_entities'] = file['loc_noun'].apply(lambda x: get_entity_lda(x))
    return file

def preprocess_lda(sent):
    """
    process text used in LDA topic modeling:
    remove digits, stem words in sentences
    we stem words to reduce the number of words in the corpus in the later process

    Parameters:
    argument 1 (str): sentence

    Returns
    list of processed sentence
    """
    sent = re.sub(r'[^\w\s]', '', sent)
    sent = remove_stopwords(sent)
    sent_list =[]
    for w in sent.split():
        w = ps.stem(w.lower())
        sent_list.append(w)
    return sent_list


def get_processed_frame_lda(file):
    """
    apply preprocess LDA text function on parallelize dataframe

    Parameters:
    argument 1 (pd.DataFrame): data frame for parallelize processing

    Returns
    dataframe processes by the applied function
    """
    file['loc_entities'] = file['loc_entities'].apply(lambda x: preprocess_lda(x))
    return file

def get_location_entities_lda(file):
    """
    get location entities for LDA topic modeling

    Parameters:
    argument 1 (pd.DataFrame): data frame contains alerts

    Returns
    dataframe with location entities
    """
    file = parallelize_dataframe(file, get_nouns_frame, num_partitions, num_cores)
    file = parallelize_dataframe(file, get_entity_frame_lda, num_partitions, num_cores)
    file = parallelize_dataframe(file, get_processed_frame_lda, num_partitions, num_cores)
    return file

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
    all_alerts_location = pd.read_sql('select alert, region, location_details, appearance_details, loc_app_details, outcome_internal from semantic.alerts', conn)
    conn.close()
    return all_alerts_location

def get_location_from_all_data(path_to_saveFile:str, environ):
    """
    read data from the database
    extract location entities and pickle the result

    parameter 1(str): path to save the location loc_entities

    parameter 2:  environment object

    """

    all_alerts_location = get_data_from_sql(environ)
    all_locations = get_location_entities_lda(all_alerts_location)
    pickle_object(all_locations, path_to_saveFile)
