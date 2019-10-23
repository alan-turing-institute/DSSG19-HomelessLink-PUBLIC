import numpy as np
import pandas as pd
import re
import time
from nltk.stem import SnowballStemmer
from multiprocessing import Pool
import psycopg2
from pandas import *
from utils.utils import *
import multiprocessing


"""
here we count the number of keywods in the text according to a mannually defined dictionary
"""

def read_dictionary(path:str) -> dict:
    handle = open(path, 'r').read()
    dictionary = eval(handle)
    return dictionary


ps = SnowballStemmer('english')

def process_words(sent:str) -> list:
    """
    Clean sentences: stem words, remove punctuation
    """
    words = str(sent).split()
    count = 0
    new_words =[]
    for w in words:
        w = ps.stem(w)
        w = re.sub(r'[^\w\s]', '', w)
        new_words.append(w)
    return ' '.join(new_words)


def count_words(sent:str, dictionary:dict):
    """
    Flag whether a word experiment_results
    """
    words = str(sent).split()
    if any(w in words for w in dictionary):
        return True
    else:
        return False


def stem_dictionary(dictionary):
    """
    Stem words in the dictionary so that they match with the cleaned text
    """
    new_dict = {}
    for k, v in dictionary.items():
        l =[]
        for word in v:
            if (isinstance(word, float) == False) and (isinstance(word, int) == False):
                word = ps.stem(word)
                l.append(word)
        new_dict[k] = l
    return new_dict


def read_and_process_dictionary(inputFile):
    """
    Read dictionary and stem words
    """
    xls = ExcelFile(inputFile)
    keywords_df = xls.parse(xls.sheet_names[0])
    activity_diction =keywords_df.to_dict('series')
    activity_diction = stem_dictionary(activity_diction)
    return activity_diction


def count_words_diction(file):
    """
    Apply clean words function
    """
    file['loc_app_details_clean'] = file['loc_app_details'].apply(lambda x: process_words(x))
    return file


def parallelize_dataframe(df, func):
    """
    Parallelise the processing of the pandas dataframe
    """
    df_split = np.array_split(df, multiprocessing.cpu_count())
    pool = Pool(multiprocessing.cpu_count())
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df['loc_app_details']


def create_count_feature(file, keyword_dictionary):
    """
    Count keywords, this function is called in the pipeline

    parameter 1: dataframe with text

    parameter 2: dictionary for activity new_words

    Return:
    data frame that flag if words in certain dictionary exist
    """
    activity_diction = read_and_process_dictionary(keyword_dictionary)
    file['loc_app_details_clean']= parallelize_dataframe(file, count_words_diction, multiprocessing.cpu_count(), multiprocessing.cpu_count())
    file['sleep'] = file['loc_app_details_clean'].apply(lambda x: count_words(x, activity_diction['sleep'])).astype(int)
    file['other_activity'] = file['loc_app_details_clean'].apply(lambda x: count_words(x, activity_diction['other_activity'])).astype(int)
    file['important_word'] = file['loc_app_details_clean'].apply(lambda x: count_words(x, activity_diction['important_word'])).astype(int)
    file['mental_health'] = file['loc_app_details_clean'].apply(lambda x: count_words(x, activity_diction['mental_health'])).astype(int)


    count_keywords = file[['alert', 'sleep', 'other_activity', 'important_word','mental_health']]
    return count_keywords


if __name__ == '__main__':
    
    # Read alerts from database'
    environs = load_environment('../.env')
    conn, cursor = connect_cursor(environs)
    all_alerts = pd.read_sql('select alert, outcome_internal, region, loc_app_details from semantic.alerts', conn)
    conn.close()
    keywords_count_all = create_count_feature(all_alerts, '../keyword_diction/keyword_dictionary.xlsx')
