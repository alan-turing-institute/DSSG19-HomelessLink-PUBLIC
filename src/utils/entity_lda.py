import warnings
warnings.filterwarnings('always')
import numpy as np
import pandas as pd
import re
import math
import multiprocessing
from sklearn.feature_extraction.text import CountVectorizer
from gensim import corpora, models
from pprint import pprint
import gensim
import pickle
import collections
import psycopg2
import time

"""
here we run lda on the extracted entities
"""


def get_score_dict(bow_corpus, lda_model_object):
    """
    get lda score for each document
    """
    all_lda_score = {}
    for i in range(len(bow_corpus)):
        lda_score ={}
        for index, score in sorted(lda_model_object[bow_corpus[i]], key=lambda tup: -1*tup[1]):
            lda_score[index] = score
            od = collections.OrderedDict(sorted(lda_score.items()))
        all_lda_score[i] = od
    return all_lda_score


def pickle_object(object, path_to_save):
    """
    pickle object
    """
    with open(path, 'wb') as handle:
        pickle.dump(object, handle, protocol = pickle.HIGHEST_PROTOCOL)


def read_entities(path_to_object):
    """
    read entities from pickle object

    Return
    dataframe with entities
    """
    pickle_object = open(path_to_object, 'rb')
    all_entities = pickle.load(pickle_object)
    pickle_object.close()
    return all_entities


def get_lda_score(featured_cohort, all_location_entities, topics_numbers, entity_col):
    """
    run LDA and get topic modeling scores
    this functtion is called in the pipeline

    parameter 1: cohort dataframe

    parameter 2: location loc_entities

    parameter 3: number of topics

    parameter 4: name of the entity column

    Return
    dataframe with LDA scores

    """
    alertid = featured_cohort['alert']
    cohort_diction = pd.merge(alertid, all_location_entities, on = 'alert', how='inner')

    dictionary = gensim.corpora.Dictionary(cohort_diction[entity_col])
    bow_corpus = [dictionary.doc2bow(doc) for doc in cohort_diction[entity_col]]

    lda_model_10 = gensim.models.LdaMulticore(bow_corpus, num_topics=topics_numbers, id2word=dictionary, passes=10, workers=math.ceil(multiprocessing.cpu_count() / 2))
    lda_score_all = get_score_dict(bow_corpus, lda_model_10)

    all_lda_score_df = pd.DataFrame.from_dict(lda_score_all)
    all_lda_score_dfT = all_lda_score_df.T
    all_lda_score_dfT = all_lda_score_dfT.fillna(0)

    all_lda_score_dfT['alert'] = cohort_diction['alert']
    return all_lda_score_dfT
