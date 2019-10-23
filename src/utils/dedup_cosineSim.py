import numpy as np
import pandas as pd
import re
import time
from nltk.stem import SnowballStemmer
from multiprocessing import Pool
from sklearn.feature_extraction.text import CountVectorizer
import psycopg2
import spacy
from sklearn.metrics import pairwise_distances
from scipy.spatial.distance import cosine

"""
here we compute the cosine similarity between a new incoming alert and a prioritised list of alerts
"""

def get_df_matrix(input_alert):
    """
    select columns for cosine similarity, here you should modify the function to select certain variables
    """
    sample = input_alert.loc[:, input_alert.columns != 'alert']
    sample_mx = sample.values
    return sample_mx

def replace_max(cosine_sim_mx):
    """
    if cosine similarity is used on topic features only, the score will be zero when it deal with alerts does not contain any
    topics selected by LDA, because these alerts have the exact same topic scores in each topic
    """
    pre_array = None
    for i in cosine_sim_mx:
        i[i > 0.99] = 0
        if pre_array is None:
            pre_array = i
        else:
            new_matrix = np.concatenate(([i, pre_array]), axis = 0)
            pre_array = new_matrix
    return pre_array.reshape(100,100)


def get_duplication_score(cosine_sim_mx, alertid, input_alert):
    """
    compute cosine similarity
    """
    cosine_sim_adjusted = replace_max(cosine_sim_mx)
    cosine_sim_adjusted_df = pd.DataFrame(cosine_sim_adjusted)
    cosine_sim_adjusted_df.columns = input_alert['alert']
    cosine_sim_adjusted_df['alert'] = input_alert['alert']
    result = cosine_sim_adjusted_df[['alert',alertid]].sort_values(by = alertid,ascending = False)
    return result

"""
read lda score
if you integrate cosine similarity in the pipeline, you should be able to retrieve the lda score from the model, in the
python feature function lda_location, lda_verbs
"""
lda_score = pd.read_pickle('./src/utils/lda_models/loc_lda_score_df_10.pkl')

# input n alert selected by the naive algorithm
sample = lda_score.head(10000)
sample_mx = get_df_matrix(sample)
cosine_sim = 1 - pairwise_distances(sample_mx, metric = 'cosine')

scores = get_duplication_score(cosine_sim, '5000Y00000Uy24U', sample)
#print(scores)
