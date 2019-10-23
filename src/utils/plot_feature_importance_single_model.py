import io
import numpy as np
import os
import pandas as pd
import psycopg2
import re
import sys
import yaml
import matplotlib.pyplot as plt
import ast
from utils.feature_importance_scaler import *
from utils.plot_average_model_feature_importance import *
import datetime
import shutil
import pickle
from datetime import datetime


"""
Plot feature importance by model ID
Plot feature importance by maximum value of feature groups
"""

def get_single_model_std_importance_df(selected_result):
    """
    Unpack dictionary to a list of data frames because feature importance of each model is stored as dictionary in SQL
    """
    std_importance_dict = ast.literal_eval(selected_result.std_feature_importance[0])
    importance_dict = ast.literal_eval(selected_result.feature_importance[0])
    indices = np.asarray(list(std_importance_dict.keys()))
    std_importance = np.asarray(list(std_importance_dict.values()))
    importance = np.asarray(list(importance_dict.values()))
    return std_importance, indices, importance


def select_result(environ, modelID):
    """
    Select result from database by model id
    """
    conn, _ = connect_cursor(environ)
    selected_result = pd.read_sql("select * from results.experiments where model={}".format(modelID), conn)
    features = pd.read_sql('select * from results.features', conn)
    conn.close()
    #selected_result= experiment_results.mask(selected_result.eq('None')).dropna()
    return selected_result, features


def get_index_importance_df(selected_result, indices, std_importance, importance, modelID, features):
    """
    Plot feature importance by model ID
    """
    importance_mx = np.vstack((indices.astype(int), std_importance))
    importance_mx = np.vstack((importance_mx, importance))
    importance_df = pd.DataFrame(importance_mx.T)
    importance_df.columns = ['index','std_importance','importance']

    all_features = features.copy()
    all_features.drop('feature_set', inplace = True, axis = 1)
    features_index = all_features.T
    features_index['index'] = range(0, len(features_index))
    features_index['features'] = features_index.index
        #merge feature label with experiment result
    result_index = pd.merge(features_index, importance_df, on ='index', how = 'inner')
    result_index_rank = result_index.sort_values('importance')
    return result_index_rank


def plot_feature_importance(selected_result, indices, std_importance, importance, modelID, features):
    """
    Plot compositional result of other functions
    """
    result_index_rank = get_index_importance_df(selected_result, indices, std_importance, importance, modelID, features)
    dir = '../plots/' + 'model_feature_importance' + '/'+ datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '/'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)
        #plt.clf()
    fig, ax = plt.subplots(figsize=(60, 10))
    #ax = feature_importance_df.plot.bar(x= 'features', y= 'mean_importance', yerr='std_importance')
    ax.bar(result_index_rank['features'], result_index_rank['importance'], yerr=result_index_rank['std_importance'])
    ax.set_xlabel('feature index', fontsize = 16)
    ax.set_ylabel('mean_importance', fontsize = 16)
    ax.tick_params(axis = 'x', labelsize = 10, rotation=90)
    plt.show()
        #ax.set_title('Model {} feature importance'. format(str(modelID))
    fig.savefig(dir + 'param_config{}_averaged_feature_importances.png'.format(modelID), bbox_inches ='tight')
    fig.savefig(dir + 'param_config{}_averaged_feature_importances.svg'.format(modelID), bbox_inches ='tight')
    plt.close(fig)


def plot_feature_importance_single_model(environs, modelID):
    """
    Feature importances of one model
    """
    selected_result, features = select_result(environs, modelID)
    std_importance, indices, importance = get_single_model_std_importance_df(selected_result)
    plot_feature_importance(selected_result, indices, std_importance, importance, modelID, features)


def get_feature_group_max(group_col, features_models_dfT):
    """
    Get max value(s) in feature group
    """
    new_fea =[]
    for i in features_models_dfT.columns:
        if i in group_col:
            new_fea.append(i)
    feature_group = features_models_dfT[new_fea].max(axis=1)
    return feature_group


def get_feature_group_df_single_model(feature_group_list, features_models_dfT):
    """
    Get dataframe containing max feature value
    """
    df_list =[]
    for feature in feature_group_list:
        feature_group_col = get_feature_group_max(feature, features_models_dfT)
        df_list.append(feature_group_col)
    feature_group_df = pd.concat(df_list, axis = 1)
    feature_group_df.columns = ["weather", "gender", "location_and_activity_topic", "alert_origin", "word_count","last_28_days","last_7_days", "last_60_days", "last_6_months", "within_50_meters", "within_250_meters", "within_1000_meters", "within_5000_meters", "distance_to_hotspot", "count_activity_keywords", "variable_flag"]
    return feature_group_df


def get_plotdf_max_feature(environs, modelID):
    """
    Get df for plotting max feature in feature group
    """
    selected_result, features = select_result(environs, modelID)
    std_importance, indices, importance = get_single_model_std_importance_df(selected_result)
    result_index_rank = get_index_importance_df(selected_result, indices, std_importance, importance, modelID, features)
    result_index_rankT = result_index_rank.set_index(['features']).T.iloc[7:]
    feature_group_list = get_feature_group_list(features)
    feature_group_df = get_feature_group_df_single_model(feature_group_list, result_index_rankT)
    return feature_group_df


def plot_max_feature_single_model(environs, modelID):
    """
    Plotting max feature in each feature group
    """
    feature_group_df = get_plotdf_max_feature(environs, modelID)

    dir = '../plots/' + 'model_max_feature_importance' + '/'+ datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '/'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)

    fig, ax = plt.subplots(figsize=(20, 10))
    ax = feature_group_df.T.sort_values('importance').plot.barh()
    plt.grid(True)
    ax.set_xlabel('feature importance score', fontsize = 12)
    ax.get_legend().remove()
    ax.set_title('Feature importance score by variable group', fontsize=16)
    fig.savefig(dir + 'model{}_max_feature_importances.png'.format(modelID), bbox_inches ='tight')
    fig.savefig(dir + 'model{}_max_feature_importances.svg'.format(modelID), bbox_inches ='tight')
    plt.close(fig)
    return


if __name__ == '__main__':

    environs = load_environment('../.env')
    # plot max feature importance in feature group, input model id
    plot_max_feature_single_model(environs, 15440)

    # plot feature importance of a single model, input model id
    plot_feature_importance_single_model(environs, 18549)
