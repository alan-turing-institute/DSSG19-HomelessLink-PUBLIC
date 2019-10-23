from sys import argv
import io
import numpy as np
import os
import pandas as pd
import psycopg2
import re
import sys
import yaml
from datetime import datetime
import shutil
from utils.utils import connect_cursor, load_environment
import ast
import matplotlib.pyplot as plt
import pickle
from utils.feature_importance_scaler import *


"""
here we plot the mean feature importance of specific algorithm OR model configurations
add standard deviation
"""

def get_importance_score_dfs(selected_result):
    """
    Unpack dictionary to a list of data frames because feature importance of each model is stored as dictionary in SQL
    """
    df_list =[]
    for i, j in zip(selected_result['feature_importance'], selected_result['model']):
        dict = ast.literal_eval(i)
        result_df = pd.DataFrame.from_dict(dict, orient= 'index').reset_index()
        result_df.columns = ['index', j]
        df_list.append(result_df)
    return df_list


def get_model_std_importance_dfs(selected_result):
    """
    Unpack dictionary to a list of data frames because feature importance of each model is stored as dictionary in sql

    Parameters: pd.DataFrame experiment result table_name

    Return

    """
    df_list =[]
    for i, j in zip(selected_result['std_feature_importance'], selected_result['model']):
        dict = ast.literal_eval(i)
        result_df = pd.DataFrame.from_dict(dict, orient= 'index').reset_index()
        result_df.columns = ['index', j]
        df_list.append(result_df)

    dfs = [df.set_index('index') for df in df_list]
    std_importance_df = pd.concat(dfs, axis = 1)
    return std_importance_df



def get_importance_mean(selected_result):
    """
    This function concatenate the dataframes in the list
    then calculates mean feature importance score from selected set of models

    Parameters:
    argument 1 (pd.series): column with feature importance values from the experiment result data frame

    Returns:
    pd.DataFrame: returnig dataframe with mean feature importance column, column name is model ID
    """

    df_list = get_importance_score_dfs(selected_result)
    dfs = [df.set_index('index') for df in df_list]
    mean_importance_df = pd.concat(dfs, axis = 1, sort=False)
    mean_importance_df_sd = mean_importance_df.copy()
    mean_importance_df['mean_importance'] = mean_importance_df.mean(axis=1)
    mean_importance_df['std_importance'] = mean_importance_df_sd.std(axis=1)
    return mean_importance_df


def get_model_std_importance(selected_result):
    mean_importance_df = get_importance_mean(selected_result)
    mean_importance_df['index'] = mean_importance_df.index
    across_model_std = mean_importance_df[['index','std_importance']]
    within_model_std = get_model_std_importance_dfs(selected_result)
    within_model_std['index'] = within_model_std.index
    std_cor = pd.merge(across_model_std, within_model_std, on = 'index', how ='inner')
    return std_cor


def importance_by_models(experiment_result):
    """
    This function select sets of classifiers from the result dataframe and return averaged feature importance
    from a set of models

    Parameters:
    argument 1 (str): Name of the classifier

    argument 2 (int): ID of parameter configurations

    argument 3 (pd.DataFrame): experiment result data frame

    Returns:
    pd.DataFrame: returnig dataframe with mean feature importance column
    """
    selected_result = experiment_result[experiment_result['feature_importance'] != 'NULL']
    #selected_result = result_clean[result_clean['param_config'] == param_config]
    selected_mean_importance_df = get_importance_mean(selected_result).reset_index().sort_values('mean_importance')
    return selected_mean_importance_df, selected_result


def merge_feature_labels(features, all_results):
    """
    Merge feature name with dataframe with mean feature importance column

    Parameters:
    argument 1 (pd.DataFrame): dataframe with index and name of the features

    argument 2 (pd.DataFrame): experiment result data dataframe

    argument 3 (str): Name of the classifiers

    argument 4 (int): ID for parameter configurations

    Returns:
    pd.DataFrame: returnig dataframe with mean feature importance column and feature label
    """
    all_features = features.copy()
    all_features.drop('feature_set', inplace = True, axis = 1)
    all_features.drop('loc_app_details', inplace = True, axis = 1)
    features_index = all_features.T
    features_index['index'] = range(0, len(features_index))
    features_index['features'] = features_index.index
    
    # Merge feature label with experiment result
    rf_result, selected_result =  importance_by_models(all_results)
    rf_result['index'] = rf_result['index'].astype('str')
    features_index['index'] = features_index['index'].astype('str')
    result_index = pd.merge(features_index, rf_result, on ='index', how = 'inner')
    result_index = result_index[['index', 'features', 'mean_importance', 'std_importance']]
    result_index = result_index.sort_values(by=['mean_importance'])
    return result_index


def plot_average_model_feature_importance(features, experiment_results, param_config, experimentID):
    """
    Plot averaged feature importance from a set of models

    Parameters:
    argument 1 (int): ID of parameter configurations

    argument 2 (pd.DataFrame): dataframe with mean feature importance column

    argument 3 (pd.DataFrame): experiment result data frame
    """
    feature_importance_df = merge_feature_labels(features, experiment_results)
    dir = '../plots/' + 'Averaged feature importance' + '/'+ datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '/'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)
    plt.clf()
    fig, ax = plt.subplots(figsize=(45, 10))
    ax.bar(feature_importance_df['features'], feature_importance_df['mean_importance'], yerr=feature_importance_df['std_importance'])
    ax.set_xlabel('feature index', fontsize = 16)
    ax.set_ylabel('mean_importance', fontsize = 16)
    ax.tick_params(axis = 'x', labelsize = 7, rotation=90)
    ax.set_title('Averaged feature importance of model sets')
    fig.savefig(dir + 'param_config{}_experiment{}_averaged_feature_importances.png'.format(str(param_config), str(experimentID)), bbox_inches ='tight')
    fig.savefig(dir + 'param_config{}_experiment{}_averaged_feature_importances.svg'.format(str(param_config), str(experimentID)), bbox_inches ='tight')
    plt.close(fig)


def get_classifier_experiment(environs, experimentID, param_config):
    conn, _ = connect_cursor(environs)
    experiment_results = pd.read_sql("select * from results.experiments where experiment={} and param_config={}".format(experimentID, param_config), conn)
    features = pd.read_sql('select * from results.features', conn)
    conn.close()
    return experiment_results, features


"""
here we group the features then plot the feature importance by feature  group
"""

def select_features_by_keyword(keywordList, features):
    """
    Select feature columns by keywords
    """
    selected_features_list = []
    for word in keywordList:
        selected_features = [col for col in features.columns if word in col]
        con_list = selected_features_list + selected_features
        selected_features_list = con_list
    return selected_features_list

def get_importance_mean2(selected_result):
    """
    Get feature importance mean
    """
    df_list = get_importance_score_dfs(selected_result)
    dfs = [df.set_index('index') for df in df_list]
    con_df = pd.concat(dfs, axis = 1, sort=False)
    return con_df


def get_model_feature_df(features, experiment_results):
    """
    Get dataframe for ploting
    """
    feature_importance_df = merge_feature_labels(features, experiment_results)
    con_df = get_importance_mean2(experiment_results)
    con_df['index'] = con_df.index
    features_models_df = pd.merge(feature_importance_df, con_df, on = 'index', how='inner')
    features_models_dfT = features_models_df.set_index(['features']).T.iloc[3:features_models_df.shape[1]-3]
    features_models_dfT.shape
    return features_models_dfT


def get_feature_group_mean(group_col, features_models_dfT):
    """
    Get mean score in feature groups
    """
    feature_group = features_models_dfT[group_col].mean(axis=1)
    return feature_group


def plot_feature_group_importance(feature_group_df):
    """
    Plot averaged feature importance of a set of models with same configs
    """
    dir = '../plots/' + 'group feature importance' + '/'+ datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '/'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)
    fig, ax = plt.subplots()
    feature_group_df.boxplot(rot = 90)
    ax.set_xlabel('feature group', fontsize = 16)
    ax.set_ylabel('mean importance across models', fontsize = 16)
    ax.tick_params(axis = 'x', labelsize = 12, rotation=90)
    ax.set_title('Averaged feature importance of model sets')
    fig.savefig(dir + 'group_feature_importances.png', bbox_inches ='tight')
    fig.savefig(dir + 'group_feature_importances.svg', bbox_inches ='tight')
    plt.show()
    plt.close(fig)


def select_features_by_keyword(keywordList, features):
    """
    Select feature columns by partical matching string
    """
    selected_features_list = []
    for word in keywordList:
        selected_features = [col for col in features.columns if word in col]
        con_list = selected_features_list + selected_features
        selected_features_list = con_list
    return selected_features_list


def get_importance_mean2(selected_result):
    """
    Get data frame that contains mean feature importance score
    """

    df_list = get_importance_score_dfs(selected_result)
    dfs = [df.set_index('index') for df in df_list]
    con_df = pd.concat(dfs, axis = 1, sort=False)
    return con_df


def get_model_feature_df(features, experiment_results):
    """
    Get mean feature imortance for plotting feature group means
    """
    feature_importance_df = merge_feature_labels(features, experiment_results)
    con_df = get_importance_mean2(experiment_results)
    con_df['index'] = con_df.index
    features_models_df = pd.merge(feature_importance_df, con_df, on = 'index', how='inner')
    features_models_dfT = features_models_df.set_index(['features']).T.iloc[3:features_models_df.shape[1]-3]
    features_models_dfT.shape
    return features_models_dfT


def get_feature_group_mean(group_col, features_models_dfT):
    """
    Get means for each feature importance group
    """
    feature_group = features_models_dfT[group_col].mean(axis=1)
    return feature_group


def plot_feature_group_importance(feature_group_df):
    """
    Plot feature importance of models with the same set of configs
    """
    dir = '../plots/' + 'group feature importance' + '/'+ datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '/'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)
    fig, ax = plt.subplots()
    feature_group_df.boxplot(rot = 90)
    #ax.boxplot(feature_group_df, 1)
    ax.set_xlabel('feature group', fontsize = 16)
    ax.set_ylabel('mean importance across models', fontsize = 16)
    ax.tick_params(axis = 'x', labelsize = 12, rotation=90)
    ax.set_title('Averaged feature importance of model sets')
    fig.savefig(dir + 'group_feature_importances.png', bbox_inches ='tight')
    fig.savefig(dir + 'group_feature_importances.svg', box_inches ='tight')
    plt.close(fig)


"""
Here we plot feature importance of a set of models with same configs across temperal folds
"""

def get_feature_group_df(feature_group_list, features_models_dfT):
    """
    Get data frame with feature importance group
    """
    df_list =[]
    for feature in feature_group_list:
        feature_group_col = get_feature_group_mean(feature, features_models_dfT)
        df_list.append(feature_group_col)
    feature_group_df = pd.concat(df_list, axis = 1)
    feature_group_df.columns = ["temperature_col","gender_col", "topic_col", "alert_origin", "word_count"," d28", "d7", "d60", "m6", "m50", "m250", "m1000", "m5000", "hotspot_col", "dict_count_col", "flag_col"]
    return feature_group_df



def get_feature_group_list(features):
    """
    Group feature importance
    """
    weather_col = select_features_by_keyword(['temperature', 'max', 'min','avg'], features)
    gender_col = select_features_by_keyword(['gender'], features)
    #day_col = select_features_by_keyword(['do'], features)
    topic_col = select_features_by_keyword(['_x', '_y'], features)
    #person_found_col = select_features_by_keyword(['person_found_'], features)
    alert_origin = select_features_by_keyword(['origin'], features)
    word_count = select_features_by_keyword(['wordcount'], features)
    d28 = select_features_by_keyword(['28d'], features)
    d7 = select_features_by_keyword(['7d'], features)
    d60 = select_features_by_keyword(['60d'], features)
    m6 = select_features_by_keyword(['6m'], features)
    m50 = select_features_by_keyword(['50m'], features)
    m250 = select_features_by_keyword(['250m'], features)
    m1000 = select_features_by_keyword(['1000m'], features)
    m5000 = select_features_by_keyword(['5000m'], features)
    hotspot_col = select_features_by_keyword(['hotspot'], features)
    dict_count_col = select_features_by_keyword(['sleep','other_activity','important_word','mental_health'], features)
    flag_col = select_features_by_keyword(['flag'], features)

    feature_group_list = [weather_col, gender_col, topic_col, alert_origin, word_count, d28, d7, d60, m6, m50, m250, m1000, m5000, hotspot_col, dict_count_col, flag_col]
    return feature_group_list


def plot_feature_group_importance_all(experiment_results, features):
    """
    Plot each feature importance group
    """
    feature_group_list = get_feature_group_list(features)
    features_models_dfT = get_model_feature_df(features, experiment_results)
    feature_group_df = get_feature_group_df(feature_group_list, features_models_dfT)
    plot_feature_group_importance(feature_group_df)


if __name__ == '__main__':
    
    environs = load_environment('../.env')
    experiment_results, features = get_classifier_experiment(environs, 62, 25)

    # Plot mean feature importance of a set of models with the same configurations
    plot_average_model_feature_importance(features, experiment_results, 62, 25)

    # Plot (mean) feature importance in feature groups among a set of models with same configs
    plot_feature_group_importance_all(experiment_results, features)
