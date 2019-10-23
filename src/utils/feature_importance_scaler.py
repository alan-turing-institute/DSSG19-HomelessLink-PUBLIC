import numpy as np

def calculate_feature_importance_rf(clf):
    """
    calculate tree classifier feature importance

    Parameters:
    argument 1 (model): model object

    Return
    standard deviation of feature importance, feature indices, mean importance score of all the trees
    """
    importances = clf.feature_importances_
    std_importances = np.std([tree.feature_importances_ for tree in clf.estimators_], axis = 0)
    indices = np.argsort(importances)[::-1]
    return std_importances, indices, importances

def calculate_feature_importance_logr(clf):
    """
    calculate logistic regression feature importance

    Parameters:
    argument 1 (model): model object

    Return
    standard deviation of feature importance, feature indices, mean importance score of all the trees
    """
    importances = clf.coef_[0]
    importances_std = np.std(importances,  axis=0)*clf.coef_
    indices = np.argsort(importances)[::-1]
    return importances_std[0], indices, importances

def calculate_feature_importance_svc(clf):
    """
    calculate SVC feature importance

    Parameters:
    argument 1 (model): model object

    Return
    feature importance, feature indices
    """
    importances = clf.coef_[0]
    #importances_std = np.std(importances,  axis=0)*clf.coef_
    indices = np.argsort(importances)[::-1]
    return importances, indices

def calculate_feature_importance_general(clf):
    importances = clf.feature_importances_
    indices = np.argsort(importances)
    return importances, indices


def get_feature_importance_dictionary(train_x, clf, classifier):
    """
    store feature importance to dictionary Key: indices, Value:importance score

    Parameters:
    argument 1 (model): data frame of train set
    argument 2 (model): model object
    argument 3 (model): name of the classifier

    Return
    dictionary of feature importance
    """
    importance_dict = {}
    std_importance_dict = {}

    if classifier.rsplit('.', 1)[-1] == 'RandomForestClassifier' or 'ExtraTreesClassifier':
        std_importances, indices, importances = calculate_feature_importance_rf(clf)

        for f in range(train_x.shape[1]-2):
            std_importance_dict[str(indices[f])] = std_importances[indices[f]]
            importance_dict[str(indices[f])] = importances[indices[f]]

    elif classifier.rsplit('.', 1)[-1] == 'LogisticRegression':
        std_importances, indices, importances = calculate_feature_importance_logr(clf)
        for f in range(train_x.shape[1]-2):
            importance_dict[str(indices[f])] = std_importances[indices[f]]

    elif classifier.rsplit('.', 1)[-1] == 'SVC':
        importances, indices = calculate_feature_importance_logr(clf)
        for f in range(train_x.shape[1]-2):
            importance_dict[str(indices[f])] = std_importances[indices[f]]

    return importance_dict, std_importance_dict
