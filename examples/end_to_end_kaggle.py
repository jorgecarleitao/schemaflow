"""
Example of solving a Kaggle problem using schemaflow's API.

This script performs an end-to-end analysis and prediction of the Kaggle exercise
https://www.kaggle.com/c/house-prices-advanced-regression-techniques

It demonstrates the advantages of explicitly declaring the types in the Pipe (using Schemaflow's API).
"""
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
import sklearn.metrics
import matplotlib.pyplot as plt

from schemaflow import types as sf_types
from schemaflow import ops as sf_ops
from schemaflow.pipe import Pipe
from schemaflow.pipeline import Pipeline


class SplitNumericCategorical(Pipe):
    fit_data = transform_data = {'x': sf_types.PandasDataFrame(schema={})}
    transform_modifies = {'x_categorical': sf_types.PandasDataFrame(schema={}),
                          'x': sf_types.PandasDataFrame(schema={})}

    fitted_parameters = {'numeric_columns': sf_types.List(str)}

    def fit(self, data: dict, parameters: dict=None):
        self['numeric_columns'] = list(data['x'].select_dtypes(include=[np.number]).columns)

    def transform(self, data: dict):
        data['x_categorical'] = data['x'].drop(self['numeric_columns'], axis=1)
        data['x'] = data['x'].loc[:, self['numeric_columns']]
        return data


class FillNaN(Pipe):
    fit_data = transform_modifies = transform_data = {
        'x': sf_types.PandasDataFrame(schema={}),
        'x_categorical': sf_types.PandasDataFrame(schema={})}

    fitted_parameters = {
        'means': sf_types.List(float),
        'most_frequent': sf_types.List(str)}

    def fit(self, data: dict, parameters: dict=None):
        self['means'] = data['x'].mean(axis=0)
        self['most_frequent'] = data['x_categorical'].mode(axis=0)

    def transform(self, data: dict):
        data['x'] = data['x'].fillna(self['means'])
        for column in data['x_categorical'].columns:
            data['x_categorical'].loc[data['x_categorical'][column].isnull(), column] = self['most_frequent'][column][0]
        return data


class JoinCategoricalAsOneHot(Pipe):
    fit_data = transform_data = {'x_categorical': sf_types.PandasDataFrame(schema={})}
    transform_modifies = {
        'x_categorical': sf_ops.Drop(),
        'x': sf_types.PandasDataFrame(schema={})
    }

    fitted_parameters = {'label': object, 'one_hot': object}

    def fit(self, data: dict, parameters: dict=None):
        df = data['x_categorical'].copy()
        self['label'] = dict((column, LabelEncoder()) for column in df.columns)
        self['transformer'] = OneHotEncoder()

        for column in self['label']:
            df.loc[:, column] = self['label'][column].fit_transform(df.loc[:, column])
        self['transformer'].fit(df.values)

    def transform(self, data: dict):
        index = data['x_categorical'].index
        for column in self['label']:
            mode = data['x_categorical'].loc[:, column].mode()[0]

            def f(x):
                if x not in self['label'][column].classes_:
                    return mode
                else:
                    return x

            data['x_categorical'].loc[:, column] = data['x_categorical'].loc[:, column].apply(f)
            data['x_categorical'].loc[:, column] = self['label'][column].transform(data['x_categorical'].loc[:, column])

        data['x_categorical'] = self['transformer'].transform(data['x_categorical'])

        df = pd.DataFrame(data['x_categorical'].toarray(), index=index)
        data['x'] = data['x'].join(df)
        del data['x_categorical']
        return data


class BaselineModel(Pipe):
    fit_data = {'x': sf_types.Array(np.float64)}
    transform_modifies = {'y_pred_baseline': sf_types.Array(np.float64)}

    fitted_parameters = {'mean': np.float64}

    def fit(self, data: dict, parameters: dict = None):
        self['mean'] = np.mean(data['y'])

    def transform(self, data: dict):
        data['y_pred_baseline'] = np.full(data['x'].shape[0], self['mean'])
        return data


class LogLassoModel(Pipe):
    transform_data = {'x': sf_types.PandasDataFrame(schema={})}
    fit_data = {'x': sf_types.PandasDataFrame(schema={}), 'y': sf_types.PandasDataFrame(schema={})}
    transform_modifies = {
        'y_pred': sf_types.Array(np.float64),
        'x': sf_ops.Drop()
    }

    fitted_parameters = {'model': LassoCV}

    def fit(self, data: dict, parameters: dict=None):
        self['model'] = LassoCV(normalize=True)
        self['model'].fit(data['x'], np.log(data['y']))

    def transform(self, data: dict):
        data['y_pred'] = np.exp(self['model'].predict(data['x']))
        del data['x']
        return data


def x_y_split(df, target_column):
    return df.drop(target_column, axis=1), df.loc[:, target_column]


def analyse_performance(df, target_column, pipeline, parameters: dict=None):
    train, test = train_test_split(df, test_size=0.2, random_state=1)

    x_train, y_train = x_y_split(train, target_column)
    x_test, y_test = x_y_split(test, target_column)

    pipeline.fit({'x': x_train, 'y': y_train.values}, parameters)

    result = pipeline.transform({'x': x_test})

    y_pred = result['y_pred']
    y_pred_baseline = result['y_pred_baseline']

    def metric(y_true, y_pred):
        return sklearn.metrics.mean_squared_error(np.log(y_true), np.log(y_pred))

    print(metric(y_test, y_pred))
    print(metric(y_test, y_pred_baseline))

    plt.plot(range(len(y_test)), y_test, 'o')
    plt.plot(range(len(y_test)), y_pred, 'o')
    plt.savefig('examples/comparison1.png')
    plt.close()

    plt.plot(y_test, y_pred, 'o', label='Lasso')
    plt.plot(y_test, y_pred_baseline, 'o', label='baseline')
    plt.plot(y_test, y_test, '-', label='')
    plt.xlabel('truth')
    plt.ylabel('pred')
    plt.legend()
    plt.savefig('examples/pred_vs_truth.png')
    plt.close()


def export_predictions(df, target_column, predict_pipeline, parameters: dict=None):

    x, y = x_y_split(df, target_column)

    predict_pipeline.fit({'x': x, 'y': y}, parameters)

    df = pd.read_csv('examples/all/test.csv', index_col='Id')

    result = predict_pipeline.transform({'x': df})['y_pred']

    pd.Series(result, name=target_column, index=df.index).to_csv('examples/submission.txt', header=True)


if __name__ == '__main__':
    predict_pipeline = Pipeline([
        SplitNumericCategorical(),
        FillNaN(),
        JoinCategoricalAsOneHot(),
        ('baseline', BaselineModel()),
        ('model', LogLassoModel())
    ])

    # this pipeline is very generic: it does not make any assumptions about the data's format.
    predict_pipeline.check_fit({'x': sf_types.PandasDataFrame({}), 'y': sf_types.Array(np.float64)}, raise_=True)
    predict_pipeline.check_transform({'x': sf_types.PandasDataFrame({})}, raise_=True)

    print('expected fit schema: ', predict_pipeline.fit_data)
    print('fitted parameters: ', predict_pipeline.fitted_parameters)

    print('expected transform schema: ', predict_pipeline.transform_data)
    print('expected outcome schema: ', predict_pipeline.transform_schema(predict_pipeline.transform_data))

    # execution of the pipeline
    target_column = 'SalePrice'

    df = pd.read_csv('examples/all/train.csv', index_col='Id')

    analyse_performance(df.copy(), target_column, predict_pipeline)

    export_predictions(df.copy(), target_column, predict_pipeline)
