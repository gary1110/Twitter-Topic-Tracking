import ast
import json
import re
import sys

import numpy as np
import pandas as pd

np.set_printoptions(threshold=sys.maxsize)

def get_matrix_by_user_daily():
    df = pd.read_csv('./doc/6_l.csv', index_col=0, parse_dates=['time'])

    # create a list contains keywords then aggregate keywords with frequency vector into a directory
    word_dirc = df['word_pre'][0]
    res = ast.literal_eval(word_dirc)
    keyword_list = []
    for i in [*res]:
        keyword_list.append(str(i))

    values_list = []
    for v in range(len(df['word_pre'])):
        # get list of values from directory
        word_dirc = df['word_pre'][v]
        res = ast.literal_eval(word_dirc)
        values_list.append(list(res.values()))
    df['matrix'] = values_list

    for v in range(len(df['matrix'])):
        # transfer list to numpy array
        df['matrix'][v] = np.array(df['matrix'][v])

    for v in range(len(df['matrix'])):
        list_int = list(map(int, df['matrix'][v]))
        # generate directories, keyword_list as key, frequency vector as value
        sortedRes = sorted(zip(list_int, keyword_list), key=lambda x: x[0], reverse=True)
        # top ten keywords
        df['matrix'][v] = sortedRes[:10]

    del df['word_pre']
    df.to_csv('./doc/daily.csv')
    return df;

def get_matrix_by_user_weekly():
    df = pd.read_csv('./doc/6_l.csv',index_col=0,parse_dates=['time'])

    # create a list contains keywords then aggregate keywords with frequency vector into a directory
    word_dirc = df['word_pre'][0]
    res = ast.literal_eval(word_dirc)
    keyword_list = []
    for i in [*res]:
        keyword_list.append(str(i))

    values_list = []
    for v in range(len(df['word_pre'])):
        # get list of values from directory
        word_dirc = df['word_pre'][v]
        res = ast.literal_eval(word_dirc)
        values_list.append(list(res.values()))
    df['matrix'] = values_list

    for v in range(len(df['matrix'])):
        # transfer list to numpy array
        df['matrix'][v] = np.array(df['matrix'][v])

    # group by week, unite frequency vector as one row for each week
    df1 = df.groupby(['user_id',pd.Grouper(key="time", freq="1W")])['matrix'].apply(np.sum).reset_index().sort_values('user_id')
    for v in range(len(df1['matrix'])):
        list_int = list(map(int, df1['matrix'][v]))
        # generate directories, keyword_list as key, frequency vector as value
        sortedRes = sorted(zip(list_int, keyword_list), key=lambda x: x[0],reverse=True)
        #top ten keywords
        df1['matrix'][v] = sortedRes[:10]

    df1.to_csv('./doc/weekly.csv')
    return df1;

def get_matrix_by_user_monthly():
    df = pd.read_csv('./doc/6_l.csv', index_col=0, parse_dates=['time'])

    # create a list contains keywords then aggregate keywords with frequency vector into a directory
    word_dirc = df['word_pre'][0]
    res = ast.literal_eval(word_dirc)
    keyword_list = []
    for i in [*res]:
        keyword_list.append(str(i))

    values_list = []
    for v in range(len(df['word_pre'])):
        # get list of values from directory
        word_dirc = df['word_pre'][v]
        res = ast.literal_eval(word_dirc)
        values_list.append(list(res.values()))
    df['matrix'] = values_list

    for v in range(len(df['matrix'])):
        # transfer list to numpy array
        df['matrix'][v] = np.array(df['matrix'][v])

    # group by week, unite frequency vector as one row for each week
    df1 = df.groupby(['user_id', pd.Grouper(key="time", freq="M")])['matrix'].apply(np.sum).reset_index().sort_values(
        'user_id')
    for v in range(len(df1['matrix'])):
        list_int = list(map(int, df1['matrix'][v]))
        # generate directories, keyword_list as key, frequency vector as value
        sortedRes = sorted(zip(list_int, keyword_list), key=lambda x: x[0], reverse=True)
        # top ten keywords
        df1['matrix'][v] = sortedRes[:10]
    df1.to_csv('./doc/monthly.csv')
    return df1;

def get_matrix_all():
    df = pd.read_csv('./doc/7_l.csv', index_col=0)

    # create a list contains keywords then aggregate keywords with frequency vector into a directory
    word_dirc = df['word_freq'][0]
    res = ast.literal_eval(word_dirc)
    keyword_list = []
    for i in [*res]:
        keyword_list.append(str(i))

    values_list = []
    for v in range(len(df['word_freq'])):
        # get list of values from directory
        word_dirc = df['word_freq'][v]
        res = ast.literal_eval(word_dirc)
        values_list.append(list(res.values()))
    df['matrix'] = values_list

    for v in range(len(df['matrix'])):
        # transfer list to numpy array
        df['matrix'][v] = np.array(df['matrix'][v])

    for v in range(len(df['matrix'])):
        list_int = list(map(int, df['matrix'][v]))
        # generate directories, keyword_list as key, frequency vector as value
        sortedRes = sorted(zip(list_int, keyword_list), key=lambda x: x[0], reverse=True)
        # top ten keywords
        df['matrix'][v] = sortedRes[:10]

    del df['word_freq']
    df.to_csv('./doc/mainMatrix.csv')
    return df;

if __name__ == '__main__':
    #print\
        (get_matrix_all());
    #print\
        (get_matrix_by_user_daily());
    #print\
        (get_matrix_by_user_weekly());
    # print\
        (get_matrix_by_user_monthly());

