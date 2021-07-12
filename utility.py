import math
import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re
import gzip
import shutil


################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

def col_header_val(df,table_config):
    '''
    replace whitespaces in the column
    and standardized column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('[^\w]','_',regex=True)
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0

def human_size(nbytes):
  suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  human = nbytes
  rank = 0
  if nbytes != 0:
    rank = int((math.log10(nbytes)) / 3)
    rank = min(rank, len(suffixes) - 1)
    human = nbytes / (1024.0 ** rank)
  f = ('%.2f' % human).rstrip('0').rstrip('.')
  return '%s %s' % (f, suffixes[rank])

def file_summary(df,table_config):
    # get file size and convert bytes to readable string
    file_type = table_config['file_type']
    file_name = table_config['file_name'] + f'.{file_type}'

    THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
    file_full_path = os.path.join(THIS_FOLDER, file_name)

    file_size = os.path.getsize(file_full_path)
    size_readable = human_size(file_size)

    # get number of columns
    number_of_cols = df.shape[1]

    # get number of rows
    number_of_rows = df.shape[0]

    # print file summary
    print('FILE SUMMARY FOR: ', file_name)
    print('Total number of rows: ', number_of_rows)
    print('Total number of columns: ', number_of_cols)
    print('File size: ', size_readable)

def saveFile(df,table_config):
    # save dataframe to text file seperated by |
    df.to_csv(r'./saved_data.txt', header=None, index=None, sep=table_config['outbound_delimiter'], mode='a')

    # comppress saved text file to gz format
    with open('./saved_data.txt', 'rb') as f_in, gzip.open(table_config['file_name'] + '.txt' + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)