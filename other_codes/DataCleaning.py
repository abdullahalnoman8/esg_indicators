import tabula
import os
import pandas
import numpy
import glob


def check_float(potential_float):
    try:
        float(potential_float)
        return True
    except ValueError:
        return False


def data_cleaning():
    data_frame = pandas.read_excel("KFW_2020/results_KFW.xlsx")
    # print(data_frame.head())
    # To remove data from a specific in to another specific index i.e index 6 to 10
    # data_frame = data_frame.drop(data_frame.iloc[:, 6:10], axis=1)
    # print(data_frame.head())
    unnamed = []
    float_value = []
    column_name_length = []
    for unnamed_column in data_frame.columns:
        # print(unnamed_column)
        if str(unnamed_column).startswith('Unnamed'):
            unnamed.append(unnamed_column)
        if check_float(str(unnamed_column)):
            float_value.append(unnamed_column)
        if len(str(unnamed_column)) > 100:
            column_name_length.append(unnamed_column)
    # print(unnamed)
    # print(float_value)
    # print(column_name_length)
    # for column in column_name_length:
    #     print(column)

    # Remove all the Unnamed Column
    data_frame = data_frame.drop(unnamed, axis=1)

    # Remove all the float value column
    data_frame = data_frame.drop(float_value, axis=1)

    # Remove all the column who's column name length is more than 100 char
    data_frame = data_frame.drop(column_name_length, axis=1)

    # Delete column by column name
    # column_names = ['Settlements –4', 'Swaps.2', 'Stage transfers', 'Closing balance.1', 'Closing balance.2'
    #     , 'bilities.', 'from 1 to 2', 'from 1 to 3', 'from 2 to 1', 'from 2 to 3', 'from 3 to 2', '13–21']
    # data_frame = data_frame.drop(column_names, axis=1)

    # Drop duplicate values in the data set if duplicate value exist
    # print("Duplicate Data: ", data_frame.duplicated())
    # data_frame = data_frame.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)

    # Find the percentage of null elements as a percentage of whole dataset.
    # rows, columns = data_frame.shape
    # cell_count = rows * columns
    # number_of_nulls = data_frame.isnull().sum().sum()
    # percentage_of_missing = (number_of_nulls / cell_count) * 100
    # print(f'Percentage of missing values: {percentage_of_missing}%')
    #
    # # print(data_frame.info())
    # data_frame.dropna(axis='columns', thresh=6, inplace=True)

    # Check to see if any rows have less than 2 elements.
    # under_threshold_removed = data_frame.dropna(axis='index', thresh=2, inplace=False)
    # under_threshold_rows = data_frame[~data_frame.index.isin(under_threshold_removed.index)]
    # print(under_threshold_rows)

    # # Check null is present or not
    # print("Null values in the dataset: ", data_frame.isna().any().sum())
    # # This will fill the null spaces with NA/NAN
    # data_frame.fillna(value=None, method=None, axis=None, inplace=False, limit=None, downcast=None)

    data_frame.to_excel(os.path.join("KFW_2020", f"cleaned_kfw.xlsx"))


data_cleaning()
