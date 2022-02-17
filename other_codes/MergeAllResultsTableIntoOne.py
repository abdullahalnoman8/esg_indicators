import os
import pandas
import glob

folder_name_tables = "allresults"


def merge_all_tables(folder_name_tables):
    folder_name = "final_results"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)

    dfs = pandas.DataFrame()
    df_list = []
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        data_file = pandas.read_excel(file_name)
        df = pandas.DataFrame(data_file)
        if dfs.empty:
            dfs = df.copy()
        else:
            dfs = df.merge(dfs, left_index=True, right_index=True,
                           how='outer',)
            # dfs.drop(dfs.filter(regex='_y$').columns.tolist(), axis=1, inplace=True)
    dfs.to_excel(os.path.join(folder_name, "dataset_results.xlsx"), header=True, index=False)


merge_all_tables(folder_name_tables)


def check_float(potential_float):
    try:
        float(potential_float)
        return True
    except ValueError:
        return False


def data_cleaning():
    file = "./final_results/dataset_results.xlsx"
    data_frame = pandas.read_excel(file)
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

    # # Find the percentage of null elements as a percentage of whole dataset.
    # rows, columns = data_frame.shape
    # cell_count = rows * columns
    # number_of_nulls = data_frame.isnull().sum().sum()
    # percentage_of_missing = (number_of_nulls / cell_count) * 100
    # print(f'Percentage of missing values: {percentage_of_missing}%')
    #
    # # print(data_frame.info())
    # data_frame.dropna(axis='columns', thresh=3, inplace=True)
    #
    # # Check to see if any rows have less than 2 elements.
    # under_threshold_removed = data_frame.dropna(axis='index', thresh=2, inplace=False)
    # under_threshold_rows = data_frame[~data_frame.index.isin(under_threshold_removed.index)]
    # print(under_threshold_rows)

    # # Check null is present or not
    # print("Null values in the dataset: ", data_frame.isna().any().sum())
    # # This will fill the null spaces with NA/NAN
    # data_frame.fillna(value=None, method=None, axis=None, inplace=False, limit=None, downcast=None)
    data_frame = data_frame.iloc[:, 0:]
    data_frame.to_excel(os.path.join("./final_results", f"cleaned.xlsx"))


data_cleaning()