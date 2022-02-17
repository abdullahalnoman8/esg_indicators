import tabula
import os
import pandas
import glob
import numpy as np


# pdf_files = []
#
#
# def list_pdf_files(dr, ext):
#     os.chdir(dr)
#     for file in glob.glob(ext):
#         pdf_files.append(file)
#
#
# list_pdf_files("./pdf_files/", "*.pdf")

file = "./pdf_files/GBI_2020.pdf"
tables = tabula.read_pdf(file, pages="all", stream=True)

pdfFileName = os.path.splitext(os.path.basename(file))[0]
print(pdfFileName)

folder_name_tables = pdfFileName + " tables"
print(folder_name_tables)


def check_float(potential_float):
    try:
        float(potential_float)
        return True
    except ValueError:
        return False


def save_all_clean_tables(tables, folder_name_tables):
    if not os.path.isdir(folder_name_tables):
        os.mkdir(folder_name_tables)

    for i, table in enumerate(tables, start=1):
        table = pandas.DataFrame(table)
        table.rename(columns={table.columns[0]: "Year"}, inplace=True)
        table = table.transpose()
        print(table)
        table.to_excel(os.path.join(folder_name_tables, f"table_{i}.xlsx"), header=False)


# call to the function

save_all_clean_tables(tables, folder_name_tables)


def merge_all_tables(folder_name_tables, pdfFileName):
    folder_name = pdfFileName
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)

    dfs = pandas.DataFrame()
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        year_txt = pdfFileName.split("_")
        df.insert(1, "Company Name", year_txt[0], allow_duplicates=False)
        # df = df.dropna(how='all', axis=1)
        # df.drop(df.columns[df.columns.str.contains('unnamed', case=False, na=False)], axis=1, inplace=True)
        check_cond = year_txt[1]
        for val in df["Year"]:
            if check_cond in str(val):
                dfs = df.merge(dfs, left_index=True, right_index=True,
                               how='outer',
                               suffixes=('', '_y'))  # add company name as one of the column (from pdf name)
                dfs.drop(dfs.filter(regex='_y$').columns.tolist(), axis=1, inplace=True)
                dfs = dfs.dropna(how='all', axis=1)
                # print(year_txt[0])
                dfs.to_excel(os.path.join(folder_name, "results_" + year_txt[0] + ".xlsx"), header=True, index=False)


merge_all_tables(folder_name_tables, pdfFileName)


def data_cleaning():
    splitted_data = pdfFileName.split("_")
    data_frame = pandas.read_excel(pdfFileName + "/results_" + splitted_data[0] + ".xlsx")
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

    # data_frame = data_frame[data_frame['Year'].astype(str).str.startswith('20')]
    # data_frame.drop(data_frame.columns[data_frame.columns.str.contains('unnamed', case=False, na=False)], axis=1, inplace=True)
    # data_frame[data_frame.columns[~data_frame.columns.str.match('^\d')]]

    data_frame.to_excel(os.path.join(pdfFileName, f"cleaned_" + splitted_data[0] + ".xlsx"))


data_cleaning()
