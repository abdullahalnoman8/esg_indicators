import tabula
import os
import pandas
import numpy
import glob

# Read a single pdf file and collect data for that
file = "pdf_files/1&1 Drillisch ESG Report.pdf"
tables = tabula.read_pdf(file, pages="all", stream = True)

# print(str(tables[0]))
print(os.path.basename(file));

pdfFileName = os.path.splitext(os.path.basename(file))[0];
print(pdfFileName)
folder_name_tables = pdfFileName + " tables"
print(folder_name_tables)


def save_all_clean_tables(tables, folder_name_tables):
    if not os.path.isdir(folder_name_tables):
        os.mkdir(folder_name_tables)

    for i, table in enumerate(tables, start=1):
        table = pandas.DataFrame(table)
        # if not table.empty:
        #     new_header = table.iloc[0]  # grab the first row for the header
        #     table = table[1:]  # take the data less the header row
        #     table.columns = new_header  # set the header row as the df header
        table.rename(columns={table.columns[0]: "Year"}, inplace=True)
        table.transpose()
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
        if dfs.empty:
            dfs = df.copy()
        else:
            dfs = df.merge(dfs, left_index=True, right_index=True,
                           how='outer', suffixes=('', '_y'))
            dfs.drop(dfs.filter(regex='_y$').columns.tolist(), axis=1, inplace=True)
            dfs = dfs.dropna(how='all', axis=1)
    dfs.to_excel(os.path.join(folder_name, "results.xlsx"), header=True, index=False)


merge_all_tables(folder_name_tables, pdfFileName)

