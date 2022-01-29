import tabula
import os
import pandas
import glob

pdf_files = []


def list_pdf_files(dr, ext):
    os.chdir(dr)
    for file in glob.glob(ext):
        pdf_files.append(file)


list_pdf_files("./pdf_files/", "*.pdf")


def save_all_clean_tables(tables, folder_name_tables):
    if not os.path.isdir(folder_name_tables):
        os.mkdir(folder_name_tables)

    for i, table in enumerate(tables, start=1):
        table = pandas.DataFrame(table)
        table.rename(columns={table.columns[0]: "Year"}, inplace=True)
        table = table.transpose()
        table.to_excel(os.path.join(folder_name_tables, f"table_{i}.xlsx"), header=False)


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
        check_cond = year_txt[1]
        for val in df["Year"]:
            if check_cond in str(val):
                dfs = df.merge(dfs, left_index=True, right_index=True,
                               how='outer',
                               suffixes=('', '_y'))
                dfs.drop(dfs.filter(regex='_y$').columns.tolist(), axis=1, inplace=True)
                dfs.to_excel(os.path.join(folder_name, "results_" + year_txt[0] + ".xlsx"), header=True, index=False)


def check_float(potential_float):
    try:
        float(potential_float)
        return True
    except ValueError:
        return False


def data_cleaning(pdfFileName):
    splitted_data = pdfFileName.split("_")
    data_frame = pandas.read_excel(pdfFileName + "/results_" + splitted_data[0] + ".xlsx")
    data_frame = data_frame.dropna(how='all', axis=1)
    data_frame.drop(data_frame.columns[data_frame.columns.str.contains('unnamed', case=False, na=False)], axis=1,
                    inplace=True)
    # data_frame[data_frame.columns[~data_frame.columns.str.match('^\d')]]
    data_frame = data_frame[data_frame['Year'].astype(str).str.startswith('20')]
    data_frame = data_frame[data_frame['Year'].astype(str).str.match('^\d')]
    data_frame.to_excel(os.path.join(pdfFileName, "cleaned_" + splitted_data[0] + ".xlsx"), index=False)
    print("cleaned table for  " + splitted_data[0] + " created")


def data_environment(pdfFileName):
    folder_name = "environment"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    splitted_data = pdfFileName.split("_")
    folder_name = "environment"
    searchfor = ['carbon', 'co2', 'paper', 'electricity', 'power', 'waste', 'emission', 'oil', 'gas', 'energy', 'air',
                 'diesel', 'petrol', 'water', 'fuel', 'cogeneration', 'photovoltaic', 'generator', 'heat', 'wood',
                 'electric',
                 'recycling and pollution']
    name = "./" + splitted_data[0] + "_" + splitted_data[1] + "/cleaned_" + splitted_data[0] + ".xlsx"
    for file_name in glob.glob(name):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        df2 = df[df.columns[df.columns.str.contains('|'.join(searchfor), na=False, case=False)]]
        df2.insert(0, "Year", df['Year'], allow_duplicates=False)
        df2.insert(1, "Company Name", df['Company Name'])
        df2['Category'] = 'Environmental'
        df2.to_excel(os.path.join(folder_name, "results_" + splitted_data[0] + ".xlsx"), header=True, index=False)
        print("Environment table for  " + splitted_data[0] + " created")


for file in pdf_files:
    tables = tabula.read_pdf(file, pages="all", stream=True)
    pdfFileName = os.path.splitext(os.path.basename(file))[0]

    folder_name_tables = pdfFileName + " tables"

    save_all_clean_tables(tables, folder_name_tables)
    merge_all_tables(folder_name_tables, pdfFileName)
    data_cleaning(pdfFileName)
    data_environment(pdfFileName)
