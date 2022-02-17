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
    split_string = pdfFileName.split("_")
    data_frame = pandas.read_excel(pdfFileName + "/results_" + split_string[0] + ".xlsx")
    data_frame = data_frame.dropna(how='all', axis=1)
    data_frame.drop(data_frame.columns[data_frame.columns.str.contains('unnamed', case=False, na=False)], axis=1,
                    inplace=True)
    data_frame[data_frame.columns[~data_frame.columns.str.match('^\d')]]
    data_frame = data_frame[data_frame['Year'].astype(str).str.startswith('20')]
    data_frame = data_frame[data_frame['Year'].astype(str).str.match('^\d')]
    data_frame.to_excel(os.path.join(pdfFileName, "cleaned_" + split_string[0] + ".xlsx"), index=False)
    print("cleaned table for  " + split_string[0] + " created")


#Environment
def data_environment(pdfFileName):
    folder_name = "environment"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    split_string = pdfFileName.split("_")
    folder_name = "environment"
    searchfor = ['carbon', 'co2', 'paper', 'electricity', 'power', 'waste', 'emission', 'oil', 'gas', 'energy', 'air',
                 'diesel', 'petrol', 'water', 'fuel', 'cogeneration', 'photovoltaic', 'generator', 'heat', 'wood',
                 'electric',
                 'recycling and pollution']
    name = "./" + split_string[0] + "_" + split_string[1] + "/cleaned_" + split_string[0] + ".xlsx"
    for file_name in glob.glob(name):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        df2 = df[df.columns[df.columns.str.contains('|'.join(searchfor), na=False, case=False)]]
        df2.insert(0, "Year", df['Year'], allow_duplicates=False)
        df2.insert(1, "Company Name", df['Company Name'])
        df2['Category'] = 'Environmental'
        df2.to_excel(os.path.join(folder_name, "results_" + split_string[0] + ".xlsx"), header=True, index=False)
        print("Environment table for  " + split_string[0] + " created")
        
def data_environment_clean(pdfFileName):
    split_string = pdfFileName.split("_")
    folder_name = "merged_carbon"
    folder_name_tables = "environment"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        
        df.rename(columns={'Total net carbon emissions in metric tons of CO  equivalents  3,390 2':'Carbon Emissions',
                            'Total carbon emissions (t) 2':'Carbon Emissions',
                            'Total emissions of greenhouse gases, CO e tonnes':'Carbon Emissions',
                            'CO  emissions from travel 2 (in kg/employee) 5432': 'Carbon Emissions'
                           }, inplace=True)
        if 'Carbon Emissions' in df.columns:
            df_carbon = df[['Year', 'Company Name', 'Carbon Emissions', 'Category']]
            df_carbon.to_excel(os.path.join(folder_name, "carbon_" + split_string[0] + ".xlsx"), header=True, index=False)
            print("Carbon table for  " + split_string[0] + " created")
        
        df.rename(columns ={'Electricity (in MWh)':'Electricity Consumption',
                            'Electric':'Electricity Consumption',
                            'Total electricity consumption':'Electricity Consumption',
                            'Electric energy (kWh)':'Electricity Consumption',
                            'Total electricity consumption (kWh)':'Electricity Consumption',
                            'Electricity consumption excl. data centers (in kWh/m2)':'Electricity Consumption'}, inplace=True)
        if 'Electricity Consumption' in df.columns:
            df_electricity = df[['Year', 'Company Name', 'Electricity Consumption', 'Category']]
            df_electricity.to_excel(os.path.join(folder_name, "electricity_" + split_string[0] + ".xlsx"), header=True, index=False)
            print("Electricity table for  " + split_string[0] + " created")
            
        df.rename(columns = {
            'Recycled paper':'Recycled Paper',
            'Recycled paper (Blue Angel)1':'Recycled Paper',
            'Recycled paper ratio 6)':'Recycled Paper',
            'Share of recycled paper':'Recycled Paper',
            'Percentage of recycled waste':'Recycled Paper'}, inplace=True)
        if 'Recycled Paper' in df.columns:
            df_paper_recycle = df[['Year', 'Company Name', 'Recycled Paper', 'Category']]
            df_paper_recycle.to_excel(os.path.join(folder_name, "paper_reycle_" + split_string[0] + ".xlsx"), header=True, index=False)
            print("Paper reycle table for  " + split_string[0] + " created")
        
        os.remove(file_name)
            
def merge_environment(pdfFileName):
    dfs = pandas.DataFrame()
    folder_electricity = "electricity_consumption_merged"
    folder_name_tables = "environment_results"
    if not os.path.isdir(folder_electricity):
        os.mkdir(folder_electricity)
    for file_name in glob.glob(folder_name_tables + "/electricity*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        dfs = df.append(dfs, ignore_index = False)
        dfs.to_excel(os.path.join(folder_electricity, "electricity_consumption_results.xlsx"), header=True, index=False)
        os.remove(file_name)
    
    dfs1 = pandas.DataFrame()
    folder_carbon = "carbon_emission_merged"
    folder_name_tables = "environment_results"
    if not os.path.isdir(folder_carbon):
        os.mkdir(folder_carbon)
    for file_name in glob.glob(folder_name_tables + "/carbon*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        dfs1 = df.append(dfs, ignore_index = False)
        dfs1.to_excel(os.path.join(folder_carbon, "carbon_emission_results.xlsx"), header=True, index=False)
        os.remove(file_name)
    
    dfs2 = pandas.DataFrame()
    folder_paper = "paper_recycled_merged"
    folder_name_tables = "environment_results"
    if not os.path.isdir(folder_paper):
        os.mkdir(folder_paper)
    for file_name in glob.glob(folder_name_tables + "/paper*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        dfs2 = df.append(dfs, ignore_index = False)
        dfs2.to_excel(os.path.join(folder_paper, "paper_recycled_results.xlsx"), header=True, index=False)
        os.remove(file_name)

#This is for merging the final carbon, electricity and paper files (last function call)    
def merge(pdfFileName):
    dfs = pandas.DataFrame()
    folder_name = "Environemnt_combined"
    folder_name_tables = "Environment"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        dfs = df.append(dfs, ignore_index = False)
        dfs.to_excel(os.path.join(folder_name, "merged_results.xlsx"), header=True, index=False)
 

#Governance   

def data_governance(pdfFileName):
    folder_name = "governance"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    split_string = pdfFileName.split("_")
    searchfor = [
        'Earnings', 'Equity', 'Price', 'Loan', 'Income', 'Expenses', 'Capital', 'cost', 'funds', 'Financial', 'debt', 'assets',
        'loss', 'interest','deposit', 'Donations', 'employment', 'issuance', 'Commitment', 'Disbursement', 'Exposures', 'lending', 
        'estimates','cost', 'profit','earnings', 'property', 'mortgages', 'Tax', 'profit', 'equity', 'deposits'
    ]
    name = "./" + split_string[0] + "_" + split_string[1] + "/cleaned_" + split_string[0] + ".xlsx"
    for file_name in glob.glob(name):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        df2 = df[df.columns[df.columns.str.contains('|'.join(searchfor), na=False, case=False)]]
        df2.insert(0, "Year", df['Year'], allow_duplicates=False)
        df2.insert(1, "Company Name", df['Company Name'])
        df2['Category'] = 'Governance'
        df2.to_excel(os.path.join(folder_name, "results_" + split_string[0] + ".xlsx"), header=True, index=False)
        print("Governance table for  " + split_string[0] + " created")

def data_governance_clean(pdfFileName):
    split_string = pdfFileName.split("_")
    folder_name = "merged_governance"
    folder_name_tables = "governance"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        
        df.rename(columns={'Net profit':'Net Profit',
                            'Profit before tax':'Profit Before Tax',
                            'Profit for the year, total operations, SEK m':'Profit for the Year',
                             'Profit before income tax':'Profit Before Tax',
                             'Profit for the year': 'Profit for the Year',
                             'Donations': 'Donations',
                             'Donations volume (€)': 'Donations',
                             'Expenditure for donations':'Donations',
                             'Total expenses':'Total Expenses',
                             'Total Expenses':'Total Expenses',
                             'Expenses':'Total Expenses'
                           }, inplace=True)
        
        if 'Profit for the Year' in df.columns:
            df_profit = df[['Year', 'Company Name', 'Profit for the Year', 'Category']]
            df_profit.to_excel(os.path.join(folder_name, "profit_" + split_string[0] + ".xlsx"), 
                               header=True, index=False)
            print("Profit table for  " + split_string[0] + " created")
            
        if 'Profit Before Tax' in df.columns:
            df_profit = df[['Year', 'Company Name', 'Profit Before Tax', 'Category']]
            df_profit.to_excel(os.path.join(folder_name, "profit_tax_" + split_string[0] + ".xlsx"), 
                               header=True, index=False)
            print("Profit before tax table for  " + split_string[0] + " created")

        if 'Total Expenses' in df.columns:
            df_profit = df[['Year', 'Company Name', 'Total Expenses', 'Category']]
            df_profit.to_excel(os.path.join(folder_name, "expenses_" + split_string[0] + ".xlsx"), 
                               header=True, index=False)
            print("Expenses table for  " + split_string[0] + " created")

        if 'Donations' in df.columns:
            df_profit = df[['Year', 'Company Name', 'Donations', 'Category']]
            df_profit.to_excel(os.path.join(folder_name, "donations_" + split_string[0] + ".xlsx"), 
                               header=True, index=False)
            print("Donations table for  " + split_string[0] + " created")
    
        
        os.remove(file_name)

def merge_governance(pdfFileName):
    dfs = pandas.DataFrame()
    folder_electricity = "governance_indicators"
    folder_name_tables = "governance"
    if not os.path.isdir(folder_electricity):
        os.mkdir(folder_electricity)
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        dfs = df.append(dfs, ignore_index = False)
        dfs.to_excel(os.path.join(folder_electricity, "gov_results.xlsx"), header=True, index=False)
        os.remove(file_name)


#Social   

def data_social(pdfFileName):
    folder_name = "social"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    split_string = pdfFileName.split("_")
    searchfor = ['risk', 'advice', 'Technology', 'Media', 'Telecom', 'Consumer', 'Retail', 'Perspective',
                 'adjustments', 'Employee', 'Responsibility', 'management', 'Security', 'Professional', 'Clients',
                 'Loyalty', 'scenario', 'age', 'Education', 'Society', 'Arts', 'Culture']
    name = "./" + split_string[0] + "_" + split_string[1] + "/cleaned_" + split_string[0] + ".xlsx"
    for file_name in glob.glob(name):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        df2 = df[df.columns[df.columns.str.contains('|'.join(searchfor), na=False, case=False)]]
        df2.insert(0, "Year", df['Year'], allow_duplicates=False)
        df2.insert(1, "Company Name", df['Company Name'])
        df2['Category'] = 'Social'
        df2.to_excel(os.path.join(folder_name, "results_" + split_string[0] + ".xlsx"), header=True, index=False)
        print("Social table for  " + split_string[0] + " created")

def data_social_clean(pdfFileName):
    split_string = pdfFileName.split("_")
    folder_name = "merged_risk"
    folder_name_tables = "social"
    if not os.path.isdir(folder_name):
        os.mkdir(folder_name)
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        
        df.rename(columns={'Number of employees':'Employees',
                            'number of employees*':'Employees',
                             'All employees':'Employees',
                             'Employees': 'Employees',
                             'Total employees': 'Employees'
                           }, inplace=True)
        
        if 'Employee' in df.columns:
            df_profit = df[['Year', 'Company Name', 'Employee', 'Category']]
            df_profit.to_excel(os.path.join(folder_name, "employee_" + split_string[0] + ".xlsx"), header=True, index=False)
            print("Employee table for  " + split_string[0] + " created")
    
        
        os.remove(file_name)

def merge_social(pdfFileName):
    dfs = pandas.DataFrame()
    folder_electricity = "risk_indicator"
    folder_name_tables = "merged_risk"
    if not os.path.isdir(folder_electricity):
        os.mkdir(folder_electricity)
    for file_name in glob.glob(folder_name_tables + "/*.xlsx"):
        df = pandas.read_excel(file_name)
        df = pandas.DataFrame(df)
        dfs = df.append(dfs, ignore_index = False)
        dfs.to_excel(os.path.join(folder_electricity, "social_results.xlsx"), header=True, index=False)
        os.remove(file_name)


for file in pdf_files:
    tables = tabula.read_pdf(file, pages="all", stream=True)
    pdfFileName = os.path.splitext(os.path.basename(file))[0]

    folder_name_tables = pdfFileName + " tables"

    save_all_clean_tables(tables, folder_name_tables)
    merge_all_tables(folder_name_tables, pdfFileName)
    data_cleaning(pdfFileName)
    data_environment(pdfFileName)
    data_environment_clean(pdfFileName)
    merge_environment(pdfFileName)
    merge(pdfFileName)
    data_governance(pdfFileName)
    data_governance_clean(pdfFileName)
    merge_governance(pdfFileName)
    data_social(pdfFileName)
    data_social_clean(pdfFileName)
    merge_social(pdfFileName)