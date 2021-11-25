import tabula
import os
# readinf the PDF file that contain Table Data
# you can find find the pdf file with complete code in below
# read_pdf will save the pdf table into Pandas Dataframe
tables = tabula.read_pdf("./pdf_files/2019_GRI_Report_DZBANK.pdf",pages="all")
print(str(tables))

# save them in a folder
folder_name = "tables"
if not os.path.isdir(folder_name):
    os.mkdir(folder_name)
# iterate over extracted tables and export as excel individually
for i, table in enumerate(tables, start=1):
    table.to_excel(os.path.join(folder_name, f"table_{i}.xlsx"), index=False)



