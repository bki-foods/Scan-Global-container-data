#!/usr/bin/env python3

import os.path
import pandas as pd
from sqlalchemy import create_engine
import urllib
import datetime
import shutil


Path_source = r'\\tx-app\Python filer\Scan Global filer'
Path_archive = r'\\tx-app\Python filer\Scan Global filer\Arkiv'
File_name = r'\BKI Scan Global data'
File_complete = Path_source + File_name + '.xlsx'
File_timestamp = os.path.getctime(File_complete) # Creation time of file
File_name_new = File_name + '_' + str(File_timestamp) + '.xlsx' # New file name
File_complete_new = Path_source + File_name_new # New file name

server_04 = "sqlsrv04"
db_ds = "BKI_Datastore"
params_ds = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server_04};DATABASE={db_ds};trusted_connection=yes"
con_ds = create_engine('mssql+pyodbc:///?odbc_connect=%s' % urllib.parse.quote_plus(params_ds))

Timestamp = datetime.datetime.now()
Script_name = 'Scan Global datafiles.py'

Cols_df_sg_rename = {'BKI REF#':'Kontraktnummer' ,'B/L':'Bill_of_lading' ,'NETTO VÆGT':'Nettovægt'
                     ,'BRUTTO VÆGT':'Bruttovægt' ,'CONTAINER NO.':'Containernummer' ,'SEGL':'Segl'
                     ,'DEPOT UD-LEVERING': 'Udlevering_depot' ,'UDLEVERING REFERENCE':'Udlevering_reference'
                     ,'DEPOTIND-LEVERING':'Indlevering_depot','INDLEVERING REFERENCE':'Indlevering_reference'
                     ,'LEVERINGS DATO':'Leveringsdato','DAGE PÅ HAVN':'Dage på havn'}
Cols_df_sg = ['Kontraktnummer' ,'Containernummer' ,'Bill_of_lading' ,'Nettovægt' ,'Bruttovægt' ,'Segl' 
              ,'Udlevering_depot' ,'Udlevering_reference' ,'Indlevering_depot' ,'Indlevering_reference', 'ETA AARHUS'
              ,'Leveringsdato','Dage på havn']

Df_log = pd.DataFrame(data= {'Date':Timestamp ,'Event':Script_name ,'Note': 'Filename: ' + File_name_new }, index=[0])


try:
    if os.path.exists( File_complete): # Check if file exists
        os.rename( File_complete , File_complete_new ) # Rename file from creation time
        
        Dic_file = pd.read_excel(File_complete_new, header=0).to_dict() # Read file into dictionary
        Df_sg = pd.DataFrame.from_dict(Dic_file) # Read dictionary into dataframe
        Df_sg.columns = Df_sg.columns.str.replace('\n', '') # Replace newline characters from column headers
        Df_sg = Df_sg.rename(columns=Cols_df_sg_rename) #Rename columns
        Df_sg = Df_sg[Cols_df_sg] # Limit columns to those present in SQL target table

        Df_sg["Leveringsdato"] = pd.to_datetime(Df_sg["Leveringsdato"], origin="1899-12-30", unit="D")
        Df_sg["ETA AARHUS"] = pd.to_datetime(Df_sg["ETA AARHUS"], origin="1899-12-30", unit="D")
        
        Df_sg = Df_sg.replace({'nan': None , 'NONE': None}) # Convert NaN values to 0 before SQL insert
        Df_sg.loc[:, 'Timestamp'] = Timestamp
        Df_sg.loc[:, 'Filnavn'] = File_name_new  
        
        Df_sg.to_sql('Container_data' ,con=con_ds ,schema="cof" ,if_exists='append' ,index=False) # Insert into SQL
        Df_log.to_sql('Log' ,con=con_ds ,schema='dbo' ,if_exists='append' ,index=False) # Write to log
    
        shutil.move(File_complete_new ,Path_archive + File_name_new)
except Exception as e:
    print(e)
    
    