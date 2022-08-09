#!/usr/bin/env python3

import os.path
import pandas as pd
from sqlalchemy import create_engine
import urllib
import datetime
import shutil


Path_source = r'\\appsrv07\Python filer\Scan Global filer'
Path_archive = r'\\appsrv07\Python filer\Scan Global filer\Arkiv'
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
                     ,'DEPOTIND-LEVERING':'Indlevering_depot','INDLEVERING REFERENCE':'Indlevering_reference'}
Cols_df_sg = ['Kontraktnummer' ,'Containernummer' ,'Bill_of_lading' ,'Nettovægt' ,'Bruttovægt' ,'Segl' 
              ,'Udlevering_depot' ,'Udlevering_reference' ,'Indlevering_depot' ,'Indlevering_reference']

Df_log = pd.DataFrame(data= {'Date':Timestamp ,'Event':Script_name ,'Note': 'Filename: ' + File_name_new }, index=[0])

try:
    if os.path.exists( File_complete): # Check if file exists
        os.rename( File_complete , File_complete_new ) # Rename file from creation time
        
        Dic_file = pd.read_excel(File_complete_new, header=0).to_dict() # Read file into dictionary
        Df_sg = pd.DataFrame.from_dict(Dic_file) # Read dictionary into dataframe
        Df_sg.columns = Df_sg.columns.str.replace('\n', '') # Replace newline characters from column headers
        Df_sg = Df_sg.rename(columns=Cols_df_sg_rename) #Rename columns
        Df_sg = Df_sg[Cols_df_sg] # Limit columns to those present in SQL target table
        Df_sg = Df_sg.replace({'nan': None , 'NONE': None}) # Convert NaN values to 0 before SQL insert
        Df_sg.loc[:, 'Timestamp'] = Timestamp
        Df_sg.loc[:, 'Filnavn'] = File_name_new
     
        Df_sg.to_sql('Container_data' ,con=con_ds ,schema="cof" ,if_exists='append' ,index=False) # Insert into SQL
        Df_log.to_sql('Log' ,con=con_ds ,schema='dbo' ,if_exists='append' ,index=False) # Write to log
    
        shutil.move(File_complete_new ,Path_archive + File_name_new)
except Exception as e:
    df_email_err = pd.DataFrame(data= {
        'Email_til': 'nmo@bki.dk' ,'Email_emne': 'Fejl i indlæsning af Scan Global fil'
         ,'Email_tekst':'Indlæsning af ' + str(File_complete) + ' fra Scan Global er fejlet. \n\n'
         + 'Nyt filnavn: ' + str(File_complete_new) + '\n\n'
         + 'Følgende fejl er opstået: \n'
         + str(e)
    } ,index=[0])
    # Create record in Email log if script fails and a files existed
    df_email_err.to_sql('Email_log' ,con=con_ds ,schema= 'dbo' ,if_exists='append' ,index=False)
    
    