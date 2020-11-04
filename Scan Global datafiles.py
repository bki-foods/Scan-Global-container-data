#!/usr/bin/env python3

import os.path
import pandas as pd
from sqlalchemy import create_engine
import urllib
import datetime

Path_source = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\1. Produktion\Råkaffe\Scan Global filer'
Path_archive = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\1. Produktion\Råkaffe\Scan Global filer\Arkiv'
File_name = r'\BKI Scan Global data'
File_complete = Path_source + File_name + '.xlsx'
File_timestamp = os.path.getctime(File_complete) # Creation time of file
File_name_new = File_name + '_' + str(File_timestamp) + '.xlsx' # New file name
File_complete_new = Path_source + File_name_new # New file name

Server = 'sqlsrv04'
Db = 'BKI_Datastore'
Schema = 'cof'
Params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 10.0};SERVER=' + Server +';DATABASE=' + Db +';Trusted_Connection=yes')
Engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % Params)

Timestamp = datetime.datetime.now()
Script_name = 'Scan Global datafiles.py'

Cols_df_sg_rename = {'BKI REF#':'Kontraktnummer' ,'B/L':'Bill_of_lading' ,'NETTO VÆGT':'Nettovægt'
                     ,'BRUTTO VÆGT':'Bruttovægt' ,'CONTAINER NO.':'Containernummer' ,'SEGL':'Segl'
                     ,'DEPOT UD-LEVERING': 'Udlevering_depot' ,'UDLEVERING REFERENCE':'Udlevering_reference'
                     ,'DEPOT IND-LEVERING':'Indlevering_depot','INDLEVERING REFERENCE':'Indlevering_reference'}
Cols_df_sg = ['Kontraktnummer' ,'Containernummer' ,'Bill_of_lading' ,'Nettovægt' ,'Bruttovægt' ,'Segl' 
              ,'Udlevering_depot' ,'Udlevering_reference' ,'Indlevering_depot' ,'Indlevering_reference']

Df_log = pd.DataFrame(data= {'Date':Timestamp ,'Event':Script_name ,'Note': 'Filename: ' + File_name_new }, index=[0])

if os.path.exists( File_complete): # Check if file exists
    os.rename( File_complete , File_complete_new ) # Rename file from creation time
    
    Dic_file = pd.read_excel(File_complete_new, header=0).to_dict() # Read file into dictionary
    Df_sg = pd.DataFrame.from_dict(Dic_file) # Read dictionary into dataframe
    Df_sg = Df_sg.rename(columns=Cols_df_sg_rename) #Rename columns
    Df_sg = Df_sg[Cols_df_sg] # Limit columns to those present in SQL target table
    Df_sg.loc[:, 'Timestamp'] = Timestamp
    Df_sg.loc[:, 'Filnavn'] = File_name_new
    
    Df_sg.to_sql('Container_data' ,con=Engine ,schema=Schema ,if_exists='append' ,index=False) # Insert into SQL
    Df_log.to_sql('Log' ,con=Engine ,schema='dbo' ,if_exists='append' ,index=False) # Write to log

print( Df_sg )