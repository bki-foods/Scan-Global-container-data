#!/usr/bin/env python3

import os.path
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

Path_source = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\1. Produktion\Råkaffe\Scan Global filer'
Path_archive = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\1. Produktion\Råkaffe\Scan Global filer\Arkiv'
File_name = r'\BKI Scan Global data'
File_complete = Path_source + File_name + '.xlsx'

File_timestamp = os.path.getctime(File_complete)
File_complete_new = Path_source + File_name + '_' + str(File_timestamp) + '.xlsx'

server = 'sqlsrv04'
db = 'BKI_Datastore'
con = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db)

print( File_complete )
print( File_complete_new )

if os.path.exists( File_complete): #Check if file exists
    os.rename( File_complete , File_complete_new ) #rename file from creating time
    
    df_sg = pd.read_excel(File_complete_new, header=0).to_dict()
    
print( df_sg )


