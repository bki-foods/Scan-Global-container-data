#!/usr/bin/env python3

import os.path

Path_source = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\1. Produktion\Råkaffe\Scan Global filer'
Path_archive = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\1. Produktion\Råkaffe\Scan Global filer\Arkiv'
File_name = r'\BKI Scan Global data'
File_complete = Path_source + File_name + '.xlsx'

File_timestamp = os.path.getctime(File_complete)
File_complete_new = Path_source + File_name + '_' + str(File_timestamp) + '.xlsx'

print( File_complete )
print( File_complete_new )

os.path.exists( File_complete)


print(os.path.exists( File_complete))

print(os.path.getctime(File_complete))

# os.path.getmtime(path)