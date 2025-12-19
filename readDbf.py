from dbfread import DBF
import os

script_dir = os.path.abspath(os.path.dirname(__file__))
addressDbfFile = os.path.join(script_dir, "usfa_nfirs_1980", "1980", "ADDRESS.DBF")
addresses = DBF(addressDbfFile)

for record in addresses:
    if record['STATE'] == 'NJ':
        print(record)
    else:
        continue