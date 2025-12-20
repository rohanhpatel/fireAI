import pandas as pd
import dataAccess

allYears = list(range(2020, 2025))
fiDfs = {}
iaDfs = {}
for y in allYears:
    fiDfs[y] = dataAccess.nj_pickle_to_df(y, 'fireincident')
    iaDfs[y] = dataAccess.nj_pickle_to_df(y, 'incidentaddress')

fiCols = fiDfs[2024].columns
iaCols = iaDfs[2024].columns
inBoth = list(set(fiCols).intersection(set(iaCols)))
#print(f"iaCols: {iaCols}")
print(f"Columns in both: {inBoth}")

mergedDfs = {}
for y in allYears:
    mergedDfs[y] = pd.merge(fiDfs[y], iaDfs[y], on=inBoth)

#print(f"All columns: {fiDfs[2020].columns}")
#print(f"Unique for CAUSE_IGN: {fiDfs[2020]['CAUSE_IGN'].value_counts()}")
#print(f"Unique for FIRST_IGN: {fiDfs[2020]['FIRST_IGN'].value_counts()}")
#print(f"Number of empty values for incident keys: {fiDfs[2020]['INCIDENT_KEY'].isna().sum()}")
#print(f"Number of empty values for cause of ignition: {fiDfs[2020]['CAUSE_IGN'].isna().sum()}")

ia_04182024 = iaDfs[2024][iaDfs[2024]['INC_DATE'] == '04182024']
ia_dn_04182024 = ia_04182024[ia_04182024['STREETNAME'].str.contains('dutch neck', case=False, na=False)]
#print(ia_dn_04182024[['INCIDENT_KEY', 'NUM_MILE', 'STREET_PRE', 'STREETNAME', 'STREETTYPE', 'STREETSUF', 'APT_NO', 'CITY', 'STATE_ID', 'ZIP5', 'ZIP4', 'X_STREET']])
ik_ia_dn_04182024 = ia_dn_04182024['INCIDENT_KEY']
fi_dn_04182024 = fiDfs[2024][fiDfs[2024]['INCIDENT_KEY'].isin(ik_ia_dn_04182024)]
print(fi_dn_04182024.head())




#print("Fire incidents on April 18, 2024 at 08520: {}")


