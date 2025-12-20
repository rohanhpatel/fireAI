import os
import pandas as pd
import sys
import time
from functions import TimeKeeper

tk = TimeKeeper("dataAccess")

def txt_to_nj_pickle(year, txtName):
    curFile = os.path.join('data', str(year), txtName + ".txt")
    if not os.path.exists(curFile):
        tk.out(f"{curFile} doesn't exist")
    curLines = []
    with open(curFile, 'r') as fFile:
        allLines = fFile.readlines()
        tk.out(f"There are {len(allLines)} lines")
        for i, line in enumerate(allLines):
            actLine = line.strip().replace("\"", "").split("^")
            curLines.append(actLine)
            if i % 10000 == 0:
                tk.out(f"At line {i}")
        tk.out("Done reading file")
    colNames = curLines[0]
    curLines = curLines[1:]
    tk.out("Converting to dataframe")
    fullDf = pd.DataFrame(curLines, columns=colNames)
    tk.end()
    tk.out("Filtering by NJ")
    njDf = fullDf[fullDf["STATE"] == "NJ"]
    tk.end()
    pickleFile = os.path.join('compressed_data', 'nj_' + str(year) + "_" + txtName + '.pkl.gz')
    tk.out("Saving as pickle file")
    njDf.to_pickle(pickleFile, compression='gzip')
    tk.end()
    return pickleFile

def nj_pickle_to_df(full_pickle_file):
    if not os.path.exists(full_pickle_file):
        tk.out(f"{full_pickle_file} doesn't exist")
        return None
    else:
        tk.out(f"Reading {full_pickle_file}")
        return pd.read_pickle(full_pickle_file)

if __name__ == "__main__":
    txt_file = sys.argv[1]
    saved_file = txt_to_nj_pickle(2024, txt_file)
    verify_df = nj_pickle_to_df(saved_file)
    if verify_df is not None:
        print(verify_df.head(10))
    