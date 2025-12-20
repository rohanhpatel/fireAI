import os
import pandas as pd
from functions import TimeKeeper

tk = TimeKeeper("dataAccess")

def save_years(years):
    for y in years:
        yearFiles = os.listdir(os.path.join('data', str(y)))
        for fname in yearFiles:
            actName = fname.split('.')[0]
            txt_to_nj_pickle(y, actName)

def txt_to_nj_pickle(year, txtName):
    curFile = os.path.join('data', str(year), txtName + ".txt")
    tk.out(f"Converting {curFile} to pickle")
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
    tk.out(f"Saving as pickle file {pickleFile}")
    njDf.to_pickle(pickleFile, compression='gzip')
    tk.end()

def nj_pickle_to_df(year, fname):
    pickleFile = os.path.join('compressed_data', 'nj_' + str(year) + "_" + fname + ".pkl.gz")
    if not os.path.exists(pickleFile):
        tk.out(f"{pickleFile} doesn't exist")
        return None
    else:
        tk.out(f"Reading {pickleFile}")
        return pd.read_pickle(pickleFile)

if __name__ == "__main__":
    #txt_file = sys.argv[1]
    #saved_file = txt_to_nj_pickle(2024, txt_file)
    #verify_df = nj_pickle_to_df(2024, txt_file)
    #if verify_df is not None:
    #    print(verify_df.head(10))
    save_years(list(range(2020,2025)))
    