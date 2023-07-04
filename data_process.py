import pandas as pd
import numpy as np
import pickle as pl
import glob
from log import Log
from tqdm import tqdm as Pbar
from datetime import datetime as DT
import pytz

from define import *

def normalize(data):
    normalized =[]
    return normalized

def load_pickle(fname):
    df = None
    with open(fname, "rb") as f:
        df = pl.loads(f.read())
    return df

def collect_deps_list(parent):
    depsList = []
    # load_pickle by trees
    pattern = parent+"/deps/*.pickle"
    depsPathList = glob.glob(pattern)
    for i in Pbar(depsPathList, desc="Loading deps list"):
        assert os.path.exists(i)
        depsList.append([i, load_pickle(i)])
    
    Log.info(f"Total {len(depsList)} deps stock loaded.")
    return depsList

def collect_tw_list(parent):
    twList = []
    pattern = parent+"/*/*.pickle"
    res = glob.glob(pattern)
    for i in Pbar(res, desc="Loading tw list"):
        if "deps" in i:
            continue
        assert os.path.exists(i)
        twList.append([i, load_pickle(i)])
    
    Log.info(f"Total {len(twList)} tw stock loaded.")
    return twList

def trim(df, timestamp):
    return df

def expand_nil(dat, method='repeat'):
    """
    expand_nil
    @input - dat : data in dataframe
    @output - expandData : dataframe
    @desc - expand missing date with repeat value
    """
    dx = dat.index
    ti = np.array(list(map(lambda x: x.timestamp(), dx)))

    trim(dat, ti)

    dt = (ti[1:]-ti[:-1]) - ONEDAYSEC
    boolDt = dt != 0
    start, end = ti[0], ti[-1]
    totalDays = int((end - start) / ONEDAYSEC + 0.5) + 1
    
    assert totalDays >= 0, f"Error in expand_nil {start}, {end}, {totalDays}, date0:{dx[0]}, date1:{dx[1]}"

    dy = np.array(dat)
    expandDat = np.zeros((totalDays, 6), dtype=np.float64)
    #dbg(start, end, dx[0], dx[-1])
  
    thistime = start
    expandDat[0,0] = thistime
    expandDat[0,1:] = dy[0,:5]
    thistime += ONEDAYSEC
    
    #dbg("expanddat", expandDat.shape, "dat shape:",np.array(dat).shape, "dt shape:", dt.shape)
    #dbg("start",dx[0])
    #dbg("end",dx[-1])
    ind = 1
    for i, b in enumerate(boolDt):
        dyInd = i + 1
        if b:
            numDay = int(dt[i] / ONEDAYSEC + 0.5)
            for j in range(numDay):
                #print(ind,f"{j}/{numDay} - {DT.fromtimestamp(thistime)}")
                expandDat[ind, 0] = thistime
                expandDat[ind, 1:] = dy[dyInd-1,:5]
                thistime += ONEDAYSEC
                ind += 1
            #print(ind,f"orig {DT.fromtimestamp(thistime)}")
            #print(1, f"j{j}, numD:{numDay}, outshape:{expandDat.shape}, ind:{ind}, dyshape:{dy.shape}, dyind:{dyInd}")    
            expandDat[ind, 0] = thistime
            expandDat[ind, 1:] = dy[dyInd,:5]
            thistime += ONEDAYSEC
            ind += 1
            #print(numDay, dx[i], dx[i+1])
        else:
            #print(ind,f"else - {DT.fromtimestamp(thistime)}")
            #print(2, expandDat[0,i].shape, expandDat[1:,i].shape)
            expandDat[ind, 0] = thistime
            expandDat[ind, 1:] = dy[dyInd,:5]
            thistime += ONEDAYSEC
            ind += 1

    data = expandDat[:,1:]
    cols = list(dat.columns[:5])
    try:
        index = list(map(lambda x : DT.fromtimestamp(x), expandDat[:,0]))
    except:
        import traceback
        Log.warn(f"{expandDat[:,0].shape}, {expandDat[:5,0]}, {map(lambda x : DT.fromtimestamp(x), expandDat[:,0])}")
        Log.warn(f"{list(map(lambda x : DT.fromtimestamp(x), expandDat[:,0]))}")
        traceback.format_exc()
        exit()
    expandData = pd.DataFrame(data, columns = cols, index = index)
    endtime = expandDat[-1, 0]
    starttime = expandDat[0, 0]

    assert expandDat[0, 0] == start, "data start time is not correct! data:{} expected:{}".format(
        starttime, DT.fromtimestamp(starttime),
        start, DT.fromtimestamp(start))

    endDiff = abs(end - endtime)
    isSurpassTol = endDiff < ENDTIME_TOL
    assert isSurpassTol, "data end time is not correct! data:{}({}) expected:{}({})".format(
        endtime, DT.fromtimestamp(endtime),
        end, DT.fromtimestamp(end))

    return expandData

def convert_time(df, dateOrig = 'tw'):
    df.index.tz_convert(TIMEZONE).to_pydatetime()
    return df

def process_data(dfList):

    # convert date time to same time zone - default tw
    for path, df in dfList:
        convert_time(df)

    # expand nil value - default with repeat
    ind = 0
    pbar = Pbar(dfList, desc="expand nil...")
    for dat in pbar:
        path, df = dat
        pbar.set_description(f"Processing {path}")
        expandDat = expand_nil(df)
        dfList[ind] = [path, expandDat]
        ind += 1

    return dfList

def main():

    parentTw = "stock"
    parentDeps = "stock"

    twList = collect_tw_list(parentTw)
    depsList = collect_deps_list(parentDeps)

    depsList = process_data(depsList)   
    twList = process_data(twList)
    


    dataSize = [3, 224, 224] # open,close,vol

if __name__ == '__main__':
    main()






