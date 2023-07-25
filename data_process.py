import pandas as pd
import numpy as np
import pickle as pl
import glob
from log import Log
from tqdm import tqdm as Pbar
from datetime import datetime as DT
import pytz
import shutil

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
    pbar = Pbar(res, desc="Loading tw list")
    for i in pbar:
        pbar.set_description(f"Processing {i}")
        if "deps" in i:
            continue
        assert os.path.exists(i)
        twList.append([i, load_pickle(i)])
    
    Log.info(f"Total {len(twList)} tw stock loaded.")
    return twList

def trim(df, timestamp):
    ti = timestamp
    ind = np.where(ti >= 0)[0][0]
    #Log.info(ind.shape)
    #[0]
    if ind > 0:
        start = df.index[0]
        df = df[ind:]
        trimed = df.index[0]
        Log.info(f"TRIM {Log.logger} from {start} to {trimed}")
    return df, ind

DBG = 1
def dbg(*args):
    if DBG:
        print(f"[DBG] {args}")

def expand_nil(dat, method='repeat'):
    """
    expand_nil
    @input - dat : data in dataframe
    @output - expandData : dataframe
    @desc - expand missing date with repeat value
    """
    dx = dat.index
    ti = np.array(list(map(lambda x: x.timestamp(), dx)))

    dat, trimInd = trim(dat, ti)

    if trimInd:
        ti = ti[trimInd:]

    dt = (ti[1:]-ti[:-1]) - ONEDAYSEC
    boolDt = dt > 1000
    start, end = ti[0], ti[-1]

    totalDays = int((end - start) / ONEDAYSEC + 0.5) + 1
    #dbg(f"total days {totalDays}, {len(boolDt)} {len(dx)} // {start}, {end}, {(end - start) / ONEDAYSEC} {totalDays}")
    #print(dat.iloc[-10:])
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

def process_nil_data(dfList, key=""):

    # convert date time to same time zone - default tw
    for path, df in dfList:
        convert_time(df)

    # expand nil value - default with repeat
    ind = 0
    pbar = Pbar(dfList, desc="expand nil...")
    for dat in pbar:
        path, df = dat
        Log.logger = path
        pbar.set_description(f"Processing {key} - {path}")
        expandDat = expand_nil(df)
        dfList[ind] = [path, expandDat]
        ind += 1

    return dfList

def debugCache(datList):
    twList = datList[TWDAT]
    for path, df in twList:
        Log.info(f"{path} {df.index[0]} --> {df.index[-1]}")

def check_stage1_cache(cache=True):
    dat = {}
    if not cache:
        return dat
    cacheMarkerList = glob.glob(STAGE1_MARKER + "*")
    cacheDataPath = STAGE1_CACHE
    if cacheMarkerList != []:
        cacheMarkerList.sort()
        cacheMarker = cacheMarkerList[-1]
        date = cacheMarker.split("_")[1]
        cacheDataPath += date
        if os.path.exists(cacheDataPath):
            Log.info(f"Found Stage-1 cache marker {cacheMarker} ... loading {cacheDataPath}...")
            dat = pl.loads(open(cacheDataPath,"rb").read())
            #debugCache(dat)
        #exit()
    else:
        Log.info("No cache found for Stage-1")
    return dat

def cache_stage1(datList, save_cache=True, update_cache=False):

    if not save_cache:
        return

    if save_cache and update_cache:
        cacheMarkerList = glob.glob(STAGE1_MARKER + "*")
        for marker in cacheMarkerList:
            date = marker.split("_")[1]
            cacheDataPath = STAGE1_CACHE + date
            os.remove(marker)
            os.remove(cacheDataPath)
            Log.info(f"Removing cache {marker}")

    quote, df = datList[TWDAT][0]
    date = df.index[-1]
    datestr = str(date.date())
    dst = STAGE1_CACHE + datestr
    marker = STAGE1_MARKER + datestr
    Log.info(f"Caching data for Stage-1 to {dst} with {marker} from {quote} ...")

    with open(dst, "wb") as f:
        pl.dump(datList, f)

    with open(marker, "w") as f:
        pass

def check_stage2_cache(isCheck):
    return

def generate_training_data_1week(dataList):
    dataSize = [5, 224, 224]

def generate_training_data_1day(dataList):
    dataSize = [5, 224, 224]
    targetSize = [5, 224]

    cdq = CACHE_DATA_Query(dataList)
    STAGE2_DIR = 'train'
    STAGE2_DIR_1DAT = 'train/oneday'

    os.makedir(STAGE2_DIR_1DAT)
    for key, item in cdq.cq.symbolCategoryMap.items():
        symbol, cat = key, item
        path = STAGE2_DIR_1DAT + "/" + cat + "/" + symbol

        if not os.path.exists(path):
            os.makedir(path)

        ## skip between 1-2weeks 
        ## randomize crop date
        # get df of symbol
        # get start, end date of symbol
        # for loop start from begin
        #   date += random next date
        #   data = cdq next_data()
        #   normalizedData = normalize_data_strategy_one(data)
        #   img1.pl = {"dat": [5,224,224], "symbol" : "", "<cat>", "begin":[5,224], "target":[5,224]}
        #   dump(img1.pl, dst)


def generate_training_data_category(dataList):
    dataSize = [5, 224, 224]


def main():

    parentTw = "stock"
    parentDeps = "stock"

    ## stage 1 process nil data
    isCheckStage1Cache = True
    isSaveStage1Cache = True
    isUpdateStage1Cache = True
    isCheckStage2Cache = False

    datList = check_stage1_cache(isCheckStage1Cache)
    if datList == {}:
        twList = collect_tw_list(parentTw)
        depsList = collect_deps_list(parentDeps)

        depsList = process_nil_data(depsList, "DEPS")   
        twList = process_nil_data(twList, "TW")

        datList = {TWDAT: twList, DEPDAT: depsList}
        cache_stage1(datList, isSaveStage1Cache, isUpdateStage1Cache)


    ## stage 2 generate training data
    stage2Dat = check_stage2_cache(isCheckStage2Cache)
    if stage2Dat == {}:
        generate_training_data(datList)

    dataSize = [3, 224, 224] # open,close,vol

if __name__ == '__main__':
    main()






