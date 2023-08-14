import sys
import os
sys.path.append(os.getcwd())
import datetime 
from utils import RandomPeriodGenerator, CacheDataQuery, DepsQuery, Top5Query, CacheSingleQuery, Day
import pickle as pl
from log import *
import traceback
import numpy as np
from define import *

CACHE_TEST_FILENAME = "cache/.stage1Dat_2023-07-18"
TEST_SYMBOL = "TSM"

class StageOneCacheGenerator(object):
    def __init__(self):
        pass

class StageTwoCacheGenerator(object):
    def __init__(self):
        pass

class Unit_Test(object):
    def __init__(self):
        self.utList = []

    def add(self, func):
        self.utList.append(func)

    def run(self):
        ret = []
        total = 0
        success = 0
        summ = len(self.utList)
        for i, ut in enumerate(self.utList):
            total += 1
            retval = 1
            Log.info(f"Running {i+1}/{summ} {ut.__name__}")
            errorMsg = ""
            try:
            	retval = ut()
            	Log.info(f"Complete {i+1}/{summ} {ut.__name__}")
            except:
            	Log.info(f"Fail {i+1}/{summ} {ut.__name__}")
            	errorMsg = traceback.format_exc()
            if (retval == 0):
                success += 1
            ret.append([i+1, ut.__name__, retval, errorMsg])

        Log.info(f"Total success rate : {success}/{total}")
        for r in ret:
            index, name, retval, errorMsg = r
            if (retval):
                Log.info(f"Failed UT({index}) : {name} Error Msg: {errorMsg}")

UT = Unit_Test()

@UT.add
def ut_RandomPeriodGenerator():

    start = datetime.datetime(2023, 2, 5).timestamp()
    end = datetime.datetime(2023, 7, 20).timestamp()
    includeStart = False
    gen = RandomPeriodGenerator(start, end, includeStart = includeStart)

    dateList = []
    for date in gen():
        dateList.append(datetime.datetime.fromtimestamp(date))

    for date in dateList:
        print(f"ut_RandomPeriodGenerator : {date}")
    return 0

@UT.add
def ut_CacheSingleQuery():
    cache = CACHE_TEST_FILENAME
    dat = pl.loads(open(cache,"rb").read())
    csq = CacheSingleQuery(dat)
    _start, _end = [2023,5,7],[2023,5,28]
    start, end = Day.get_date_period(_start, _end)
    #dat = csq.query_single_symbol_df_time(TEST_SYMBOL, start, end)
    duration = 28 - 7 + 1
    dat = csq.query_single_symbol_df_period(TEST_SYMBOL, start, duration)
    print(f"ut_CacheSingleQuery : {dat}")
    return 0

@UT.add
def ut_DepsQuery():
    cache = CACHE_TEST_FILENAME
    dat = pl.loads(open(cache,"rb").read())
    cq = CacheSingleQuery(dat)
    dq = DepsQuery(dat, cq)

    _start, _end = [2023,5,7],[2023,5,28]
    start, end = Day.get_date_period(_start, _end)
    duration = 28 - 7 + 1
    img = np.zeros(IMG_SIZE, dtype=np.float32)
    target = np.zeros((DEPS_DEPTH, STOCK_DEPTH), dtype=np.float32)
    dq.fill_one_duration_target_one_day(TEST_SYMBOL, start, duration, img, target)
    print(f"ut_DepsQuery(img) : {img}")
    print(f"ut_DepsQuery(target) : {target}")
    return 0

@UT.add
def ut_Top5Query():
    cache = CACHE_TEST_FILENAME
    dat = pl.loads(open(cache,"rb").read())
    cq = CacheSingleQuery(dat)
    t5q = Top5Query(dat, cq)
    return 0

if __name__ == "__main__":
    UT.run()



