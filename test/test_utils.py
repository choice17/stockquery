import sys
import os
sys.path.append(os.getcwd())
import datetime 
from utils import RandomPeriodGenerator, CacheDataQuery, DepsQuery, Top5Query, CacheSingleQuery
import pickle as pl
from log import *

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
        for ut in self.utList:
            total += 1
            Log.info(f"Running {ut.__name__}")
            retval = ut()
            if (retval == 0):
                success += 1
            ret.append([ut.__name__, retval])

        Log.info(f"Total success rate : {success}/{total}")
        for r in ret:
            name, retval = r
            if (retval):
                Log.info(f"Failed UT : {name}")

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
    cache = "cache/.stage1Dat_2023-07-18"
    dat = pl.loads(open(cache,"rb").read())
    csq = CacheSingleQuery(dat)
    return 0

@UT.add
def ut_DepsQuery():
    cache = "cache/.stage1Dat_2023-07-18"
    dat = pl.loads(open(cache,"rb").read())
    cq = CacheSingleQuery(dat)
    dq = DepsQuery(dat, cq)
    return 0

@UT.add
def ut_Top5Query():
    cache = "cache/.stage1Dat_2023-07-18"
    dat = pl.loads(open(cache,"rb").read())
    cq = CacheSingleQuery(dat)
    t5q = Top5Query(dat, cq)
    return 0

if __name__ == "__main__":
    UT.run()



