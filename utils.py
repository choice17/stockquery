from datetime import datetime as DT
#from multiprocessing import Pool, Queue, Manager
from threading import Thread
import functools
import yfinance as yf
from log import *
import queue
from collections import defaultdict
from define import *
import pickle as pl
import random
import re

random.seed(2023)

class _Day(object):

    def get_date_begin_num(self, stime):
        d = DT.fromtimestamp(stime)
        return self.get_date_begin_datetime(d)

    def get_date_begin_datetime(self, d):
        num = DT(d.year,d.month, d.day).timestamp()
        return num

    def get_date_end_num(self, stime):
        d = DT.fromtimestamp(stime)
        return self.get_date_begin_datetime(d)

    def get_date_end_datetime(self, d):
        num = DT(d.year,d.month, d.day, 23, 59, 59).timestamp()
        return num

    def get_date_period_num(self, stime):
        d = DT.fromtimestamp(stime)
        period = self.get_date_period_datetime(d)
        return period

    def get_date_period_datetime(self, d):
        begin = DT(d.year,d.month, d.day).timestamp()
        end = DT(d.year,d.month, d.day, 23, 59, 59).timestamp()
        return begin, end

Day = _Day()

def query_symbol(symbol, period="99y", start="", end=""):
    df = yf.Ticker(symbol)
    d = None
    if period != "1d":
        d = df.history(period=period)
    else:
        d = df.history(period="1d", start=start, end=end)
    return d


def current_date_str():
    return str(DT.now().timestamp()).split(" ")[0]


def current_date_time(_time="low"):
    currentDate = ""
    if _time == "low":
        return DT.now().timestamp()+1
    elif _time == "high":
        return DT.now().timestamp()+86399

class func_queue(object):
    def __init__(self, func, totalRun):
        self.func = func
        #self.q = queue
        self.tr = totalRun
        try:
            functools.update_wrapper(self, target)
        except:
            pass

    def __call__(self, *args):
        self.f(args)
        #self.q.put(1)
        print(f"[INFO] run Succeed on {args}")
        return 0

# def func_queue(queue, func, totalRun):
#     def run_func_queue(*args):    
#         func(args)
#         queue.put(1)
#         print(f"[INFO] {queue.qsize()}/{totalRun} run Succeed on {args}")
#         return 0
#     return run_func_queue

def run_multiprocess(function, listArgs, poolSize=4):
    #m = Manager()
    q = queue.Queue()
    totalRun = len(listArgs)
    pool = Pool(poolSize)
    qfunc = func_queue(function, totalRun)
    pool.map(qfunc, listArgs)
    pool.close()
    pool.join()
    return 0

def run_multiThreading(function, listArgs, poolSize=4):
    #m = Manager()
    q = Queue()
    totalRun = len(listArgs)
    pool = Pool(poolSize)
    qfunc = func_queue(function, totalRun)
    pool.map(qfunc, listArgs)
    pool.close()
    pool.join()
    return 0


def closest_value_index(input_list, input_value):
    arr = np.asarray(input_list)
    i = (np.abs(arr - input_value)).argmin()
    return arr[i], i

class CacheSingleQuery(object):

    def __init__(self, cache):
        self.dat = cache
        self.build_index()

    def build_index(self):
        self.indexMap = {}
        self.depsList = []

        ind = 0
        for pair in self.dat[TWDAT]:
            path, _ = pair
            splits = path.split(DELIMITER)
            cat, symbol = splits[1], splits[2][:-7]
            self.indexMap[symbol] = [TWDAT, ind, cat, path]
            ind += 1

        ind = 0
        for pair in self.dat[DEPDAT]:
            path, _ = pair
            splits = re.split(f"/|\\\\", path)
            cat, symbol = splits[1], splits[2][:-7]
            self.indexMap[symbol] = [DEPDAT, ind, cat, path]
            self.depsList.append(symbol)
            ind += 1


    def query_single_symbol_df(self, symbol):
        try:
            dataSection, ind, _, _ = self.indexMap[symbol]
        except KeyError:
            Log.info(f"Cannot find {symbol} in cache!")
            exit(1)

        path, df =  self.dat[dataSection][ind]
        return df

    def query_single_symbol_df_time(self, symbol, start, end):
        try:
            dataSection, ind, _, _ = self.indexMap[symbol]
        except KeyError:
            log(f"Cannot find {symbol} in cache!")
            exit(1)

        path, df =  self.dat[dataSection][ind]
        
        timeInd = df.index
        _, startInd = closest_value_index(timeInd, start)
        _, endInd = closest_value_index(timeInd, end)

        dfOut = df.iloc[startInd:endInd]
        return dfOut

    def query_single_symbol_df_time_with_one_day_target(self, symbol, start, end):
        try:
            dataSection, ind, _, _ = self.indexMap[symbol]
        except KeyError:
            log(f"Cannot find {symbol} in cache!")
            exit(1)

        path, df =  self.dat[dataSection][ind]
        
        timeInd = df.index
        _, startInd = closest_value_index(timeInd, start)
        _, endInd = closest_value_index(timeInd, end)

        dfOut = df.iloc[startInd:endInd]
        tarOut = df.iloc[endInd+1]
        return dfOut, tarOut

class Top5Query(object):

    def __init__(self, cacheFile, cacheQueryer, datSize = TOP_SIZE, imgSize = IMG_SIZE):
        Log.info("Loading top6 info...")
        self.dat = cacheFile
        self.top6Dict = defaultdict(lambda : [])
        self.symbolCategoryMap = {}
        self.imgSize = imgSize
        self.datSize = datSize
        self.load_top6_file()
        self.cq = cacheQueryer

    def load_top6_file(self):
        f = open(TOP6_FILE, "r").readlines()
        for l in f:
            l = l.rstrip()
            splits = l.split(",")
            cat, symbol = l[0], l[1][:-7]
            self.top6Dict[cat].append(symbol)
            self.symbolCategoryMap[symbol] = cat

    def fill_one_period_top5(self, symbol, start, end, img):
        """
        @desc : query one set of symbol data with dataSize by start and end time in python datetime number
        @symbol[in] : string 
        @start[in] : linux datetime number request period start time
        @end[in] : linux datetime number request period end time
        @img[out] : DEPTH x QUERY_SIZE x DATE_DEPTH
        """
        assert img.shape == IMG_SIZE
        cat = self.symbolCategoryMap[symbol]
        symbolList = [symbol] + [i for i in self.top6Dict[cat] if i != symbol]
        top5 = 5
        for ind in range(top5):
            sym = symbolList[ind]
            dat = self.cq.query_single_symbol_df_time(sym, start, end)
            img[:, ind, :] = np.array(dat)
        return
            

    def fill_one_period_top5_target_one_day(self, symbol, start, end, img, target):
        """
        @desc : query one set of symbol data with dataSize by start and end time in python datetime number
        @symbol[in] : string 
        @start[in] : linux datetime number request period start time
        @end[in] : linux datetime number request period end time
        @img[out] : DEPTH x QUERY_SIZE x DATE_DEPTH
        @target[out] : DEPTH X QUERY_SIZE
        """
        assert img.shape == IMG_SIZE
        cat = self.symbolCategoryMap[symbol]
        symbolList = [symbol] + [i for i in self.top6Dict[cat] if i != symbol]
        top5 = 5
        for ind in range(top5):
            sym = symbolList[ind]
            dat, tar = self.cq.query_single_symbol_df_time_with_one_day_target(sym, start, end)
            npDat = np.array(dat)
            if w < STOCK_DEPTH:
                img[:, ind, STOCK_DEPTH-w:] = npDat
            else:
                img[:, ind, :] = np.array(dat)
            target[:, ind] = np.array(tar)
        return

    def fill_one_period_top5_target_one_week(self, symbol, start, end, img, target):
        pass

    def fill_one_period_top_target_category(self, symbol, start, end, img, target):
        pass

class DepsQuery(object):

    def __init__(self, cacheFile, cacheQueryer, datSize = BOTTOM_SIZE, imgSize = IMG_SIZE):
        Log.info(f"Loading deps info ...")
        self.dat = cacheFile
        self.cq = cacheQueryer

    def fill_one_period_deps(self, start, end, img):
        assert img.shape == IMG_SIZE, "image shape is not expected"
        assert len(self.cq.depsList) == DEPS_DEPTH, "deps shape is not expected"
        ind = 5
        for sym in self.cq.depsList:
            dat = self.cq.query_single_symbol_df_time(sym, start, end)
            npDat = np.array(dat)
            w = npDat.shape[1]
            if w < STOCK_DEPTH:
                img[:, ind, STOCK_DEPTH-w:] = npDat
            else:
                img[:, ind, :] = npDat
            ind += 1

    def fill_one_period_period_target_one_day(self, symbol, start, end, img, target):
        assert img.shape == IMG_SIZE, "image shape is not expected"
        assert len(self.cq.depsList) == DEPS_DEPTH, "deps shape is not expected"
        ind = 5
        for sym in self.cq.depsList:
            dat, tar = self.cq.query_single_symbol_df_time_with_one_day_target(sym, start, end)
            npDat = np.array(dat)
            tarDat = np.array(tar)
            w = npDat.shape[1]
            if w < STOCK_DEPTH:
                img[:, ind, STOCK_DEPTH-w:] = npDat
            else:
                img[:, ind, :] = npDat
                target[:, ind] = np.array(tar)
            ind += 1

    def fill_one_period_period_target_one_week(self, symbol, start, end, img, target):
        pass

    def fill_one_period_period_target_category(self, symbol, start, end, img, target):
        pass

class CacheDataQuery(object):

    def __init__(self, cacheData):
        self.cache = cacheData
        self.cq = CacheSingleQuery(cacheData)
        self.top5Query = TOP5_Query(cacheData, self.cq)
        self.depsQuery = DEPS_Query(cacheData, self.cq)

    def get_symbol_start_end_time(symbol):
    	symbolData = self.cache

    def fill_one_period(self, symbol, start, end, img):
        self.top5Query.fill_one_period_top5(symbol, start, end, img)
        self.depsQuery.fill_one_period_deps(symbol, start, end, img)
        return

    def fill_one_period_target_one_day(self, symbol, start, end, img, target):
        self.top5Query.fill_one_period_top5_target_one_day(symbol, start, end, img, target)
        self.depsQuery.fill_one_period_deps_target_one_day(symbol, start, end, img, target)
        return

    def fill_one_period_target_one_week(self, symbol, start, end, img, target):
        self.top5Query.fill_one_period_top5_target_one_week(symbol, start, end, img, target)
        self.depsQuery.fill_one_period_deps_target_one_week(symbol, start, end, img, target)
        return

    def fill_one_period_target_category(self, symbol, start, end, img, target):
        self.top5Query.fill_one_period_top5_target_category(symbol, start, end, img, target)
        self.depsQuery.fill_one_period_deps_target_category(symbol, start, end, img, target)
        return


class RandomPeriodGenerator(object):

    def __init__(self, start, end, skipPeriod=7, randomRate=0.5, includeStart=False):
        """
        @desc return generator of random period date the skip period = skipPeriod + (int) randomRate * (random()[0-1] - 0.5) * skipPeriod
        @start[in] period start date
        @end[in] period end date
        @skipPeriod[in] unit : days, control the skip period default 7 days
        @randomRate[in] range : [0 - randomRate] 
        """
        self.start = start
        self.end = end
        self.skip = skipPeriod
        self.rate = randomRate
        self.includeStart = includeStart
        self.DAYNUM = 86400

    def getRandom(self):
        randomness = int((random.random() - self.rate) * self.skip)
        #print(f"DBG100 : getRandom: {randomness}")
        return randomness

    def getRandomDate(self):
        period = self.skip + self.getRandom()
        return period

    def __call__(self):
        start = self.start
        end = self.end
        if self.includeStart:
            start += self.getRandom() * self.DAYNUM

        begin = 0
        while start < end:
            if begin == 0:
                begin = 1
                yield start
            start += self.getRandomDate() * self.DAYNUM
            if start > end:
            	break
            yield start
            



