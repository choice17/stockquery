from datetime import datetime as DT
#from multiprocessing import Pool, Queue, Manager
from threading import Thread
import Queue
import functools
import yfinance as yf
from log import *
from collections import Queue, defaultdict
from define import *
import pickle as pl

class _Day(object):

	def get_date_begin_num(self, stime):
		d = DT.fromtimestamp(stime)
		return self.get_date_begin_datetime(d)

	def get_date_begin_datetime(self, d):
	    number = DT(d.year,d.month, d.day).timestamp()
		return number

	def get_date_end_num(self, stime):
		d = DT.fromtimestamp(stime)
		return self.get_date_begin_datetime(d)

	def get_date_end_datetime(self, d):
	    number = DT(d.year,d.month, d.day, 23, 59, 59).timestamp()
		return number

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
    q = Queue()
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

class CACHE_Single_Query(object):

	def __init__(self, cache):
		self.dat = cache

	def build_index(self):
		self.indexMap = {}

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
			splits = path.split(DELIMITER)
			cat, symbol = splits[1], splits[2][:-7]
			self.indexMap[symbol] = [DEPDAT, ind, cat, path]
			ind += 1


	def query_single_symbol_df(self, symbol):
		try:
			dataSection, ind, _, _ = self.indexMap[symbol]
		except KeyError:
			log(f"Cannot find {symbol} in cache!")
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

		return df

class TOP6_Query(object):

	def __init__(self, cacheFile, cacheQueryer, datSize = TOP_SIZE, imgSize = IMG_SIZE):
		log("Loading top6 info...")
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
			splits = l.splits(",")
			cat, symbol = l[0], l[1][:-7]
			self.top6Dict[cat].append(symbol)
			self.symbolCategoryMap[symbol] = cat

	def query_one_top6(self, symbol, start, end, img)
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
		top6 = 6
		for ind in range(top6):
			sym = symbolList[ind]
			#
			pass
			

class DEPS_Query(object):

	def __init__(self, cacheFile, cacheQueryer, datSize = BOTTOM_SIZE, imgSize = imgSize = IMG_SIZE):
		log(f"Loading deps info ...")
		self.dat = cacheFile
		self.cq = cacheQueryer

	def query_one_deps(self, start, end, img):
		pass


class CACHE_DATA_Query(object):
	pass
