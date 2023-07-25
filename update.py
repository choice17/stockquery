import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import yfinance as yf
import os
from datetime import datetime as DT
import glob
import pickle as pl
import pandas as pd
from tqdm import tqdm as Pbar
from utils import *

STOCK_GLOB_KEY = "/*/*.pickle"
DELIMITER = "\\"

HEADER = {'USER-AGENT': "Mozilla/5.0"}
FMT = "https://tw.stock.yahoo.com/class-quote?sectorId=%d&exchange=TAI"

SECTORS ={1:"cement",
        2:"food",
        3:"plastic",
        4:"textile",
        6:"electric",
        7:"electricalcable",
        37:"chemistry",
        38:"biotech",
        9:"glass",
        10:"paper",
        11:"steel",
        12:"rubber",
        13:"motor",
        40:"semiconductor",
        41:"computer",
        42:"photoelectric",
        43:"communication",
        44:"electronicparts",
        45:"electricappliance",
        46:"itservice",
        47:"otherelectric",
        19:"construction",
        20:"shipping",
        21:"sightseeing",
        22:"finance",
        24:"departmentstore",
        39:"gasoline",
        }

def log(*args):
    print("[INFO] ", *args)

def query(symbol, period="99y", start="", end=""):
    df = yf.Ticker(symbol)
    d = None
    if period != "1d":
        d = df.history(period=period)
    else:
        d = df.history(period="1d", start=start, end=end)
    return d

def dump(dfData, path):
    dfData.to_pickle(path)

def collect_indexes_from_dir(fromDir):
    indexes = glob.glob(fromDir + STOCK_GLOB_KEY)
    indexDict = {}
    indexList = []
    for i in indexes:
        splits = i.split(DELIMITER)
        #print(1, i, splits)
        #exit()
        path, cat, symbol = '/'.join(splits), splits[-2], splits[-1][:-7]
        item = [cat, path, symbol]
        #print(2, item)
        indexList.append(item)
    return indexList    

# todo

def update_index_singleprocess(args):
    path, cat, symbol, date = args
    yyyy, mm, dd = int(date[0]), int(date[1]), int(date[2])
    reqDate = DT(yyyy, mm, dd)
    dateNum = DT(yyyy, mm, dd).timestamp()
    dateLow = DT(yyyy, mm, dd, 0, 0, 1).timestamp()
    dateHigh = DT(yyyy, mm, dd, 23, 59, 59).timestamp()
    checkDate = True
    log(f"Expected update time {date}")
    listempty = False

    df = None
    concatDf = None
    start = end = 0
    with open(path, "rb") as f:
        raw = f.read()
        if raw != b'':
            if listempty:
                return
            df = pl.loads(raw)
        else:
            if listempty:
                log(f"Empty {path}!")
                return
    
    if df is not None:
        start = df.index[0].to_pydatetime()
        end = df.index[-1].to_pydatetime()
        #pbar.set_description(f"Processing {symbol} {start} - {end} update {date}\n")
        startNum = start.timestamp()
        endNum = end.timestamp()
        if endNum > dateHigh:
            log(f"Date of {symbol} from {path} exceed request date {end} vs expected {reqDate}")
            return
        if endNum > dateLow:
            return
        queryStartStr = str(DT.fromtimestamp(endNum + 86400)).split(" ")[0]
        queryEndStr = "-".join(date)
        log(f"Query {symbol} from {queryStartStr} to {queryEndStr}")
        queryDf = query(symbol, period="1d", start=queryStartStr, end=queryEndStr)
        concatDf = pd.concat([df, queryDf])
        #with open(path, "wb") as f:
        #    pl.dumps(concatDf, f)
        concatDf.to_pickle(path)
        log(f"[1] Succeed to collect {symbol}, data since {df.index[0]} to {end} and update to {queryEndStr}")
        #log(f"{ind}/{totalInd}: Dumping {symbol} to {path}")
    else:
        df = query(symbol, period="max")
        #with open(path, "wb") as f:
        #    pl.dumps(concatDf, f)
        df.to_pickle(path)
        log(f"[2] Succeed to collect {symbol}, data since {df.index[0]} to {df.index[-1]}")
        #log(f"{ind}/{totalInd}: Dumping {symbol} to {path}")


def update_indexes_multiprocess(indexes, date, poolSize):

    indexes = [i.append(date) for i in indexes]
    run_multiprocess(update_index_singleprocess, indexes, poolSize)
    return 0

def update_indexes_data(fromDir, date=[], listempty=False, multiprocess=False, poolSize=4):
    '''
    @input fromDir(string)
    @input date(string in list [yyyy,mm,dd])
    '''
    indexes = collect_indexes_from_dir(fromDir)
    log(f"Collected {len(indexes)} indexes from {fromDir}.")
    dateNum = 0
    checkDate = False

    if multiprocess:
        log(f"Running multiprocess update index! on pool size = {poolSize}")
        return update_indexes_multiprocess(indexes, date, poolSize)

    if date != [] and len(date) == 3:
        yyyy, mm, dd = int(date[0]), int(date[1]), int(date[2])
        reqDate = DT(yyyy, mm, dd)
        dateNum = DT(yyyy, mm, dd).timestamp()
        dateLow = DT(yyyy, mm, dd, 0, 0, 1).timestamp()
        dateHigh = DT(yyyy, mm, dd, 23, 59, 59).timestamp()
        checkDate = True
        log(f"Expected update time {date}")
    else:
        dateLow = DT.now().timestamp() + 1
        dateHigh = DT.now().timestamp() + 86399
        checkDate = True
        log(f"Expected update to current time {dateHigh}")

    totalInd = len(indexes)
    #pbar = Pbar(indexes, desc="Updating - index")
    ind = 0
    for dat in indexes:
        ind += 1
        cat, path, symbol = dat
        df = None
        concatDf = None
        start = end = 0
        with open(path, "rb") as f:
            raw = f.read()
            if raw != b'':
                if listempty:
                    continue
                df = pl.loads(raw)
            else:
                if listempty:
                    log(f"{ind}/{totalInd} Empty {path}!")
                    continue
        
        if df is not None:
            start = df.index[0].to_pydatetime()
            end = df.index[-1].to_pydatetime()
            #pbar.set_description(f"Processing {symbol} {start} - {end} update {date}\n")
            startNum = start.timestamp()
            endNum = end.timestamp()
            if endNum >= dateHigh:
                log(f"Date of {symbol} from {path} exceed request date {end} vs expected {reqDate}")
                continue
            if endNum >= dateLow:
                continue
            queryStartStr = str(DT.fromtimestamp(endNum + 86400)).split(" ")[0]
            queryEndStr = "-".join(date)
            log(f"Query {symbol} from {queryStartStr} to {queryEndStr}")
            queryDf = query(symbol, period="1d", start=queryStartStr, end=queryEndStr)
            concatDf = pd.concat([df, queryDf])
            #with open(path, "wb") as f:
            #    pl.dumps(concatDf, f)
            concatDf.to_pickle(path)
            log(f"[1] Succeed to collect {symbol}, data since {df.index[0]} to {end} and update to {queryEndStr}")
            log(f"{ind}/{totalInd}: Dumping {symbol} to {path}")
        else:
            df = query(symbol, period="max")
            #with open(path, "wb") as f:
            #    pl.dumps(concatDf, f)
            df.to_pickle(path)
            log(f"[2] Succeed to collect {symbol}, data since {df.index[0]} to {df.index[-1]}")
            log(f"{ind}/{totalInd}: Dumping {symbol} to {path}")



DIR= os.getcwd() + "/stock"
INDEX_MAP_FILE = 'index_map_tw.txt'
FEAT_INDEX_FILE = "quotes.txt"

updateTime = str(DT.now()).split(" ")[0].split("-")
fromDir = "stock"
listempty = False
multiprocess = False
print(updateTime)

update_indexes_data(fromDir, updateTime, listempty=listempty, multiprocess=multiprocess)