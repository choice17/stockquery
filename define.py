# Defines
import os
from datetime import datetime as DT
from collections import defaultdict

MAIN = "gui/main.ui"
STOCKDIR = "stock"
STOCKCATEGORY = {i:[] for i in os.listdir(STOCKDIR) if os.path.isdir(STOCKDIR+"/"+i)}
STOCKCACHE = defaultdict(lambda : None)
ISSTOCKCACHE = True
TIMEZONE = 'Asia/Taipei'
ONEDAYSEC = 86400

# 2 hr
ENDTIME_TOL = 3600*2 
# DATEORIG = DT.fromtimestamp(0).timestamp()

STAGE1_MARKER = ".stage1_"
STAGE1_CACHE = ".stage1Dat_"

TWDAT = "twList"
DEPDAT = "depsList"

STAGE2_DIR = 'train'
STAGE2_DIR_1WEEK = 'train/oneweek'
STAGE2_DIR_1DAT = 'train/oneday'
STAGE2_DIR_CAT = 'train/category'

TOP6_FILE = ".top6.txt"

DEPTH = 5 # open close high low volume
DATE_DEPTH = 224 # 224 days
STOCK_DEPTH = 224 # 224 symbol per image
QUERY_DEPTH = 5 # query
DEPS_DEPTH = 219 # dependencies

IMG_SIZE = [DEPTH, STOCK_DEPTH, DATE_DEPTH]
TOP_SIZE = [DEPTH, QUERY_DEPTH, DATE_DEPTH]
BOTTOM_SIZE = [DEPTH, DEPS_DEPTH, DATE_DEPTH]


## stage one data format
## { TWDAT : 
##    [[path, dataframe [open, close, high, low, volume] ]
##      ... ]    
##   DEPDAT :
##    [[path, dataframe [open, close, high, low, volume] ]
##      ... ]    
## }
