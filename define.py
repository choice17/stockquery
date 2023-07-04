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
DATEORIG = DT.fromtimestamp(0).timestamp()

