import pandas as pd
import numpy as np
import pickle as pl

def normalize(data):
        normalized =[]
        return normalized

def expand_nil(df, method='repeat'):
        return df

def load_pickle(fname):
        df = []
        return df

def collect_deps_list(parent):
        depsList = []
        # load_pickle by trees
        return depsList

def collect_tw_list(parent):
        twList = []
        # load_pickle by trees
        return twList

def convert_time(df, dateOrig = 'tw'):
        return df

def process_data(dfList):

        # convert date time to same time zone - default tw
        for df in defList:
                convert_time(df)

        # expand nil value - default with repeat
        for df in dfList:
                expand_nil(df)

        return dfList

def main():

        parentTw = ""
        parentDeps = ""

        twList = collect_tw_list(parentTw)
        depsList = collect_deps_list(parentDeps)

        twList = process_data(twList)
        depsList = process_data(depsList)


        dataSize = [3, 224, 224] # open,close,vol







