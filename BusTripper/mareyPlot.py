#pick a route (shape)
#get stops
#get stop locations
#linref to normalize lengths
#draw y axis

#get trips for shape (different blocks)
#get stop times for trips
#draw x axis

#connect trip stops with lines

#pick day
# get trips for shape and day
#get events for trips = true arrival/departure times
#overlay real trips on schedule

import numpy as np
import matplotlib.pyplot as plt

import shapely

import BusTripper

try:
    from pygtfs import gtfsDbManager
except ImportError:
    import os,sys
    path, filename = os.path.split(__file__)
    sys.path.append(os.path.abspath(os.path.join(path,"../assigner/")))
    from pygtfs import gtfsDbManager

gdb = gtfsDbManager.GtfsDbManager("../data/dbus_140221.db")
