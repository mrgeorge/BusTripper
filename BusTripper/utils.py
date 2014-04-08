import numpy as np
import pandas as pd
from datetime import datetime
from collections import Iterable

# np datetime64[W] weeks start on 1970/1/1 (Thursday)
# subtract to have weeks start on Monday
npWeekdayOffset = np.timedelta64(datetime(1970,1,1).weekday()+1, 'D')
weekInSecs = 7*24*60*60

def getDepotCoords():
    return np.array((43.3172, -1.96143)) # lat, lon

def printCurrentTime():
    print datetime.now().time().isoformat()

# vectorized function taken from Jacob (great circle approx)
def metersBetweenLatLonPair(lat1, lon1, lat2, lon2):
    """Copied from Jacob's version in pygtfs/util.py"""
    R = 6372795. # Radius of the earth in meters
    dLat = np.deg2rad(lat2-lat1)
    dLon = np.deg2rad(lon2-lon1)
    a = (np.sin(dLat/2.) * np.sin(dLat/2.) +
         np.cos(np.deg2rad(lat1)) * np.cos(np.deg2rad(lat2)) *
         np.sin(dLon/2.) * np.sin(dLon/2.))

    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.-a))
    d = R * c # Distance in meters
    return d

def getWeekday(dt):
    """Convert Unix time in ms to day of week

    Input:
        dt - pandas datetime series
    Return:
        pandas series with day of week (int64)
        (Monday == 0 ... Sunday == 6)
    """
    return dt.apply(pd.datetime.weekday)

def getWeekSecs(dt):
    # numpy datetime casting is much faster than operations on datetime objectsa
    dtvals = dt.values
    return ((dtvals - dtvals.astype("datetime64[W]")).astype("timedelta64[s]")
            - npWeekdayOffset).astype("int64") % weekInSecs

def secsToMeters(secs, speed=10.):
    """Convert time in seconds to distaince in meters given avg speed in m/s)"""
    return speed * secs.astype('float64')

def latlonToMeters(latitude, longitude):
    """Convert lat,lon separation from depot to meters"""
    refLat,refLon = getDepotCoords()
    latDist = metersBetweenLatLonPair(latitude,longitude,refLat,longitude)
    lonDist = metersBetweenLatLonPair(latitude,longitude,latitude,refLon)
    return (latDist, lonDist)

def parseTripID(tripID):
    """Split tripID str (or str array) into components"""
    if isinstance(tripID, Iterable):
        serviceID = np.array([trip[0:4] if trip is not None else None
                            for trip in tripID])
        routeID = np.array([trip[4:8] if trip is not None else None
                            for trip in tripID])
        blockID = np.array([trip[8:12] if trip is not None else None
                            for trip in tripID])
        departureTime = np.array([trip[12:16] if trip is not None else None
                            for trip in tripID])
        direction = np.array([trip[16:] if trip is not None else None
                            for trip in tripID])
    elif tripID is not None:
        serviceID = tripID[0:4]
        routeID = tripID[4:8]
        blockID = tripID[8:12]
        departureTime = tripID[12:16]
        direction = tripID[16:]
    else:
        serviceID = routeID = blockID = departureTime = direction = None
    return (serviceID, routeID, blockID, departureTime, direction)
