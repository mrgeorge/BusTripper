import numpy as np
import datetime

def getDepotCoords():
    return np.array((43.3172, -1.96143)) # lat, lon

def printCurrentTime():
    print datetime.datetime.now().time().isoformat()

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
