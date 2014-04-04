import numpy as np
import datetime
from collections import Iterable

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

# two versions of datetime functions follow
# first uses datetime objects which aren't vectorized so are slow
# second uses numpy datetime and timedelta data types which are vectorized and faster
#   but they aren't consistent with timezone or which day starts the week
# test agreement, apply necessary shifts, and delete whichever approach fails

def unixmsToDatetime(unixms):
    """Convert Unix time in ms to datetime object

    Input: unixms - single long int or array of long ints
    Returns: datetime object or object array of same length as input
    """
    try:
        return [datetime.datetime.fromtimestamp(tt/1000.) for tt in unixms]
    except TypeError:
        return datetime.datetime.fromtimestamp(unixms/1000.)

def unixmsToWeekday(unixms):
    """Convert Unix time in ms to day of week Mon=0, Sun=6"""
    dts = unixmsToDatetime(unixms)
    try:
        return [dt.weekday() for dt in dts]
    except TypeError:
        return dts.weekday()

def unixmsToDaySecs(unixms):
    """Convert Unix time in ms to seconds since midnight

    e.g., 12:01:00am = 60, 1:00:00am = 3600
    """
    dts = unixmsToDatetime(unixms)
    try:
        return [(dt - dt.replace(hour=0, minute=0, second=0)).seconds for dt in dts]
    except TypeError:
        return (dts - dts.replace(hour=0, minute=0, second=0)).seconds

def unixmsToWeekSecs(unixms):
    """Convert Unix time in ms to seconds since start of week (Mon 12am)

    e.g., 12:01:00am Monday = 60, 1:00:00am Monday = 3600
    """
    return np.array(unixmsToWeekday(unixms))*60.*60.*24. + np.array(unixmsToDaySecs(unixms))

def unixmsToDatetime2(unixms):
    return np.array(unixms).view(dtype = "datetime64[ms]")

def unixmsToWeekday2(unixms):
    """numpy weeks start on Thursday apparently"""
    dts = unixmsToDatetime2(unixms)
    return (dts - dts.astype('datetime64[W]').astype('datetime64[s]')).astype('timedelta64[D]')

def unixmsToDaySecs2(unixms):
    """timezone probably off"""
    dts = unixmsToDatetime2(unixms)
    return (dts - dts.astype('datetime64[D]').astype('datetime64[s]')).astype('timedelta64[s]')

def unixmsToWeekSecs2(unixms):
    dts = unixms.view(dtype="datetime64[ms]")
    return (dts - dts.astype('datetime64[W]').astype('datetime64[s]')).astype('timedelta64[s]')

def secsToMeters(secs, speed=10.):
    """Convert time in seconds to distaince in meters given avg speed in m/s)"""
    return speed * secs.astype('float64')

# three versions of latlonToMeters follow:
# first uses accurate ellipsoidal model, but is slow
# second uses less accurate (~0.5%) great circle model, but still slow
# third uses vectorized function taken from Jacob, should give same value as great circle
# test for accuracy and speed and then remove the failing ones

def latlonToMeters(latitude, longitude):
    """Convert lat,lon separation from depot to meters"""
    if not hasGeopy:
        raise ImportError("geopy")

    refLat,refLon = getDepotCoords()
    latDist = np.array([geopy.distance.distance((lat,lon), (refLat,lon)).meters
                        for lat,lon in zip(latitude,longitude)])
    lonDist = np.array([geopy.distance.distance((lat,lon), (lat,refLon)).meters
                        for lat,lon in zip(latitude,longitude)])
    return (latDist, lonDist)

def latlonToMeters2(latitude, longitude):
    """Convert lat,lon separation from depot to meters"""
    if not hasGeopy:
        raise ImportError("geopy")

    refLat,refLon = getDepotCoords()
    latDist = np.array([geopy.distance.great_circle((lat,lon), (refLat,lon)).meters
                        for lat,lon in zip(latitude,longitude)])
    lonDist = np.array([geopy.distance.great_circle((lat,lon), (lat,refLon)).meters
                        for lat,lon in zip(latitude,longitude)])
    return (latDist, lonDist)

def latlonToMeters3(latitude, longitude):
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
    elif trip is not None:
        serviceID = trip[0:4]
        routeID = trip[4:8]
        blockID = trip[8:12]
        departureTime = trip[12:16]
        direction = trip[16:]
    else:
        serviceID = routeID = blockID = departureTime = direction = None
    return (serviceID, routeID, blockID, departureTime, direction)
