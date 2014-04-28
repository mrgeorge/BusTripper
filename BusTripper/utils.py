import os
import re
import numpy as np
import pandas as pd
from datetime import datetime
from collections import Iterable
import sqlite3

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
    # numpy datetime casting is much faster than operations on datetime objects
    dtvals = dt.values.astype("datetime64[ns]") # strip tz if necessary
    return ((dtvals - dtvals.astype("datetime64[W]").astype("datetime64[ns]")).astype("timedelta64[s]")
            - npWeekdayOffset).astype("int64") % weekInSecs

def getDayHours(dt, tzHrOffset=0):
    """Return fraction hours since start of day"""
    # numpy datetime casting is much faster than operations on datetime objects
    dtvals = dt.values.astype("datetime64[ns]") # strip tz if necessary
    dayHrs = ((dtvals - dtvals.astype("datetime64[D]").astype("datetime64[ns]")).astype("timedelta64[s]")
           ).astype("float64") / 60. / 60. + tzHrOffset % 24.
    return dayHrs

def secsToMeters(secs, speed=10.):
    """Convert time in seconds to distance in meters given avg speed in m/s)"""
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


def getGTFSFileList(gtfsDir, agency="dbus"):
    """Returns list of filenames with format <gtfsDir><agency>_YYMMDD.db"""
    dateFmt = "[0-9]{6}" # regex for YYMMDD
    fileFmt = "^"+agency+"_"+dateFmt+".db$"

    gtfsFileList = []

    for ff in os.listdir(gtfsDir):
        if re.match(fileFmt, ff):
            gtfsFileList.append(gtfsDir+ff)

    return gtfsFileList

def buildServiceCalendar(gtfsDir, agency="dbus", exclude0000=True):
    """Concatenate all GTFS calendar tables into a dataframe

    Inputs:
        gtfsDir - path to directory with GTFS db files
        agency - string prefix for db filenames
    Returns:
        serviceCalendar - pandas dataframe with calendars, duplicates removed
    """
    gtfsFileList = getGTFSFileList(gtfsDir, agency=agency)

    # Copy calendar table from each GTFS db into :memory: db
    fc = sqlite3.connect(":memory:")
    for ii, gdbFile in enumerate(gtfsFileList):
        fc.execute("ATTACH DATABASE '{}' AS gdb".format(gdbFile))
        if ii==0:
            cur = fc.execute("CREATE TABLE main.calendar AS SELECT * FROM gdb.calendar")
        else:
            cur = fc.execute("INSERT INTO main.calendar SELECT * FROM gdb.calendar")
        fc.execute("DETACH DATABASE gdb")

    # Extract concatenated calendar into pandas dataframe
    query = "SELECT * FROM main.calendar"
    sc = pd.io.sql.frame_query(query, fc).drop_duplicates()
    fc.close()

    ### Clean up dataframe

    # Remove special service ID
    if exclude0000:
        sc = sc[sc['service_id'] != '0000']

    # Sort and re-index
    sc.sort(("service_id", "start_date", "end_date"), inplace=True)
    sc.reset_index(inplace=True, drop=True)

    # Convert start and end dates to datetime objects
    default = datetime(1970,1,1)
    sc['start_date'] = sc['start_date'].apply(lambda x:
        pd.datetools.dateutil_parse(x, default=default)[0])
    sc['end_date'] = sc['end_date'].apply(lambda x:
        pd.datetools.dateutil_parse(x, default=default)[0])

    return sc

def getServiceForDate(dt, serviceCalendar):
    """Search GTFS calendars for service info given a datetime object

    Input:
        dt - datetime object
        serviceCalendar - pd dataframe with calendar data
    Returns:
        serviceID
    """

    dayDict = {1:"monday", 2:"tuesday", 3:"wednesday", 4:"thursday",
               5:"friday", 6:"saturday", 7:"sunday"}
    match = ((serviceCalendar[dayDict[dt.isoweekday()]] == '1') &
             (serviceCalendar['start_date'] <= dt) &
             (serviceCalendar['end_date'] >= dt))
    return serviceCalendar[['service_id','start_date','end_date']][match]
