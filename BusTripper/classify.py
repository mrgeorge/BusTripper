import numpy as np
import datetime
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier

try:
    import geopy
    hasGeopy=True
except ImportError:
    hasGeopy=False
    
import eventsDBManager
import utils

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
    return unixmsToWeekday(unixms)*60.*60.*24. + unixmsToDaySecs

def secsToMeters(secs, speed=10.):
    """Convert time in seconds to distaince in meters given avg speed in m/s)"""
    return speed * secs

def latlonToMeters(latitude, longitude):
    if not hasGeopy:
        raise ImportError(geopy)

    refLat,refLon = utils.getDepotCoords()
    latDist = np.array([geopy.distance.distance((lat,lon), (refLat,lon)).meters
                        for lat,lon in zip(latitude,longitude)])
    lonDist = np.array([geopy.distance.distance((lat,lon), (lat,refLon)).meters
                        for lat,lon in zip(latitude,longitude)])
    return latDist, lonDist

def getData(dbFileLoc, startDate, endDate):

    startDate = '2013-12-01'
    endDate = '2013-12-31'
    cols = ("time","longitude","latitude","trip_id")
    db = eventsDBManager.EventsDB(dbFileLoc)
    rec = db.selectData(tableName="rlev",date=(startDate, endDate), cols=cols)

    return rec

def preprocess(rec):

    # convert trip_id strings to unique integers for classification
    le = LabelEncoder()
    le.fit(rec['trip_id'])
    yTrain = le.transform(rec['trip_id'])

    # constuct design matrix
    latDist, lonDist = latlonToMeters(rec['latitude'], rec['longitude'])
    xTrain = np.array([unixmsToWeekSecs(rec['time']), latDist, longDist])

    return (xTrain, yTrain, le)

def classify(dbFileLoc):
    print "reading training data"
    trainingData = getData(dbFileLoc, '2013-12-01', '2013-12-31')
    print "reading test data"
    testData = getData(dbFileLoc, '2014-01-01', '2014-01-07')

    print "preprocessing training data"
    xTrain, yTrain, le = preprocess(trainingData)
    print "preprocessing test data"
    xTest, yTest, = preprocess(testData)

    print "training KNN model"
    knn = KNeighborsClassifier()
    knn.fit(xTrain, yTrain)

    print "predicting on test data"
    yHat = knn.predict(xTest)

    print float(len((yHat == yTest).nonzero()[0]))/len(yTest), len(yTest)
