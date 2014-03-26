import numpy as np
import datetime
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import confusion_matrix

try:
    import geopy.distance
    hasGeopy=True
except ImportError:
    hasGeopy=False
    
import eventsDBManager
import utils
import plot

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

    refLat,refLon = utils.getDepotCoords()
    latDist = np.array([geopy.distance.distance((lat,lon), (refLat,lon)).meters
                        for lat,lon in zip(latitude,longitude)])
    lonDist = np.array([geopy.distance.distance((lat,lon), (lat,refLon)).meters
                        for lat,lon in zip(latitude,longitude)])
    return (latDist, lonDist)

def latlonToMeters2(latitude, longitude):
    """Convert lat,lon separation from depot to meters"""
    if not hasGeopy:
        raise ImportError("geopy")

    refLat,refLon = utils.getDepotCoords()
    latDist = np.array([geopy.distance.great_circle((lat,lon), (refLat,lon)).meters
                        for lat,lon in zip(latitude,longitude)])
    lonDist = np.array([geopy.distance.great_circle((lat,lon), (lat,refLon)).meters
                        for lat,lon in zip(latitude,longitude)])
    return (latDist, lonDist)

def latlonToMeters3(latitude, longitude):
    """Convert lat,lon separation from depot to meters"""
    refLat,refLon = utils.getDepotCoords()
    latDist = utils.metersBetweenLatLonPair(latitude,longitude,refLat,longitude)
    lonDist = utils.metersBetweenLatLonPair(latitude,longitude,latitude,refLon)
    return (latDist, lonDist)

def getData(dbFileLoc, startDate, endDate):
    cols = ("time","longitude","latitude","trip_id")
    db = eventsDBManager.EventsDB(dbFileLoc)
    rec = db.selectData(tableName="rlev",date=(startDate, endDate), cols=cols)

    return rec

def encodeLabels(trainLabels, testLabels):
    # convert trip_id strings to unique integers for classification
    le = LabelEncoder()
    le.fit(np.concatenate((trainLabels,testLabels)))
    yTrain = le.transform(trainLabels)
    yTest = le.transform(testLabels)

    return (yTrain, yTest)

def preprocess(rec):

    # constuct design matrix
    utils.printCurrentTime()
    print "scaling time"
    weekSecs = unixmsToWeekSecs2(rec['time'])
    timeScaled = secsToMeters(weekSecs)
    utils.printCurrentTime()
    print "scaling distance"
    latDist, lonDist = latlonToMeters3(rec['latitude'], rec['longitude'])
    xData = np.array([timeScaled, latDist, lonDist]).T

    return xData

def classify(dbFileLoc):
    utils.printCurrentTime()
    print "reading training data"
    trainingData = getData(dbFileLoc, '2013-12-01', '2014-01-07')
    utils.printCurrentTime()
    print "reading test data"
    testData = getData(dbFileLoc, '2014-01-08', '2014-01-15')

    utils.printCurrentTime()
    print "preprocessing training data"
    xTrain = preprocess(trainingData)
    utils.printCurrentTime()
    print "preprocessing test data"
    xTest = preprocess(testData)
    utils.printCurrentTime()
    print "encoding labels"
    yTrain, yTest = encodeLabels(trainingData['trip_id'], testData['trip_id'])

    utils.printCurrentTime()
    print "training KNN model"
    knn = KNeighborsClassifier(n_neighbors=10)
    knn.fit(xTrain, yTrain)

    utils.printCurrentTime()
    print "predicting on test data"
#    print "Score = {}".format(knn.score(xTest, yTest))

#    utils.printCurrentTime()
#    print "getting predictions again"
    yHat = knn.predict(xTest)
    print "Score = {}".format(float(len((yHat == yTest).nonzero()[0]))/len(yTest))
    utils.printCurrentTime()
    print "getting confusion matrix"
    cm = confusion_matrix(yTest, yHat)
    utils.printCurrentTime()
    print "plotting confusion matrix"
    plot.plotConfusionMatrix(np.log10(1+cm), showPlot=True)
    utils.printCurrentTime()
    plot.plotHistograms((yTrain, yTest, yHat), ("red","green","blue"),
                        ("Training", "Test (Actual)", "Test (Predicted)"),
                        "Trips", log=True, showPlot=True)
    print "Done"


    return (trainingData, testData, xTrain, xTest, yTrain, yTest, yHat, knn, cm)
