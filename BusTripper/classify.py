import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix
import scipy.stats

import eventsDBManager
import utils
import plot
import dtw

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
    """Convert time in seconds to distance in meters given avg speed in m/s)"""
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
    cols = ("time","longitude","latitude","trip_id","device_id")
    db = eventsDBManager.EventsDB(dbFileLoc)
    df = db.selectData(tableName="rlev",date=(startDate, endDate), cols=cols)

    return df

def encodeLabels(trainLabels, testLabels):
    """Convert trip_id strings to unique integer labels for classification"""
    encoder = LabelEncoder()
    encoder.fit(np.concatenate((trainLabels,testLabels)))
    yTrain = encoder.transform(trainLabels)
    yTest = encoder.transform(testLabels)

    return (yTrain, yTest, encoder)

def preprocess(df, nPts=1):
    """Rescale features and constuct design matrix for classifier"""

    utils.printCurrentTime()
    print "scaling distance"
    df["latDist"], df["lonDist"] = utils.latlonToMeters(df["latitude"], df["longitude"])

    utils.printCurrentTime()
    if nPts > 1:
        print "Sequencing data"
        seqFrame = getSequences(df, nPts=nPts)
        xData = seqFrame['sequence'] # nested frames
        labels = seqFrame['label'].apply(lambda x: x[2]) # list of tripIDs

    else:
        print "scaling time"
        df["weekSecs"] = utils.getWeekSecs(df["time"])
        df["timeScaled"] = utils.secsToMeters(df["weekSecs"])
        xData = df[["timeScaled", "latDist", "lonDist"]]
        labels = df["trip_id"]

    return (xData, labels)

def getSequences(df, nPts=10):
    """Aggregate date by date, device, and trip into chunks of length nPts

    Inputs:
        df - pandas dataframe
        nPts - number of points per group (default = 10)
    Returns:
        seqFrame - dataframe with labels and sequences
            labels are date/deviceID/tripID tuples
            sequences are dataframes with cols = time/lat/lon and nPts rows
    """

    # Create date column so we can group by day
    df['date'] = df['time'].apply(lambda x: x.date())

    # Replace None with string so we can index even on missing tripIDs
    df.replace({"trip_id":{None:"None"}}, inplace=True)

    # Sort and group the data
    dfg = df.sort_index(by=("date","device_id","trip_id","time")).groupby(
        ("date","device_id","trip_id"))

    # Data structure in this section is a bit complicated:
    # One ddtSequence is a dataframe of length nPts with lat/lon time series
    # ddtSequences is an array of these frames for one date/device/trip group
    # ddtFrame is a dataframe with ddtSequences and a copy of ddt label for each
    # ddtFrames is a list of these frames for each ddt
    # seqFrame is the dataframe concatenation of the above frames
    #     each row has a ddt label and a ddtSequence frame
    ddtFrames = []
    for dateDevTrip, grp in dfg:
        nTot = len(grp)
        nSequences = np.floor_divide(nTot, nPts)
        ddtSequences = np.empty(nSequences, dtype=object)
        for ss in range(nSequences):
            ddtSequences[ss] = grp[["time", "latitude", "longitude"]][ss*nPts:(ss+1)*nPts].set_index("time")
        ddtFrame = pd.DataFrame(data = {"label":[dateDevTrip for ii in range(nSequences)], "sequence":ddtSequences})
        ddtFrames.append(ddtFrame)

    seqFrame = pd.concat(ddtFrames, ignore_index=True)

    return seqFrame

def summarizeClassifications(yTrue, yPred, encoder):
    """Print summary stats and show confusion matrices for class predictions

    Inputs:
        yTrue - array of true class labels
        yPred - array of predict class labels (same length as yTrue)
        encoder - LabelEncoder object to get trip IDs
    """
    nData = yTrue.size
    nonNull = (yTrue!=0)
    nNonNull = nonNull.nonzero()[0].size

    serviceIDTrue, routeIDTrue, blockIDTrue, departureTimeTrue, directionTrue =\
        utils.parseTripID(encoder.inverse_transform(yTrue))
    serviceIDPred, routeIDPred, blockIDPred, departureTimePred, directionPred =\
        utils.parseTripID(encoder.inverse_transform(yPred))

    trueArrs = (yTrue, serviceIDTrue, routeIDTrue, blockIDTrue,
                           departureTimeTrue, directionTrue)
    predArrs = (yPred, serviceIDPred, routeIDPred, blockIDPred,
                           departureTimePred, directionPred)
    labels = ("Trip ID", "Service ID", "Route ID", "Block ID",
              "Departure Time", "Direction")

    print "{}/{} ({:0.1f}%) trips correct".format(
        (yTrue == yPred).nonzero()[0].size, nData,
        (yTrue == yPred).nonzero()[0].size*100/float(nData))
    print "Predicted {} Null trips".format((yPred == 0).nonzero()[0].size)
    print "Of above, {} were actual Null trips".format(
        ((yPred == 0) & (yTrue == 0)).nonzero()[0].size)
    print "There were actually {} Null trips".format(
        (yTrue == 0).nonzero()[0].size)

    for yt, yp, label in zip(trueArrs, predArrs, labels):
        print "{}/{} ({:0.1f}%) {} correct".format(
            (yt == yp).nonzero()[0].size, nData,
            (yt == yp).nonzero()[0].size*100/float(nData),
            label)

    print "====Eliminating null trips from yTrue===="
    for yt, yp, label in zip(trueArrs, predArrs, labels):
        print "{}/{} ({:0.1f}%) {} correct".format(
            (yt[nonNull] == yp[nonNull]).nonzero()[0].size, nNonNull,
            (yt[nonNull] == yp[nonNull]).nonzero()[0].size*100/float(nNonNull),
            label)

    print "====Confusion matrices===="
    for ytl, ypl, label in zip(trueArrs, predArrs, labels):
        print label
        yt, yp, encoder = encodeLabels(ytl, ypl)
        cm = confusion_matrix(yt, yp)
        plot.plotConfusionMatrix(np.log10(1+cm), title=label, showPlot=True)

def dtwClassifier(xTrain, yTrain, xTest):

    yTest = np.empty(xTest.size, dtype='int64')
    for ii, test in enumerate(xTest):
        print "{} / {}".format(ii, xTest.size)
        minDist = np.inf
        for train,label in zip(xTrain, yTrain):
            if dtw.RDTW(test.values, train.values) < minDist:
                yTest[ii] = label

    return yTest

def classify(dbFileLoc, nPts=1):
    utils.printCurrentTime()
    print "reading training data"
    trainingData = getData(dbFileLoc, '2013-12-01', '2014-01-07')
    utils.printCurrentTime()
    print "reading test data"
    testData = getData(dbFileLoc, '2014-01-08', '2014-01-15')

    utils.printCurrentTime()
    print "preprocessing training data"
    xTrain, labelsTrain = preprocess(trainingData, nPts=nPts)
    utils.printCurrentTime()
    print "preprocessing test data"
    xTest, labelsTest = preprocess(testData, nPts=nPts)
    utils.printCurrentTime()
    print "encoding labels"
    yTrain, yTest, encoder = encodeLabels(labelsTrain, labelsTest)

    utils.printCurrentTime()
    if nPts == 1:
        print "training classifier"
        clf = KNeighborsClassifier(n_neighbors=10)
        # clf = DecisionTreeClassifier(max_depth=10)
        clf.fit(xTrain, yTrain)

        utils.printCurrentTime()
        print "predicting on test data"
        yHat = clf.predict(xTest)
    else:
        print "predicting with DTW on test data"
        yHat = dtwClassifier(xTrain, yTrain, xTest)

    utils.printCurrentTime()
    summarizeClassifications(yTest, yHat, encoder)

    print "Done"

    return (trainingData, testData, xTrain, xTest, yTrain, yTest, yHat,
            clf, encoder)
