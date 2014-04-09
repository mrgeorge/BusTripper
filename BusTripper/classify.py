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

def preprocess(df):
    """Rescale features and constuct design matrix for classifier"""
    utils.printCurrentTime()
    print "scaling time"
    weekSecs = utils.getWeekSecs(df['time'])
    timeScaled = utils.secsToMeters(weekSecs)
    utils.printCurrentTime()
    print "scaling distance"
    latDist, lonDist = utils.latlonToMeters(df['latitude'], df['longitude'])
    xData = np.array([timeScaled, latDist.values, lonDist.values]).T

    return xData

def sortByDeviceTime(rec):
    """Sort data recarray by device_id and time

    Sorting is not done in place and returns a new copy
    """
    ind = rec.argsort(order=("device_id","time"))
    return (rec[ind], ind)

class Sequence(object):
    def __init__(self, dateDevTrip, times, lats, lons):
        self.date = dateDevTrip[0]
        self.deviceID = dateDevTrip[1]
        self.tripID = dateDevTrip[2]
        self.times = times
        self.lats = lats
        self.lons = lons

def getSequences(df, nPts=10):
    """Aggregate date by device_id into chunks of length nPts

    Inputs:
        rec - location data recarray
        nPts - number of points per group (default = 10)
    Returns:
        recGrouped - grouped location data recarray, sorted by device and time
    """
    df['date'] = df['time'].apply(lambda x: x.date())
    dfg = df.sort_index(by=("date","device_id","trip_id","time")).groupby(
        ("date","device_id","trip_id"))
    seqBucket = []
    for dateDevTrip, grp in dfg:
        nTot = len(grp)
        nSequences = np.floor_divide(nTot, nPts)
        sequences = np.empty(nSequences, dtype=object)
        for ss in range(nSequences):
            times, lats, lons = [grp[col][ss*nPts:(ss+1)*nPts].values \
                                 for col in ("time", "latitude", "longitude")]
            sequences[ss] = Sequence(dateDevTrip, times, lats, lons)
        seqBucket.append(sequences)
    return seqBucket

def getRecentAssignments(recSorted, yHatSorted, ind, nPts=10):
    """Get list of trip assignments for a device_id in recent window

    Inputs:
        recSorted - data recarray sorted by device_id and time
        yHatSorted - trip assignments (encoded ids) sorted like recSorted
        ind - index of recSorted to start from
        nPts - number of trip assignments to return (default = 10)
    Returns:
        recarray of trip assignments for device_id in given window
        (note: list may be shorter than nPts if insufficient data exists)
    """

    device_id = recSorted['device_id'][ind]
    time = recSorted['time'][ind]
    recWindow = recSorted[np.max([0,ind-nPts-1]):np.max([0,ind])]
    yHatWindow = yHatSorted[np.max([0,ind-nPts-1]):np.max([0,ind])]
    devMatch = (recWindow['device_id'] == device_id)
    return yHatWindow[devMatch]

def smoothAssignments(rec, yHat, yTrue, **kwargs):
    sortInd = np.argsort(rec, order=("device_id","time"))
    newAssignments = yHat[sortInd]
    for ii in xrange(rec.size):
        window = getRecentAssignments(rec[sortInd], yHat[sortInd], ii, **kwargs)
        try:
            mode = scipy.stats.mode(window)[0][0]
            newAssignments[ii] = mode
            if((yTrue[sortInd][ii] == yHat[sortInd][ii]) &
               (mode == yTrue[sortInd][ii])):
                print "keeping correct"
            elif((yTrue[sortInd][ii] == yHat[sortInd][ii]) &
                 (mode != yTrue[sortInd][ii])):
                print "changing to incorrect"
            elif((yTrue[sortInd][ii] != yHat[sortInd][ii]) &
                 (mode == yTrue[sortInd][ii])):
                print "changing to correct"
            else:
                print "incorrect before and after"
        except UnboundLocalError:
            print "insufficent data"
            pass # leave original assignment
    newAssignments[sortInd] = newAssignments # return to original order
    return newAssignments

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
    yTrain, yTest, encoder = encodeLabels(trainingData['trip_id'],
                                          testData['trip_id'])

    utils.printCurrentTime()
    print "training classifier"
    clf = KNeighborsClassifier(n_neighbors=10)
#    dtree = DecisionTreeClassifier(max_depth=10)
    clf.fit(xTrain, yTrain)

    utils.printCurrentTime()
    print "predicting on test data"
    yHat = clf.predict(xTest)
    utils.printCurrentTime()
    summarizeClassifications(yTest, yHat, encoder)

    print "Done"

    return (trainingData, testData, xTrain, xTest, yTrain, yTest, yHat,
            clf, encoder)
