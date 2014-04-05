import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import confusion_matrix
import scipy.stats

try:
    import geopy.distance
    hasGeopy=True
except ImportError:
    hasGeopy=False
    
import eventsDBManager
import utils
import plot


def getData(dbFileLoc, startDate, endDate):
    cols = ("time","longitude","latitude","trip_id","device_id")
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
    weekSecs = utils.unixmsToWeekSecs2(rec['time'])
    timeScaled = utils.secsToMeters(weekSecs)
    utils.printCurrentTime()
    print "scaling distance"
    latDist, lonDist = utils.latlonToMeters3(rec['latitude'], rec['longitude'])
    xData = np.array([timeScaled, latDist, lonDist]).T

    return xData

def sortByDeviceTime(rec):
    """Sort data recarray by device_id and time

    Sorting is not done in place and returns a new copy
    """
    ind = rec.argsort(order=("device_id","time"))
    return (rec[ind], ind)

def getRecentAssignments(recSorted, yHatSorted, ind, nPts=10, dtMin=None):
    """Get list trip assignments for device_id in recent window

    Inputs:
        recSorted - data recarray sorted by device_id and time
        yHatSorted - trip assignments (encoded ids) sorted like recSorted
        ind - index of recSorted to start from
        nPts - number of trip assignments to return (default = 10)
        dtMin - if nPts is None, dtMin defines time window to return in minutes
    Returns:
        recarray of trip assignments for device_id in given window
        (note: list may be shorter than nPts if insufficient data exists)
    """

    device_id = recSorted['device_id'][ind]
    time = recSorted['time'][ind]

    if nPts is not None:
        recWindow = recSorted[np.max([0,ind-nPts-1]):np.max([0,ind])]
        yHatWindow = yHatSorted[np.max([0,ind-nPts-1]):np.max([0,ind])]
        devMatch = (recWindow['device_id'] == device_id)
        return yHatWindow[devMatch]
    elif dtMin is not None:
        window = ((recSorted['time'][:ind] > time - dtMin*60*1000) &
                  (recSorted['device_id'] == device_id))
        return yHatSorted[window]
    else:
        raise ValueError((nPts, dtMin))

def smoothAssignments(rec, yHat, yTrue, **kwargs):
    sortInd = np.argsort(rec, order=("device_id","time"))
    newAssignments = yHat[sortInd]
    for ii in xrange(rec.size):
        window = getRecentAssignments(rec[sortInd], yHat[sortInd], ii, **kwargs)
        try:
            mode = scipy.stats.mode(window)[0][0]
            newAssignments[ii] = mode
            if((yTrue[sortInd][ii] == yHat[sortInd][ii]) & (mode == yTrue[sortInd][ii])):
                print "keeping correct"
            elif((yTrue[sortInd][ii] == yHat[sortInd][ii]) & (mode != yTrue[sortInd][ii])):
                print "changing to incorrect"
            elif((yTrue[sortInd][ii] != yHat[sortInd][ii]) & (mode == yTrue[sortInd][ii])):
                print "changing to correct"
            else:
                print "incorrect before and after"
        except UnboundLocalError:
            print "insufficent data"
            pass # leave original assignment
    newAssignments[sortInd] = newAssignments # return to original order
    return newAssignments

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
