import numpy as np
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
