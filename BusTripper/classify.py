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

try:
    import dtw
    hasDTW = True
except:
    hasDTW = False

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
    """Rescale features and constuct design matrix for classifier

    Inputs:
        df - pandas data frame
        nPts - int length of sequences to use
               if nPts==1, asssume features are rescaled location,time
               else assume features are time series and group the data
    """

    utils.printCurrentTime()
    print "scaling distance"
    df["latDist"], df["lonDist"] = utils.latlonToMeters(df["latitude"], df["longitude"])

    utils.printCurrentTime()
    if nPts != 1:
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
        nPts - number of points per sequence (default = 10)
               if nPts<1, group by trip (i.e. sequence length = trip length)
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
        if nPts < 1:
            nPts = nTot
            nSequences = 1
        else:
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
    # if nPts == 1, pass that value
    # else pass nPts=0 so training set is split by trip
    xTrain, labelsTrain = preprocess(trainingData, nPts=int(nPts==1))
    utils.printCurrentTime()
    print "preprocessing test data"
    # here split test set by nPts regardless of training or test
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
    elif hasDTW:
        print "predicting with DTW on test data"
        yHat = dtwClassifier(xTrain, yTrain, xTest)
    else:
        print "DTW module not available, can't use nPts > 1"
        raise ImportError(dtw)

    utils.printCurrentTime()
    summarizeClassifications(yTest, yHat, encoder)

    print "Done"

    return (trainingData, testData, xTrain, xTest, yTrain, yTest, yHat,
            clf, encoder)
