'''
Created on Sep 2, 2013

@author: jacob
'''

class Trip(object):

    def __init__(self, tripId, blockId, routeId, shapeId):
        self.tripId = tripId
        self.blockId = blockId
        self.routeId = routeId
        self.shapeId = shapeId
        self.stopTimeList = []
    
    
    def __eq__(self, other):
        return self.tripId == other.tripId
    
    
    def __cmp__(self, other):
        firstDep = self.getFirstDepartureMillis()
        firstDepOther = other.getFirstDepartureMillis()
        if (firstDep < firstDepOther):
            return -1
        elif (firstDep > firstDepOther):
            return 1
        else:
            return 0
        
    
    def __repr__(self):
        return "Trip(%s)" % (self.tripId)
    
    
    def addStopTime(self, stopTime):
        if (stopTime not in self.stopTimeList):
            self.stopTimeList.append(stopTime)
            self.stopTimeList.sort()
            
    
    def getFirstDepartureMillis(self):
        return self.stopTimeList[0].depTimeMillis
    
    
    def getLastArrivalMillis(self):
        return self.stopTimeList[-1].arrTimeMillis
    
    
    def getLastStopId(self):
        return self.stopTimeList[-1].stopId
    
    
    def getLastStopSequence(self):
        return self.stopTimeList[-1].stopSequence
    
    
    def getStopSequencesForPost(self, postKm, thresh):
        arrStop = -1
        depStop = -1
        for stopTime in self.stopTimeList:
            if (stopTime.postKm < postKm + thresh):
                arrStop = stopTime.stopSequence
        
        for stopTime in self.stopTimeList:
            if (stopTime.postKm < postKm - thresh):
                depStop = stopTime.stopSequence
            
        return arrStop, depStop
    
    
    def getStopTimeForStopSequence(self, stopSeq):
        for stopTime in self.stopTimeList:
            if stopSeq == stopTime.stopSequence:
                return stopTime
