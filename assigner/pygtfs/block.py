'''
Created on Sep 2, 2013

@author: jacob
'''

class Block(object):

    def __init__(self, blockId, serviceId):
        self.blockId = blockId
        
        self.tripDict = {}
        
        # serviceId corresponds to "calendar" information
        self.serviceId = serviceId
        
        
    def __eq__(self, other):
        return self.blockId == other.blockId
    
    
    def __str__(self, *args, **kwargs):
        return "Block(block_id=%s)" % (self.blockId)


    def __repr__(self):
        return "Block(%s)" % (self.blockId)
    
        
    def addTrip(self, trip):
        if (trip.tripId not in self.tripDict):
            self.tripDict[trip.tripId] = trip


    def getTripForTime(self, millisSinceDaystart):
        # handle edge cases (time before first trip or after last trip)
        firstTrip = self.getFirstTrip()
        firstTripStart = firstTrip.getFirstDepartureMillis()
        
        if (millisSinceDaystart < firstTripStart):
            return firstTrip

        lastTrip = self.getLastTrip()
        lastTripStart = lastTrip.getFirstDepartureMillis()

        if (millisSinceDaystart >= lastTripStart):
            return lastTrip

        # now handle intermediate trips
        tripList = self.tripDict.values()
        tripList.sort()

        for i, trip in enumerate(tripList):
            if (i < len(tripList) - 1):
                firstDep = trip.getFirstDepartureMillis()
                firstDepNext = tripList[i+1].getFirstDepartureMillis()
                if (millisSinceDaystart >= firstDep and millisSinceDaystart < firstDepNext):
                    return trip
                
        return None
        

    def getFirstTrip(self):
        tripList = self.tripDict.values()
        tripList.sort()
            
        return tripList[0]


    def getLastTrip(self):
        tripList = self.tripDict.values()
        tripList.sort()
            
        return tripList[-1]
