'''
Created on Sep 8, 2013

@author: jacob
'''
import time

class LocationBucket(object):
    '''
    classdocs
    '''


    def __init__(self, deviceId):
        '''
        Constructor
        '''
        self.deviceId = deviceId
        self.locationList = []
        self.time = 0
        
    
    def addLocation(self, loc):
        try:
            if loc.hasLatAndLon():
                for oldLoc in self.locationList:
                    if oldLoc.ts == loc.ts:
                        return False
                self.locationList.append(loc)
                self.locationList.sort(key=lambda loc: loc.ts)
            else:
                return False
        except Exception:
            return False
        
        
    def clearOld(self, ageMillis):
        tNow = self.time
        tOld = tNow - ageMillis
        
        self.locationList = [loc for loc in self.locationList if loc.ts >= tOld]
        self.locationList.sort(key=lambda loc: loc.ts)
        
        
    def getRecent(self, ageMillis):
        tNow = self.time
        tOld = tNow - ageMillis
        
        locList = [loc for loc in self.locationList if loc.ts >= tOld]
        locList.sort(key=lambda loc: loc.ts)
        return locList
        
        
    def checkAgainstTrip(self, trip):
        pass