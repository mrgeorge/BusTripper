'''
Created on Sep 8, 2013

@author: jacob
'''
from tripDistances import TripDistances
from assignedTrips import AssignedTrips
import logging
import pprint

class TripClassifier(object):
    '''
    classdocs
    '''


    def __init__(self, gtfsData, loggerName):
        '''
        Constructor
        '''
        self.gtfsData = gtfsData
        
        self.tripDistances = TripDistances(self.gtfsData, loggerName)
        self.assignedTrips = AssignedTrips(self.gtfsData, loggerName,
                                           self.tripDistances)
        
        self.logger = logging.getLogger(loggerName)
        
        self.manualBlocks = {}
        
        self.time = 0
        
        
    def newRawLocation(self, loc):
        deviceId = loc.deviceId
        self.tripDistances.addLocation(loc)
        self.assignedTrips.newRawLocation(loc)
#         if self.assignedTrips.isDeviceAssigned(deviceId):
#             # Check for end of trip.
#             self.checkForEndOfTrip(loc.deviceId)
        
        
    def getMaxOtherProb(self, block, deviceId):
        # Look over the other devices, and see if this block is more 
        # likely for this device than for other devices. This helps
        # to prevent early-morning confusion.
        probMaxOther = 0.0
        for otherId in self.tripDistances.locationBuckets:
            if (otherId != deviceId and 
                not self.assignedTrips.isDeviceAssigned(otherId)):
                otherProb = [x['prob'] for x in
                            self.tripDistances.candidateBlocks[otherId]
                            if x['block'] == block]
                if (otherProb is not None and len(otherProb) > 0 
                    and otherProb[0] > probMaxOther):
                    probMaxOther = otherProb[0]
        return probMaxOther
      
      
    def chooseObviousBlock(self, deviceId):
        possibilities = sorted(self.tripDistances.candidateBlocks[deviceId],
                               key = lambda possDict: possDict['avgMillis'])
        
#         if (deviceId == 'c08084c15b99f9f'):
#             pprint.pprint(possibilities)
            
        if (len(possibilities) == 1):
            probThis = possibilities[0]['prob']
            probMaxOther = self.getMaxOtherProb(possibilities[0]['block'], 
                                                deviceId)
            
            if (probThis - probMaxOther > 0.25):
                return possibilities[0]['block'], possibilities[0]['postKm']
            
#             if (deviceId == 'c08084c15b99f9f'):
#                 print probThis
#                 print probMaxOther
            
            
        elif (len(possibilities) > 1):
            # If we have more than one candidate trip, and the best match is
            # substantially better than the others, then choose it.
            prob0 = possibilities[0]['prob']
            prob1 = 0.
            # get next best unassigned trip:
            for otherBlockDict in possibilities[1:]:
                block = otherBlockDict['block']
                if (block not in self.assignedTrips.getAssignedBlocks()):
                    prob1 = otherBlockDict['prob']
            
            probMaxOther = self.getMaxOtherProb(possibilities[0]['block'], 
                                                deviceId)
            
            if (prob0 - prob1 > 0.25 and prob0 - probMaxOther > 0.25):
                return possibilities[0]['block'], possibilities[0]['postKm']
#             if (deviceId == 'c08084c15b99f9f'):
#                 print prob0
#                 print prob1
#                 print probMaxOther
        
        return None, None
      
        
    def chooseObviousTripManualBlock(self, deviceId):
        possibilities = sorted(self.tripDistances.candidateTrips[deviceId],
                               key = lambda tripDict: tripDict['avgMillis'])
        
        for possibility in possibilities:
            prob = possibility['prob']
        
            # Reasonable probability threshold.
            if (prob < 0.05):
                continue
            
            else:
                # see if the given trip is in our manually assigned blocks:
                for blockId in self.manualBlocks[deviceId]:
                    block = self.gtfsData.getBlockFromBlockId(blockId)
                    possTrip = possibility['trip']
                    if (possTrip.tripId in block.tripDict):
                        # If it's in a manually-assigned block, then choose it.
                        return possTrip, possibility['postKm']
        
        return None, None
    
    
    def checkAllUnassignedForObviousBlocks(self):
        # Generate the list of trip distances
        self.tripDistances.checkAllBucketsBlind()
        
        # for all devices:
        for deviceId in self.tripDistances.locationBuckets:
            if self.assignedTrips.isDeviceAssigned(deviceId):
                assignment = self.assignedTrips.assignedTripDict[deviceId]
                currTrip = assignment.trip
                recentLoc = self.tripDistances.locationBuckets[deviceId].locationList[-1]
                
                self.logger.info("Device %s assigned to %r" % (deviceId, currTrip))
                self.logger.info("Post-km = %f, arrivedStop = %d" % (assignment.post, assignment.arrivedStop))
                self.logger.info("lat = %f, lon = %f" % (recentLoc.lat, recentLoc.lon))
            else:
                # First, see if we have a manual block assignment.
                if deviceId in self.manualBlocks:
                    trip, post = self.chooseObviousTripManualBlock(deviceId)
                    if trip is not None:
                        self.assignedTrips.assignUnassignedTripToDevice(deviceId, 
                                                                        self.time,
                                                                        trip, post)
                    
                else:
                    # No manual assignment. See if there's an obvious best block.
                    block, post = self.chooseObviousBlock(deviceId)
                    if block is not None:
                        trip = self.tripDistances.getBestTrip(deviceId, block)
                        self.assignedTrips.assignUnassignedTripToDevice(deviceId, 
                                                                        self.time,
                                                                        trip,
                                                                        block, 
                                                                        post)
    
    
    def updateTime(self, time):
        self.time = time
        self.tripDistances.updateTime(time)
