'''
Created on Sep 19, 2013

@author: jacob
'''

import logging
from assignedTrip import AssignedTrip
import util
import time
from pygtfs.projectedLocation import createJsonFromProjLoc
import json
import pprint
from __future__ import division

class AssignedTrips(object):
    '''
    classdocs
    '''


    def __init__(self, gtfsData, loggerName, tripDistances):
        '''
        Constructor
        '''
        
        self.gtfsData = gtfsData
        self.logger = logging.getLogger(loggerName)
        self.tripDistances = tripDistances
        
        self.assignedTripDict = {}
        
        
    def isDeviceAssigned(self, deviceId):
        return deviceId in self.assignedTripDict
        
        
    def getOldAssignedDeviceList(self):
        oldList = []
        for deviceId in self.assignedTripDict:
            tenMinutes = 10*60*1000
            recentLocs = self.tripDistances.locationBuckets[deviceId].getRecent(tenMinutes)
            if (len(recentLocs) == 0):
                self.logger.debug("%s had old data. Deassigning trip." 
                                  % (deviceId))
                oldList.append(deviceId)
        return oldList
    
                
    def getInaccurateAssignedDeviceList(self):
        badList = []
        for deviceId in self.assignedTripDict:
            assignedBlock = self.assignedTripDict[deviceId].block

            candidateBlocks = [x['block'] for x in 
                               self.tripDistances.candidateBlocks[deviceId]]
            
            if assignedBlock not in candidateBlocks:
                self.logger.debug("%s had bad block %s. Deassigning trip." 
                                  % (deviceId, assignedBlock))
                badList.append(deviceId)
        return badList

    def getAccuracy(self):
        badList = getInaccurateAssignedDeviceList()
        goodList = []
        for deviceID in self.assignedTripDict:
            assignedBlock = self.assignedTripDict[deviceId].block

            candidateBlocks = [x['block'] for x in 
                               self.tripDistances.candidateBlocks[deviceId]]

            if assignedBlock in candidateBlocks:
                goodList.append(deviceId)
        accuracy = len(goodList) / (len(goodList) + len(badList))
        print accuracy
        return accuracy


    
    
    def getBacktrackingDeviceList(self):
        backtrackList = []
        for deviceId in self.assignedTripDict:
            if self.assignedTripDict[deviceId].hasBacktracked():
                self.logger.debug("%s backtracked too far. Deassigning trip." 
                                  % (deviceId))
                backtrackList.append(deviceId)
        return backtrackList
    
    
    def clearBadAssignedTrips(self):
        badDeviceList = self.getOldAssignedDeviceList() + \
                        self.getInaccurateAssignedDeviceList() + \
                        self.getBacktrackingDeviceList()
                        
        # get unique elements
        badDeviceList = list(set(badDeviceList))
        
        for deviceId in badDeviceList:
            self.assignedTripDict.pop(deviceId, None)
        
                
    def newRawLocation(self, rawLoc):
        # important actions taken every time a new raw_loc is received
        projLoc = self.projectLocation(rawLoc)
        
        if projLoc is not None:    
            jsonString = createJsonFromProjLoc(projLoc)
            ## NOTE FOR DATA SCIENCE PROJECT: you'll need to decide what
            # you want to do with the resulting "projected location."
            # Nothing, maybe.
            pass
                
        self.clearBadAssignedTrips()
        
        
    def sendEvent(self, trip, stopSeq, projLoc, arrival=None):
        stopTime = trip.getStopTimeForStopSequence(stopSeq)
        stopId = stopTime.stopId
        stop = self.gtfsData.getStopFromStopId(stopId)
        if (projLoc.routeId == None or projLoc.tripId == None 
            or projLoc.deviceId == None or arrival == None or stopId == None
            or projLoc.ts == None):
            # Can't send an event!
            self.logger.warn("Failed to send event. Some data was null.")
            return
        
        
        postKm = stopTime.postKm
        
        eventDict = {}
        
        eventDict['device_id'] = projLoc.deviceId
        
        eventDict['stop_id'] = stopId
        eventDict['stop_latitude'] = stop.stopLat
        eventDict['stop_longitude'] = stop.stopLon
        eventDict['stop_sequence'] = stopSeq
        eventDict['stop_postmile'] = postKm*1000.
        
        eventDict['trip_id'] = projLoc.tripId
        eventDict['route_id'] = projLoc.routeId
        
        dayStart = self.gtfsData.getDaystartFromTimestampMillis(projLoc.ts)
        
        if (arrival):
            eventDict['time'] = projLoc.ts
            arrTimeSch = dayStart + stopTime.arrTimeMillis
            delay = projLoc.ts - arrTimeSch
            eventDict['delay'] = delay
            eventDict['type'] = 0
        else:
            eventDict['time'] = projLoc.ts
            depTimeSch = dayStart + stopTime.depTimeMillis
            delay = projLoc.ts - depTimeSch
            eventDict['delay'] = delay
            eventDict['type'] = 1
        
        jsonString = json.dumps(eventDict)
        
        ## NOTE FOR DATA SCIENCE PROJECT: do something with these
        # generated events?
        
        
    def getAssignedTrips(self):
        tripList = []
        for deviceId in self.assignedTripDict:
            tripList.append(self.assignedTripDict[deviceId].trip)
            
        return tripList


    def getAssignedBlocks(self):
        blockList = []
        for deviceId in self.assignedTripDict:
            blockList.append(self.assignedTripDict[deviceId].block)
            
        return blockList
    
        
    def projectLocation(self, rawLoc, postTarget=None):
        deviceId = rawLoc.deviceId
        rawTime = rawLoc.ts
        projLoc = None
        if (self.isDeviceAssigned(deviceId) and 
            rawTime > self.assignedTripDict[deviceId].time):
            trip = self.assignedTripDict[deviceId].trip
            shape = self.gtfsData.getShapeFromShapeId(trip.shapeId)
            
            # get target postmile
            if postTarget is None:
                currentPost = self.assignedTripDict[deviceId].post
                dt = rawTime - self.assignedTripDict[deviceId].time
                postTarget = currentPost + dt*util.vfid
#                 print currentPost, postTarget
                
            projLoc = self.gtfsData.projectToShapeWithTarget(shape, 
                                                             rawLoc,
                                                             postTarget)
            projLoc.tripId = trip.tripId
            projLoc.routeId = trip.routeId
            projLoc.deviceId = rawLoc.deviceId
            projLoc.ts = rawLoc.ts
            projLoc.speed = rawLoc.speed
            projLoc.bearing = rawLoc.bearing
            projLoc.accuracy = rawLoc.accuracy
            
            self.assignedTripDict[deviceId].post = projLoc.postKm
            self.assignedTripDict[deviceId].time = rawTime
            
#             print postKm
            
            stopSeqArr, stopSeqDep = \
                trip.getStopSequencesForPost(projLoc.postKm, 
                                             util.thresh)
                
#             print stopSeqArr, stopSeqDep
#             print self.assignedTripDict[deviceId].arrivedStop, \
#                 self.assignedTripDict[deviceId].departedStop
                
            if stopSeqArr > self.assignedTripDict[deviceId].arrivedStop:
                # We've arrived at a stop! Update trip information.
                self.assignedTripDict[deviceId].arrivedStop = stopSeqArr
                
                timeString = time.strftime("%H:%M:%S", time.localtime(int(rawTime/1000)))
                
                # Send an event!
                self.logger.info("Device %s (on %r) arrived at stop seq %d at %s!" 
                                      % (deviceId, trip, stopSeqArr, timeString))
                self.sendEvent(trip=trip, stopSeq=stopSeqArr, projLoc=projLoc, arrival=True)
                
                
            if stopSeqDep > self.assignedTripDict[deviceId].departedStop:
                # We've departed from a stop! Update trip information.
                self.assignedTripDict[deviceId].departedStop = stopSeqDep
                timeString = time.strftime("%H:%M:%S", time.localtime(int(rawTime/1000)))
                
                # Send an event!
                self.logger.info("Device %s (on %r) departed from stop seq %d at %s!" 
                                      % (deviceId, trip, stopSeqArr, timeString))
                self.sendEvent(trip=trip, stopSeq=stopSeqDep, projLoc=projLoc, arrival=False)
                
                
                if stopSeqDep == trip.getLastStopSequence():
                    # We've arrived at the final stop! Try to sequence
                    # the next trip.
                    self.logger.info("%r ended. Sequencing next trip." 
                                      % trip)
                    
                    remainingKm = shape.pointList[-1]['post'] - projLoc.postKm
                    sequenced = self.sequenceNextTrip(deviceId, -remainingKm)
#                     if (sequenced):
                        # We've updated to the next trip on this block.
                    
                        # Check if there is remaining projection to do (previous
                        # projection ended at end of previous shape).
#                         postTarget -= projLoc.postKm
#                         if (postTarget > 0):
#                             self.projectLocation(rawLoc, postTarget)
                    
            return projLoc
        
        
#     def checkForEndOfTrip(self, deviceId):
#         currentTrip = self.assignedTrips[deviceId]
#         lastStopId = currentTrip.getLastStopId()
#         lastStop = self.gtfsData.getStopFromStopId(lastStopId)
#         
#         # 50-meter circle for arrivals
#         thresholdKm = 0.05
#         tripEnded = self.tripDistances.checkForArrival(deviceId, 
#                                                        lastStop.stopLat, 
#                                                        lastStop.stopLon,
#                                                        thresholdKm)
#         if (tripEnded):
#             self.logger.debug("Trip %r ended. Sequencing next trip." 
#                               % currentTrip)
#             self.sequenceNextTrip(deviceId)
            
    
    def assignTrip(self, deviceId, startTime, trip, block, post):
        stopSeqArr, stopSeqDep = \
            trip.getStopSequencesForPost(post, util.thresh)
        self.assignedTripDict.pop(deviceId, None)
        self.assignedTripDict[deviceId] = AssignedTrip(trip, block, 
                                                       startTime, post, 
                                                       stopSeqArr,
                                                       stopSeqDep)        
    
            
    def assignUnassignedTripToDevice(self, deviceId, startTime, newTrip, newBlock, post):
        match = False
        # Check to see if the given trip is already assigned.
        if newTrip in self.getAssignedTrips():
            match = True
        
        if (not match):
            # No existing trips match this trip. Assign it.
            self.logger.info("Assigning trip %r to device %s" % (newTrip, deviceId))
            self.assignTrip(deviceId, startTime, newTrip, newBlock, post)
        
        pass
        
            
    def sequenceNextTrip(self, deviceId, initPost=0.):
        currentTrip = self.assignedTripDict[deviceId].trip
        currentBlock = self.assignedTripDict[deviceId].block
        nextTrip = self.gtfsData.getNextTripInBlock(currentTrip.tripId)
        prevTime = self.assignedTripDict[deviceId].time
        post = initPost
        if nextTrip is not None:
            self.logger.debug("Assigning next trip %r." % nextTrip)
            self.assignUnassignedTripToDevice(deviceId, prevTime, nextTrip, 
                                              currentBlock, post)
            return True
        else:
            self.logger.debug("No next trip (end of block). Deassigning device.")
            self.assignedTripDict.pop(deviceId, None)
            return False
        
