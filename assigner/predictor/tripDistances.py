'''
Created on Sep 8, 2013

@author: jacob
'''
from pygtfs.locationBucket import LocationBucket
from pygtfs.util import kmBetweenLatLonPair
from util import probGivenDeviation
import math
from scipy import integrate
import numpy
import pprint
import logging

class TripDistances(object):
    '''
    classdocs
    '''


    def __init__(self, gtfsData, loggerName):
        '''
        Constructor
        '''
        self.gtfsData = gtfsData
        
        self.logger = logging.getLogger(loggerName)
        
        self.locationBuckets = {}
        # self.candidateTrips = {}
        self.candidateBlocks = {}
        self.time = 0
        self.daystart = 0

        # 12 km/hr fiducial speed, in km/millisecond
        self.vfid = 12.0/60.0/60.0/1000.0

        # 2 km/hr fiducial "speed", in km/millisecond: this is used to penalize
        # lateral distances more heavily -- essentially arbitrarily determined
        self.vfid_perp = 2.0/60.0/60.0/1000.0

        # Always consider locations from the last 10 minutes
        self.ageMillis = 1000*60*10
        
        
    def addLocation(self, loc):
        if (loc.deviceId not in self.locationBuckets):
            self.locationBuckets[loc.deviceId] = LocationBucket(loc.deviceId)
            
        self.locationBuckets[loc.deviceId].addLocation(loc)
        
        
    def checkAllBucketsBlind(self):
        for deviceId in self.locationBuckets:
            self.checkBucketAgainstAllBlocks(deviceId)
        

    def checkBucketAgainstAllBlocks(self, deviceId):
        candidateBlocks = []
        self.logger.debug("Generating candidate blocks for device %s." % (deviceId))
        avgMillisMin = 4*60*60*1000
        for block in self.gtfsData.getTodayBlocks(self.time):
            avgMillis, postKm = self.avgMillisFromBlock(deviceId, block)
            # print block
            # print avgMillis
            if (avgMillis < avgMillisMin):
                avgMillisMin = avgMillis
                # print trip.tripId
                # print avgMillis
            if (avgMillis < 40*60*1000):
                # this is a candidate (<~40 minutes off schedule)
                candidateDict = {}
                candidateDict['avgMillis'] = avgMillis
                candidateDict['block'] = block
                candidateDict['postKm'] = postKm
                candidateDict['prob'] = probGivenDeviation(avgMillis)
                candidateBlocks.append(candidateDict)
                    
        self.candidateBlocks[deviceId] = candidateBlocks
        printList = sorted(candidateBlocks, key=lambda k: k['prob'])
        self.logger.debug(pprint.pformat(printList))
    

    def avgMillisFromBlock(self, deviceId, block):
        locList = self.locationBuckets[deviceId].getRecent(self.ageMillis)
        
        distSeries = []
        timeSeries = []
        
        if len(locList) <= 1:
            # Return arbitrary large deviation if we have few
            # location in the bucket.
            return 4*60*60*1000, 0.0
        
        finalPost = None

        # if deviceId == 'c0808a41c93a39f':
        #     print block

        
        for loc in locList:
            time = loc.ts
            trip = block.getTripForTime(time - self.daystart)
            nextTrip = self.gtfsData.getNextTripInBlock(trip.tripId)

            firstDep = trip.getFirstDepartureMillis() + self.daystart
            lastArr = trip.getLastArrivalMillis() + self.daystart

            postTrip = self.getPostmileAtTime(time, trip)
            if (loc.ts > firstDep and loc.ts < lastArr):
                postTrip = self.getPostmileAtTime(time, trip)
                timeDist = 0                
            elif (loc.ts <= firstDep):
                postTrip = self.getPostmileAtTime(firstDep, trip)
                timeDist = firstDep - loc.ts
            elif (loc.ts >= lastArr):
                # Special case: if we're in between the last stop of this trip
                # and the first departure of the next, need to appropriately 
                # interpolate. If there is no next trip, then we're past the last 
                # trip of the block and we need to genuinely add some extra
                # time 'distance.'
                if nextTrip is not None:
                    self.getPostmileAtTime(loc.ts, trip)
                    timeDist = 0
                else:
                    postTrip = self.getPostmileAtTime(lastArr, trip)
                    timeDist = loc.ts - lastArr
            
            shape = self.gtfsData.getShapeFromShapeId(trip.shapeId)
            totalPost = shape.pointList[-1]['post']

            projLoc = self.gtfsData.projectToShape(shape, loc)
            postKm = projLoc.postKm
            perpKm = projLoc.perpKm

            postDist = abs(postTrip - postKm)
            # "half-circle fix' : if we've passed slightly into the next trip,
            # the difference between postKms will be very large (almost the full 
            # length of the shape), but in reality it should be small. (Think of
            # the distance between 11 and 1 on a clock.)
            if postDist > totalPost/2.0:
                postDist = totalPost - postDist
            
            # store (and return) the postKm of the most recent rawloc:
            finalPost = postKm
            
            dist = math.sqrt(((postDist)/self.vfid)**2 + timeDist**2.0 + (perpKm/self.vfid_perp)**2)
            distSeries.append(dist)
            timeSeries.append(float(time))

            if deviceId == 'c08084c15b99f9f':
                print time, loc.lat, loc.lon, trip, postTrip, postKm
                print dist
        
        # integrate distance time series (divided by total time) to find average distance
        totTime = float(locList[-1].ts - locList[0].ts)
        
        # an arbitrary weight, to ensure that more data is better:
        increaseFactor = (float(self.ageMillis)/totTime)**2.0
        
        dInt = integrate.simps(distSeries, timeSeries)
        dWeight = 1.0
        
        distAvg = numpy.mean(distSeries)
        distSeriesNorm = numpy.absolute(numpy.array(distSeries) - distAvg)
        
        # calculate "flatness" of locations wrt trip (larger integral means
        # that our locations are changing substantially wrt trip locations)
        fInt = integrate.simps(distSeriesNorm, timeSeries)
        # penalize this much more heavily (factor of 4.0 would be "neutral", I think)
        fWeight = 6.0

        # if deviceId == 'c08084c15b99f9f':
        #     print block
        #     print distSeries
                
        avgMillis = (dWeight*dInt + fWeight*fInt)/totTime*increaseFactor
        return avgMillis, finalPost
        
    
    def getLocAtTime(self, timeMillis, trip):
        tRelMillis = timeMillis - self.daystart
        nextTrip = self.gtfsData.getNextTripInBlock(trip.tripId)

        for i, st in enumerate(trip.stopTimeList):
            if (tRelMillis >= st.arrTimeMillis and tRelMillis <= st.depTimeMillis):
                # Should be waiting at the stop
                stop = self.gtfsData.getStopFromStopId(st.stopId)
                return stop.stopLat, stop.stopLon
            
            if (i+1 < len(trip.stopTimeList)):
                stNext = trip.stopTimeList[i+1]
                if (st.depTimeMillis < tRelMillis and tRelMillis < stNext.arrTimeMillis):
                    # Should be between two stops.
                    # OVERLY SIMPLIFIED IMPLEMENTATION!
                    # This function does simple interpolation between adjacent stops.
                    prevStop = self.gtfsData.getStopFromStopId(st.stopId)
                    nextStop = self.gtfsData.getStopFromStopId(stNext.stopId)
                    
                    pct = (tRelMillis-st.depTimeMillis)/float(stNext.arrTimeMillis-st.depTimeMillis)
                    
                    btwLat = prevStop.stopLat + pct*(nextStop.stopLat - prevStop.stopLat)
                    btwLon = prevStop.stopLon + pct*(nextStop.stopLon - prevStop.stopLon)
                    
                    return btwLat, btwLon

            else:
                # we're beyond the final stop
                if nextTrip is not None:
                    stNext = nextTrip.stopTimeList[0]
                    pct = (tRelMillis-st.depTimeMillis)/float(stNext.arrTimeMillis-st.depTimeMillis)

                    prevStop = self.gtfsData.getStopFromStopId(st.stopId)
                    nextStop = self.gtfsData.getStopFromStopId(stNext.stopId)

                    btwLat = prevStop.stopLat + pct*(nextStop.stopLat - prevStop.stopLat)
                    btwLon = prevStop.stopLon + pct*(nextStop.stopLon - prevStop.stopLon)
                    
                    return btwLat, btwLon

                
                
    def getPostmileAtTime(self, timeMillis, trip):
        # OVERLY SIMPLIFIED IMPLEMENTATION!
        # This function does simple postmile interpolation between adjacent stops.

        tRelMillis = timeMillis - self.daystart
        
        shape = self.gtfsData.getShapeFromShapeId(trip.shapeId)
        nextTrip = self.gtfsData.getNextTripInBlock(trip.tripId)
        
        for i, st in enumerate(trip.stopTimeList):
            if (tRelMillis >= st.arrTimeMillis and tRelMillis <= st.depTimeMillis):
                # Should be waiting at the stop
                print "waiting at stop..."
                print tRelMillis, st, st.arrTimeMillis, st.depTimeMillis
                return st.postKm

            if (i+1 < len(trip.stopTimeList)):
                stNext = trip.stopTimeList[i+1]
                if (st.depTimeMillis < tRelMillis and tRelMillis < stNext.arrTimeMillis):
                    # Should be between two stops.
                    pct = (tRelMillis-st.depTimeMillis)/float(stNext.arrTimeMillis-st.depTimeMillis)

                    print "between stops..."
                    print st, st.postKm, stNext, stNext.postKm, pct
                    
                    return st.postKm + pct*(stNext.postKm - st.postKm)
            else:
                # we're beyond the final stop
                if nextTrip is not None:
                    stNext = nextTrip.stopTimeList[0]
                    pct = (tRelMillis-st.depTimeMillis)/float(stNext.arrTimeMillis-st.depTimeMillis)

                    nextPost = stNext.postKm + shape.pointList[-1]['post']

                    print "beyond final stop..."
                    print st, st.postKm, stNext, nextPost, pct
                    
                    return st.postKm + pct*(nextPost - st.postKm)

                
            
    def getLastLocation(self, deviceId):
        tenMinutes = 10*60*1000;
        if self.locationBuckets[deviceId] is not None:
            locList = self.locationBuckets[deviceId].getRecent(tenMinutes)
            if len(locList) > 0:
                return locList[-1]
            
        return None
    
    
    def updateTime(self,time):
        self.time = time
        
        daystart = self.gtfsData.getDaystartFromTimestampMillis(time)
        self.daystart = max(self.daystart, daystart)
        
        for deviceId in self.locationBuckets:
            self.locationBuckets[deviceId].time = time


    def getBestTrip(self, deviceId, block):
        # This basically assumes we've already selected a block. Now find the
        # closest trip on that block.

        locList = self.locationBuckets[deviceId].getRecent(self.ageMillis)
        lastLoc = locList[-1]
        
        tripList = block.tripDict.values()
        tripList.sort()

        time = lastLoc.ts

        distList = []

        # loop over trips and find the closest one
        for trip in enumerate(tripList):
            trip = block.getTripForTime(time - self.daystart)

            firstDep = trip.getFirstDepartureMillis() + self.daystart
            lastArr = trip.getLastArrivalMillis() + self.daystart

            postTrip = self.getPostmileAtTime(time, trip)
            if (time > firstDep and time < lastArr):
                postTrip = self.getPostmileAtTime(time, trip)
                timeDist = 0
            elif (time <= firstDep):
                postTrip = self.getPostmileAtTime(firstDep, trip)
                timeDist = firstDep - time
            elif (time >= lastArr):
                postTrip = self.getPostmileAtTime(lastArr, trip)
                timeDist = time - lastArr
            
            shape = self.gtfsData.getShapeFromShapeId(trip.shapeId)
            projLoc = self.gtfsData.projectToShape(shape, lastLoc)
            postKm = projLoc.postKm
            perpKm = projLoc.perpKm
            
            dist = math.sqrt(((postTrip-postKm)/self.vfid)**2 + timeDist**2.0 + (perpKm/self.vfid_perp)**2)
            item = {'dist' : dist,
                    'trip' : trip }
            distList.append(item)
        
        sortedList = sorted(distList, key=lambda k: k['dist'])
        bestTrip = sortedList[0]['trip']

        return bestTrip
