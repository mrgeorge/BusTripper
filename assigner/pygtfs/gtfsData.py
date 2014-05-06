'''
Created on Sep 2, 2013

@author: jacob
'''

from pygtfs.trip import Trip
from pygtfs.block import Block
from pygtfs.projectedLocation import ProjectedLocation
from pygtfs.gtfsDbManager import GtfsDbManager
from pygtfs.stopTime import StopTime
import time
from datetime import datetime
import pytz
import calendar
import math
from util import kmBetweenLatLonPair, kmPerDeg
from pygtfs.location import Location
import logging

class GtfsData(object):
    '''
    classdocs
    '''

    def __init__(self, agency, dbFileLoc, loggerName=None, service_id=None):
        '''
        Constructor
        '''
        
        if loggerName is not None:
            self.loggerName = loggerName
        else:
            self.loggerName = "gtfsData"
            
        self.logger = logging.getLogger(self.loggerName)
        
        self.logger.info('Initializing GtfsData')
        
        self.agency = agency
        
#         dbFileLoc = "../../../res/beartransit.db"
#         dbFileLoc = "../../../res/virtualDbus.db"
        
        dbManager = GtfsDbManager(dbFileLoc)
        
        self.blockDict = {}
        self.stopDict = {}
        self.shapeDict = {}
        self.serviceDict = {}
        
        self.service_id = service_id

        # Treat 4AM as the changeover between days (strictly speaking, we
        # should match trips on a per-trip basis)
        self.zeroHour = 4
        
        self.timezone = dbManager.getTimezone()
        
        self.dayDict = {0: 'monday',
                        1: 'tuesday',
                        2: 'wednesday',
                        3: 'thursday',
                        4: 'friday',
                        5: 'saturday',
                        6: 'sunday'}
        
        self.logger.info('Working on shapes')
        for shapeId in dbManager.getShapeIds():
            self.addShape(dbManager.getShape(shapeId))
        
        self.logger.info('Working on stops')
        for stop in dbManager.getStops():
            self.addStop(stop)
            
        self.logger.info('Working on services')
        if self.service_id is None:
            for serviceId in dbManager.getServiceIds():
                self.addService(dbManager.getService(serviceId))
        else:
            self.addService(dbManager.getService(self.service_id))
        
        self.logger.info('Creating temporary dict of stop times')
        t_start = time.time()
        tempStopTimeDict = dbManager.getAllStopTimes()
        t_end = time.time()
        self.logger.info(str(t_end-t_start) + 's to create dict of stop times')
        
        self.logger.info('Creating temporary dict of trips')
        t_start = time.time()
        tempTripDict = dbManager.getTripsDict()
        t_end = time.time()
        self.logger.info(str(t_end-t_start) + 's to create dict of trips')
        
        for blockId in dbManager.getBlockIds(self.service_id):
            serviceId = dbManager.getServiceIdForBlockId(blockId)
            tripsInBlock = dbManager.getTripsForBlockId(blockId)
            self.addBlock(self.createBlock(blockId, serviceId, tripsInBlock,
                                           tempStopTimeDict, tempTripDict))
            
    
    # to handle pickling
    def __getstate__(self):
        d = dict(self.__dict__)
        del d['logger']
        return d
    
    def __setstate__(self, d):
        self.__dict__.update(d)
        self.logger = logging.getLogger(self.loggerName)
        
            
    def createBlock(self, blockId, serviceId, tripsInBlock, stopTimeDict, tripDict):
        '''
        Return a Block object corresponding to the given id.
            
        Keyword arguments:
        blockId -- string representing block id.
        '''
        
        # Instantiate empty block:
        block = Block(blockId, serviceId)
        self.logger.info('Working on block ' + blockId)
        
        for tripId in tripsInBlock:
            self.logger.debug(tripId)
            try:
                shapeId = tripDict[tripId]['shapeId']
                routeId = tripDict[tripId]['routeId']
                stopTimesDictList = stopTimeDict[tripId]
                trip = Trip(tripId, blockId, routeId, shapeId)
                shape = self.getShapeFromShapeId(shapeId)
                for stopTime in stopTimesDictList:
                    stop = self.getStopFromStopId(stopTime['stopId'])
                    lat, lon = stop.stopLat, stop.stopLon
                    loc = Location()
                    loc.lat = lat
                    loc.lon = lon
                    projLoc = self.projectToShape(shape, loc)
                    newStopTime = StopTime(tripId,
                                           stopTime['stopId'],
                                           stopTime['stopSequence'],
                                           stopTime['arrTimeMillis'],
                                           stopTime['depTimeMillis'],
                                           projLoc.postKm)
                    trip.addStopTime(newStopTime)
                    
                block.addTrip(trip)
            except Exception as e:
                self.logger.warning('Failed to create trip ' + tripId + '.')
                #self.logger.except(e)
        
        return block
        
        
    def addBlock(self, block):
        if (block.blockId not in self.blockDict):
            self.blockDict[block.blockId] = block
            
    
    def addStop(self, stop):
        if (stop.stopId not in self.stopDict):
            self.stopDict[stop.stopId] = stop
            
            
    def addShape(self, shape):
        if (shape.shapeId not in self.shapeDict):
            self.shapeDict[shape.shapeId] = shape
            
            
    def addService(self, service):
        if (service.serviceId not in self.serviceDict):
            self.serviceDict[service.serviceId] = service
            
    
    def inBlockList(self, block):
        return (block in self.blockDict.values())
    
    
    def getBlockFromBlockId(self, blockId):
        for block in self.blockDict.values():
            if block.blockId == blockId:
                return block
                
        return None
    
    
    def getTripFromTripId(self, tripId):
        for block in self.blockDict.values():
            if tripId in block.tripDict:
                return block.tripDict[tripId]
                
        return None
    
    
    def getNextTripInBlock(self, tripId):
        trip = self.getTripFromTripId(tripId)
        tripList = None
        for block in self.blockDict.values():
            if tripId in block.tripDict:
                tripList = block.tripDict.values()
                break
        
        if tripList is not None:
            tripList.sort()
            thisTripIndex = tripList.index(trip)
            if (thisTripIndex < len(tripList) - 1):
                nextTripIndex = thisTripIndex + 1
                return tripList[nextTripIndex]
            else:
                return None
            
        return None
    
    
    def getStopFromStopId(self, stopId):
        if stopId in self.stopDict:
            return self.stopDict[stopId]
        
        return None
    
    
    def getShapeFromShapeId(self, shapeId):
        if shapeId in self.shapeDict:
            return self.shapeDict[shapeId]
        
        return None
    
    def projectToShape_Shapely(self, shape, rawLoc):
        """Replacement for projectToShape if user has shapely"""
        try:
            import shapely.geometry
        except ImportError:
            return self.projectToShape(shape, rawLoc)

        point = shapely.geometry.asPoint((rawLoc.lon, rawLoc.lat))
        line = shapely.geometry.asLineString([[pt['lon'], pt['lat']]
                                              for pt in shape.pointList])
        projPt = line.interpolate(line.project(point))

        projLoc = ProjectedLocation()
        projLoc.lat = projPt.y
        projLoc.lon = projPt.x
        projLoc.perpKm = kmBetweenLatLonPair(point.y, point.x,
                                             projPt.y, projPt.x)
        projLoc.postKm = (line.project(projPt, normalized=True) *
                          shape.pointList[-1]['post'])

        return projLoc

    def projectToShape(self, shape, rawLoc):
        # lat, lon constitute the origin of our local coordinate system.
        # Latitudes represent y-distances, longitudes x-distances (need to
        # correct for the local latitude, though). Iterate over points in 
        # the shape to find the point on a line segment which is closest to
        # the given origin.
        
        # Without worrying about ellipsoidalness, this gives the relationship 
        # between the length of a point of latitude and the length of a point
        # of longitude (to within ~0.5% for ~spherical Earth).
        
        lat = rawLoc.lat
        lon = rawLoc.lon
        
        convFactor = math.cos(lat*math.pi/180.)
        
        perpDistMin = 0.
        
        for i, point in enumerate(shape.pointList):
            if (i < len(shape.pointList) - 1):
                y1 = point['lat']-lat
                y2 = shape.pointList[i+1]['lat']-lat
                x1 = (point['lon']-lon)*convFactor
                x2 = (shape.pointList[i+1]['lon']-lon)*convFactor
                xproj, yproj = self.getClosestPointOnSegment(0., 0., x1, x2, y1, y2)
                perpDist = (xproj**2.0 + yproj**2.0)**0.5
                if (i == 0 or perpDist < perpDistMin):
                    perpDistMin = perpDist
                    xmin, ymin = xproj, yproj
                    segLen = ((x2-x1)**2.0 + (y2-y1)**2.0)**0.5
                    if (segLen > 0.0):
                        frac = ((xproj-x1)**2.0 + (yproj-y1)**2.0)**0.5 / segLen
                        postKm = point['post'] + frac*(shape.pointList[i+1]['post'] - point['post'])
                    else:
                        postKm = point['post']
                        
        # Calculate lateral distance.
        lonProj = xmin/convFactor + lon
        latProj = ymin + lat
        perpKm = kmBetweenLatLonPair(lat, lon, latProj, lonProj)
        
        projLoc = ProjectedLocation()
        projLoc.lat = latProj
        projLoc.lon = lonProj
        projLoc.postKm = postKm
        projLoc.perpKm = perpKm
        
        return projLoc
    
    
    def projectToShapeWithTarget(self, shape, rawLoc, postTarget):
        # lat, lon constitute the origin of our local coordinate system.
        # Latitudes represent y-distances, longitudes x-distances (need to
        # correct for the local latitude, though). Iterate over points in 
        # the shape to find the point on a line segment which is closest to
        # the given origin.
        
        # Without worrying about ellipsoidalness, this gives the relationship 
        # between the length of a point of latitude and the length of a point
        # of longitude (to within ~0.5% for ~spherical Earth).
        lat = rawLoc.lat
        lon = rawLoc.lon
        
        convFactor = math.cos(lat*math.pi/180.)
        
        sumMin = 0.
        
        for i, point in enumerate(shape.pointList):
            if (i < len(shape.pointList) - 1):
                y1 = point['lat']-lat
                y2 = shape.pointList[i+1]['lat']-lat
                x1 = (point['lon']-lon)*convFactor
                x2 = (shape.pointList[i+1]['lon']-lon)*convFactor
                
                # project to segment
                xproj, yproj = self.getClosestPointOnSegment(0., 0., x1, x2, y1, y2)
                
                # get lateral distance
                perpDist = (xproj**2.0 + yproj**2.0)**0.5
                segLen = ((x2-x1)**2.0 + (y2-y1)**2.0)**0.5
                
                # get the post-km
                if (segLen > 0.0):
                    frac = ((xproj-x1)**2.0 + (yproj-y1)**2.0)**0.5 / segLen
                    postKm = point['post'] + frac*(shape.pointList[i+1]['post'] - point['post'])
                else:
                    postKm = point['post']
                    
                postDev = math.fabs(postKm - postTarget)/kmPerDeg
                        
                if (i == 0 or (perpDist+postDev) < sumMin):
                    sumMin = perpDist+postDev
                    xmin, ymin = xproj, yproj
#                     print "postKm, postTarget:"
#                     print postKm, postTarget
#                     print "sumMin, perpDist, postDev"
#                     print sumMin, perpDist, postDev
                    postKmMin = postKm
                        
        # Calculate lateral distance.
        lonProj = xmin/convFactor + lon
        latProj = ymin + lat
        perpKm = kmBetweenLatLonPair(lat, lon, latProj, lonProj)
        
        projLoc = ProjectedLocation()
        projLoc.lat = latProj
        projLoc.lon = lonProj
        projLoc.postKm = postKmMin
        projLoc.perpKm = perpKm
        
        return projLoc
        
        
    def getClosestPointOnSegment(self, xp, yp, x1, x2, y1, y2):
        segLen = ((x2-x1)**2.0 + (y2-y1)**2.0)**0.5
        if (segLen == 0.0):
            return x1, y1
        
        t = ((xp-x1)*(x2-x1) + (yp-y1)*(y2-y1))/(segLen*segLen)
        
        if (t < 0.):
            return x1, y1
        elif (t > 1.):
            return x2, y2
        xproj = x1 + t*(x2-x1)
        yproj = y1 + t*(y2-y1)
        
        return xproj, yproj
    
    
    def getDaystartFromTimestampMillis(self, timeMillis):
        mydate = self.getDatetimeForTimestampMillis(timeMillis)
        return self.getTimestampMillisForDate(mydate, 0, 0, 0)
        
        
    def getDatetimeForTimestampMillis(self, timestampMillis):
        timestamp = timestampMillis/1000
        mytz = pytz.timezone(self.timezone)
        mydatetime = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.timezone('UTC')).astimezone(mytz)
        return mydatetime
    
    
    def getTimestampMillisForDate(self, date, hour, minute, sec):
        # date should be a datetime object
        
        # initialize time strings for given datetime object
        dateFormat = "%Y-%m-%d %H:%M:%S"
        timeStr = "%04d-%02d-%02d %02d:%02d:%02d" % (date.year, date.month,
                                                     date.day, hour, minute, sec)

        # localize to given time zone
        mytz = pytz.timezone(self.timezone)
        mytime = mytz.localize(datetime.strptime(timeStr,dateFormat))

        # get timestamps in milliseconds
        stamp = calendar.timegm(mytime.utctimetuple())*1000

        return stamp
    
    def getTimestampForDateString(self, dateString, hour, minute, sec):
        # dateString should be in YYYYMMDD format
        
        # handle hours > 
        addDays = 0
        while (hour > 23):
            addDays += 1
            hour -= 24
            
        dateFormat = "%Y%m%d %H:%M:%S"
        timeStr = dateString + " %02d:%02d:%02d" % (hour, minute, sec)
        
        # localize to time zone
        mytz = pytz.timezone(self.timezone)
        mytime = mytz.localize(datetime.strptime(timeStr,dateFormat))
        
        # get timestamps in milliseconds
        stamp = calendar.timegm(mytime.utctimetuple())*1000
        
        stamp += addDays*24*60*60*1000

        return stamp
    
    
    def getTodayBlocks(self, time):
        date = self.getDatetimeForTimestampMillis(time)
        
        #dayInt is 0 for Sunday, 1 for Monday, etc
        dayInt = date.weekday()
        
        blockList = []
        for block in self.blockDict.values():
            service = self.serviceDict[block.serviceId]
            
            # dates are in YYYYMMDD string format
            startDate = service.startDate
            endDate = service.endDate
            
            startTime= self.getTimestampForDateString(startDate, self.zeroHour, 0, 0)
            endTime= self.getTimestampForDateString(endDate, self.zeroHour, 0, 0)
            
            if (service.dayList[dayInt] and startTime < time
                and endTime > time):
                blockList.append(block)
                 
        return blockList
