'''
Created on Sep 2, 2013

@author: jacob
'''

import sqlite3
from pygtfs.stopTime import StopTime
from pygtfs.stop import Stop
from pygtfs.shape import Shape
from pygtfs.service import Service
import logging
import time

class GtfsDbManager(object):
    '''
    classdocs
    '''


    def __init__(self, dbFileLoc, loggerName = None, service_id = None):
        self._dbFileLoc = dbFileLoc
        
        self.conn = sqlite3.connect(dbFileLoc)
        if loggerName is not None:
            self.loggerName = loggerName
        else:
            self.loggerName = "GtfsDbManager"
        self.logger = logging.getLogger(loggerName)
        
        self.service_id = service_id
        self.ensure_extra_tables()


    def ensure_extra_tables(self):
#        create_trips_table = "CREATE TABLE IF NOT EXISTS trips_simple AS select trip_id," \
#        + " route_id, block_id, service_id, direction FROM trips, directions WHERE" \
#        + " trips.shape_id = directions.shape_id;"
        create_trips_table = """CREATE TABLE IF NOT EXISTS trips_simple AS
            SELECT trip_id, route_id, block_id, service_id, direction_id
            FROM trips;
        """
        t_start = time.time()
        self.conn.execute(create_trips_table);
        t_end = time.time()
        self.logger.debug(str(t_end-t_start) + " s to create trips table")
        
        create_stop_times_table = "CREATE TABLE IF NOT EXISTS stop_times_simple AS" \
        + " SELECT trip_id, stop_id, " \
        + " (cast(substr(arrival_time, 1,2) AS int)/24)*24*3600 + strftime('%s', substr('0' ||" \
        + " cast(cast(substr(arrival_time, 1,2) AS int) % 24 AS string) ||" \
        + " substr(arrival_time,-6), -8)) % 86400 AS arr_sec," \
        + " (cast(substr(departure_time, 1,2) AS int)/24)*24*3600 + strftime('%s', substr('0' ||" \
        + " cast(cast(substr(departure_time, 1,2) AS int) % 24 AS string) ||" \
        + " substr(departure_time,-6), -8)) % 86400 AS dep_sec, stop_sequence FROM (select" \
        + " trip_id, substr('0'||arrival_time, -8) AS arrival_time," \
        + " substr('0'||departure_time, -8) AS departure_time, stop_id, stop_sequence" \
        + " FROM stop_times);"
        
        t_start = time.time()
        self.conn.execute(create_stop_times_table);
        t_end = time.time()
        self.logger.debug(str(t_end-t_start) + " s to create stop_times_simple")
        
#         create_index = "CREATE INDEX `idx1` ON `stop_times_simple` (`trip_id` ASC);"
#         
#         t_start = time.time()
#         self.conn.execute(create_index);
#         t_end = time.time()
#         print str(t_end-t_start) + " s to create index on stop_times_simple"
        
#         headways_table_name = "headways_" + self.service_id
        
#         self.logger.debug("creating table " + headways_table_name)
#         
#         create_headways_table = "CREATE TABLE IF NOT EXISTS " + headways_table_name \
#         + " AS SELECT t2.trip_id as trip_id, st1.stop_id as stop_id," \
#         + " min((st2.arrival_sec - st1.arrival_sec + 86400) % 86400) as headway" \
#         + " FROM stop_times_simple AS st1," \
#         + " stop_times_simple AS st2, trips_simple AS t1, trips_simple AS t2 WHERE" \
#         + " st1.trip_id=t1.trip_id AND st2.trip_id=t2.trip_id AND st1.stop_id =" \
#         + " st2.stop_id AND st1.trip_id <> t2.trip_id" \
#         + " AND t1.route_id=t2.route_id AND t1.direction=t2.direction GROUP" \
#         + " BY t2.trip_id, st1.stop_id ORDER BY t2.trip_id, st1.stop_id;"
#         
#         self.logger.debug(create_headways_table)
#         
#         t_start = time.time()
#         self.conn.execute(create_headways_table)
#         t_end = time.time()
#         self.logger.debug(str(t_end-t_start) + " s to create headways table")
        
    
    def getBlockIds(self, route_id_list=None):
        '''
        Return the list of blocks in the GTFS database.
        
        '''
        
        sqlQuery = "select distinct block_id from trips" 
        if (self.service_id is not None and route_id_list is not None):
            route_id_tup = tuple(route_id_list)
            sqlQuery += " where service_id = ? and route_id in ({0})".format(', '.join('?' for _ in route_id_tup))
            queryTuple = (self.service_id, ) + route_id_tup
            cursor = self.conn.execute(sqlQuery,queryTuple)
        elif (self.service_id is not None):
            sqlQuery += " where service_id = ?"
            cursor = self.conn.execute(sqlQuery,(self.service_id,))
        elif (route_id_list is not None):
            route_id_tup = tuple(route_id_list)
            sqlQuery += " where route_id in ({0})".format(', '.join('?' for _ in route_id_tup))
            cursor = self.conn.execute(sqlQuery,route_id_tup)
        else:
            sqlQuery += " order by block_id asc"
            cursor = self.conn.execute(sqlQuery)
        
        blockList = []
        
        for row in cursor:
            blockId = row[0]
            blockList.append(blockId)
            
        return blockList
    

    def getStopDemand(self,stopId):
        stopDemandDict = {}
        
        sqlQuery = "select hour, beta, route_id from demand " \
         + "where stop_id = ? and service_id = ?"
        
        cursor = self.conn.execute(sqlQuery,(stopId,self.service_id))
        
        routeIdList = []
        for row in cursor:
            if row[2] not in routeIdList:
                routeIdList.append(row[2])
                stopDemandDict[row[2]] = {}
            hr = '0' + str(row[0])
            stopDemandDict[row[2]][hr[-2:]] = row[1]
        
        return stopDemandDict
    
    
    def getServiceIds(self):
        '''
        Return the list of service ids in the GTFS database.
        
        '''
        
        sqlQuery = "select distinct service_id from calendar" \
            + " order by service_id asc"
                
        serviceIdList = []
        
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            serviceId = row[0]
            serviceIdList.append(serviceId)
            
        return serviceIdList
    
    
    def getStops(self, stopIdList=None):
        '''
        Return the list of stops in the GTFS database.
        
        '''

        stopList = []
        if stopIdList is None:
            sqlQuery = "select stop_id, stop_lat, stop_lon, stop_name from stops" \
                + " order by stop_id asc"
            cursor = self.conn.execute(sqlQuery)
        else:
            sqlQuery = "select stop_id, stop_lat, stop_lon, stop_name from stops" \
                + " where stop_id in ({0}) order by stop_id asc".format(', '.join(['?' for ii in stopIdList]))
            cursor = self.conn.execute(sqlQuery, tuple(stopIdList))

        for row in cursor:
            stop = Stop(row[0], float(row[1]), float(row[2]), stopName=row[3])
            stopList.append(stop)

        if stopIdList is not None:
            nStops = len(stopList)
            ind = [0] * nStops
            for ii in range(nStops):
                ss = Stop(stopIdList[ii],0,0) # dummy stop
                ind[ii] = stopList.index(ss)

            return [stopList[ii] for ii in ind]

        return stopList


    def getUniqueStops(self, route_id_list):
        '''
        Return the list of stops in the GTFS database that are visited on
        a given service and route.
        
        '''
        # Select the distinct trips covered in the route and service provided
        tripList = []
        route_id_tup = tuple(route_id_list)
        tripSqlQuery = "select trip_id from trips"

        if self.service_id is not None:
            tripSqlQuery += " where service_id = ?"
            tripSqlQuery += " and route_id in ({0})".format(', '.join('?' for _ in route_id_tup))
            queryTuple = (self.service_id, ) + route_id_tup
        else:
            tripSqlQuery += " where route_id in ({0})".format(', '.join('?' for _ in route_id_tup))
            queryTuple = route_id_tup
        
        # Select the distinct stops that will be visited
        stopList = []
        stopSqlQuery = "select distinct stops.stop_id, stops.stop_lat, stops.stop_lon" \
        " from stops" \
        " inner join stop_times on stops.stop_id = stop_times.stop_id" \
        " where stop_times.trip_id in ({0})".format(tripSqlQuery)

        stopCursor = self.conn.execute(stopSqlQuery, queryTuple)
        for row in stopCursor:
            stopList.append((row[0],row[1],row[2]))
        return stopList
    
    
    def getSchList(self, blockId):
        '''
        Return the list of schedule objects for the simulation tool.
        
        NOTE: should really be moved to GtfsData.
        
        '''
        
        schList = []
        
        # Retrieve all the trip ids for the given block
        tripList = self.getTripsForBlockId(blockId)
        
        # Retrieve all the scheduled arrivals/departures within all trips
        
        sqlQuery = "SELECT stop_times_simple.stop_sequence, " \
            + "arr_sec, dep_sec, stop_times_simple.stop_id, " \
            + "stop_times_simple.trip_id, trips_simple.block_id, trips_simple.route_id, trips_simple.direction " \
            + "FROM stop_times_simple " \
            + "INNER JOIN trips_simple ON trips_simple.trip_id = stop_times_simple.trip_id " \
            + "WHERE trips_simple.trip_id in ({0}) ".format(', '.join('?' for _ in tripList)) \
            + "ORDER BY arr_sec"
                    
        cursor = self.conn.execute(sqlQuery,tripList)
        for i, row in enumerate(cursor):
            first_stop_of_trip = False
            
            if i>0:
                prev_trip_id = schList[-1]['trip_id']
                prev_stop_id = schList[-1]['stop_id']
                if row[4] != prev_trip_id and row[3] == prev_stop_id:
                    schList[-1]['stop_seq'] = int(row[0])
                    schList[-1]['departure'] = int(row[2])
                    schList[-1]['trip_id'] = row[4]
                    schList[-1]['route_id'] = row[6]
                    schList[-1]['direction'] = row[7]
                    first_stop_of_trip = True
                    
            if not first_stop_of_trip:
                arrDict = {}
                arrDict['stop_seq'] = int(row[0])
                arrDict['arrival'] = int(row[1])
                arrDict['stop_id'] = row[3]
                arrDict['trip_id'] = row[4]
                arrDict['block_id'] = row[5]
                arrDict['route_id'] = row[6]
                arrDict['direction'] = row[7]
                if i > 0:
                    schList.append(arrDict)
                
                depDict = arrDict.copy()
                
                del depDict['arrival']
                depDict['departure'] = int(row[2])
                schList.append(depDict)
                
        end_dep = 'departure' in schList[-1]
        if end_dep:
            schList = schList[:-1]
                
        return schList
    
     
    def getShapeIds(self):
        '''
        Return the list of shape ids in the GTFS database.
        
        '''
        
        sqlQuery = "select distinct shape_id from shapes" \
            + " order by shape_id asc"
                
        shapeIdList = []
        
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            shapeIdList.append(row[0])
            
        return shapeIdList
    
    
    def getShape(self, shapeId):
        '''
        Return the shape corresponding to the given shape id in the GTFS database.
        
        '''
        
        sqlQuery = "select shape_id, shape_pt_lat, shape_pt_lon, "\
            + " shape_pt_sequence from shapes" \
            + " where shape_id=" + shapeId \
            + " order by shape_pt_sequence asc"
                
        shape = Shape(shapeId)
        
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            shape.addPoint(float(row[1]), float(row[2]), int(row[3]))
            
        return shape
    
    
    def getService(self, serviceId=None):
        '''
        Return the calendar information corresponding to the given 
        service id in the GTFS database.
        
        '''
        
        if serviceId is None and self.service_id is None:
            raise Exception('Need a service id!')
        elif serviceId is None:
            serviceId = self.service_id
        
        sqlQuery = "select start_date, end_date, monday, tuesday, "\
            + " wednesday, thursday, friday, "\
            + " saturday, sunday from calendar" \
            + " where service_id='" + serviceId + "'"
        
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            startDate = row[0]
            endDate = row[1]
            dayList = [x=='1' for x in row[2:9]]
            
            service = Service(serviceId, startDate, endDate, dayList)
        
        return service
    
    
    def getTripsForBlockId(self, blockId):
        '''
        Return the list of trips associated with the block id.
        
        Keyword arguments:
        blockId -- string representing block id.
        '''
        
        sqlQuery = "SELECT DISTINCT trip_id " \
                + "FROM trips " \
                + "WHERE block_id  = \"" + blockId + "\""
                
        tripIdList = []
        
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            tripId = row[0]
            tripIdList.append(tripId)
            
        return tripIdList
    
    
    def getTripsDict(self):
        '''
        Return a dictionary associating route and shape ids with trip ids.
        '''
        
        sqlQuery = "SELECT trip_id, shape_id, route_id, service_id " \
                + "FROM trips" \
                
        tripsDict = {}
        
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            tripDict = {'routeId' : row[2],
                        'shapeId' : row[1],
                        'serviceId' : row[3]}
            tripsDict[row[0]] = tripDict
            
        return tripsDict
        
    
    def getStopTimesForTripId(self, tripId):
        '''
        Return the shape id for the given trip.
        
        Keyword arguments:
        tripId -- string representing trip id.
        '''
        
        stopTimesList = []
        
        sqlQuery = "SELECT stop_sequence, " \
            + "arr_sec, dep_sec, stop_id " \
            + "FROM stop_times_simple " \
            + "WHERE trip_id = \"" + tripId + "\" " \
            + "ORDER BY stop_sequence"
            
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            stopTimeDict = {}
            stopTimeDict['stopSequence'] = int(row[0])
            stopTimeDict['arrTimeMillis'] = int(row[1])*1000
            stopTimeDict['depTimeMillis'] = int(row[2])*1000
            stopTimeDict['stopId'] = row[3]
            stopTimesList.append(stopTimeDict)
            
        return stopTimesList
    
    
    def getAllStopTimes(self):
        '''
        Return the shape id for the given trip.
        
        Keyword arguments:
        tripId -- string representing trip id.
        '''
        
        stopTimesDict = {}
        
        sqlQuery = "SELECT trip_id, stop_sequence, " \
            + "substr('0'||arrival_time,-8), substr('0'||departure_time,-8), stop_id " \
            + "FROM stop_times " \
            + "ORDER BY trip_id"
            
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            trip_id = row[0]
            if trip_id not in stopTimesDict:
                stopTimesDict[trip_id] = []
                
            stopTimeDict = {}
            stopTimeDict['stopSequence'] = int(row[1])
            stopTimeDict['arrTimeMillis'] = self.getMillisFromTimeString(row[2])
            stopTimeDict['depTimeMillis'] = self.getMillisFromTimeString(row[3])
            stopTimeDict['stopId'] = row[4]
            stopTimesDict[trip_id].append(stopTimeDict)
            
        return stopTimesDict
    
    
    def getShapeIdForTripId(self, tripId):
        '''
        Return the shape id for the given trip.
        
        Keyword arguments:
        tripId -- string representing trip id.
        '''
        
        shapeId = None
        
        sqlQuery = "SELECT shape_id from trips where trip_id='" + tripId + "'"
            
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            shapeId = row[0]
            
        return shapeId
    
    
    def getServiceIdForBlockId(self, blockId):
        '''
        Return the service id for the given block.
        
        Keyword arguments:
        blockId -- string representing GTFS block id.
        '''
        
        serviceId = None
        
        sqlQuery = "SELECT distinct service_id from trips where block_id='" + blockId + "'"
            
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            serviceId = row[0]
            
        return serviceId
    
    
    def getRouteIdForTripId(self, tripId):
        '''
        Return the route id for the given trip.
        
        Keyword arguments:
        tripId -- string representing trip id.
        '''
        
        routeId = None
        
        sqlQuery = "SELECT route_id from trips where trip_id='" + tripId + "'"
            
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            routeId = row[0]
            
        return routeId
    
    
    def get_stop_schedule(self, route_id_list, stop_id):
        '''
        Return the stop_times associated with a given stop / route / service_id.
        
        Keyword arguments:
        service_id, route_id, stop_id -- strings.
        '''
        
        sql_query = "SELECT trips_simple.trip_id, direction, arr_sec, block_id, stop_sequence" \
        + " FROM trips_simple, stop_times_simple" \
        + " WHERE trips_simple.trip_id=stop_times_simple.trip_id" \
        + " AND stop_id=? AND route_id=? AND service_id=?"\
        + " ORDER BY arr_sec;"
        
        sched_dict = {}
        for route_id in route_id_list:
            sched_dict[route_id] = {}
            
            cursor = self.conn.execute(sql_query,(stop_id, route_id,
                                                  self.service_id))
            for row in cursor:
                trip_id = row[0]
                direction = row[1]
                arrival_sec = int(row[2])
                block_id = row[3]
                stop_seq = row[4]
                
                if direction not in sched_dict[route_id]:
                    sched_dict[route_id][direction] = []
                    
                el = {'trip_id' : trip_id,
                      'arrival_time' : arrival_sec,
                      'block_id' : block_id, 
                      'stop_seq' : stop_seq}
                sched_dict[route_id][direction].append(el)
            
        return sched_dict
    
    
    def getMillisFromTimeString(self, timeString):
        hour, minute, second = timeString.split(":")
        return (int(second) + 60*int(minute) + 3600*int(hour))*1000
    
    
    def getTimezone(self):
        sqlQuery = "select agency_timezone from agency"
        
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            timezone = row[0] 
        
        return timezone


    ### Methods added to help with development of mareyPlot

    def getRouteIds(self):
        '''
        Return the list of route ids in the GTFS database.
        
        '''

        sqlQuery = "select distinct route_id from routes" \
            + " order by route_id asc"

        routeIdList = []

        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            routeIdList.append(row[0])

        return routeIdList

    def getServiceIdsForDay(self, day, exclude0000=True):
        """Return list of service_ids that match a given day

        Inputs:
            day - int monday=1, sunday =7
        """

        dayDict = {1:"monday", 2:"tuesday", 3:"wednesday", 4:"thursday",
                   5:"friday", 6:"saturday", 7:"sunday"}
        sqlQuery = "select service_id from calendar where {} = 1;".format(dayDict[day])
        serviceIdList = []
        cursor = self.conn.execute(sqlQuery)
        for row in cursor:
            serviceIdList.append(row[0])

        # Get rid of weird serviceId
        if exclude0000:
            serviceIdList = [ii for ii in serviceIdList if ii != '0000']

        return serviceIdList

    def getTripIdsForRouteIdServiceIds(self, routeId, serviceIdList):
        sqlQuery = "select trip_id from trips_simple where route_id = ?"\
            + " and service_id in ({})".format(', '.join('?' for ii in serviceIdList))
        queryTuple = (routeId,) + tuple(serviceIdList)

        tripIdList = []
        cursor = self.conn.execute(sqlQuery, queryTuple)
        for row in cursor:
            tripIdList.append(row[0])

        return tripIdList
