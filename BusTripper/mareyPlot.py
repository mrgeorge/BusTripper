#pick a route (shape)
#get stops
#get stop locations
#linref to normalize lengths
#draw y axis

#get trips for shape (different blocks)
#get stop times for trips
#draw x axis

#connect trip stops with lines

#pick day
# get trips for shape and day
#get events for trips = true arrival/departure times
#overlay real trips on schedule

import numpy as np
import matplotlib.pyplot as plt

import shapely.geometry

import eventsDBManager

try:
    from pygtfs import gtfsDbManager
except ImportError:
    import os,sys
    path, filename = os.path.split(__file__)
    sys.path.append(os.path.abspath(os.path.join(path,"../assigner/")))
    from pygtfs import gtfsDbManager

gdb = gtfsDbManager.GtfsDbManager("../data/dbus_140221.db")

# pick a routeID and shapeID for now
routeID = u'5'
shapeID = u'50101'
day = 5 # 1=Monday, 7=Sunday

serviceIDs = gdb.getServiceIdsForDay(day)
shape = gdb.getShape(shapeID)
shapeCoords = [[pt['lat'], pt['lon']] for pt in shape.pointList]
shapeLS = shapely.geometry.asLineString(shapeCoords)

tripsDict = gdb.getTripsDict()
tripMatches = [ii for ii in tripsDict if tripsDict[ii]["routeId"]==routeID and \
               tripsDict[ii]["shapeId"]==shapeID and \
               tripsDict[ii]["serviceId"] in serviceIDs]

# all of these trips have the same shape
# get list of stops in order and relative distances
stopIDsOnShape = [stop['stopId'] for stop in gdb.getStopTimesForTripId(tripMatches[0])]
stopList = gdb.getStops(stopIdList = stopIDsOnShape)
stopNames = [s.stopName for s in stopList]
stopCoords = [[s.stopLat, s.stopLon] for s in stopList]
stopCoordsMP = shapely.geometry.asMultiPoint(stopCoords)
stopDistances = [shapeLS.project(smp, normalized=True) for smp in stopCoordsMP]

for tripID in tripMatches:
    stopTimesOnTrip = gdb.getStopTimesForTripId(tripID)
    stopHrs = np.array([[s['arrTimeMillis'], s['depTimeMillis']] for s in stopTimesOnTrip]).flatten()/1000./60./60.

    plt.plot(stopHrs, np.repeat(stopDistances,2))

plt.show()
