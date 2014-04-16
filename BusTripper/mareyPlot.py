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
import matplotlib.cm
from sklearn.preprocessing import LabelEncoder
import datetime

import shapely.geometry

import eventsDBManager
import utils

try:
    from pygtfs import gtfsDbManager
except ImportError:
    import os,sys
    path, filename = os.path.split(__file__)
    sys.path.append(os.path.abspath(os.path.join(path,"../assigner/")))
    from pygtfs import gtfsDbManager

gdb = gtfsDbManager.GtfsDbManager("../data/dbus_140221.db")
edb = eventsDBManager.EventsDB("../data/dbus_events.db")

# pick a routeID and shapeID for now
routeID = u'5'
shapeID = u'50101'
dt = datetime.datetime(2014,1,10)

serviceIDs = gdb.getServiceIdsForDate(dt)
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

le = LabelEncoder()
cmap = matplotlib.cm.jet
tripColors = le.fit_transform(tripMatches) % cmap.N

minHr = 25
maxHr = -1

allStopTimes = gdb.getAllStopTimes(tripIdList = tripMatches)

plt.clf()

for tripID, color in zip(tripMatches, tripColors):
#    stopTimesOnTrip = gdb.getStopTimesForTripId(tripID)
    stopTimesOnTrip = allStopTimes[tripID]
    stopHrs = np.array([[s['arrTimeMillis'], s['depTimeMillis']] for s in stopTimesOnTrip]).flatten()/1000./60./60.
    thisMinHr, thisMaxHr = np.min(stopHrs), np.max(stopHrs)
    if thisMinHr < minHr:
        minHr = thisMinHr
    if thisMaxHr > maxHr:
        maxHr = thisMaxHr

    events = edb.getEventsForTripDate(tripID, dt.date().isoformat())
    eventHrs = utils.getDayHours(events['time'], tzHrOffset=1)
    eventStopInds = [stopIDsOnShape.index(ev) for ev in events['stop_id']]
    eventDistances = [stopDistances[ind] for ind in eventStopInds]

    plt.plot(stopHrs, np.repeat(stopDistances,2), color=cmap(color))
    plt.plot(eventHrs, eventDistances, lw=5, ls='--', color=cmap(color), alpha=0.5)

ax = plt.gca()
ax.set_yticks(stopDistances)
ylabels = ax.get_yticks().tolist()
ylabels = stopNames
ax.set_yticklabels(ylabels)
ax.set_xticks(range(int(np.floor(minHr)), int(np.ceil(maxHr))))
plt.grid(True)

plt.show()
