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
    import os, sys
    path, filename = os.path.split(__file__)
    sys.path.append(os.path.abspath(os.path.join(path,"../assigner/")))
    from pygtfs import gtfsDbManager

def mareyPlot(routeID, shapeID, date, startHour, gtfsDBFile, eventsDBFile):
    gdb = gtfsDbManager.GtfsDbManager(gtfsDBFile)
    edb = eventsDBManager.EventsDB(eventsDBFile)

    dt = datetime.datetime(2014, 1, date)
    endHour = startHour + 6

    serviceIDs = gdb.getServiceIdsForDate(dt)
    shape = gdb.getShape(shapeID)
    shapeCoords = [[pt['lat'], pt['lon']] for pt in shape.pointList]
    shapeLS = shapely.geometry.asLineString(shapeCoords)

    tripsDict = gdb.getTripsDict()
    tripMatches = [ii for ii in tripsDict \
                   if tripsDict[ii]["routeId"]==routeID and \
                   tripsDict[ii]["shapeId"]==shapeID and \
                   tripsDict[ii]["serviceId"] in serviceIDs]

    # all of these trips have the same shape
    # get list of stops in order and relative distances
    stopIDsOnShape = [stop['stopId'] for stop in \
                      gdb.getStopTimesForTripId(tripMatches[0])]
    stopList = gdb.getStops(stopIdList = stopIDsOnShape)
    stopNames = [s.stopName for s in stopList]
    stopCoords = [[s.stopLat, s.stopLon] for s in stopList]
    stopCoordsMP = shapely.geometry.asMultiPoint(stopCoords)
    stopDistances = [shapeLS.project(smp, normalized=True) \
                     for smp in stopCoordsMP]

    le = LabelEncoder()
    cmap = matplotlib.cm.coolwarm
    tripColors = np.int64(np.array(le.fit_transform(tripMatches)) * np.float(cmap.N) /
                        len(tripMatches))

    minHr = 25
    maxHr = -1

    allStopTimes = gdb.getAllStopTimes(tripIdList = tripMatches)

    fig, ax = plt.subplots(figsize=(12,4))
    for tripID, color in zip(tripMatches, tripColors):
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

        ax.plot(stopHrs, np.repeat(stopDistances,2), color=cmap(color))
        ax.plot(eventHrs, eventDistances, lw=5, ls='--', color=cmap(color), alpha=0.5)

    ax.set_yticks(stopDistances)
    ylabels = ax.get_yticks().tolist()
    ylabels = stopNames
    ax.set_yticklabels(ylabels)

    ax.set_xticks(range(int(np.floor(minHr)), int(np.ceil(maxHr))))
    ax.set_xlim((startHour, endHour))
    ax.set_xlabel("Time")
    ax.grid(True)

    return fig
