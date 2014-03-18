'''
Created on Sep 20, 2013

@author: jacob
'''
from pygtfs.location import Location
import json

def createJsonFromProjLoc(projLoc):
    projDict = {}
    
    projDict['device_id'] = projLoc.deviceId
    projDict['time'] = projLoc.ts
    
    if projLoc.lat is not None:
        projDict['latitude'] = projLoc.lat
    if projLoc.lon is not None:
        projDict['longitude'] = projLoc.lon
    if projLoc.speed is not None:
        projDict['speed'] = projLoc.speed
    if projLoc.bearing is not None:
        projDict['bearing'] = projLoc.bearing
    if projLoc.accuracy is not None:
        projDict['accuracy'] = projLoc.accuracy
    if projLoc.postKm is not None:
        projDict['postmile'] = projLoc.postKm*1000.
    if projLoc.routeId is not None:
        projDict['route_id'] = projLoc.routeId
    if projLoc.tripId is not None:
        projDict['trip_id'] = projLoc.tripId
    
    return json.dumps(projDict)

class ProjectedLocation(Location):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        super(ProjectedLocation, self).__init__()
        
        self.postKm = None
        self.perpKm = None
        self.tripId = None
        self.routeId = None