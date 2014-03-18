'''
Created on Sep 2, 2013

@author: jacob
'''
import json
from pprint import pprint

# def createLocationFromProtobuf(locationMessage):
#     loc = Location()
#     
#     loc.deviceId = locationMessage.device_id
#     loc.ts = locationMessage.time
#     
#     if locationMessage.HasField('latitude'):
#         loc.lat = locationMessage.latitude
#     if locationMessage.HasField('longitude'):
#         loc.lon = locationMessage.longitude
#     if locationMessage.HasField('speed'):
#         loc.speed = locationMessage.speed
#     if locationMessage.HasField('bearing'):
#         loc.bearing = locationMessage.bearing
#     if locationMessage.HasField('accuracy'):
#         loc.accuracy = locationMessage.accuracy
#     
#     return loc
    
    
def createLocationFromJson(locationMessage):
    loc = Location()
    
    tempLoc = json.loads(locationMessage)
    
    loc.deviceId = tempLoc['device_id']
    loc.ts = tempLoc['time']
    
    if 'latitude' in tempLoc:
        loc.lat = tempLoc['latitude']
    if 'longitude' in tempLoc:
        loc.lon = tempLoc['longitude']
    if 'speed' in tempLoc:
        loc.speed = tempLoc['speed']
    if 'bearing' in tempLoc:
        loc.bearing = tempLoc['bearing']
    if 'accuracy' in tempLoc:
        loc.accuracy = tempLoc['accuracy']
    
    return loc


class Location(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.deviceId = None
        self.lat = None
        self.lon = None
        self.ts = None
        self.speed = None
        self.bearing = None
        self.accuracy = None
        
        
    def __str__(self):
        returnString = "location message from device %s at time %d" % (self.deviceId, self.ts)
        if (self.lat is not None):
            returnString += "\nlatitude is %f" % (self.lat)
        if (self.lon is not None):
            returnString += "\nlongitude is %f" % (self.lon)
        if (self.speed is not None):
            returnString += "\nspeed is %f" % (self.speed)
        if (self.bearing is not None):
            returnString += "\nbearing is %f" % (self.bearing)
        if (self.accuracy is not None):
            returnString += "\naccuracy is %f" % (self.accuracy)
            
        return returnString
    
    
    def hasLatAndLon(self):
        return self.lat is not None and self.lon is not None
        