'''
Created on Sep 8, 2013

@author: jacob
'''
import math

kmPerDeg = 111.

def kmBetweenLatLonPair(lat1, lon1, lat2, lon2):
    R = 6371 # Radius of the earth in km                                                       
    dLat = degToRad(lat2-lat1)                                              
    dLon = degToRad(lon2-lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(degToRad(lat1)) * math.cos(degToRad(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
        
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1-a));
    d = R * c # Distance in km                                                                 
    return d

def degToRad(deg):
    return deg * (math.pi/180.0)