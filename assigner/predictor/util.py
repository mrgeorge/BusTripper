'''
Created on Sep 19, 2013

@author: jacob
'''

import scipy.stats as stats

# distance threshold in km for departures and arrivals
thresh = 0.025

# fiducial bus speed, 12 km/hr in km/ms:
vfid = 12./60./60./1000.

# kilometers per degree of latitude
kmPerDeg = 111.

tDof = 3.761
tLoc = 56.431
# tScale = 73.738
tScale = 150.738

def probGivenDeviation(deviationMillis):
    dev = deviationMillis/1000.
    tCdf = stats.t.cdf(dev, tDof, tLoc, tScale)
    if (tCdf > 0.5):
        prob = (1.0-tCdf)*2.0
    else:
        prob = (tCdf)*2.0
            
#         print deviationMillis
#         print prob
    return prob