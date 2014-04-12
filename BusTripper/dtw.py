import numpy as np
import rpy2.robjects.numpy2ri
from rpy2.robjects.packages import importr

import utils

# Set up our R namespaces
R = rpy2.robjects.r
DTW = importr('dtw') # requires package installation
rpy2.robjects.numpy2ri.activate() # for converting np arrays to R

def DTWCost(val1, val2, distType="euclidean"):
    if distType == "euclidean":
        diff = np.array([val2 - val1]).flatten()
        return np.sqrt(diff.dot(diff)) # l-2 norm, faster than np.linalg.norm
    elif distType == "greatcircle":
        return utils.metersBetweenLatLonPair(val1[0], val1[1], val2[0], val2[1])
    else:
        raise ValueError(distType)

def DTWDistance(arr1, arr2):
    """Wikipedia implementation of basic DTW"""
    nRows = len(arr1)
    nCols = len(arr2)
    DTW = np.zeros((nRows+1, nCols+1))

    DTW[:, 0] = np.inf
    DTW[0, :] = np.inf
    DTW[0, 0] = 0.

    for ii in np.arange(1, nRows+1):
        for jj in np.arange(1, nCols+1):
            cost = DTWCost(arr1[ii-1], arr2[jj-1])
            DTW[ii, jj] = cost + np.min([DTW[ii-1, jj],
                                         DTW[ii  , jj-1],
                                         DTW[ii-1, jj-1]])
    return DTW[nRows,nCols]

def RDTW(arr1, arr2):
    """Call R's DTW and return distance"""
    # Calculate the alignment vector and corresponding distance
    return R.dtw(arr1, arr2, distance_only=True, open_begin=True,open_end=True,
                 step_pattern=R('rabinerJuangStepPattern(3, "c", TRUE)')
                 ).rx('distance')[0][0]
