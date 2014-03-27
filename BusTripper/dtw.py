import numpy as np
import rpy2.robjects.numpy2ri
from rpy2.robjects.packages import importr

# Set up our R namespaces
R = rpy2.robjects.r
DTW = importr('dtw') # requires package installation


def DTWCost(val1, val2):
    """Euclidean distance - could expand this for other cost options"""
    diff = np.array([val2-val1])
    return np.sqrt(diff.dot(diff)) # l-2 norm, faster than np.linalg.norm

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
    return R.dtw(arr1, arr2, keep=True).rx('distance')[0][0]
