import matplotlib.pyplot as plt
import numpy as np

import utils

def showDepot():
    circ = plt.Circle(utils.getDepotCoords()[::-1], radius=0.001, color='gray',
                      fill=False, lw=3, ls='dashed')
    ax = plt.gca()
    ax.add_patch(circ)

def plotCoords(rec, plotFilename=None, showPlot=False, colorType="time"):
    """Plot GPS coordinates as a map

    Inputs:
        rec - recarray with latitude,longitude columns
        plotFilename - string with path to save plot files
        showPlot - boolean (True) for interactive plot
        colorType - "time" or "speed" used to color points
    Outputs:
        plots of GPS stream displayed or saved
    """

    if colorType == "time":
        cArr = (rec.time-np.min(rec.time))/1000./3600
        cLabel = "Hours since start of day"
    elif colorType == "speed":
        cArr = rec.speed
        cLabel = "Speed (m/s)"
    else:
        raise ValueError(colorType)

    plt.clf()
    plt.scatter(rec.longitude, rec.latitude,
                c=cArr, s=10, linewidth=0,
                alpha=0.5)
    cbar = plt.colorbar()
    cbar.set_label(cLabel)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    showDepot()

    if showPlot:
        plt.show()
    if plotFilename is not None:
        plt.savefig(plotFilename)

def plotConfusionMatrix(cm, plotFilename=None, showPlot=False):
    plt.imshow(cm, interpolation="nearest")
    plt.colorbar()
    plt.xlabel("Predicted Trip")
    plt.ylabel("True Trip")

    if showPlot:
        plt.show()
    if plotFilename is not None:
        plt.savefig(plotFilename)
