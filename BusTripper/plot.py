import matplotlib.pyplot as plt
import matplotlib.cm
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

def plotConfusionMatrix(cm, title=None, plotFilename=None, showPlot=False):
    plt.clf()
    plt.imshow(cm, interpolation="nearest", origin="lower",
               cmap=matplotlib.cm.jet)
    plt.colorbar()
    plt.xlabel("Prediction")
    plt.ylabel("Truth")

    if title is not None:
        plt.title(title)

    if showPlot:
        plt.show()
    if plotFilename is not None:
        plt.savefig(plotFilename)

def plotHistograms(arrs, colors, labels, xlabel, log=False,
                   plotFilename=None, showPlot=False):
    xMin = np.min(np.concatenate(arrs))
    xMax = np.max(np.concatenate(arrs))
    xRange = (xMin,xMax)
    nBins = xMax - xMin

    plt.clf()
    for arr, color, label in zip(arrs,colors,labels):
        plt.hist(arr, bins=nBins, range=xRange, color=color, label=label,
                 histtype="step", log=log)

    plt.xlabel(xlabel)
    plt.ylabel("N")
    plt.legend()

    xBuffer = 0.05*(xMax-xMin)
    plt.xlim((xMin-xBuffer, xMax+xBuffer))

    if log:
        plt.ylim(ymin = 0.07)

    if showPlot:
        plt.show()
    if plotFilename is not None:
        plt.savefig(plotFilename)
