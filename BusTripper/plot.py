import matplotlib.pyplot as plt
import matplotlib.cm
import numpy as np

import utils

def setLims(ax):
    ax.set_xlim((-2.03, -1.9284))
    ax.set_ylim((43.269, 43.33))

def showDepot(ax):
    circ = plt.Circle(utils.getDepotCoords()[::-1], radius=0.001, color='gray',
                      fill=False, lw=3, ls='dashed')
    ax.add_patch(circ)

def plotCoords(df, plotFilename=None, showPlot=True, colorType="time",
               bgMapFilename=None, figsize=None):
    """Plot GPS coordinates as a map

    Inputs:
        df - pandas dataframe with locations
        plotFilename - string with path to save plot files
        showPlot - boolean (True) for interactive plot
        colorType - "time" or "speed" used to color points
    Outputs:
        plots of GPS stream displayed or saved
    """

    if figsize is not None:
        fig = plt.figure(figsize=figsize)
    else:
        fig = plt.figure()

    ax = fig.add_subplot(111)

    setLims(ax)

    if bgMapFilename is not None:
        left, right = ax.get_xlim()
        bottom, top = ax.get_ylim()
        img = plt.imread(bgMapFilename)
        plt.imshow(img, extent = [left, right, bottom, top], zorder = 0,
                   alpha=0.8)
        plt.axis('off')
    else:
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')

    showDepot(ax)

    if colorType == "time":
        cArr = utils.getDayHours(df['time'])
        cLabel = "Time (hours)"
    elif colorType == "speed":
        cArr = df['speed'].values
        cLabel = "Speed (m/s)"
    else:
        raise ValueError(colorType)

    plt.scatter(df['longitude'], df['latitude'],
                c=cArr, s=10, linewidth=0,
                alpha=0.5)
    cbar = plt.colorbar()
    cbar.set_label(cLabel)

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
