import matplotlib.pyplot as plt
import numpy as np

import io

def getDepotCoords():
    return (-1.96143, 43.3172) # lon, lat

def showDepot():
    circ = plt.Circle(getDepotCoords(), radius=0.001, color='gray',
                      fill=False, lw=3, ls='dashed')
    ax = plt.gca()
    ax.add_patch(circ)

def plotDeviceDate(cur, date='2013-08-01', deviceID='c08080a19e816bf',
            plotFilename=None, showPlot=False, colorType="time"):
    """Plot GPS stream over one day for a single device

    Inputs:
        cur - cursor to SQLite db (dbus_events.db)
        date - YYYY-MM-DD format for date selection
        deviceID - string
        plotFilename - string with path to save plot files
        showPlot - boolean (True) for interactive plot
        colorType - "time" or "speed" used to color points
    Outputs:
        plots of GPS stream displayed or saved
    """

    rec = io.selectData(cur, date=date, deviceID=deviceID)

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
    plt.title("{}   {}".format(date,deviceID))

    showDepot()

    if showPlot:
        plt.show()
    if plotFilename is not None:
        plt.savefig(plotFilename)

