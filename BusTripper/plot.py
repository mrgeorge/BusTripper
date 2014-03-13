import matplotlib.pyplot as plt
import numpy as np

import io

def example(cur, plotDir):
    """Plot GPS stream over one day for a single device

    Inputs:
        cur - cursor to SQLite db (dbus_events.db)
        plotDir - directory to save plot files
    Outputs:
        plots of GPS stream saved to plotDir
    """

    rec = io.selectData(cur, date = '2013-08-01', deviceID='c08080a19e816bf')

    # color = time
    plt.clf()
    plt.scatter(rec.longitude, rec.latitude,
                c=(rec.time-np.min(rec.time))/1000./3600, s=10, linewidth=0,
                alpha=0.5)
    cbar = plt.colorbar()
    cbar.set_label("hours since start of day")
    plt.xlabel('longitude')
    plt.ylabel('latitude')
    plt.savefig(plotDir + "fig1.png")

    # color = speed
    plt.clf()
    plt.scatter(rec.longitude, rec.latitude, c=rec.speed, s=10, linewidth=0,
                alpha=0.5)
    cbar = plt.colorbar()
    cbar.set_label("speed")
    plt.xlabel('longitude')
    plt.ylabel('latitude')
    plt.savefig(plotDir + "fig2.png")
