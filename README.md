BusTripper
==========

BusTripper is a project in development to improve transit scheduling 
and provide automated real-time arrival predictions. Using GPS data 
from Android devices onboard a fleet of buses, we can identify their
concurrent routes and compare their locations against a published 
schedule. BusTripper will learn from historical data to optimize 
routes and wait times, and aid in the synchronization of transit
systems to prevent issues like bus bunching.

BusTripper is being developed as part of a data science class project 
at UC Berkeley in coordination with Via Analytics.

External Dependencies
---------------------

BusTripper currently uses the following packages:

* python, and common libaries like numpy, pandas, sklearn, matplotlib, re, and
  sqlite3

* [R](http://www.r-project.org/), the
  [DTW package](http://dtw.r-forge.r-project.org/) for R, and the
  python wrapper [rpy2](http://rpy.sourceforge.net/rpy2.html)

* [shapely](https://pypi.python.org/pypi/Shapely) (optional) is used
  for a visualization tool.
