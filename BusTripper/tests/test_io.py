import os
import sys
import numpy as np

try:
    import BusTripper
except ImportError: # add parent dir to python search path
    path, filename = os.path.split(__file__)
    sys.path.append(os.path.abspath(os.path.join(path,"../")))
    import BusTripper


def test_io():
    pass
