#May not be necessary

class LocationManager(object):

	def __init__(self, predictor):
		self.predictor = predictor

	def update(self):
		#currently needs a way of pulling someRawLocation from some DB file to feed to predictor

		self.predictor.newRawLocation(someRawLocation)


#python '/Users/davidtran/BusTripper/assigner/predictor/predictor.py' -a "DBus" -g '/Users/davidtran/BusTripper/data/dbus_events.db'