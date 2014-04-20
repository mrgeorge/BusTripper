
class rawLocation(object):
	def __init__(self, device_id, time, lat, lon, spd=None, bearing=None, accuracy=None, driver_id=None, bus_id=None, dt=None):
		self.deviceId = device_id
		self.ts = time
		self.lat = lat
		self.lon = lon
		self.spd = spd
		self.bearing = bearing
		self.accuracy = accuracy
		self.driver_id = driver_id
		self.bus_id = bus_id
		self.dt = dt

	def hasLatAndLon(self):
		return self.lat is not None and self.lon is not None