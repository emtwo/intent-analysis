import numpy as np
import matplotlib.dates as dates
from sklearn.cluster import DBSCAN

class Interest:
	def __init__(self, name):
		self.name = name
		self.dates = []
		self.weights = []
		self.weightSum = 0

	def addDateWeightPair(self, date, weight):
		self.dates.append(date)
		self.weights.append(weight)

	def computeDimensions(self, yVal):
		# Compute DBSCAN
		numberedDates = [dates.date2num(date) for date in self.dates]
		self.db = DBSCAN(eps=2, min_samples=1).fit(np.array(zip(numberedDates, [yVal] * len(self.dates))))

		self.clusterCount = len(set(self.db.labels_)) - (1 if -1 in self.db.labels_ else 0)
		self.maxWeight = max(self.weights)
		self.dayCount = len(self.dates)
		self.yVal = yVal
