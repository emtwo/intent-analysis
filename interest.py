class Interest:
	def __init__(self, name):
		self.name = name
		self.dates = []
		self.weights = []
		self.weightSum = 0

	def addDateWeightPair(self, date, weight):
		self.dates.append(date)
		self.weights.append(weight)

	def setDimensions(self, maxWeight, clusterCount):
		self.maxWeight = maxWeight
		self.clusterCount = clusterCount
		self.dayCount = len(self.dates)