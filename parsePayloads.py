import json
import itertools
import datetime
import psycopg2
import sys
from interest import Interest
from day import Day
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.dates as dates
from sklearn.cluster import DBSCAN

###############################################################################
## SQL Queries
###############################################################################

def executeQuery(query):
	con = None
	try:
	  con = psycopg2.connect(database='up_research', user='up')
	  cur = con.cursor()
	  cur.execute(query)
	  result = cur.fetchall()
	  return result
	except psycopg2.DatabaseError, e:
	  print 'Error %s' % e
	  sys.exit(1)
	finally:
	  if con:
		con.close()

def loadAllFromPostgres(type, namespace, uuid=None, count=1):
	userIds = executeQuery("select * from submission_interests as s left join users as u on s.user_id=u.id where uuid='" + uuid + "'") if uuid else executeQuery("select distinct user_id from submission_interests order by user_id asc")
	if not userIds: 
		print "No such user exists"
		return

	for i in xrange(count):
		allUserInterests = executeQuery("select * from submission_interests where user_id=" + str(userIds[i][0]) + " and type_namespace = '" + type + "." + namespace + "'")
		points = mapInterestToDatesSQL(allUserInterests)
		points = plotInterestsTimeline(points)
		computeIntents(points)

def mapInterestToDatesSQL(interestData):
	# Populate points data
	points = {}
	for interestElement in interestData:
		interest = executeQuery("select name from categories where id='" + str(interestElement[5]) + "'")[0][0]
		day = interestElement[0]
		if interest not in points:
			points[interest] = Interest(interest)
		points[interest].addDateWeightPair(day, sum(eval(interestElement[2])))

	# Compute dimensions for each interest
	for index, interest in enumerate(points):
		points[interest].computeDimensions(index)
	return points

###############################################################################
## JSON Queries
###############################################################################

def daysPostEpochToDate(dayCount):
	return datetime.datetime.fromtimestamp(int(dayCount) * 24 * 60 * 60)

def mapInterestToDates(interests, type, namespace):
	# Populate points data
	points = {}
	for day in interests:
		interestList = interests[day][type][namespace]
		for interest in interestList:
			if interest not in points:
				points[interest] = Interest(interest)
			points[interest].addDateWeightPair(daysPostEpochToDate(day), sum(interestList[interest]))

	# Compute dimensions for each interest
	for index, interest in enumerate(points):
		points[interest].computeDimensions(index)
	return points

def loadSinglePayload(file, type, namespace):
	file = open(file)
	payload = json.loads(file.readline())
	return mapInterestToDates(payload["interests"], type, namespace)

def savePayload(payload):
	f = open('interests', 'w+')
	f.write(payload)
	f.close()

def loadAllFromJSON(file, type, namespace, uuid=None, count=1):
	json_data = open(file);
	if (uuid): count = 1

	foundID = False
	for index, line in enumerate(json_data):
		if (not uuid and index == count) or foundID:
			break 
		if uuid:
			currID = json.loads(line)[1]["uuid"]
			if currID != uuid:
				continue
			foundID = True
			savePayload(line)
		data = json.loads(line)
		points = mapInterestToDates(data[1]["interests"], type, namespace)
		points = plotInterestsTimeline(points)
		computeIntents(points)

###############################################################################
## Generic Functionality
###############################################################################

def computeIntents(points):
	sortedByWeight = sorted(points.items(), key=lambda (k, v): v.maxWeight, reverse=True)
	sortedByClusterCount = sorted(points.items(), key=lambda (k, v): v.clusterCount, reverse=True)
	sortedByDayCount = sorted(points.items(), key=lambda (k, v): v.dayCount)

	rankWeight = 1
	rankCluster = 1
	rankDay = 1
	for i in xrange(len(points)):
		if (i > 0 and (sortedByWeight[i - 1][1].maxWeight != sortedByWeight[i][1].maxWeight)):
			rankWeight += 1
		if (i > 0 and (sortedByClusterCount[i - 1][1].clusterCount != sortedByClusterCount[i][1].clusterCount)):
			rankCluster += 1
		if (i > 0 and (sortedByDayCount[i - 1][1].dayCount != sortedByDayCount[i][1].dayCount)):
			rankDay += 1

		points[sortedByWeight[i][0]].weightSum += rankWeight
		points[sortedByClusterCount[i][0]].weightSum += rankCluster
		points[sortedByDayCount[i][0]].weightSum += rankDay

	sortedBySummedWeight = sorted(points.items(), key=lambda (k, v): v.weightSum)
	print [i[0] for i in sortedBySummedWeight]

def plotInterest(interest):
	dateToWeightMap = {}
	for d, w in zip(interest.dates, interest.weights):
		dateToWeightMap[d] = w

	labelSet = set(interest.db.labels_)
	colors = cm.Spectral(np.linspace(0, 1, len(labelSet)))
	for k, col in zip(labelSet, colors):
		if k == -1: col = 'k' # black for noise.
		class_members = [index[0] for index in np.argwhere(interest.db.labels_ == k)]
		for index in class_members:
			date = interest.dates[index]
			plt.scatter(date, interest.yVal, color=col, s=dateToWeightMap[date])

def plotInterestsTimeline(points):
	plt.xticks(rotation=25)
	plt.yticks(list(xrange(len(points))), [interest for interest in points])
	plt.margins(0.05)

	for index, interest in enumerate(points):
		plotInterest(points[interest])

	plt.show()
	return points

# Load data
loadAllFromPostgres("keywords", "edrules", count=1)
#points = loadAllFromJSON("payloads.txt", "keywords", "edrules", count=3)

#points = loadSinglePayload("marinas_interests", "keywords", "58-cat")
#plotInterestsTimeline(points)
