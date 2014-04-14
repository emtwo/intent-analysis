import json
import itertools
import datetime
import psycopg2
import sys
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
		plotInterestsTimeline(points)

def mapInterestToDatesSQL(interestData):
	points = {}
	for interestElement in interestData:
		interest = executeQuery("select name from categories where id='" + str(interestElement[5]) + "'")[0][0]
		day = interestElement[0]
		if interest not in points:
			points[interest] = {'x':[], 'weight':[]}
		points[interest]['x'].append(day)
		points[interest]['weight'].append(sum(eval(interestElement[2])))
	return points

###############################################################################
## JSON Queries
###############################################################################

def daysPostEpochToDate(dayCount):
	return datetime.datetime.fromtimestamp(int(dayCount) * 24 * 60 * 60)

def mapInterestToDates(interests, type, namespace):
	points = {}
	for day in interests:
		interestList = interests[day][type][namespace]
		for interest in interestList:
			if interest not in points:
				points[interest] = {'x':[], 'weight':[]}
			points[interest]['x'].append(daysPostEpochToDate(day))
			points[interest]['weight'].append(sum(interestList[interest]))
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
		plotInterestsTimeline(points)

###############################################################################
## Generic Functionality
###############################################################################

def cluster(points, weights):
	# Create an array with multiple entries per day instead of a weight per day
	clusterArr = []
	pointToWeightMap = {}
	for p, w in zip(points, weights):
		pointToWeightMap[dates.num2date(p[0])] = w
		for i in xrange(w): clusterArr.append(p)

	# Compute DBSCAN
	db = DBSCAN(eps=2, min_samples=1).fit(np.array(clusterArr))
	labelSet = set(db.labels_)

	# Number of clusters in labels, ignoring noise if present.
	n_clusters_ = len(labelSet) - (1 if -1 in db.labels_ else 0)
	print n_clusters_
	
	colors = cm.Spectral(np.linspace(0, 1, len(labelSet)))
	for k, col in zip(labelSet, colors):
		if k == -1: col = 'k' # black for noise.
		class_members = [index[0] for index in np.argwhere(db.labels_ == k)]
		for index in class_members:
			point = clusterArr[index]
			date = dates.num2date(point[0])
			plt.scatter(date, point[1], color=col, s=pointToWeightMap[date])

def plotInterestsTimeline(points):
	plt.xticks(rotation=25)
	plt.yticks(list(xrange(len(points))), [interest for interest in  points])
	plt.margins(0.05)

	for index, interest in enumerate(points):
		x = points[interest]['x']
		y = [index] * len(x)
		s = points[interest]['weight'];
		cluster(zip([dates.date2num(xval) for xval in x], y), s)

	plt.show()

#print executeQuery('SELECT version()')
loadAllFromJSON("payloads.txt", "keywords", "edrules", uuid="1c4a61042de89448a24002ad5983cd2b")
# Load data
#points = loadSinglePayload("marinas_interests", "keywords", "58-cat")
#plotInterestsTimeline(points)
#points = loadAllFromJSON("payloads.txt", "keywords", "edrules", count=3)
