import argparse
import pickle
import threading
import psycopg2 as ppg
import hashlib

class parser(object):
	"""docstring for parser"""
	def __init__(self):
		super(parser, self).__init__()

	def createServerFromArgs(self, args):
		userver = {}
		# parse the args and create a server object and return
		userver["host"] = args.host
		userver["port"] = args.port
		userver["database"] = args.database
		if hasattr(args, "username"):
			userver["username"] = args.username
		if hasattr(args, "password"):
			if args.password is None:
				userver["password"] = ""
			else:
				userver["password"] = args.password
		return userver

	def readFromFile(self, filename):
		return pickle.load(open(filename, "rb"))

	def writeIntoFile(self, filename, obj):
		pickle.dump(obj, open(filename, "wb"))

class QueryDeploy(threading.Thread):
	"""docstring for QueryDeploy"""
	def __init__(self, site, query):
		super(QueryDeploy, self).__init__()
		# self.callback = arg
		self.site = site
		self.query = query

	def run(self):

		res = []
		# run the query
		with ppg.connect(
				host=self.site["host"],
				port=self.site["port"],
				dbname=self.site["database"],
				user=self.site["username"],
				password=self.site["password"]
				) as conn:
			try:
				with conn.cursor() as cur:
					cur.execute(self.query)
					res = cur.fetchall()
			except ppg.Error as e:
				print(e)
			except ppg.ProgrammingError as e:
				print(e)
		self.res = res

	def join(self):
		super(QueryDeploy, self).join()
		return self.res

class TabletController(object):
	"""docstring for TabletController"""
	def __init__(self, nTablets, siteList):
		super(TabletController, self).__init__()
		self.tablets = nTablets
		self.siteList = siteList

	def hashFunction(self, key):
		return abs(hash(key)) % self.tablets

	def giveSitesList(self, query):
		# Parse insert tree to primary key, relation
		# Concat the primary ket to get string and then hash to get tablet id
		# Get the siteId from the map of (tableid, siteid)
		# return the (site, query)
		pass

	def createTabletMappingForRelation(self, query):
		#Check if mapping already exists
		# If not create an entry of the mapping between (tableid, siteid) for the relation
		# Then add this map to the master map with the relation map as the key
		pass