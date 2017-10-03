import argparse
import pickle
import threading
import psycopg2 as ppg
import hashlib
import random

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
	def __init__(self, nTablets, siteList, schema_data):
		super(TabletController, self).__init__()
		self.tablets = nTablets
		self.siteList = siteList
		self.master_map = {}
		self.schema_data = schema_data;

	def hashFunction(self, key):
		return abs(hash(key)) % self.tablets

	def giveSitesList(self, stmt):
		# Parse insert tree to get primary key, relation
		# Concat the primary key to get string and then hash to get tablet id
		# Get the siteId from the map of (tableid, siteid)
		# return the (site, query)
		if "InsertStmt" in stmt.keys():
			relname = stmt["InsertStmt"]["relation"]["RangeVar"]["relname"]
			valList = stmt["InsertStmt"]["selectStmt"]["SelectStmt"]["valuesLists"];
			# assume valList contains only one list
			if len(valList) != 1:
				print("Multiple values cannot be inserted")
				return None
			
			primary_key_attrs = self.schema_data["pkmetadata"][relname] # list of (index, attr_name) tuples
			primary_key_index, primary_key_name = zip(*primary_key_attrs)
			pk_attr_str = ""
			val_index = 0
			for i in valList[0]:
				if val_index in primary_key_index:
					valElement = i["A_Const"]["val"]
					for key in valElement.keys():
						for key1 in valElement[key]:
							v = valElement[key][key1]
							pk_attr_str += str(v)
				val_index += 1

			print("pk attr string ", pk_attr_str)
			if pk_attr_str == "":
				tablet_id = random.randint(0, self.tablets - 1)
			else:
				tablet_id = self.hashFunction(pk_attr_str)

			print("tablet id: ", tablet_id)
			return [self.master_map[relname][tablet_id]]

		elif "SelectStmt" in stmt.keys():
			# where clause of type "attr = value"
			from_list = stmt["SelectStmt"]["fromClause"]
			if len(from_list) > 1:
				return self.siteList

			relname = from_list[0]["RangeVar"]["relname"]
			if "whereClause" not in stmt["SelectStmt"].keys():
				return self.siteList
				
			attrList = stmt["SelectStmt"]["whereClause"]["A_Expr"]["lexpr"]["ColumnRef"]["fields"]
			primary_key_attrs = self.schema_data["pkmetadata"][relname]
			primary_key_index, primary_key_name = zip(*primary_key_attrs)
			if len(attrList) == 1:
				attr = attrList[0]["String"]["str"]
				# check that primary key is attr
				if len(primary_key_attrs) == 1 and primary_key_name[0] == attr:
					pk_attr_str = ""	
					valElement = stmt["SelectStmt"]["whereClause"]["A_Expr"]["rexpr"]["A_Const"]["val"]
					for key in valElement.keys():
						for key1 in valElement[key]:
							v = valElement[key][key1]
							pk_attr_str += str(v)

					print("pk attr string ", pk_attr_str)
					if pk_attr_str == "":
						return self.siteList
					else:
						tablet_id = self.hashFunction(pk_attr_str) # hashes to different tablet ids for same query in different runs of the program
						print("tablet id: ", tablet_id)
						return [self.master_map[relname][tablet_id]]
				else:
					return self.siteList		

		else:
			return self.siteList

				
	def createTabletMappingForRelation(self, tree):
		# If not create an entry of the mapping between (tableid, siteid) for the relation

		mapping = {}
		for i in range(0, self.tablets):
			mapping[i] = self.siteList[i % len(self.siteList)]

		self.master_map[tree["relation"]["RangeVar"]["relname"]] = mapping

		# Then add this map to the master map with the relation map as the key
		pass
