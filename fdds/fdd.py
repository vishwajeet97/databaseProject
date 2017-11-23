from pg_query import Node, parse_sql, parser
from tabulate import tabulate
from .helpers import QueryDeploy
from .helpers import TabletController
from .utils import changeAvgInQueryToSumCount, insertIntoSelectFromGroupby, aggregateVariableLocator, pgSum, pgMax, pgMin, pgAvg, printer

import json
import psycopg2 as ppg

class fdd(object):
	"""docstring for fdd"""
	def __init__(self):
		super(fdd, self).__init__()

		self.site_dict = {}
		self.query_site = {}
		self.site_iterator = 0
		self.mastersite = 0
		self.masterserver = {}

		self.tbc = TabletController(list(self.site_dict.keys()), self.site_dict)

		# self.schema_data = {}

		# self.schema_data["stmts"] = list()
		# self.schema_data["pkmetadata"] = {

	def createMetadataSchema(self):
		create_site_info = "create table site_info (host varchar, port varchar, database varchar, username varchar, password varchar, primary key(host, port, database, username, password))"
		create_relations = "create table relations (relation_name varchar primary key)"
		create_relation_info = "create table relation_info (attribute_name varchar, attribute_index integer, relation_name varchar, is_pk boolean, primary key(attribute_name, relation_name), foreign key(relation_name) references relations)"
		create_tablet_info = "create table tablet_info (relation_name varchar, tablet_number integer, host varchar, port varchar, database varchar, username varchar, password varchar, tuple_count integer, primary key(relation_name, tablet_number), foreign key(relation_name) references relations, foreign key(host, port, database, username, password) references site_info)"
		create_stmt_list  = [create_site_info, create_relations, create_relation_info, create_tablet_info]
		for stmt in create_stmt_list:
			with ppg.connect(
				host=self.masterserver["host"],
				port=self.masterserver["port"],
				dbname=self.masterserver["database"],
				user=self.masterserver["username"],
				password=self.masterserver["password"]
			) as conn:
				try:
					with conn.cursor() as cur:
						cur.execute(stmt)
						res = cur.fetchall()
				except ppg.Error as e:
					printer("Metadata Creation Error", e)
				except ppg.ProgrammingError as e:
					printer("Metadata Creation Error", e)

	def initializeMetadata(self):	# initialize system state on start up
		init_sites = "select * from site_info"
		thread = QueryDeploy(self.masterserver, init_sites)
		thread.start()
		sites = thread.join() # list of tuples
		# check:
		site_index_dict = {}
		for i, site in enumerate(sites):
			server_dict = dict(zip(["host", "port", "database", "username", "password"], list(site)))
			self.site_dict[i] = server_dict
			site_index_dict[site] = i

		get_relations = "select * from relations"
		thread = QueryDeploy(self.masterserver, get_relations)
		thread.start()
		relns = thread.join()

		pk_dict = {}
		for reln in relns:
			init_pkmetadata ="select attribute_index, attribute_name from relation_info where relation_name = %s and is_pk = true"
			thread = QueryDeploy(self.masterserver, init_pkmetadata%(reln[0]))
			thread.start()
			attr_index_name = thread.join()
			pk_dict[reln[0]] = attr_index_name

		master_map = {}
		for reln in relns:
			master_map[reln[0]]={}
			get_tablet_site = "select tablet_number, host, port, database, username, password from tablet_info where relation_name = %s"
			thread = QueryDeploy(self.masterserver, get_tablet_site%(reln[0]))
			thread.start()
			tablet_site = thread.join()
			for t in tablet_site:
				site_index = site_index_dict[t[1:]]
				master_map[reln[0]][t[0]] = site_index

		# initialize tablet controller
		data = {}
		data[0] = {}
		data[0]["stmts"] = []
		data[0]["pkmetadata"] = pk_dict
		data[1] = master_map
		self.tbc.setMetaData(data)
		

	def displayServers(self):
		# prints the list of sites included in the system
		table = [[server["host"], server["port"], server["database"], server["username"], server["password"]] for key, server in self.site_dict.items()]
		print(tabulate(table, headers=["Host", "Port", "Database", "Username", "Password"], tablefmt="psql"))


	def displayResult(self, result, colName):
		table = [[x for x in tup] for tup in result]
		print(tabulate(table, headers=colName, tablefmt="psql"))

	def createRemoteServersAndForeignSchemas(self):

		masterquery = ""
		snfdw = "postgres_fdw"

		masterquery += " drop EXTENSION IF EXISTS " + snfdw + " cascade;"
		masterquery += " create EXTENSION " + snfdw + ";"
		
		for siteid, server in self.site_dict.items():
			if siteid == self.mastersite:
				continue

			sn = "site" + str(siteid)
			snserver = sn + "_server"
			snschema = sn

			masterquery += " create SERVER " + snserver
			masterquery += " FOREIGN DATA WRAPPER " + snfdw
			masterquery += " OPTIONS ("
			masterquery += " host '" + server["host"] + "',"
			masterquery += " port '" + server["port"] + "',"
			masterquery += " dbname '" + server["database"] + "'"
			masterquery += " );"

			masterquery += " create USER MAPPING "
			masterquery += " FOR " + self.site_dict[self.mastersite]["username"]
			masterquery += " SERVER " + snserver
			masterquery += " OPTIONS ("
			masterquery += " user '" + server["username"] + "',"
			masterquery += " password '" + server["password"] + "'"
			masterquery += " );"

		try:
			thread = QueryDeploy(self.site_dict[self.mastersite], masterquery)
			thread.start()
			res = thread.join()
		except Exception as e:
			printer("FTable Init Error", e)

	def reinitialiseTBC(self):

		perm = True
		if len(self.tbc.getMetaData()[0]["stmts"]) != 0:
			inp = input("WARNING: All schema data will be lost. Continue? (y/n) ")
			if inp != "Y" and inp != "y":
				perm == False

		if perm:
			del self.tbc
			self.tbc = TabletController(list(self.site_dict.keys()), self.site_dict)

			self.createRemoteServersAndForeignSchemas()

	def setMasterServer(self, userver):
		self.masterserver = userver
		pass

	def addServer(self, userver):
		# add server to the list of sites
		self.site_dict[self.site_iterator] = userver
		self.site_iterator += 1

		# update the tablet controller
		self.reinitialiseTBC()

	def addConfig(self, uinfo):
		for server in uinfo[1]:
			self.addServer(server)

		self.tbc.setMetaData(uinfo[0])

		printer("Debug", "List of sites: " +  str(uinfo[1]))
		printer("Debug", "Schema loaded: " + str(uinfo[0]))

	def getConfig(self):
		return [self.tbc.getMetaData(), list(self.site_dict.values())]

	def deleteServer(self, userver):
		# delete server from the list of sites
		for key, server in self.site_dict.items():
			if server["host"] == userver["host"] and server["port"] == userver["port"] and server["database"] == userver["database"]:
				del self.site_dict[key]
		
		# update the tablet controller
		self.reinitialiseTBC()

	def freezeSchema(self):
		pass

	def SelectStmt(self, stmt, qString):
		# if there is no target in select then don't worry about executing the query
		if("targetList" not in stmt["SelectStmt"].keys()):
			pass
		else:
			qString = changeAvgInQueryToSumCount(qString)
			if("groupClause" in stmt["SelectStmt"].keys()):
				qString = insertIntoSelectFromGroupby(qString)
			self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		
	def InsertStmt(self, stmt, qString):
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass

	def UpdateStmt(self, stmt, qString):
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass

	def DeleteStmt(self, stmt, qString):
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		# update tuple counts in TabletController
		pass

	def DropStmt(self, stmt, qString):
		# modify tablet controller after parsing
		# for site in list(self.site_dict.keys()):
			# self.query_site[site] = [qString]
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass

	def CreateStmt(self, stmt, qString):
		# create tablet controller after parsing

		self.tbc.createTableMetaData(stmt)
		
		# for site in list(self.site_dict.keys()):
			# self.query_site[site] = [qString]
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass

	def executeQuery(self, qString):
		# form the parse tree
		root = parse_sql(qString)

		qj = json.dumps(root, indent=4)
		# print(qj)

		# qString = qString

		if len(root) == 1:
			stmt = root[0]["RawStmt"]["stmt"]

			if "SelectStmt" in stmt.keys():
				self.SelectStmt(stmt, qString)
			
			elif "InsertStmt" in stmt.keys():
				self.InsertStmt(stmt, qString)
			
			elif "DeleteStmt" in stmt.keys():
				self.DeleteStmt(stmt, qString)
			
			elif "UpdateStmt" in stmt.keys():
				self.UpdateStmt(stmt, qString)
			
			elif "DropStmt" in stmt.keys():
				self.DropStmt(stmt, qString)
			
			elif "CreateStmt" in stmt.keys():
				self.CreateStmt(stmt, qString)
		else:
			printer("Error", "Enter only one query at a time")
			return

		# mux based on type of query
		# form the sub queries
		# determine the sites the sub queries are run on
		# spwan threads and deploy qqueries
		# receive call backs form the threads
		# combine the results
		# display the results

		# save QueryDeploy objects in array
		threads = {}
		description = []
		# print(self.site_dict)
		for key, squery in self.query_site.items():
			threads[key] = {}
			for i, iquery in enumerate(squery):
				threads[key][i] = QueryDeploy(self.site_dict[key], iquery)
				threads[key][i].start()
			printer("Debug", str(key) + str(squery))

		self.query_site.clear()

		res = {}
		for key, sitethreads in threads.items():
			res[key] = {}
			for ind, thread in sitethreads.items():
				res[key][ind] = thread.join()
			for ind, thread in sitethreads.items():
				description = thread.returnDescription()
				break

		printer("Debug", "Site-wise Query-wise Results")
		# take union of resultsets
		for key, result in res.items():
			printer("Debug", str(key) + str(result))

		printer("Debug", "Aggregated Results")

		stmt = root[0]["RawStmt"]["stmt"]
		if "SelectStmt" in stmt.keys() and "targetList" in stmt["SelectStmt"]:
			finalResult = self.aggregateResults(res, qString, stmt["SelectStmt"])
		else:
			finalResult = []

		self.displayResult(finalResult, description)
		pass

	def aggregateResults(self, res, qString, selectStmt):
		"""
		Not going to work for average for the time being as it involves changing the query
		For average every avg should be replaced by sum and count and at the time
		of aggregation should have the original query as well
		Should return a list of tuples which can be printed as is
		"""
		# First find if the query has group by, if yes then call aggregate group by
		# otherwise check if the select has aggregate functions if yes then call aggregate select functions
		# otherwise call the aggregate normal functions
		if "groupClause" in selectStmt.keys():
			numberOfGroupVariables = len(selectStmt["groupClause"])
			return self.aggregateGroupBy(res, selectStmt, numberOfGroupVariables)
		else:
			total_target = len(selectStmt["targetList"])
			count = 0
			for target in selectStmt["targetList"]:
				if "FuncCall" in target["ResTarget"]["val"]:
					count += 1

			if count == total_target:
				return self.aggregateInSelect(res, selectStmt)
			elif count == 0:
				return self.aggregateBasic(res)

	def aggregateGroupBy(self, res, selectStmt, numberOfGroupVariables):
		finalResultDict = {}
		aggDict = aggregateVariableLocator(selectStmt, numberOfGroupVariables)
		for siteKey, siteResult in res.items():
			for tabletKey, tabletResult in siteResult.items():
				for record in tabletResult:
					groupList = []
					recordList = []
					for i in range(0, len(record)):
						if i < numberOfGroupVariables:
							groupList.append(record[i])
						else:
							recordList.append(record[i])

					groupTuple = tuple(groupList)

					if groupTuple not in finalResultDict.keys():
						finalResultDict[groupTuple] = recordList
						continue

					recordOldList = finalResultDict[groupTuple]
					
					for i, operation in aggDict.items():
						if operation == "max":
							recordOldList[i] = pgMax(recordOldList[i], recordList[i])
						if operation == "min":
							recordOldList[i] = pgMin(recordOldList[i], recordList[i])							
						if operation == "count":
							recordOldList[i] += recordList[i] 
						if operation == "avg":
							recordOldList[i] = pgSum(recordOldList[i],recordList[i])							
							recordOldList[i+1] += recordList[i+1]
						if operation == "sum":
							recordOldList[i] = pgSum(recordOldList[i],recordList[i])

					finalResultDict[groupTuple] = recordOldList

		finalResult = []

		for keys, record in finalResultDict.items():
			recordList = []
			index = 0
			while(index < len(record)):
				if index not in aggDict.keys():
					recordList.append(record[index])
				elif aggDict[index] != "avg":
					recordList.append(record[index])
				else:
					recordList.append(pgAvg(record[index], record[index+1]))
					index += 1
				index += 1

			finalResult.append(tuple(recordList))

		return finalResult

	def aggregateInSelect(self, res, SelectStmt):
		aggDict = aggregateVariableLocator(SelectStmt, 0)

		overallValList = {}
		for i, op in aggDict.items():							
			if op == "count":
				overallValList[i] = 0
			elif op == "avg":
				overallValList[i] = None							
				overallValList[i+1] = 0
			else:
				overallValList[i] = None 

		for siteKey, siteResult in res.items():
			for tabletKey, tabletResult in siteResult.items():
				if len(tabletResult) > 0:
					record = tabletResult[0]
					for i, operation in aggDict.items():
						if operation == "max":
							overallValList[i] = pgMax(overallValList[i], record[i])
						if operation == "min":
							overallValList[i] = pgMin(overallValList[i], record[i])							
						if operation == "count":
							overallValList[i] += record[i]  
						if operation == "avg":
							overallValList[i] = pgSum(overallValList[i],record[i])							
							overallValList[i+1] += record[i+1]
						if operation == "sum":
							overallValList[i] = pgSum(overallValList[i],record[i])
		import operator
		a = [ y for x, y in sorted(overallValList.items(), key=operator.itemgetter(0) )]
		return(list(tuple(a)))

	def aggregateBasic(self, res):
		finalResult = []
		for siteKey, siteResult in res.items():
			for tabletKey, tabletResult in siteResult.items():
				finalResult.extend(tabletResult)

		return finalResult
