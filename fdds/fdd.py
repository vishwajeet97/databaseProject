from pg_query import Node, parse_sql, parser
from tabulate import tabulate
from .helpers import QueryDeploy
from .helpers import TabletController
from .utils import changeAvgInQueryToSumCount, insertIntoSelectFromGroupby, aggregateVariableLocator
import json

class fdd(object):
	"""docstring for fdd"""
	def __init__(self):
		super(fdd, self).__init__()

		self.site_dict = {}
		self.query_site = {}
		self.site_iterator = 0

		self.tbc = TabletController(list(self.site_dict.keys()))

		# self.schema_data = {}

		# self.schema_data["stmts"] = list()
		# self.schema_data["pkmetadata"] = {

	def displayServers(self):
		# prints the list of sites included in the system
		table = [[server["host"], server["port"], server["database"], server["username"], server["password"]] for key, server in self.site_dict.items()]
		print(tabulate(table, headers=["Host", "Port", "Database", "Username", "Password"], tablefmt="psql"))

	def reinitialiseTBC(self):

		perm = True
		if len(self.tbc.getMetaData()["stmts"]) != 0:
			inp = input("WARNING: All schema data will be lost. Continue? (y/n) ")
			if inp != "Y" and inp != "y":
				perm == False

		if perm:
			del self.tbc
			self.tbc = TabletController(list(self.site_dict.keys()))

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

		print("List of sites: ", uinfo[1])
		print("Schema loaded: ", uinfo[0])

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
		print(qString,changeAvgInQueryToSumCount(qString))

		# if there is no target in select then don't worry about executing the query
		if("targetList" not in stmt["SelectStmt"].keys()):
			pass
		else:
			qString = changeAvgInQueryToSumCount(qString)
			print(qString)
			if("groupClause" in stmt["SelectStmt"].keys()):
				qString = insertIntoSelectFromGroupby(qString)
			print(qString)
			self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		
	def InsertStmt(self, stmt, qString):
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass

	def UpdateStmt(self, stmt, qString):
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass

	def DeleteStmt(self, stmt, qString):
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
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
		try:
			root = parse_sql(qString)
		except parser.ParseError as e:
			print(e)
			return

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
		# print(self.site_dict)
		for key, squery in self.query_site.items():
			threads[key] = {}
			for i, iquery in enumerate(squery):
				threads[key][i] = QueryDeploy(self.site_dict[key], iquery)
				threads[key][i].start()
			print(key, squery)

		self.query_site.clear()

		res = {}
		for key, sitethreads in threads.items():
			res[key] = {}
			for ind, thread in sitethreads.items():
				res[key][ind] = thread.join()

		print("Site-wise Query-wise Results")
		# take union of resultsets
		for key, result in res.items():
			print(key, result)

		print("Aggregated Results")

		stmt = root[0]["RawStmt"]["stmt"]
		if "SelectStmt" in stmt.keys() and "targetList" in stmt["SelectStmt"]:

			finalResult = self.aggregateResults(res, qString, stmt["SelectStmt"])
		else:
			finalResult = []

		print(finalResult)
		pass

	def aggregateResults(self, res, qString, selectStmt):
		"""
		Not going to work for average for the time being as it involves changing the query
		For average every avg should be replaced by sum and count and at the time
		of aggregation should have the original query as well
		Should return a list of tuples which can be printed as is.
		"""
		# First find if the query has group by, if yes then call aggregate group by
		# otherwise check if the select has aggregate functions if yes then call aggregate select functions
		# otherwise call the aggregate normal functions
		if "groupClause" in selectStmt.keys():
			numberOfGroupVariables = len(selectStmt["groupClause"])
			return self.aggregateGroupBy(res, selectStmt, numberOfGroupVariables)
		else:
			return self.aggregateInSelect(res, selectStmt)


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
					
					for index, operation in aggDict.items():
						if operation == "max":
							recordOldList[i] = max(recordOldList[i], recordList[i])
						if operation == "min":
							recordOldList[i] = min(recordOldList[i], recordList[i])							
						if operation == "count":
							recordOldList[i] += recordList[i] 
						if operation == "avg":
							recordOldList[i] += recordList[i]							
							recordOldList[i+1] += recordList[i+1]
						if operation == "sum":
							recordOldList[i] += recordList[i]

					finalResultDict[groupTuple] = recordOldList

		finalResult = []

		for keys, record in finalResultDict.items():
			recordList = []
			index = numberOfGroupVariables
			while(index < len(record)):
				if index not in aggDict.keys():
					recordList.append(record[index])
				elif aggDict[index] != "avg":
					recordList.append(record[index])
				else:
					recordList.append(record[index]*1.0/record[index+1])
					index += 1
				index += 1

			finalResult.append(tuple(recordList))

		return finalResult

	def aggregateInSelect(self, res, SelectStmt):
		ansList = []
		aggVariableLoc = aggregateVariableLocator(SelectStmt, 0)
		numTargetVariable = len(SelectStmt["targetList"])
		index = 0

		while index < numTargetVariable:
			if(index in aggVariableLoc.keys()):
				funcname = aggVariableLoc[index]
				if funcname.lower() == "min":
					min_num = res[0][0][index]
					for key, res_tuple in res.items():
						if(res_tuple[0][index] is not None and res_tuple[0][index] < min_num):
							min_num = res_tuple[0][index]
					ansList.append(min_num)
				elif funcname.lower() == "max":
					max_num = res[0][0][index]
					for key, res_tuple in res.items():
						if(res_tuple[0][index] is not None and res_tuple[0][index] > max_num):
							max_num = res_tuple[0][index]
					ansList.append(max_num)
				elif funcname.lower() == "count":
					count_num = 0
					for key, res_tuple in res.items():
						if(res_tuple[0][index] is not None):
							count_num = count_num + res_tuple[0][index]
					ansList.append(count_num)
				elif funcname.lower() == "sum":
					sum_num = 0
					for key, res_tuple in res.items():
						if(res_tuple[0][index] is not None):
							sum_num = sum_num + res_tuple[0][index]
					ansList.append(sum_num)
				elif funcname.lower() == "avg":
					sum_num = 0
					for key, res_tuple in res.items():
						if(res_tuple[0][index] is not None):
							sum_num = sum_num + res_tuple[0][index]
					index += 1
					count_num = 0
					for key, res_tuple in res.items():
						if(res_tuple[0][index] is not None):
							count_num = count_num + res_tuple[0][index]
					avg_num = sum_num / count_num
					ansList.append(avg_num)
			else:
				ansList.append(res[0][0][index])
			index += 1

		return ansList



