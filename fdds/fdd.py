from pg_query import Node, parse_sql, parser
from tabulate import tabulate
from .helpers import QueryDeploy
from .helpers import TabletController
import json

NTABLETS = 20

class fdd(object):
	"""docstring for fdd"""
	def __init__(self):
		super(fdd, self).__init__()

		self.site_dict = {}
		self.query_site = {}
		self.site_iterator = 0
		self.schema_data = dict()

		self.schema_data["stmts"] = list()
		self.schema_data["pkmetadata"] = {}


	def displayServers(self):
		# prints the list of sites included in the system
		table = [[server["host"], server["port"], server["database"], server["username"], server["password"]] for key, server in self.site_dict.items()]
		print(tabulate(table, headers=["Host", "Port", "Database", "Username", "Password"], tablefmt="psql"))

	def addServer(self, userver):
		# add server to the list of sites
		# update the tablet controller
		self.site_dict[self.site_iterator] = userver
		self.site_iterator += 1

	def addConfig(self, uinfo):
		self.schema_data = uinfo[0]
		for server in uinfo[1]:
			self.addServer(server)

		print("List of sites: ", uinfo[1])
		print("Schema loaded: ", uinfo[0])

	def getConfig(self):
		return [self.schema_data, list(self.site_dict.values())]

	def deleteServer(self, userver):
		# delete server from the list of sites
		# update the tablet controller
		for key, server in self.site_dict.items():
			if server["host"] == userver["host"] and server["port"] == userver["port"] and server["database"] == userver["database"]:
				del self.site_dict[key]
				return

	def freezeSchema(self):
		self.tbc = TabletController(NTABLETS, list(self.site_dict.keys()), self.schema_data)

		for operation in self.schema_data["stmts"]:
			self.tbc.createTabletMappingForRelation(operation["CreateStmt"])

	def SelectStmt(self, stmt, qString):
		if stmt["SelectStmt"]["op"] == 0:
			if len(stmt["SelectStmt"]["fromClause"]) == 1:
				self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
				# for s in sites:
					# self.query_site[s] = qString

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
		for site in list(self.site_dict.keys()):
			self.query_site[site] = [qString]
		# self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass

	def CreateStmt(self, stmt, qString):
		# create tablet controller after parsing
		self.schema_data["stmts"].append(stmt)

		# update primary key meta data
		relname = stmt["CreateStmt"]["relation"]["RangeVar"]["relname"]
		pkarray = []
		
		tbelts = stmt["CreateStmt"]["tableElts"]
		
		pknamelist = []
		pkdone = False

		for item in tbelts:
			if "Constraint" in item.keys():
				if item["Constraint"]["contype"] == 5:
					pknamelist = [ x["String"]["str"] for x in item["Constraint"]["keys"] ]
					pkdone = True

		count = 0
		for item in tbelts:
			if "ColumnDef" in item.keys():
				if pkdone:
					if item["ColumnDef"]["colname"] in pknamelist:
						index_name = (count, item["ColumnDef"]["colname"])
						pkarray.append(index_name)
				else:
					if "constraints" in item["ColumnDef"].keys():
						for cons in item["ColumnDef"]["constraints"]:
							# print(cons, type(cons))
							if cons["Constraint"]["contype"] == 5:
								index_name = (count, item["ColumnDef"]["colname"])
								pkarray.append(index_name)
				count += 1


		self.schema_data["pkmetadata"][relname] = pkarray
		
		for site in list(self.site_dict.keys()):
			self.query_site[site] = [qString]
		# self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
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

		finalResult = self.aggregateResults(res, self.query_info, qString, stmt)

		print(finalResult)

		pass

	def aggregateResults(self, result, qinfo, qString, qtree):
		"""
		Return the final tuples which needs to be printed in the form of list of tuples
		"""
		finRes = []

		if "SelectStmt" in stmt and stmt["SelectStmt"]["op"] == 0 and len(stmt["SelectStmt"]["fromClause"]) == 1:
			aggDict = {}
			if "groupClause" not in stmt["SelectStmt"]:
				isAggPresent = False
				for index, ResTarget in enumerate(stmt["SelectStmt"]["targetList"]):
					if "FuncCall" in ResTarget["ResTarget"]["val"]:
						isAggPresent = True
						funcname = ResTarget["ResTarget"]["val"]["FuncCall"]["funcname"][0]["String"]["str"]
						if funcname == "min":
							min_num = res[0][0][index]
							for key, res_tuple in res.items():
								if(res_tuple[0][index] is not None and res_tuple[0][index] < min_num):
									min_num = res_tuple[0][index]
							aggDict[index] = min_num
							pass
						elif funcname =="max":
							max_num = res[0][0][index]
							for key, res_tuple in res.items():
								if(res_tuple[0][index] is not None and res_tuple[0][index] > max_num):
									max_num = res_tuple[0][index]
							aggDict[index] = max_num
							pass
						elif funcname == "count":
							count_num = 0
							for key, res_tuple in res.items():
								if(res_tuple[0][index] is not None):
									count_num = count_num + res_tuple[0][index]
							aggDict[index] = count_num
							pass
						elif funcname == "sum":
							sum_num = 0
							for key, res_tuple in res.items():
								if(res_tuple[0][index] is not None):
									sum_num = sum_num + res_tuple[0][index]
							aggDict[index] = sum_num
							pass
						elif funcname == "avg":
							pass
						
				if isAggPresent:
					for key, value in aggDict.items():
						
				else:

			else:
		 		# Groupby is to be handled here
		 		pass

		return finRes