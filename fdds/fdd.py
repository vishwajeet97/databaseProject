from pg_query import Node, parse_sql, parser
from tabulate import tabulate
from .helpers import QueryDeploy
from .helpers import TabletController
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
		self.query_site = self.tbc.getSiteQueryMapping(stmt, qString)
		pass
		
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

		# finalResult = self.aggregateResults(res, self.query_info, qString, stmt)

		# print(finalResult)
		pass

	def aggregateResults(self, result, qinfo, qString, qtree):

		aggDict = {}

		finRes = []

		if not qinfo["aggregate"]:

			for site, qress in result.items():
				for k, qres in qress.items():
					finRes.extend(qres)
			return finRes

		else:

			funclist = qinfo["aggregatetype"]

			return aggDict


	def print_new(self, res, stmt):
		if "SelectStmt" in stmt:
			if stmt["SelectStmt"]["op"] == 0:
				if len(stmt["SelectStmt"]["fromClause"]) == 1:
					aggDict = {}
					if "groupClause" not in stmt["SelectStmt"]:
						for index, ResTarget in enumerate(stmt["SelectStmt"]["targetList"]):
							if "FuncCall" in ResTarget["ResTarget"]["val"]:
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
							else:
								aggDict.append(res[0][index])

						for key, result in aggDict.items():
							print(result, end=' ')
					else:
				 		# Handle Group by clause
				 		for key, result in res.items():
				 			print(key, result)
				else:
					# join for multiple tables here
					for key, result in res.items():
			 			print(key, result)
				print("\n")
			else:
				# Not a select statement
				for key, result in res.items():
			 		print(key, result)
		else:
			for key, result in res.items():
			 		print(key, result)
