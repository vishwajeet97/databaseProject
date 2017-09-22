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
		self.schema_operations = []

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
		self.schema_operations = uinfo[0]
		for server in uinfo[1]:
			self.addServer(server)

	def getConfig(self):
		return [self.schema_operations, list(self.site_dict.values())]

	def deleteServer(self, userver):
		# delete server from the list of sites
		# update the tablet controller
		for key, server in self.site_dict.items():
			if server["host"] == userver["host"] and server["port"] == userver["port"] and server["database"] == userver["database"]:
				del self.site_dict[key]
				return

	def freezeSchema(self):
		self.tbc = TabletController(NTABLETS, list(self.site_dict.keys()))

		for operation in self.schema_operations:
			self.tbc.createTabletMappingForRelation(operation["CreateStmt"])

	def SelectStmt(self, stmt):
		stmt = stmt["SelectStmt"]
		if stmt["op"] == 0:
			if len(stmt["fromClause"]) == 1:
				for key, server in self.site_dict.items():
					self.query_site[key] = self.qString

	def InsertStmt(self, stmt):
		site = self.tbc.giveSitesList(stmt)[0]
		self.query_site[site] = self.qString
		pass

	def UpdateStmt(self, stmt):
		site = self.tbc.giveSitesList(stmt)[0]
		self.query_site[site] = self.qString
		pass

	def DeleteStmt(self, stmt):
		pass

	def DropStmt(self, stmt):
		# modify tablet controller after parsing
		for key, server in self.site_dict.items():
			self.query_site[key] = self.qString
		pass

	def CreateStmt(self, stmt):
		# create tablet controller after parsing
		self.schema_operations.append(stmt)
		
		for key, server in self.site_dict.items():
			self.query_site[key] = self.qString
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

		self.qString = qString

		if len(root) == 1:
			stmt = root[0]["RawStmt"]["stmt"]

			if "SelectStmt" in stmt.keys():
				self.SelectStmt(stmt)
			
			elif "InsertStmt" in stmt.keys():
				self.InsertStmt(stmt)
			
			elif "DeleteStmt" in stmt.keys():
				self.DeleteStmt(stmt)
			
			elif "UpdateStmt" in stmt.keys():
				self.UpdateStmt(stmt)
			
			elif "DropStmt" in stmt.keys():
				self.DropStmt(stmt)
			
			elif "CreateStmt" in stmt.keys():
				self.CreateStmt(stmt)

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
			threads[key] = QueryDeploy(self.site_dict[key], squery)
			threads[key].start()
			print(key, self.query_site[key])

		self.query_site.clear()

		res = {}
		for key, thread in threads.items():
			res[key] = thread.join()

		# take union of resultsets
		for key, result in res.items():
			print(key, result)

		pass