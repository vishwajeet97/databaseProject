from pg_query import Node, parse_sql
from tabulate import tabulate
from .helpers import QueryDeploy
import json

class fdd(object):
	"""docstring for fdd"""
	def __init__(self):
		super(fdd, self).__init__()

		self.site_dict = {}
		self.query_site = {}
		self.site_iterator = 0

	def displayServers(self):
		# prints the list of sites included in the system
		table = [[server["host"], server["port"], server["database"], server["username"], server["password"]] for key, server in self.site_dict.items()]
		print(tabulate(table, headers=["Host", "Port", "Database", "Username", "Password"], tablefmt="psql"))

	def addServer(self, userver):
		# add server to the list of sites
		# update the tablet controller
		self.site_dict[self.site_iterator] = userver
		self.site_iterator += 1

	def addServers(self, uservers):
		for server in uservers:
			self.addServer(server)

	def getServersList(self):
		return list(self.site_dict.values())

	def deleteServer(self, userver):
		# delete server from the list of sites
		# update the tablet controller
		for key, server in self.site_dict.items():
			if server["host"] == userver["host"] and server["port"] == userver["port"] and server["database"] == userver["database"]:
				del self.site_dict[key]
				return

	def SelectStmt(self, stmt):
		if stmt["op"] == 0:
			if len(stmt["fromClause"]) == 1:
				for key, server in self.site_dict.items():
					self.query_site[key] = self.qString

	def InsertStmt(self):
		pass

	def UpdateStmt(self):
		pass

	def DeleteStmt(self):
		pass

	def executeQuery(self, qString):
		# form the parse tree
		root = parse_sql(qString)
		qj = json.dumps(root, indent=4)

		self.qString = qString

		# print(type(root), type(root[0]["RawStmt"]))
		# print(qj)
		# print(len(root))

		if len(root) == 1:
			print(root[0]["RawStmt"]["stmt"])
			stmt = root[0]["RawStmt"]["stmt"]
			if stmt["SelectStmt"] is not None:
				self.SelectStmt(stmt["SelectStmt"])
			elif stmt["InsertStmt"] is not None:
				self.InsertStmt(stmt["InsertStmt"])
			elif stmt["DeleteStmt"] is not None:
				self.DeleteStmt(stmt["DeleteStmt"])
			elif stmt["UpdateStmt"] is not None:
				self.UpdateStmt(stmt["UpdateStmt"])

		# mux based on type of query
		# form the sub queries
		# determine the sites the sub queries are run on
		# spwan threads and deploy qqueries
		# receive call backs form the threads
		# combine the results
		# display the results

		# save QueryDeploy objects in array
		threads = {}
		for key, server in self.site_dict.items():
			threads[key] = QueryDeploy(server, self.query_site[key])
			# threads[key].start()

		res = {}
		# for key, thread in threads.items():
			# res[key] = thread.join()

		# take union of resultsets
		# for key, result in res.items():
			# print(key, result)

		pass