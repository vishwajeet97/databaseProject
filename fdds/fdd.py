import pg_query
from tabulate import tabulate
from .helpers import QueryDeploy

class fdd(object):
	"""docstring for fdd"""
	def __init__(self):
		super(fdd, self).__init__()

		self.site_dict = {}
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

	def executeQuery(self, qString):
		# form the parse tree
		# mux based on type of query
		# form the sub queries
		# determine the sites the sub queries are run on
		# spwan threads and deploy qqueries
		# receive call backs form the threads
		# combine the results
		# display the results

		# save QueryDeploy objects in array
		list_s = []
		for key, server in self.site_dict.items():
			s = QueryDeploy(server, qString)
			list_s.append(s)
			s.start()

		for s in list_s:
			res = s.join()
			print(res)
			
		# take union of resultsets


		pass