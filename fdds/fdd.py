import pg_query
from tabulate import tabulate
from .helpers import QueryDeploy

from .helpers import server

class fdd(object):
	"""docstring for fdd"""
	def __init__(self):
		super(fdd, self).__init__()

		self.site_dict = {}
		self.site_iterator = 0

	def displayServers(self):
		# prints the list of sites included in the system
		table = [[server["host"], server["port"], server["database"]] for key, server in self.site_dict.items()]
		print(tabulate(table, headers=["Host", "Port", "Database"], tablefmt="psql"))

	def addServer(self, userver):
		# add server to the list of sites
		# update the tablet controller
		self.site_dict[self.site_iterator] = userver
		self.site_iterator += 1

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

		for key, server in self.site_dict.items():
			s = QueryDeploy(server, qString)
			s.start()
			res = s.join()
			print(res)
			# take union of resultsets


		pass