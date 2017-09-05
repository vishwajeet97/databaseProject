import argparse
import threading
import psycopg2 as ppg

class parser(object):
	"""docstring for parser"""
	def __init__(self):
		super(parser, self).__init__()

	def createServerFromArgs(args):
		userver = server()
		# parse the args and create a server object and return
		return userver

class server(object):
	"""docstring for server"""
	def __init__(self, arg):
		super(server, self).__init__()
		self.arg = arg
		userver = {}
		# parse the args and create a server object and return
		userver["host"] = args.host
		userver["port"] = args.port
		userver["database"] = args.database
		if hasattr(args, "username") and hasattr(args, "password"):
			userver["username"] = args.username
			userver["password"] = args.password
		return userver

class QueryDeploy(threading.Thread):
	"""docstring for QueryDeploy"""
	def __init__(self, site, query):
		super(QueryDeploy, self).__init__()
		# self.callback = arg
		self.site = site
		self.query = query

	def run():
		# run the query
		with ppg.connect(
				host=site["host"],
				port=site["port"],
				dbname=site["database"],
				user=site["username"],
				password=site["password"]
				) as conn:
			try:
				resultSet = conn.execute(query)
			except ppg.Error as e:
				raise e
		# return resultSet
		self.rs = resultSet

	def join():
		super(QueryDeploy, self).join()
		return self.rs
