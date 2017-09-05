import argparse
import threading
import psycopg2 as ppg

class parser(object):
	"""docstring for parser"""
	def __init__(self):
		super(parser, self).__init__()

	def createServerFromArgs(args):
		userver = {}
		# parse the args and create a server object and return
		userver["host"] = args.host
		userver["port"] = args.port
		userver["database"] = args.database
		if args.username is not None:
			userver["username"] = args.username
			userver["password"] = args.password
		return userver

class QueryDeploy(threading.Thread):
	"""docstring for QueryDeploy"""
	def __init__(self, site):
		super(QueryDeploy, self).__init__()
		# self.callback = arg
		self.site = site

	def run():
		# run the query
		with ppg.connect(
				host=site["host"],
				port=site["port"],
				dbname=site["database"],
				user=site["username"],
				password=site["password"]
				) as conn:

		# self.rs = resultFromQuery

	def join():
		super(QueryDeploy, self).join()
		return self.rs