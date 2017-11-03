import psycopg2 as ppg
import subprocess
import pickle

class parser(object):
	"""docstring for parser"""
	def __init__(self):
		super(parser, self).__init__()

	def createServerFromArgs(self, args):
		userver = {}
		# parse the args and create a server object and return
		userver["host"] = args.host
		userver["port"] = args.port
		userver["database"] = args.database
		if hasattr(args, "username"):
			userver["username"] = args.username
		if hasattr(args, "password"):
			if args.password is None:
				userver["password"] = ""
			else:
				userver["password"] = args.password
		return userver

	def readFromFile(self, filename):
		return pickle.load(open(filename, "rb"))

	def writeIntoFile(self, filename, obj):
		pickle.dump(obj, open(filename, "wb"))

def moveTablets(fromSite, toSite, tableName):
	"""
	fromSite, toSite : Contains the information about the site in form of a dict
	tableName : Relation name as strings
	Result : Creates a table at toSite with the same name as tableName and drops the
	table at fromSite
	"""
	fromHost = fromSite["host"]
	fromPort = fromSite["port"]
	fromDatabase = fromSite["database"]
	fromUser = fromSite["username"]
	fromPassword = fromSite["password"]

	toHost = toSite["host"]
	toPort = toSite["port"]
	toDatabase = toSite["database"]
	toUser = toSite["username"]
	toPassword = toSite["password"]	

	if fromPassword == "":
		fromPassStr = "-w %s"
	else:
		fromPassStr = "-W %s"

	if toPassword == "":
		toPassStr = "-w %s"
	else:
		toPassStr = "-W %s"

	bashCommand = "pg_dump -h %s -p %s -Fc -o -U %s " + fromPassStr + " -t %s -d %s | pg_restore -h %s -p %s -U %s " + toPassStr + " -d %s" 



	bashCommand = bashCommand % (fromHost, fromPort, fromUser, fromPassword, tableName, fromDatabase, toHost, toPort, toUser, toPassword, toDatabase)

	# print(bashCommand)

	process = subprocess.Popen(bashCommand, shell=True, stdout=subprocess.PIPE)
	output, error = process.communicate()

	# print(output, error)

	res = []
	# run the query
	with ppg.connect(
			host=fromHost,
			port=fromPort,
			dbname=fromDatabase,
			user=fromUser,
			password=fromPassword
			) as conn:
		try:
			with conn.cursor() as cur:
				cur.execute('DROP TABLE "%s";' % tableName)
				res = cur.fetchall()
		except ppg.Error as e:
			print(e)
		except ppg.ProgrammingError as e:
			print(e)

	# print(res)

def changeRelNameInQuery(querys, cname, tname):
	querys = querys.replace(" " + cname + " ", " " + tname + " ")
	querys = querys.replace(" " + cname + "(", " " + tname + "(")
	querys = querys.replace(" " + cname + ".", " " + tname + ".")
	return querys

def changeAggrTypeInQuery(querys, caggr, taggr):
	qs = querys.replace(caggr + "(", taggr + "(")
	return qs.replace(caggr + " (", taggr + "(")