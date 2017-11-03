import psycopg2 as ppg
import subprocess
import pickle
import re

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
	return querys.replace(cname, tname) # doesn't work if relation name occurs in other parts of query

def changeAggrTypeInQuery(querys, caggr, taggr):
	qs = querys.replace(caggr + "(", taggr + "(")
	return qs.replace(caggr + " (", taggr + "(")

def changeAvgInQueryToSumCount(querys):
	equivalent_string = querys.replace(",", " , ")
	equivalent_string = equivalent_string.replace("(", " ( ")
	equivalent_string = equivalent_string.replace(")", " ) ")
	equivalent_string = equivalent_string.replace(";", " ; ")

	list_word = equivalent_string.split(' ')
	list_word = [word for word in list_word if word != '']
	alterQuery = ''

	index = 0
	while(index < len(list_word)):
		if list_word[index].lower == "avg" and index+3 < len(list_word) and list_word[index+1] == "(" and list_word[index+3] == ")":
			alterQuery += " sum(%s), count(%s)" % (list_word[index+2], list_word[index+2])
			index += 4
		else:
			alterQuery += ' ' + list_word[index]
			index += 1

	return alterQuery

def insertIntoSelectFromGroupby(querys):
	equivalent_string = querys.replace(",", " , ")
	equivalent_string = equivalent_string.replace("(", " ( ")
	equivalent_string = equivalent_string.replace(")", " ) ")
	equivalent_string = equivalent_string.replace(";", " ; ")


	list_word = equivalent_string.split(' ')
	list_word = [word for word in list_word if word != '']

	group_index = 0
	for index, word in enumerate(list_word):
		if(word.lower() == "group"):
			group_index = index
			break
	group_index += 2

	list_insert = []
	
	while(group_index < len(list_word)):
		if list_word[group_index] != "(" and list_word[group_index] != ")" and list_word[group_index] != "," and list_word[group_index] != ";":
			list_insert.append(list_word[group_index])
		group_index += 1

	alterQuery = 'select '

	for word in list_insert:
		alterQuery += "%s, " % word

	for index, word in enumerate(list_word):
		if(index != 0):
			alterQuery += word + ' '

	return alterQuery

def aggregateVariableLocator(SelectStmt, startingIndex):
	aggDict = {}
	index = startingIndex
	for ResTarget in SelectStmt["targetList"]:
		if "FuncCall" in ResTarget["ResTarget"]["val"]:
			funcname = ResTarget["ResTarget"]["val"]["FuncCall"]["funcname"][0]["String"]["str"]
			if funcname.lower() == "min":
				aggDict[index] = "min"
				pass
			elif funcname.lower() == "max":
				aggDict[index] = "max"
				pass
			elif funcname.lower() == "count":
				aggDict[index] = "count"
				pass
			elif funcname.lower() == "sum":
				aggDict[index] = "sum"
				pass
			elif funcname.lower() == "avg":
				aggDict[index] = "avg"
				index += 1
				pass
		index += 1
	return aggDict

