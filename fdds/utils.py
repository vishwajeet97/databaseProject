import psycopg2 as ppg
import subprocess

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
	return querys.replace(cname, tname)

def changeAggrTypeInQuery(querys, caggr, taggr):
	qs = querys.replace(caggr + "(", taggr + "(")
	return qs.replace(caggr + " (", taggr + "(")