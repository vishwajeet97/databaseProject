import psycopg2 as ppg
import subprocess

def moveTablets(fromSite, toSite, fromTablet, toTablet):
	"""
	fromSite, toSite : Contains the information about the site in form of a dict
	fromTablet, toTabler : Relation name as strings
	"""
	fromHost = fromSite["host"]
	fromPort = fromSite["port"]
	fromUser = fromSite["username"]
	fromPassword = fromSite["password"]

	toHost = toSite["host"]
	toPort = toSite["port"]
	toUser = toSite["username"]
	toPassword = toSite["password"]	

	bashCommand = "pg_dump -h %s -p %s -Fc -o -U %s -w %s -t %s -d %s | psql -h %s -p %s -U %s -w %s -t %s -d %s" 

	bashCommand = bashCommand % (fromHost, fromPort, fromUser, fromTablet, fromDatabase, toHost, toPort, toUser, toTablet, toDatabase)

	process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
	output, error = process.communicate()

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
				cur.execute('DROP TABLE "%s";' % fromTablet)
				res = cur.fetchall()
		except ppg.Error as e:
			print(e)
		except ppg.ProgrammingError as e:
			print(e)

	print error, res