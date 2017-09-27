import psycopg2 as ppg
import subprocess

def moveTablets(fromSite, toSite, fromTablet, toTablet):
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

	print error