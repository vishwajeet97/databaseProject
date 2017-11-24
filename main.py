from fdds.fdd import fdd
from fdds.utils import parser
import argparse
import sys
import signal
from fdds.helpers import QueryDeploy

APPLICATION_NAME = "fdds v1"

dispServerHelpString = "Displays all the intergrated child databases"
addServerHelpString = "Integrates the given database into the system"
masterServerHelpString = "Sets the master server to the argument"
delServerHelpString = "Disintegrates the given database from the system"
freezeSchemaString = "Freezes the schema; Can't perform any schema operations later"
executeQueryHelpString = "Executes the given query on the integrated system"
debugHelpString = "Turn debugging on/off"
runHelpString = "Runs commands in the script file specified"
loadServersConfigHelpString = "Loads configurations of sites and schema details from the given file"
saveServersConfigHelpString = "Saves configurations of sites and schema details in the given file"
helpHelpString = "Prints summary of all available commands"
exitHelpString = "Exits duh!"

db = fdd()
psr = parser()

def getStringFromArray(arg):
	st = ""
	for i in arg:
		st += i + " "
	return st

def fDispServer(args):
	db.displayServers()
	pass

def fMasterServer(args):
	userver = psr.createServerFromArgs(args)
	db.setMasterServer(userver)
	check_metadata = "select relname from pg_class where relname='site_info'"
	thread = QueryDeploy(db.masterserver, check_metadata)
	thread.start()
	res = thread.join()
	if len(res) == 0:
		db.createMetadataSchema()
	else:
		db.initializeMetadata()
	pass

def fAddServer(args):
	userver = psr.createServerFromArgs(args)
	db.addServer(userver)
	pass
	#wf

def fDelServer(args):
	userver = psr.createServerFromArgs(args)
	db.deleteServer(userver)
	pass
	#sac

def fLoadServersConfig(args):
	uinfo = psr.readFromFile(args.filename)
	db.addConfig(uinfo)
	pass

def fSaveServersConfig(args):
	uinfo = db.getConfig()
	psr.writeIntoFile(args.filename, uinfo)
	pass

def fHelp(args):
	cmdparser.print_help()

def fExecuteQuery(args):
	# get string from args
	db.executeQuery(getStringFromArray(args.queryString))
	pass
	#usbv

def fDebug(args):
	import fdds.utils as utls
	utls.debug = (args.mode == "True" or args.mode == "true")

def fRun(args):
	fname = args.scriptfilename
	with open(fname, "r") as f:
		commands = f.readlines()
		commands = [x.strip() for x in commands]

	try:
		for x in commands:
			args = cmdparser.parse_args(x.split(' '))
			args.func(args)
	except Exception as e:
		print("Error in script file: ", e)
		import traceback
		traceback.print_exc()

def fFreeze(args):
	# get string from args
	db.freezeSchema()
	pass
	#usbv

def fExit(args):
	# Maintain the database before exiting
	if args is "SystemExit":
		return
	elif args == "Signal":
		print("")
		sys.exit()
	else:
		import os
		os._exit(0)

def sigHandler(signum, frame):
	if signum == signal.SIGINT:
		fExit("Signal")

# cmdparser funcs
cmdparser = argparse.ArgumentParser(description=APPLICATION_NAME, prog='', add_help=False)
subparsers = cmdparser.add_subparsers()

dispServerParse = subparsers.add_parser('display', description=dispServerHelpString)
dispServerParse.set_defaults(func=fDispServer)

masterServerParse = subparsers.add_parser('master', description=masterServerHelpString)
masterServerParse.set_defaults(func=fMasterServer)
masterServerParse.add_argument('host')
masterServerParse.add_argument('port')
masterServerParse.add_argument('database')
masterServerParse.add_argument('username')
masterServerParse.add_argument('--password')

addServerParse = subparsers.add_parser('add', description=addServerHelpString)
addServerParse.set_defaults(func=fAddServer)
addServerParse.add_argument('host')
addServerParse.add_argument('port')
addServerParse.add_argument('database')
addServerParse.add_argument('username')
addServerParse.add_argument('--password')

delServerParse = subparsers.add_parser('del', description=delServerHelpString)
delServerParse.set_defaults(func=fDelServer)
delServerParse.add_argument('host')
delServerParse.add_argument('port')
delServerParse.add_argument('database')

freezeParse = subparsers.add_parser('freeze', description=freezeSchemaString)
freezeParse.set_defaults(func=fFreeze)

loadServersConfigParse = subparsers.add_parser('load', description=loadServersConfigHelpString)
loadServersConfigParse.set_defaults(func=fLoadServersConfig)
loadServersConfigParse.add_argument('filename')

saveServersConfigParse = subparsers.add_parser('save', description=saveServersConfigHelpString)
saveServersConfigParse.set_defaults(func=fSaveServersConfig)
saveServersConfigParse.add_argument('filename')

executeQueryParse = subparsers.add_parser('execute', description=executeQueryHelpString)
executeQueryParse.set_defaults(func=fExecuteQuery)
executeQueryParse.add_argument('queryString', nargs="+")

debugParse = subparsers.add_parser('debug', description=debugHelpString)
debugParse.set_defaults(func=fDebug)
debugParse.add_argument('mode')

runParse = subparsers.add_parser('run', description=runHelpString)
runParse.set_defaults(func=fRun)
runParse.add_argument('scriptfilename')

helpParse = subparsers.add_parser('help', description=helpHelpString)
helpParse.set_defaults(func=fHelp)

exitParse = subparsers.add_parser('exit', description=exitHelpString)
exitParse.set_defaults(func=fExit)

def main():
	print("")
	cmdparser.print_help()
	signal.signal(signal.SIGINT, sigHandler)
	while True:
		print("")
		cmd_string = input("fdds$ ")
		# read master database


		try:
			args = cmdparser.parse_args(cmd_string.split(' '))
			args.func(args)
		except SystemExit as s:
			fExit("SystemExit")
		except Exception as e:
			import traceback
			traceback.print_exc()

if __name__ == '__main__':
	main()