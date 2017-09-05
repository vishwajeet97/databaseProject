from fdds.fdd import fdd
from fdds.helpers import parser
from fdds.helpers import server
import argparse
import sys
import signal

APPLICATION_NAME = "fdds v1"

dispServerHelpString = "Displays all the intergrated child databases"
addServerHelpString = "Integrates the given database into the system"
delServerHelpString = "Disintegrates the given database from the system"
executeQueryHelpString = "Executes the given query on the integrated system"
helpHelpString = "Prints summary of all available commands"
exitHelpString = "Exits duh!"

db = fdd()
psr = parser()

def fDispServer(args):
	db.displayServers()
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

def fHelp(args):
	cmdparser.print_help()

def fExecuteQuery(args):
	# get string from args
	db.executeQuery(args.queryString)
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

dispServerParse = subparsers.add_parser('dispServer', description=dispServerHelpString)
dispServerParse.set_defaults(func=fDispServer)

addServerParse = subparsers.add_parser('addServer', description=addServerHelpString)
addServerParse.set_defaults(func=fAddServer)
addServerParse.add_argument('host')
addServerParse.add_argument('port')
addServerParse.add_argument('username')
addServerParse.add_argument('password')
addServerParse.add_argument('database')

delServerParse = subparsers.add_parser('delServer', description=delServerHelpString)
delServerParse.set_defaults(func=fDelServer)
delServerParse.add_argument('host')
delServerParse.add_argument('port')
delServerParse.add_argument('database')

executeQueryParse = subparsers.add_parser('executeQuery', description=executeQueryHelpString)
executeQueryParse.set_defaults(func=fExecuteQuery)
executeQueryParse.add_argument('queryString', nargs="+")

helpParse = subparsers.add_parser('help', description=helpHelpString)
helpParse.set_defaults(func=fHelp)

exitParse = subparsers.add_parser('exit', description=exitHelpString)
exitParse.set_defaults(func=fExit)

def main():
	cmdparser.print_help()
	signal.signal(signal.SIGINT, sigHandler)
	while True:
		cmd_string = input("fdds$ ")
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