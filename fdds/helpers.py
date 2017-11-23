import threading
import psycopg2 as ppg
import hashlib
import random

from .utils import changeRelNameInQuery as crn
from .utils import getRelationNameListR

class QueryDeploy(threading.Thread):
	"""docstring for QueryDeploy"""
	def __init__(self, site, query):
		super(QueryDeploy, self).__init__()
		# self.callback = arg
		self.site = site
		self.query = query

	def run(self):

		res = []
		# run the query
		with ppg.connect(
				host=self.site["host"],
				port=self.site["port"],
				dbname=self.site["database"],
				user=self.site["username"],
				password=self.site["password"]
				) as conn:
			try:
				with conn.cursor() as cur:
					cur.execute(self.query)
					res = cur.fetchall()
					colName = [desc[0] for desc in cur.description]
			except ppg.Error as e:
				print(e)
			except ppg.ProgrammingError as e:
				print(e)
		self.res = res
		self.colName = colName

	def join(self):
		super(QueryDeploy, self).join()
		return self.res

	def returnDescription(self):
		return self.colName

NTABLETS = 20

class TabletController(object):
	"""docstring for TabletController"""
	def __init__(self, siteList, siteDict):
		super(TabletController, self).__init__()
		self.tablets = NTABLETS
		self.siteList = siteList
		self.siteDict = siteDict
		self.master_map = {}
		self.schema_data = {}
		self.site_tablet_tupleCt = {}	# number of tuples in each tablet in each site

		self.schema_data["stmts"] = []
		self.schema_data["pkmetadata"] = {}

		self.mastersite = 0

	def getTupleCt(self, site, tablet_name):
		query = "select count(*) from " + tablet_name;
		res = QueryDeploy(site, query)
		return res

	def setMetaData(self, data):
		self.schema_data = data[0]
		self.master_map = data[1]
		print("master_map")
		print(self.master_map)
		print("site list ")
		print(self.siteList)
		
		'''self.site_tablet_tupleCt = {site: {} for site in self.siteList}
		print("initial site_tablet_tupleCt: ");
		print(self.site_tablet_tupleCt)
		for reln, mapping in self.master_map.items():
			for tablet_id, site in mapping.items():
				self.site_tablet_tupleCt[site][reln] = {}
				thread = self.getTupleCt(self.siteDict[site], reln + "_" + str(tablet_id))
				thread.start()
				ct = thread.join()
				self.site_tablet_tupleCt[site][reln][tablet_id] = ct

		print("updated site_tablet_tupleCt")
		print(self.site_tablet_tupleCt)'''
		
		# for operation in self.schema_data["stmts"]:
		# 	self.createTabletMappingForRelation(operation["CreateStmt"])

	def getMetaData(self):
		return [ self.schema_data, self.master_map ]

	def createTableMetaData(self, stmt, masterserver):

		self.schema_data["stmts"].append(stmt)

		# update primary key meta data
		relname = stmt["CreateStmt"]["relation"]["RangeVar"]["relname"]
		pkarray = []
		
		tbelts = stmt["CreateStmt"]["tableElts"]
		
		pknamelist = []
		pkdone = False

		for item in tbelts:
			if "Constraint" in item.keys():
				if item["Constraint"]["contype"] == 5:
					pknamelist = [ x["String"]["str"] for x in item["Constraint"]["keys"] ]
					pkdone = True

		count = 0
		for item in tbelts:
			if "ColumnDef" in item.keys():
				if pkdone:
					if item["ColumnDef"]["colname"] in pknamelist:
						index_name = (count, item["ColumnDef"]["colname"])
						pkarray.append(index_name)

				else:
					if "constraints" in item["ColumnDef"].keys():
						for cons in item["ColumnDef"]["constraints"]:
							# print(cons, type(cons))
							if cons["Constraint"]["contype"] == 5:
								index_name = (count, item["ColumnDef"]["colname"])
								pkarray.append(index_name)
				count += 1

		self.schema_data["pkmetadata"][relname] = pkarray

		# insert relation in relations table
		insert_reln = "insert into relations values ('%s')"
		thread = QueryDeploy(masterserver, insert_reln%relname)
		thread.start()
		thread.join()

		# insert only primary key attributes into relation_info

		for pk in pkarray:
			insert_attr = "insert into relation_info values ('%s', %d, '%s', %s)"
			thread = QueryDeploy(masterserver, insert_attr%(pk[1], pk[0], relname, "true"))
			thread.start()
			thread.join()

		self.createTabletMappingForRelation(stmt["CreateStmt"], masterserver)


	def hashFunction(self, key):
		encoded_key = key.encode('utf-8')
		return abs(int(hashlib.sha1(encoded_key).hexdigest(), 16)) % self.tablets

	def getRelName(self, stmt):
		relNameList = []
		if "InsertStmt" in stmt.keys():
			relName = stmt["InsertStmt"]["relation"]["RangeVar"]["relname"]
			relNameList.append(relName)
		elif "SelectStmt" in stmt.keys():
			from_list = stmt["SelectStmt"]["fromClause"]
			for from_item in from_list:
				relNameList.extend(getRelationNameListR(from_item))
		elif "CreateStmt" in stmt.keys():
			relName = stmt["CreateStmt"]["relation"]["RangeVar"]["relname"]
			relNameList.append(relName)
		elif "DropStmt" in stmt.keys():
			relsName = [ x["String"]["str"] for x in stmt["DropStmt"]["objects"][0] ]
			relNameList = relsName
		
		return relNameList

	def joinQueryBuilder(self, qstring, from_list):

		joinQuery = ""

		# import schema
		for siteid in self.siteList:
			if siteid == self.mastersite:
				continue

			sn = "site" + str(siteid)
			snserver = sn + "_server"
			snschema = sn

			joinQuery += " drop SCHEMA IF EXISTS " + sn + " cascade;"
			joinQuery += " create SCHEMA " + sn + ";"

			joinQuery += " IMPORT FOREIGN SCHEMA public"
			joinQuery += " from SERVER " + snserver
			joinQuery += " INTO " + sn + ";"	

		# build relations
		relations = []
		for d in from_list:
			relations.extend(getRelationNameListR(d))

		joinQuery += "with "

		for relation in relations:
			
			joinQuery += relation + " as ("

			for i, ( tid, siteid ) in enumerate(self.master_map[relation].items()):

				tblname = relation + "_" + str(tid)
				if siteid != self.mastersite:
					scname = "site" + str(siteid)
					tblname = scname + "." + tblname

				temp = "( select * from " + tblname + " )"

				if i != 0:
					joinQuery += " union "

				joinQuery += temp
			
			joinQuery += ") , "

		joinQuery = joinQuery[0:-2]

		# append qstring
		joinQuery += qstring

		print("Join Query Build: ", relations, joinQuery)
		return joinQuery

	def getSiteQueryMapping(self, stmt, qstring):
		# Parse insert tree to get primary key, relation
		# Concat the primary key to get string and then hash to get tablet id
		# Get the siteId from the map of (tableid, siteid)
		# return the (site, query)

		relNameList = self.getRelName(stmt)
		print(relNameList)
		print("master map")
		print(self.master_map)
		default = {}
		print("siteList")
		for site in self.siteList:
			default[site] = []
		print(self.siteList)

		for i in range(0, self.tablets):
			qr = qstring
			for relname in relNameList:
				qr = crn(qr, relname, relname + "_" + str(i))
			for relName in relNameList:
				si = self.master_map[relname][i]
				default[si].append(qr)

		# print("default")
		print(default)

		aggreg_default = {}
		aggreg_default[self.mastersite] = []

		print("aggreg_default")
		print(aggreg_default)

		if "InsertStmt" in stmt.keys():
			relname = stmt["InsertStmt"]["relation"]["RangeVar"]["relname"]
			valList = stmt["InsertStmt"]["selectStmt"]["SelectStmt"]["valuesLists"];
			# assume valList contains only one list
			if len(valList) != 1:
				print("Multiple values cannot be inserted")
				return default
			
			primary_key_attrs = self.schema_data["pkmetadata"][relname] # list of (index, attr_name) tuples
			if len(primary_key_attrs) == 0:
				tablet_id = random.randint(0, self.tablets - 1)
			else:
				primary_key_index, primary_key_name = zip(*primary_key_attrs)
				pk_attr_val = []
				pk_attr_str = ""
				val_index = 0
				for i in valList[0]:
					if val_index in primary_key_index:
						pk_attr_val_el = []
						for x in primary_key_attrs:
							if x[0] == val_index:
						 		pk_attr_name = x[1]
						 		pk_attr_val_el.append(pk_attr_name)

						valElement = i["A_Const"]["val"]
						for key in valElement.keys():
							for key1 in valElement[key]:
								v = valElement[key][key1]
								pk_attr_val_el.append(v)
								pk_attr_val.append(pk_attr_val_el)
					val_index += 1

				pk_attr_val.sort()  # so that different order of attributes in query will return same tablet_id
				print(pk_attr_val)
				pk_attr_str = ""
				pk_attr_names, pk_attr_values = zip(*pk_attr_val)
				for v in pk_attr_values:
					pk_attr_str += str(v)

				print("pk attr string ", pk_attr_str)
				if pk_attr_str == "":
					tablet_id = random.randint(0, self.tablets - 1)
				else:
					tablet_id = self.hashFunction(pk_attr_str)

			print("tablet id: ", tablet_id)
			ret = {}
			site = self.master_map[relname][tablet_id]
			ret[site] = [crn(qstring, relname, relname + "_" + str(tablet_id))]
			# update count of tuples in tablet
			# self.site_tablet_tupleCt[site][relname][tablet_id] = self.site_tablet_tupleCt[site][relname][tablet_id] + 1
			return ret

		elif "SelectStmt" in stmt.keys():
			from_list = stmt["SelectStmt"]["fromClause"]

			if len(from_list) > 1 or ("JoinExpr" in [list(x.keys())[0] for x in from_list]):
				aggreg_default[self.mastersite].append(self.joinQueryBuilder(qstring, from_list));
				return aggreg_default

			relname = from_list[0]["RangeVar"]["relname"]
			if "whereClause" not in stmt["SelectStmt"].keys():
				return default

			primary_key_attrs = self.schema_data["pkmetadata"][relname]
			primary_key_index, primary_key_name = zip(*primary_key_attrs)

			whereClause = stmt["SelectStmt"]["whereClause"]
			if "BoolExpr" not in whereClause.keys():
				# where clause of type "attr = value"
				# assuming where clause is not of type (A1, A2) = ('val1', 'val2')
				attrList = whereClause["A_Expr"]["lexpr"]["ColumnRef"]["fields"]

				compOp = whereClause["A_Expr"]["name"][0]["String"]["str"]
				if compOp != "=":
					return default
				
				attr = attrList[0]["String"]["str"] 
				# check that primary key is attr
				if len(primary_key_attrs) == 1 and primary_key_name[0] == attr:
					pk_attr_str = ""	
					valElement = whereClause["A_Expr"]["rexpr"]["A_Const"]["val"] 
					for key in valElement.keys():
						for key1 in valElement[key]:
							v = valElement[key][key1]
							pk_attr_str += str(v)

					# print("pk attr string ", pk_attr_str)
					if pk_attr_str == "":
						return default
					else:
						tablet_id = self.hashFunction(pk_attr_str)
						print("tablet id: ", tablet_id)
						retmap = {}
						for site in self.siteList:
							retmap[site] = []

						qr = crn(qstring, relname, relname + "_" + str(tablet_id))
						retmap[self.master_map[relname][tablet_id]] = [qr]
						return retmap
				else:
					return default
			else:
				# where clause involving boolean expressions (possibly nested)
				# find values of primary key attributes
				boolexpr = whereClause["BoolExpr"]
				for a in boolexpr["args"]:
					if "BoolExpr" in a.keys():
						return default

				if boolexpr["boolop"] != 0:
					return default
		
				args = boolexpr["args"]
				arg_attr = list()
				for a in args:
					column = a["A_Expr"]["lexpr"]["ColumnRef"]["fields"][0]["String"]["str"]
					arg_attr.append(column)
					compOp = a["A_Expr"]["name"][0]["String"]["str"]
					if compOp != "=":
						print("no =")
						return default



				for pk in primary_key_name:
					if pk not in arg_attr:
						return default

				pk_attr_val = []
				for a in args: 
					column = a["A_Expr"]["lexpr"]["ColumnRef"]["fields"][0]["String"]["str"]
					if column not in primary_key_name:
						continue
					pk_attr_val_el = [column]
					valElement = a["A_Expr"]["rexpr"]["A_Const"]["val"] 
					for key in valElement.keys():
						for key1 in valElement[key]:
							v = valElement[key][key1]
							pk_attr_val_el.append(v)
							pk_attr_val.append(pk_attr_val_el)
							#pk_attr_str += str(v)

				pk_attr_val.sort()  # so that different order of attributes in query will return same tablet_id
				print(pk_attr_val)
				pk_attr_str = ""
				pk_attr_names, pk_attr_values = zip(*pk_attr_val)
				for v in pk_attr_values:
					pk_attr_str += str(v)

				print("pk_attr_str " + pk_attr_str)
				tablet_id = self.hashFunction(pk_attr_str)
				print("tablet id: ", tablet_id)
				retmap = {}
				for site in self.siteList:
					retmap[site] = []

				qr = crn(qstring, relname, relname + "_" + str(tablet_id))
				retmap[self.master_map[relname][tablet_id]] = [qr]
				return retmap

		elif "CreateStmt" in stmt.keys():

			relname = stmt["CreateStmt"]["relation"]["RangeVar"]["relname"]
			retmap = {}
			for site in self.siteList:
				retmap[site] = []

			for i in range(0, self.tablets):
				si = self.master_map[relname][i]
				qr = crn(qstring, relname, relname + "_" + str(i))
				retmap[si].append(qr)

			# initialize tuple counts
			for reln, mapping in self.master_map.items():
				for tablet_id, site in mapping.items():
					self.site_tablet_tupleCt[site][relname][tablet_id] = 0

			# update 
			# return default
			return retmap

		elif "DropStmt" in stmt.keys():

			relsname = [ x["String"]["str"] for x in stmt["DropStmt"]["objects"][0] ]
			retmap = {}
			for site in self.siteList:
				retmap[site] = []

			for i in range(0, self.tablets):
				for rel in relsname:
					si = self.master_map[rel][i]
					qr = "drop table " + rel + "_" + str(i)
					retmap[si].append(qr)

			for rel in relsname:
				del self.master_map[rel]
				del self.schema_data["pkmetadata"][rel]

			# set tuple counts of dropped tables to 0
			for reln, mapping in self.master_map.items():
				for relName in relsname:
					for tablet_id, site in mapping.items():
						self.site_tablet_tupleCt[site][relName][tablet_id] = 0

			# return default
			return retmap

		else:
			print(default)
			return default
		
	def createTabletMappingForRelation(self, tree, masterserver):
		# If not create an entry of the mapping between (tableid, siteid) for the relation
		relName = tree["relation"]["RangeVar"]["relname"]
		mapping = {}
		for i in range(0, self.tablets):
			mapping[i] = self.siteList[i % len(self.siteList)]

		self.master_map[relName] = mapping

		# insert into tablet_info table
		pwd = ''
		if masterserver["password"] is not None:
			pwd = masterserver["password"]
		update_tablet_stmt = "insert into tablet_info values ('%s', %d, '%s', '%s', '%s', '%s', '%s', %d)"
		for tablet_num, site_index in mapping.items():
			server = self.siteDict[site_index]
			thread = QueryDeploy(masterserver, update_tablet_stmt%(relName, tablet_num, server["host"], server["port"], server["database"], server["username"], pwd, 0))	
			thread.start()
			thread.join()

		print(self.master_map)

		for tablet_id, site in mapping.items():
			self.site_tablet_tupleCt[site] = {}
			self.site_tablet_tupleCt[site][relName] = {}

		for reln, mapping in self.master_map.items():
			for tablet_id, site in mapping.items():
				self.site_tablet_tupleCt[site][relName][tablet_id] = 0

		
