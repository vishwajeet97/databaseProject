from pg_query import parse_sql
import json

while 1:
	try:
		print(json.dumps(parse_sql(input()), indent=3))
	except Exception as e:
		print(e)
