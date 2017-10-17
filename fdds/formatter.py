from pg_query import parse_sql
import json

while 1:
	try:
		a = parse_sql(input())
		print(json.dumps(a, indent=3))
	except Exception as e:
		print(e)
