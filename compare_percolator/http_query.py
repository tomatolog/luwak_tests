import os, time, sys, json, requests
from urlparse import urlparse
from icecream import ic

def die(s):
	print (s)
	sys.exit ( 1 )

timer = time.time

if not sys.argv[1:]:
	print ("""Usage: http_query.py.py [OPTIONS] path2queries
Options are:
--limit\t\tuse N first queries
--url\t\tadress:port/endpoint for requests
--multi\t\tbatch N queries per request
""")
	sys.exit ( 0 )

url = '127.0.0.1:8380/json/pq/pq/doc'
limit = 0
multi = 0
qpath = './'

i = 1
while (i<len(sys.argv)):
	arg = sys.argv[i]
	if arg=='--limit':
		i += 1
		limit = int(sys.argv[i])
	elif arg=='--url':
		i += 1
		url = sys.argv[i]
	elif arg=='--multi':
		i += 1
		multi = int(sys.argv[i])
	elif arg[0:1]=='-':
		die ( 'unknown argument %s' % sys.argv[i] )
	else:
		qpath = arg
	i += 1

queries = []
for f in os.listdir(qpath):
	fpath = os.path.join ( qpath, f ) 
	if os.path.isfile ( fpath ):
		with open(fpath, 'r') as q:
			queries.append ( q.readline() )
	if limit>0 and len(queries)>=limit:
		break
		
	if len(queries) % 100 == 0:
		sys.stdout.write ( "read %d %s\r" % ( len(queries), f) )
		
	
headers = {'Content-Type': 'application/json'}
errors = 0

print("")

starttime = timer()

qid = 0
q_full = []
while qid<len(queries):
	q = { "id" : str(qid+1), "query" : { "ql": queries[qid] } }
	if multi==0:
		q_full = q
	elif len(q_full)<multi:
		q_full.append(q)
		qid = qid + 1
		continue

	try:
		res = requests.post(url, data=json.dumps(q_full), headers=headers)
	except:
		print("Connection refused at %d" % qid )
		time.sleep(15)
		continue
		
	if multi>0:
		q_full = []
		
	#ic(res.status_code, res.json(), q)
	reply = res.json()
	if res.status_code>299:
		errors = errors + 1

	qid = qid + 1
	if qid % 100 == 0:
		sys.stdout.write ( "posted %d\r" % qid )

tm = timer() - starttime
print ('elapsed %.3f sec, queries %d, bad %d' % ( tm, len(queries), errors ))
