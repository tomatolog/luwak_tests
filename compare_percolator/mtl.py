import os, time, sys, random, MySQLdb, operator, gzip, re
from threading import Thread
from threading import Event

##########################################################################

NUM_THREADS = 1
idx = 'pq'
qport = 8306
h = '127.0.0.1'
dump_reply = True

if os.name=="nt":
	mytime = time.clock
else:
	mytime = time.time

def die ( msg ):
	print msg
	sys.exit ( 1 )

def escapeString(string):
	return re.sub(r"([=\(\)|\-!@~\"&/\\\^\$\=\<\'])", r"\\\1", string)
	
class PQ ( Thread ):
	def __init__ ( self, tid, docs ):
		Thread.__init__ ( self )
		self.iter = 0;
		self.docs = docs
		self.tm = 0
		self.doc_count = 0
		
	def run ( self ):
		self.conn = MySQLdb.connect ( host=h, user="root", passwd="", db="", port=qport )
		self.cursor = self.conn.cursor ()
	
		for doc in docs:
			text_zipped = ''
			with gzip.open(doc, 'rb') as f:
				text_zipped = f.read()
			text = escapeString(text_zipped)
			
			#print ( "CALL PQ ('%s', '%s', 0 as docs_json)" % (idx, text) )
			start = mytime()
			self.cursor.execute ( "CALL PQ ('%s', '%s', 0 as docs_json)" % (idx, text) )
			rows = self.cursor.fetchall()
			end = mytime() - start
			self.tm = self.tm + end
			self.iter = self.iter + 1
			doc_count = len(rows)
			self.doc_count = self.doc_count + doc_count
			if dump_reply:
				doc_list = ''
				if doc_count>0:
					doc_list = ", ".join('%d'%(r[0]-1) for r in rows)
				print "%s %d %s" % ( os.path.basename(doc), doc_count, doc_list )
				#"{$name} {$rowsCount} " . implode(", ",$ids) . "\t\t\t\n";
		
			
##########################################################################

docs_path = ""

i = 1
while (i<len(sys.argv)):
	arg = sys.argv[i]
	i += 1
	if arg=='-n' or arg=='--thd':
		NUM_THREADS = int(sys.argv[i])
		i += 1
	elif arg=='--host':
		h = sys.argv[i]
		i += 1
	elif arg=='--total':
		dump_reply = False
	else:
		docs_path = arg

docs = []		
for f in os.listdir(docs_path):
	fpath = os.path.join ( docs_path, f ) 
#	print f
#	print fpath
	if os.path.isfile ( fpath ) and os.path.splitext ( f )[1]=='.gz':
		docs.append ( fpath )
docs.sort()
#print docs
	
thds = [ PQ ( tid, docs [ tid*len(docs)/NUM_THREADS : (tid+1)*len(docs)/NUM_THREADS ] ) for tid in range(NUM_THREADS) ]

start = mytime()
[ t.start() for t in thds ]
alive = len(thds)
thd_tm = 0
		
while alive>0:
	alive = 0
	for t in thds:
		thd_tm = max(thd_tm, t.tm)
		if t.isAlive():
			alive = alive + 1
	elapsed = mytime() - start
	if not dump_reply:
		sys.stdout.write ( "elapsed %d thd=%d %.3f \r" % ( elapsed, alive, thd_tm ) )
		sys.stdout.flush()
	time.sleep (0.5)

tm = mytime() - start

doc_count = 0
for t in thds:
	doc_count = t.doc_count + doc_count

print 'matched %d docs (%d) in %.3f sec (%.3f docs/sec), total %.3f sec' % ( len(docs), doc_count, thd_tm, len(docs)/thd_tm, tm )
