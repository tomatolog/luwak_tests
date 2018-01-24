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
		self.q_count = 0;
		self.docs = docs
		self.docs_total = len(self.docs)
		self.it = 0
		self.tm = 0
		self.doc_count = 0
		
	def run ( self ):
		self.conn = MySQLdb.connect ( host=h, user="root", passwd="", db="", port=qport )
		self.cursor = self.conn.cursor ()
	
		while self.it<self.docs_total:
			src,text,count = self.getDoc()
			q = "CALL PQ ('%s', %s, 0 as docs_json)" % (idx, text)
			
			start = mytime()
			self.cursor.execute ( q )
			rows = self.cursor.fetchall()
			end = mytime() - start
			
			self.tm = self.tm + end
			self.q_count = self.q_count + count
			doc_count = len(rows)
			self.doc_count = self.doc_count + doc_count
			if dump_reply:
				doc_list = ''
				if doc_count>0:
					doc_list = ", ".join('%d'%(r[0]-1) for r in rows)
				print "%s %d %s" % ( os.path.basename(src), doc_count, doc_list )
	
	def getDoc ( self ):
		text = ''
		src = []
		count = 0
		if batch<2:
			src.append ( self.docs[self.it][0] )
			text = "'" + self.docs[self.it][1] + "'"
			self.it = self.it + 1
			count = 1
		else:
			text = '('
			it = 0
			while self.it + it<self.docs_total and it<batch:
				src.append ( self.docs[self.it + it][0] )
				if it>0:
					text = text + ','
				text = text + "'" + self.docs[self.it + it][1] + "'"
				it = it + 1
			text = text + ')'
			self.it = self.it + it
			count = it
		
		return (src, text, count)
			
##########################################################################

docs_path = ""
docs_split = True
batch = 1

i = 1
while (i<len(sys.argv)):
	arg = sys.argv[i]
	i += 1
	if arg=='--thd':
		NUM_THREADS = int(sys.argv[i])
		i += 1
	elif arg=='--host':
		h = sys.argv[i]
		i += 1
	elif arg=='--total':
		dump_reply = False
	elif arg=='--docs-all':
		docs_split = False
	elif arg=='--batch':
		batch = int(sys.argv[i])
		i += 1
	elif arg.startswith('-'):
		die ( 'unknown option "%s"' % arg )
	else:
		docs_path = arg

docs = []
docs_src = []		
for f in os.listdir(docs_path):
	fpath = os.path.join ( docs_path, f ) 
	if os.path.isfile ( fpath ) and os.path.splitext ( f )[1]=='.gz':
		docs_src.append ( fpath )
docs_src.sort()
for doc in docs_src:
	text_zipped = ''
	with gzip.open(doc, 'rb') as f:
		text_zipped = f.read()
	text = escapeString(text_zipped)
	docs.append ( (doc, text) )

if docs_split:
	q_total = len(docs)	
	thds = [ PQ ( tid, docs [ tid*len(docs)/NUM_THREADS : (tid+1)*len(docs)/NUM_THREADS ] ) for tid in range(NUM_THREADS) ]
else:
	q_total = len(docs) * NUM_THREADS	
	thds = [ PQ ( tid, docs ) for tid in range(NUM_THREADS) ]


start = mytime()
[ t.start() for t in thds ]
alive = len(thds)
thd_tm = 0
		
while alive>0:
	alive = 0
	q = 0
	for t in thds:
		thd_tm = max(thd_tm, t.tm)
		q = q + t.q_count
		if t.isAlive():
			alive = alive + 1
	elapsed = mytime() - start
	if not dump_reply:
		sys.stdout.write ( "thd=%d, elapsed=%d(%.3f), q=%d(%d) \r" % ( alive, elapsed, thd_tm, q, q_total ) )
		sys.stdout.flush()
	time.sleep (0.5)

tm = mytime() - start

doc_count = 0
for t in thds:
	doc_count = t.doc_count + doc_count

print '\ndocs send %d, q matched %d, in %.3f sec, (%.3f docs/sec), total %.3f sec, threads %d' % ( q_total, doc_count, thd_tm, len(docs)/thd_tm, tm, NUM_THREADS )
