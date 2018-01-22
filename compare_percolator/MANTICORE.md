Luwak performance comparison with Manticore Percolator
===

Downloading test documents
====

The `download-random-wp-text.py` script downloads random articles from Wikipedia. It takes
two parameters, the number of articles to download and the document in which to save them.
e.g.:

```bash
$ mkdir docs
$ python download-random-wp-text.py 5000 docs
```	

This will download 5000 random articles and save the text as .gz files in docs/. 

Creating queries
====

The `generate-queries.py` script can be used to generate random queries in Lucene query 
parser format that might be used stright via SphinxQL interface too. It uses the
downloaded documents as input, and attempts to generate "reaslistic" queries
(in the sense that they will match some documents).

```bash
python generate-queries.py --count=100000 --docdir=docs --querydir=queries --MUST=10 --NOT=2
python generate-queries.py --count=100000 --docdir=docs --querydir=queries --MUST=100 --NOT=20
python generate-queries.py --count=100000 --docdir=docs --querydir=queries --MUST=20 --wild
```

The number of MUST and NOT terms, the wildcard prefix length are
all configurable on the command line. Queries are output as separate files in the
specified directory.

Running Luwak tests
====

The Luwak test app loads the queries into an in-memory index before processing the 
documents. It does not include this load time in the documents-per-second calculation,
since query load time will be an insignificant overhead in any long-running monitor
application (it is also possible to store the index on disk similarly to Percolator).

To run the test app, give it the location of the query directory and document 
directory, e.g.:

```bash
$ java -Xms4G -jar luwak_test/target/luwak_test-0.0.1-SNAPSHOT.jar queries docs
```		   

Running Manticoresearch tests
====

At first searchd daemon should be started

```bash
$ mkdir data
$ searchd -c pq.conf
```

then queries should be loaded into daemon

```bash
$ cd queries
$ mysql -P8306 -h0 -e "truncate rtindex pq;"
$ for n in `ls`; do echo $n; mysql -P8306 -h0 -e "insert into pq values('`cat $n`');"; done;
```

then test started

```bash
$ python mtl.py compare_percolator/docs
```
