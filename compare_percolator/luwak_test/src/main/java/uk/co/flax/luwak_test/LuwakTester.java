package uk.co.flax.luwak_test;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.Arrays;
import org.apache.commons.io.comparator.NameFileComparator ;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.QueryMatch;
//import uk.co.flax.luwak.matchers.ExplainingMatch;
//import uk.co.flax.luwak.matchers.ExplainingMatcher;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.MatchAllPresearcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

/**
 * Luwak test class
 */
public class LuwakTester 
{
    public static void main(String[] args) throws Exception 
    {
    	CharArraySet stopwords = new CharArraySet(0, false);
    	SimpleAnalyzer analyzer = new SimpleAnalyzer();
    	LuceneQueryParser parser = new LuceneQueryParser("text", analyzer);
    	Monitor monitor = new Monitor(parser, new TermFilteredPresearcher());
//    	Monitor monitor = new Monitor(parser, new MatchAllPresearcher());

    	// load the queries
    	File query_dir = new File(args[0]);
    	File[] query_files = query_dir.listFiles(new FileFilter() { 
    		public boolean accept(File f) { return f.getName().endsWith(".txt"); }
    	});

    	int count = 0;
    	for (File query_file : query_files) {
    		FileInputStream fis = new FileInputStream(query_file);
    		String query_text = IOUtils.toString(fis);
    		fis.close();
    		String query_id = query_file.getName();
    		query_id = query_id.substring(0, query_id.length() - 4);
        	MonitorQuery mq = new MonitorQuery(query_id, query_text);
        	monitor.update(mq);
        	count++;
        	if (count % 1000 == 0) {
        		System.out.println("loaded " + count + " queries");
        	}
    	}

    	int limit = 1000000;
    	if (args.length == 3) {
    		limit = Integer.parseInt(args[2]);
    	}
    	
    	
    	// run the documents through the monitor
    	File doc_dir = new File(args[1]);
    	File[] doc_files = doc_dir.listFiles(new FileFilter() { 
    		public boolean accept(File f) { return f.getName().endsWith(".gz"); }
    	});
		
		Arrays.sort(doc_files, NameFileComparator.NAME_COMPARATOR);

		long t0 = System.currentTimeMillis();
		long tpq = 0;
		count = 0;
		long docs = 0;
    	for (File doc_file : doc_files) {
    		FileInputStream fis = new FileInputStream(doc_file);
    		String doc_text = IOUtils.toString(new GZIPInputStream(fis));
    		fis.close();
		
        	InputDocument doc = InputDocument.builder(doc_file.getName())
        			.addField("text", doc_text, analyzer).build();
        	
			long tpq0 = System.currentTimeMillis();
			Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
			tpq += ( System.currentTimeMillis() - tpq0 );
			
			ArrayList<Integer> query_ids = new ArrayList<Integer>(matches.getMatchCount());
			for (QueryMatch match : matches) {
				query_ids.add(Integer.parseInt(match.getQueryId()));
			}
			Collections.sort(query_ids);
			String qids = "";
			String sep = "";
			for (Integer q : query_ids) {
				qids += sep + Integer.toString(q);
				sep = ", ";
			}
			
			System.out.println(doc_file.getName() + " " + matches.getMatchCount() + " " + qids );

			count ++;
			docs += matches.getMatchCount();
        	if (count == limit) break;

//        	ArrayList<String> query_ids = new ArrayList<String>(matches.getMatchCount());
//        	for (QueryMatch match : matches) {
//        		query_ids.add(match.getQueryId());
//        	}
//        	Collections.sort(query_ids);
//        	for (String tmp : query_ids) {
//        		System.out.println(tmp);
//        	}
    	}
    	
		long t1 = System.currentTimeMillis();
		float dt0 = ((float)tpq / 1000.0f );
		float dt1 = ((float)(t1-t0) / 1000.0f );
    	System.out.println("matched " + count + " docs (" + docs + ")  in " +  String.format("%.3f", dt0) + " sec (" +
    	    String.format("%.3f", count / dt0) + " docs/sec), total " + String.format("%.3f", dt1));
    	
    	monitor.close();
    }
}
