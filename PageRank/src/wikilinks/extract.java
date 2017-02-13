package wikilinks;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import wikilinks.AdjacencyGraphMR.AdjacencyGraphMapper;
import wikilinks.AdjacencyGraphMR.AdjacencyGraphReducer;
import wikilinks.WikiExtractorMR.WikiLinkMapper;
import wikilinks.WikiExtractorMR.WikiLinkReducer;
import xmlparser.XMLInputFormat;

public class extract {

	// Map reduce job 1: Parse wiki XML file and extract outlinks (skip red
	// links)
	public static int extractWikiLinks(String input, String output) throws Exception {

		Configuration c = new Configuration();
		c.set("xmlinput.start", "<page>");
		c.set("xmlinput.end", "</page>");
		Job conf = Job.getInstance(c);
		conf.setJarByClass(WikiExtractorMR.class);
		conf.setJobName("WikiExtractorMR");

		conf.setInputFormatClass(XMLInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(WikiLinkMapper.class);
		conf.setReducerClass(WikiLinkReducer.class);

		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		return conf.waitForCompletion(true) ? 0 : 1;
	}

	// Map reduce job 2: Generate adjacency graph from job1 output
	public static void getAdjacencyGraph(String input, String output) throws Exception {
		Job conf = Job.getInstance(new Configuration());
		conf.setJarByClass(AdjacencyGraphMR.class);
		conf.setJobName("AdjacencyGraphMR");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(AdjacencyGraphMapper.class);
		conf.setReducerClass(AdjacencyGraphReducer.class);

		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		System.exit(conf.waitForCompletion(true) ? 0 : 1);
	}

	public static HashMap<String, String> initDirStruct(String outDirName) {
		HashMap<String, String> outStruct = new HashMap<String, String>();
		outStruct.put("graphRes", outDirName + "graph");
		outStruct.put("MR1Temp", outDirName + "temp");
		return outStruct;
	}

	public static void main(String[] args) throws Exception {

		// Setup output directory structure
		HashMap<String, String> outDirs = extract.initDirStruct(args[1]);
		// Start Wiki link extractor map-reduce job
		int res = extract.extractWikiLinks(args[0], outDirs.get("MR1Temp"));
		if (res == 0) {
			// Start Adjacency graph map-reduce job
			extract.getAdjacencyGraph(outDirs.get("MR1Temp"), outDirs.get("graphRes"));
		}

	}
}
