package pagerank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pagerank.PageCountMR.PageCountMapper;
import pagerank.PageCountMR.PageCountReducer;
import pagerank.PageRankMR.PageRankMapper;
import pagerank.PageRankMR.PageRankReducer;
import pagerank.PageRankSortMR.PageRankSortMapper;
import pagerank.PageRankSortMR.PageRankSortReducer;

public class PageRank {

	static int pages = 0;

	// Map reduce job 1: Count number of pages
	public static int countPageTitles(String input, String output) throws Exception {

		Configuration c = new Configuration();
		Job conf = Job.getInstance(c);
		conf.setJarByClass(PageCountMR.class);
		conf.setJobName("PageCount");

		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(PageCountMapper.class);
		conf.setReducerClass(PageCountReducer.class);

		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		return conf.waitForCompletion(true) ? 0 : 1;
	}

	// Map reduce job 2: Calculate page rank
	public static int getPageRank(String input, String output) throws Exception {
		Configuration c = new Configuration();
		c.set("pages", String.valueOf(pages));
		Job conf = Job.getInstance(c);
		conf.setJarByClass(PageRankMR.class);
		conf.setJobName("PageRank");
		
		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		return conf.waitForCompletion(true) ? 0 : 1;

	}

	// Map reduce job 3: Sort by desc page rank and filter
	public static int sortDesc(String input, String output) throws Exception {
		Configuration c = new Configuration();
		c.set("pages", String.valueOf(pages));
		Job conf = Job.getInstance(c);
		conf.setJarByClass(PageRankMR.class);
		conf.setJobName("PageRankSort");

		conf.setInputFormatClass(TextInputFormat.class);
		conf.setSortComparatorClass(DecreasingComparator.class);
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PageRankSortMapper.class);
		conf.setReducerClass(PageRankSortReducer.class);
		conf.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		return conf.waitForCompletion(true) ? 0 : 1;

	}

	public static HashMap<String, String> initDirStruct(String outDirName) {
		HashMap<String, String> outStruct = new HashMap<String, String>();

		String tmp = outDirName + "/temp/";
		outStruct.put("tempDir", tmp);
		outStruct.put("tempCountFile", tmp + "/countJob/parts-merged");

		outStruct.put("temp1", tmp + "/job1/");
		outStruct.put("temp2", tmp + "/job2/");
		outStruct.put("pageRankInput", tmp + "/job2/iter0/input.out");

		outStruct.put("countFile", tmp + "/num_nodes");
		outStruct.put("iter1", tmp + "/iter1/");
		outStruct.put("iter8", tmp + "/iter8/");

		outStruct.put("iter1File", tmp + "/iter1.out");
		outStruct.put("iter8File", tmp + "/iter8.out");

		return outStruct;
	}

	private static void formatInputFile(FileSystem fs, String in, String out) {

		double initPageRank = 1.0 / (double) pages;

		try {
			OutputStream os = fs.create(new Path(out));

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(in))));
			String s = null;
			while ((s = br.readLine()) != null) {
				String tokens[] = s.split("\t");

				PageRankNode n = new PageRankNode();
				n.setPageRank(initPageRank);
				n.setAdjLinks(Arrays.copyOfRange(tokens, 1, tokens.length));
				IOUtils.write(tokens[0] + '\t' + n.toString() + '\n', os);
			}
			os.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fsout = FileSystem.get(new URI(args[1]), conf);
		FileSystem fsin = FileSystem.get(new URI(args[1]), conf);

		// Setup output directory structure
		HashMap<String, String> outDirs = initDirStruct(args[1]);

		FileUtil.copyMerge(fsin, new Path(args[0] + "/"), fsin, new Path(outDirs.get("tempCountFile")), false, conf,
				"");

		int res = countPageTitles(outDirs.get("tempCountFile"), outDirs.get("temp1"));
		if (res == 0) {

			FileUtil.copyMerge(fsout, new Path(outDirs.get("temp1")), fsout, new Path(outDirs.get("countFile")), false,
					conf, "");
			FileUtil.copy(fsout, new Path(outDirs.get("countFile")), fsout, new Path(args[1] + "/num_nodes"), false, false, conf);
			fsout.delete(new Path(args[1] + "/.num_nodes.crc"),false);
			BufferedReader br = new BufferedReader(
					new InputStreamReader(fsout.open(new Path(outDirs.get("countFile")))));
			pages = Integer.parseInt(br.readLine());
			br.close();

			formatInputFile(fsout, outDirs.get("tempCountFile"), outDirs.get("pageRankInput"));

			// Iterative Map Reduce
			for (int i = 1; i <= 8; i++) {
				getPageRank(outDirs.get("temp2") + "/iter" + (i - 1), outDirs.get("temp2") + "/iter" + (i));
			}

		}

		sortDesc(outDirs.get("temp2") + "/iter8", outDirs.get("iter8"));
		FileUtil.copyMerge(fsout, new Path(outDirs.get("iter8")), fsout, new Path(outDirs.get("iter8File")), false,
				conf, "");
		FileUtil.copy(fsout, new Path(outDirs.get("iter8File")), fsout, new Path(args[1] + "/iter8.out"), false, false, conf);
		fsout.delete(new Path(args[1] + "/.iter8.out.crc"),false);
		
		sortDesc(outDirs.get("temp2") + "/iter1", outDirs.get("iter1"));
		FileUtil.copyMerge(fsout, new Path(outDirs.get("iter1")), fsout, new Path(outDirs.get("iter1File")), false,
				conf, "");
		FileUtil.copy(fsout, new Path(outDirs.get("iter1File")), fsout, new Path(args[1] + "/iter1.out"), false, false, conf);
		fsout.delete(new Path(args[1] + "/.iter1.out.crc"),false);
	}

}