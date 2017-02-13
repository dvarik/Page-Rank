package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class PageRankMR {
	
	static Logger logger = Logger.getLogger(PageRankMR.class);

	public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String tokens[] = value.toString().split("\t",2);
			PageRankNode node = PageRankNode.getPageRankObj(tokens[1]);
			
			String outVal = "0.0" + "\t" + "";
			
			if (node.getAdjLinks() != null && node.getAdjLinks().length > 0) {
				double newRank = node.getPageRank() / (double)node.getAdjLinks().length;
				for (int i = 0; i < node.getAdjLinks().length; i++) {
					context.write(new Text(node.getAdjLinks()[i]), new Text(String.valueOf(newRank)));
				}
				node.setPageRank(0.0);
				outVal = node.toString();
			}	
			
			context.write(new Text(tokens[0]),new Text(outVal));

		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

		public static final double DAMPING_FACTOR = 0.85;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String pagesStr = conf.get("pages");
			int pages = Integer.parseInt(pagesStr);

			double summedPageRanks = 0.0;
			PageRankNode origNode = new PageRankNode();
			
			for (Text textValue : values) {
				
				PageRankNode node = PageRankNode.getPageRankObj(textValue.toString());

				if (node.getAdjLinks() != null) {
					origNode = node;
				} else {
					summedPageRanks += node.getPageRank();
				}
				
			}
			
			double dampingFactor = ((1.0 - DAMPING_FACTOR) / (double) pages);

			double newPageRank = dampingFactor + (DAMPING_FACTOR * summedPageRanks);

			origNode.setPageRank(newPageRank);
			
			context.write(key, new Text(origNode.toString()));
		}
	}

}
