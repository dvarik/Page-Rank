package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankSortMR {

	public static class PageRankSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		private DoubleWritable rank = new DoubleWritable();
		private Text title = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] tokens = line.split("\t", 3);
			title.set(tokens[0]);
			rank.set(Double.parseDouble(tokens[1]));
			context.write(rank, title);
		}

	}

	public static class PageRankSortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String pagesStr = conf.get("pages");
			int pages = Integer.parseInt(pagesStr);

			final double threshold = 5.0d / (double)pages;

			for (Text val : values) {
				if (key.get() >= threshold) {
					context.write(val, key);
				} else {
					return;
				}
			}
		}
	}

}
