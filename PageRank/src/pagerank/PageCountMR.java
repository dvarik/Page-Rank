package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PageCountMR {

	public static class PageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final Text title = new Text("title");
		private final IntWritable count = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			context.write(title, count);
		}
	}

	public static class PageCountReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int totalCount = 0;

			for (IntWritable v : values) {
				totalCount++;
			}

			context.write(new IntWritable(totalCount), null);
		}
	}
}
