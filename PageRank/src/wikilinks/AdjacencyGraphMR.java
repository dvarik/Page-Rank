package wikilinks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjacencyGraphMR {

	public static class AdjacencyGraphMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text title = new Text();
		private Text link = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String[] parts = line.split("\t", -1);
			title.set(parts[0]);
			link.set("");

			if (parts.length > 1) {
				link.set(parts[1]);
			}
			context.write(title, link);
		}
	}

	public static class AdjacencyGraphReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();

			for (Text v : values) {
				String val = v.toString();
				if (!val.equals("")) {
					sb.append(val + '\t');
				}
			}

			if (sb.length() > 0) {
				sb.setLength(sb.length() - 1); // remove last tab
				context.write(key, new Text(sb.toString()));
			} else {
				context.write(key, null);
			}
		}
	}
}
