package wikilinks;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

public class WikiExtractorMR {

	public static class WikiLinkMapper extends Mapper<Object, Text, Text, Text> {

		final static Pattern OUTLINK_PATTERN = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])", Pattern.DOTALL);

		Text title = new Text();
		Text titleMarker = new Text("_titlemarker_");
		Text outlink = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] vals = getTitleAndText(line);

			if (!vals[0].isEmpty()) {
				title.set(vals[0].replaceAll(" ", "_"));
				// mark it as title, so that reducer can remove red links using
				// this
				context.write(title, titleMarker);

				if (!vals[1].isEmpty()) {
					String text = vals[1];
					Matcher link = OUTLINK_PATTERN.matcher(text);
					while (link.find()) {
						outlink.set(link.group(1).replaceAll(" ", "_"));
						context.write(outlink, title);
					}
				}
			}
		}

		/**
		 * Use dom4j parser to parse each page and return title and text data
		 * 
		 * @param xml
		 * @return
		 */
		private static String[] getTitleAndText(String xml) {

			String elms[] = new String[2];

			SAXReader reader = new SAXReader();
			try {
				Document doc = reader.read(new StringReader(xml));
				Element rootElement = doc.getRootElement();
				elms[0] = rootElement.selectSingleNode("title").getText();
				Node revision = rootElement.selectSingleNode("revision");
				elms[1] = revision.selectSingleNode("text").getText();
			} catch (DocumentException e1) {
				e1.printStackTrace();
			}
			return elms;
		}
	}

	public static class WikiLinkReducer extends Reducer<Text, Text, Text, Text> {

		private String titleMarker = new String("_titlemarker_");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashSet<String> pages = new HashSet<>();
			for (Text val : values) {
				pages.add(val.toString());
			}
			// check if link is title to eliminate red links
			if (pages.contains(titleMarker)) {
				context.write(null, key);
				pages.remove(titleMarker);
				pages.remove(key.toString());

				for (String p : pages) {
					context.write(new Text(p), new Text(key + "\t"));
				}
			}
		}
	}
}
