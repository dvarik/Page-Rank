package pagerank;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class PageRankNode {

	private double pageRank = 0.0;

	private String[] adjLinks = null;

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public String[] getAdjLinks() {
		return adjLinks;
	}

	public void setAdjLinks(String[] adjLinks) {
		this.adjLinks = adjLinks;
	}

	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);

		if (getAdjLinks() != null) {
			sb.append("\t").append(StringUtils.join(getAdjLinks(), "\t"));
		}

		return sb.toString();
	}

	public static PageRankNode getPageRankObj(String value) {

		String tokens[] = value.split("\t");
		
		PageRankNode node = new PageRankNode();
		node.setPageRank(Double.parseDouble(tokens[0]));

		if (tokens.length > 1) {
			node.setAdjLinks(Arrays.copyOfRange(tokens, 1, tokens.length));
		}

		return node;
	}

}
