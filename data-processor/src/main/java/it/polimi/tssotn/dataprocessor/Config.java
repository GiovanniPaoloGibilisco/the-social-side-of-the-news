package it.polimi.tssotn.dataprocessor;

import com.beust.jcommander.Parameter;

public class Config {

	@Parameter(names = { "-t", "--tweetPath" })
	public String tweetsPath = "target/tweets.json";

	@Parameter(names = { "-n", "--newsPath" })
	public String newsPath = "target/news.json";

	@Parameter(names = { "-o", "--outputPath" })
	public String outputPath = "target/output.json";

	@Parameter(names = { "-h", "--help" }, help = true)
	public boolean help;

}
