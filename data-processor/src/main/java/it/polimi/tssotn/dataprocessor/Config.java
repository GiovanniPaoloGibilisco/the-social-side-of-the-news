package it.polimi.tssotn.dataprocessor;

import com.beust.jcommander.Parameter;

public class Config {

	@Parameter(names = { "-t", "--tweetPath" })
	public String tweetsPath = "tweets.json";

	@Parameter(names = { "-n", "--newsPath" })
	public String newsPath = "news.json";

	@Parameter(names = { "-o", "--outputPath" })
	public String outputPath = "output.json";

	@Parameter(names = { "-h", "--help" }, help = true)
	public boolean help;

}
