package it.polimi.bigdataclass.thesocialsideofthenews;

import com.beust.jcommander.Parameter;

public class Config {

	@Parameter(names = { "-t", "--tweetPath" }, required = true)
	public String tweetsPath;

	@Parameter(names = { "-n", "--newsPath" }, required = true)
	public String newsPath;

	@Parameter(names = { "-o", "--outputPath" }, required = true)
	public String outputPath;

	@Parameter(names = { "-h", "--help" }, help = true)
	public boolean help;

}
