package it.polimi.tssotn.dataprocessor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Config {

	private static Config _instance;

	private Config() {
	}

	public static Config getInstance() {
		if (_instance == null) {
			_instance = new Config();
		}
		return _instance;
	}

	public static void init(String[] args) {
		_instance = new Config();
		new JCommander(_instance, args);
	}

	@Parameter(names = { "-t", "--tweetPath" }, required = true)
	public String tweetsPath;

	@Parameter(names = { "-n", "--newsPath" }, required = true)
	public String newsPath;

	@Parameter(names = { "-o", "--outputPathBase" }, required = true)
	public String outputPathBase;

	@Parameter(names = { "-i", "--appid" }, required = true)
	public String app_id;

	@Parameter(names = { "-k", "--appkey" }, required = true)
	public String app_key;

	@Parameter(names = { "-l", "--runLocal" })
	public boolean runLocal;

	@Parameter(names = { "-m", "--minMatches" })
	public Integer minMatches = 1;

}
