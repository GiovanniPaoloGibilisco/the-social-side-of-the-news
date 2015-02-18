package it.polimi.tssotn.dataprocessor;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ExtractorConfig implements Serializable {

	/**
	 * generated serial version UID
	 */
	private static final long serialVersionUID = 5087417577620830639L;

	private static ExtractorConfig _instance;

	static final Logger logger = LoggerFactory
			.getLogger(InfluenceCalculator.class);

	private ExtractorConfig() {
	}

	public static ExtractorConfig getInstance() {
		if (_instance == null) {
			_instance = new ExtractorConfig();
		}
		return _instance;
	}

	public static void init(String[] args) {
		_instance = new ExtractorConfig();
		new JCommander(_instance, args);
		logger.debug("Configuration: --newsPath {} --outputPathBase {} --appid {} --appkey {} --runlocal {} ",new Object[] { _instance.newsPath, _instance.outputPathBase, _instance.app_id, _instance.app_key, _instance.runLocal});
	}


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


}
