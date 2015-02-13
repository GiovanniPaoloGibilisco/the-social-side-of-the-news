package it.polimi.tssotn.dataloader;

import it.polimi.tssotn.common.SparkUtils;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

public class DataLoader {

	private static final Logger logger = LoggerFactory
			.getLogger(DataLoader.class);

	private static final String twitterDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/social-pulse-milano/data";
	private static final String newsDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/milanotoday/data";
	private static final String app_id = "5bb0e37c";
	private static final String app_key = "593dc19bbaaa08aad799e6a57db362cf";

	private static boolean runLocal = true;

	public static void main(String[] args) {
		JavaSparkContext sparkContext = null;
		try {

			Configuration hadoopConf = new Configuration();
			FileSystem hadoopFileSystem = FileSystem.get(hadoopConf);
			SparkConf sparkConf = new SparkConf()
					.setAppName("tssotn-data-loader");
			if (runLocal)
				sparkConf.setMaster("local[1]");
			sparkContext = new JavaSparkContext(sparkConf);

			Map<String, String> commonPars = new HashMap<String, String>();
			commonPars.put("$app_id", app_id);
			commonPars.put("$app_key", app_key);

			JavaRDD<JsonObject> tweetsChunks = SparkUtils.restToRDD(
					twitterDataURL, "$offset", "$limit", 1000, 0, 269290,
					commonPars, sparkContext);
			JavaRDD<JsonObject> newsChunks = SparkUtils.restToRDD(newsDataURL,
					"$offset", "$limit", 1000, 0, 3014, commonPars,
					sparkContext);

			SparkUtils.saveToFile(tweetsChunks, "loaded-data/tweets");
			SparkUtils.saveToFile(newsChunks, "loaded-data/news");

		} catch (Exception e) {
			logger.error("Unknown error", e);
		} finally {
			if (sparkContext != null)
				sparkContext.stop();
		}
	}

}
