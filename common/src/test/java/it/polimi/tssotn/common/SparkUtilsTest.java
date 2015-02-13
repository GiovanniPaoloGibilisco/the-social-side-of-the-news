package it.polimi.tssotn.common;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;

public class SparkUtilsTest {

	private static final String twitterDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/social-pulse-milano/data";
	private static final String newsDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/milanotoday/data";
	private static final String app_id = "5bb0e37c";
	private static final String app_key = "593dc19bbaaa08aad799e6a57db362cf";
	
	private JavaSparkContext sparkContext;

	@Before
	public void setUp() throws IOException {
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");
		Configuration hadoopConf = new Configuration();
		FileSystem hadoopFileSystem = FileSystem.get(hadoopConf);
		SparkConf sparkConf = new SparkConf()
				.setAppName("tssotn-data-loader").setMaster("local[1]");
		sparkContext = new JavaSparkContext(sparkConf);
	}

	@Test
	public void twitterDataShouldBeDownloaded() {
		Map<String, String> commonPars = new HashMap<String, String>();
		commonPars.put("$app_id", app_id);
		commonPars.put("$app_key", app_key);
		JavaRDD<JsonObject> tweetsChunks = SparkUtils.restToRDD(twitterDataURL,
				"$offset", "$limit", 1000, 0, 2000, commonPars,sparkContext);
		assertTrue(tweetsChunks.count() == 2);
	}
	
	@Test
	public void milanoTodayDataShouldBeDownloaded() {
		Map<String, String> commonPars = new HashMap<String, String>();
		commonPars.put("$app_id", app_id);
		commonPars.put("$app_key", app_key);
		JavaRDD<JsonObject> milanoTodayChunks = SparkUtils.restToRDD(newsDataURL,
				"$offset", "$limit", 500, 0, 3014, commonPars, sparkContext);
		assertTrue(milanoTodayChunks.count() > 0);
	}

	@After
	public void tearDown() {
		sparkContext.stop();
		sparkContext = null;
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");
	}
}
