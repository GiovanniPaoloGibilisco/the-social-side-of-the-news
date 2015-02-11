package it.polimi.bigdataclass.thesocialsideofthenews;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InfluenceCalculatorTest {

	private SparkContext sc;
	private Config config;

	@Before
	public void setUp() throws IOException {
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");

		SparkConf conf = new SparkConf().setAppName(
				"The-social-side-of-the-news").setMaster("local[1]");
		config = new Config();
		sc = new SparkContext(conf);
		InfluenceCalculator.initHadoopFileSystem();
	}

	@Test
	public void inputDataShouldExist() throws Exception {
		Path inputTweets = new Path(config.tweetsPath);
		InfluenceCalculator.checkDataExists(inputTweets);
	}

	@After
	public void tearDown() {
		sc.stop();
		sc = null;
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");
	}

}
