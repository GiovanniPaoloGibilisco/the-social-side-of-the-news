package it.polimi.tssotn.dataprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class InfluenceCalculatorTest {
	private SparkConf sparkConf;
	private JavaSparkContext sc;
	private Configuration hc;
	private FileSystem hadoopFileSystem;
	private Config config;
	private final String filteredTweetsPath = "filteredTweets.json";

	@Before
	public void setUp() throws Exception {
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");

		config = new Config();

		hc = new Configuration();
		InfluenceCalculator.initHadoopFileSystem(hc);
		hadoopFileSystem = InfluenceCalculator.hadoopFileSystem;

		sparkConf = new SparkConf().setAppName("The-social-side-of-the-news")
				.setMaster("local[1]");
		InfluenceCalculator.initSpark(sparkConf);
		sc = InfluenceCalculator.sparkContext;

		hadoopFileSystem.copyFromLocalFile(
				new Path(getClass().getResource("/tweets.json").toURI()),
				new Path(config.tweetsPath));
		hadoopFileSystem.copyFromLocalFile(
				new Path(getClass().getResource("/news.json").toURI()),
				new Path(config.newsPath));
		hadoopFileSystem
				.copyFromLocalFile(
						new Path(getClass().getResource("/filteredTweets.json")
								.toURI()), new Path(filteredTweetsPath));
	}

	@Test
	public void splitByDataShouldProduce2News() {
		Path inputNews = new Path(config.newsPath);
		JavaRDD<String> newsFile = sc.textFile(inputNews.toString(), 1).cache();
		JavaRDD<String> splittedNews = InfluenceCalculator.splitByRow(newsFile);
		assertTrue(splittedNews.count() == 2);
	}

	@Test
	public void splitByDataShouldProduce9Tweets() {
		Path inputTweets = new Path(config.tweetsPath);
		JavaRDD<String> tweetFile = sc.textFile(inputTweets.toString(), 1)
				.cache();
		JavaRDD<String> splittedTweets = InfluenceCalculator
				.splitByRow(tweetFile);
		assertTrue(splittedTweets.count() == 9);
	}

	@Test
	public void filterOutEmptEntitiesShouldrRemove1Tweet() {
		Path inputTweets = new Path(config.tweetsPath);
		JavaRDD<String> tweetFile = sc.textFile(inputTweets.toString(), 1);
		JavaRDD<String> splittedTweets = InfluenceCalculator
				.splitByRow(tweetFile);
		JavaRDD<String> noEmptyTweets = InfluenceCalculator
				.filterOutEmptyEntities(splittedTweets);

		Path filteredTweets = new Path(filteredTweetsPath);
		JavaRDD<String> filteredTweeFile = sc.textFile(
				filteredTweets.toString(), 1);
		JavaRDD<String> splittedFilteredTweets = InfluenceCalculator
				.splitByRow(filteredTweeFile);

		assertEquals(splittedFilteredTweets.collect(), noEmptyTweets.collect());
	}

	@Test
	public void extractPairsShouldCreateNewsEntityPairs() {
		Path newsFilePath = new Path(config.newsPath);
		JavaRDD<String> newsFile = sc.textFile(newsFilePath.toString(), 1);
		JavaRDD<String> splittedNews = InfluenceCalculator.splitByRow(newsFile);
		JavaPairRDD<String, String> newsEntityListMap = InfluenceCalculator
				.extractPairs(splittedNews, "link", "entities");

		List<Tuple2<String, String>> collectedNewsEntity = newsEntityListMap
				.collect();

		List<Tuple2<String, String>> expectedNewsEntities = new ArrayList<Tuple2<String, String>>();
		expectedNewsEntities.add(new Tuple2<String, String>("N1", "E1"));
		expectedNewsEntities.add(new Tuple2<String, String>("N2", "E2,E3"));

		assertTrue(collectedNewsEntity.equals(expectedNewsEntities));

	}

	@After
	public void tearDown() throws IllegalArgumentException, IOException {
		try {
			hadoopFileSystem.delete(new Path(config.tweetsPath), false);
			hadoopFileSystem.delete(new Path(config.newsPath), false);
			hadoopFileSystem.delete(new Path(filteredTweetsPath), false);
		} finally {
			sc.stop();
			sc = null;
			System.clearProperty("spark.driver.port");
			System.clearProperty("spark.hostPort");
		}
	}

}
