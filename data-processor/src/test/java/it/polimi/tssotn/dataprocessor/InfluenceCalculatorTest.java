package it.polimi.tssotn.dataprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class InfluenceCalculatorTest {
	// private SparkConf sparkConf;
	// private JavaSparkContext sparkContext;
	// private Configuration hc;
	// private FileSystem hadoopFileSystem;
	// private Config config;
	// private final String filteredTweetsPath = "filteredTweets.json";
	//
	// @Before
	// public void setUp() throws Exception {
	// System.clearProperty("spark.driver.port");
	// System.clearProperty("spark.hostPort");
	// Configuration hadoopConf = new Configuration();
	// hadoopFileSystem = FileSystem.get(hadoopConf);
	// SparkConf sparkConf = new SparkConf().setAppName("tssotn-data-loader")
	// .setMaster("local[1]");
	// sparkContext = new JavaSparkContext(sparkConf);
	//
	// hadoopFileSystem.copyFromLocalFile(
	// new Path(getClass().getResource("/tweets.json").toURI()),
	// new Path(config.tweetsPath));
	// hadoopFileSystem.copyFromLocalFile(
	// new Path(getClass().getResource("/news.json").toURI()),
	// new Path(config.newsPath));
	// hadoopFileSystem
	// .copyFromLocalFile(
	// new Path(getClass().getResource("/filteredTweets.json")
	// .toURI()), new Path(filteredTweetsPath));
	// }

	@Before
	public void setUp() {
		Config.getInstance().app_id = System.getProperty("app_id");
		Config.getInstance().app_key = System.getProperty("app_key");
	}

	@Test
	public void threeJsonObjectsShouldBeFound() {
		String string = "{acia,}}{aef,aefa,aef,aef,[afeafa]efa{EE},{}},{sa},{{acia,},{aefa}}{";
		List<String> jsons = InfluenceCalculator.splitJsonObjects(string);
		assertTrue(jsons.size() == 3);
	}

	@Test
	public void entitiesInDerbyShouldNotBeEmpty() {
		Tuple2<String, Set<String>> entities = InfluenceCalculator
				.extractEntities("http://www.milanotoday.it/sport/inter-milan-1-0-palacio-22-dicembre-2013.html",Config.getInstance());
		assertFalse(entities._2.isEmpty());
	}

	// @Test
	// public void splitByDataShouldProduce2News() {
	// Path inputNews = new Path(config.newsPath);
	// JavaRDD<String> newsFile = sparkContext.textFile(inputNews.toString(),
	// 1);
	// JavaRDD<String> splittedNews = InfluenceCalculator.splitByRow(newsFile);
	// JavaRDD<JsonObject> news = InfluenceCalculator.parseRows(splittedNews);
	// assertTrue(news.count() == 2);
	// }
	//
	// @Test
	// public void splitByDataShouldProduce9Tweets() {
	// Path inputTweets = new Path(config.tweetsPath);
	// JavaRDD<String> tweetFile = sparkContext.textFile(inputTweets.toString(),
	// 1);
	// JavaRDD<String> splittedTweets =
	// InfluenceCalculator.splitByRow(tweetFile);
	// JavaRDD<JsonObject> tweets =
	// InfluenceCalculator.parseRows(splittedTweets);
	// assertTrue(tweets.count() == 9);
	// }
	//
	// @Test
	// public void filterOutEmptEntitiesShouldrRemove1Tweet() {
	// Path inputTweets = new Path(config.tweetsPath);
	// JavaRDD<String> tweetFile = sparkContext.textFile(inputTweets.toString(),
	// 1);
	// JavaRDD<String> splittedTweets = InfluenceCalculator
	// .splitByRow(tweetFile);
	// JavaRDD<JsonObject> tweets =
	// InfluenceCalculator.parseRows(splittedTweets);
	// JavaRDD<JsonObject> noEmptyTweets =
	// InfluenceCalculator.filterOutEmptyEntities(tweets);
	//
	// JavaRDD<String> filteredTweeFile = sparkContext.textFile(new
	// Path(filteredTweetsPath).toString(), 1);
	// JavaRDD<String> splittedFilteredTweets = InfluenceCalculator
	// .splitByRow(filteredTweeFile);
	// JavaRDD<JsonObject> filteredTweets =
	// InfluenceCalculator.parseRows(splittedFilteredTweets);
	// assertEquals(filteredTweets.count(), noEmptyTweets.count());
	// }
	//
	// @Test
	// public void extractPairsShouldCreateNewsEntityPairs() {
	// Path newsFilePath = new Path(config.newsPath);
	// JavaRDD<String> newsFile = sparkContext.textFile(newsFilePath.toString(),
	// 1);
	// JavaRDD<String> splittedNews = InfluenceCalculator.splitByRow(newsFile);
	// JavaRDD<JsonObject> news = InfluenceCalculator.parseRows(splittedNews);
	// JavaPairRDD<String, String> newsEntityListMap = InfluenceCalculator
	// .extractPairs(news, "link", "entities");
	//
	// List<Tuple2<String, String>> collectedNewsEntity = newsEntityListMap
	// .collect();
	//
	// List<Tuple2<String, String>> expectedNewsEntities = new
	// ArrayList<Tuple2<String, String>>();
	// expectedNewsEntities.add(new Tuple2<String, String>("N1", "E1"));
	// expectedNewsEntities.add(new Tuple2<String, String>("N2", "E2,E3"));
	// assertTrue(collectedNewsEntity.equals(expectedNewsEntities));
	// }
	//
	// @Test
	// public void splitByValueAndSwapShouldCreate3News(){
	// Path newsFilePath = new Path(config.newsPath);
	// JavaRDD<String> newsFile = sparkContext.textFile(newsFilePath.toString(),
	// 1);
	// JavaRDD<String> splittedNews = InfluenceCalculator.splitByRow(newsFile);
	// JavaRDD<JsonObject> news = InfluenceCalculator.parseRows(splittedNews);
	// JavaPairRDD<String, String> newsEntityListMap =
	// InfluenceCalculator.extractPairs(news, "link", "entities");
	// JavaPairRDD<String, String> splitted =
	// InfluenceCalculator.splitValuesAndSwapKeyValue(newsEntityListMap);
	// assertTrue(splitted.count() == (long) 3);
	//
	// }
	//
	// @Test
	// public void processingShouldProduceSomeOutput() throws Exception{
	// InfluenceCalculator.processInputs(config.tweetsPath, config.newsPath,
	// config.outputPath);
	// }

	//
	// @After
	// public void tearDown() throws IllegalArgumentException, IOException {
	// try {
	// hadoopFileSystem.delete(new Path(config.tweetsPath), false);
	// hadoopFileSystem.delete(new Path(config.newsPath), false);
	// hadoopFileSystem.delete(new Path(filteredTweetsPath), false);
	// } finally {
	// sparkContext.stop();
	// sparkContext = null;
	// System.clearProperty("spark.driver.port");
	// System.clearProperty("spark.hostPort");
	// }
	// }

}
