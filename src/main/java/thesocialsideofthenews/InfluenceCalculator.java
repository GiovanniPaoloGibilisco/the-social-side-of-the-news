package thesocialsideofthenews;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InfluenceCalculator {
	static final Logger logger = LoggerFactory
			.getLogger(InfluenceCalculator.class);

	static private SparkConf conf = new SparkConf().setAppName(
			"The social side of the news").setMaster("local[1]");
	static private JavaSparkContext sc = new JavaSparkContext(conf);
	static private FileSystem fs;

	public static void main(String[] args) {

		try {

			Configuration hadoopConf = new Configuration();
			fs = FileSystem.get(hadoopConf);

			Path inputTweets = new Path("tweets.json");
			Path inputNews = new Path("news.json");

			checkDataExists(inputTweets);
			checkDataExists(inputNews);

			// split news
			JavaRDD<String> news = splitByRow(inputNews);

			news = news.filter(new Function<String, Boolean>() {
				public Boolean call(String news) throws Exception {
					JsonParser parser = new JsonParser();
					JsonObject jsonNews = parser.parse(news).getAsJsonObject();
					return jsonNews.get("entities") != null;
				}
			});

			JavaPairRDD<String, String> newsEntityListMap = news
					.mapToPair(new PairFunction<String, String, String>() {
						public Tuple2<String, String> call(String news)
								throws Exception {
							JsonParser parser = new JsonParser();
							JsonObject jsonNews = parser.parse(news)
									.getAsJsonObject();
							String link = jsonNews.get("link").getAsString();
							String entities = jsonNews.get("entities")
									.getAsString();
							return new Tuple2<String, String>(link, entities);
						}
					});

			JavaPairRDD<String, String> newsEntityMap = newsEntityListMap
					.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
						public Iterable<Tuple2<String, String>> call(
								Tuple2<String, String> news) throws Exception {
							List<String> entities = Arrays.asList(news._2
									.split(","));
							List<Tuple2<String, String>> newsList = new ArrayList<Tuple2<String, String>>();
							for (String entity : entities) {
								newsList.add(new Tuple2<String, String>(
										news._1, entity));
							}
							return newsList;
						}
					});

			// split tweets
			JavaRDD<String> tweets = splitByRow(inputTweets);

			tweets = tweets.filter(new Function<String, Boolean>() {
				public Boolean call(String tweet) throws Exception {
					JsonParser parser = new JsonParser();
					JsonObject jsonTweet = parser.parse(tweet)
							.getAsJsonObject();
					return jsonTweet.get("entities") != null;
				}
			});

			JavaPairRDD<String, String> tweetEntityListMap = tweets
					.mapToPair(new PairFunction<String, String, String>() {
						public Tuple2<String, String> call(String tweet)
								throws Exception {
							JsonParser parser = new JsonParser();
							JsonObject jsonTweet = parser.parse(tweet)
									.getAsJsonObject();
							String link = jsonTweet.get("timestamp")
									.getAsString();
							String entities = jsonTweet.get("entities")
									.getAsString();
							return new Tuple2<String, String>(link, entities);
						}
					});

			JavaPairRDD<String, String> tweetEntityMap = tweetEntityListMap
					.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
						public Iterable<Tuple2<String, String>> call(
								Tuple2<String, String> tweet) throws Exception {
							List<String> entities = Arrays.asList(tweet._2
									.split(","));
							List<Tuple2<String, String>> tweets = new ArrayList<Tuple2<String, String>>();
							for (String entity : entities) {
								tweets.add(new Tuple2<String, String>(tweet._1,
										entity));
							}
							return tweets;
						}
					});

		} catch (IOException e) {
			logger.error("Wrong Hadoop configuration", e);
		} catch (DataNotFoundException e) {
			logger.error("Input Data not found", e);
		} catch (Exception e) {
			logger.error("Unknown error", e);
		} finally {
			sc.stop();
		}
	}

	private static JavaRDD<String> splitByRow(Path data) {
		JavaRDD<String> dataByRow = sc.textFile(data.toString(), 1)
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String s) throws Exception {
						return Arrays.asList(s.split("\n"));
					}
				}).cache();
		return dataByRow;
	}

	private static void checkDataExists(Path inputData) throws Exception {
		if (!fs.exists(inputData)) {
			throw new DataNotFoundException(inputData.toString());
		}
	}

}
