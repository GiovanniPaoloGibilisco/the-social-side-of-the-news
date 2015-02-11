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
import org.apache.spark.api.java.function.Function2;
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

		final int min_entities_matching = 1;
		final String outputFile = (args[2] != null) ? args[2] : "output.txt";

		try {

			Configuration hadoopConf = new Configuration();
			fs = FileSystem.get(hadoopConf);

			Path inputTweets = new Path(args[0]);
			Path inputNews = new Path(args[1]);

			checkDataExists(inputTweets);
			checkDataExists(inputNews);

			JavaRDD<String> newsByRow = splitByRow(inputNews);

			newsByRow = newsByRow.filter(new Function<String, Boolean>() {
				public Boolean call(String news) throws Exception {
					JsonParser parser = new JsonParser();
					JsonObject jsonNews = parser.parse(news).getAsJsonObject();
					return jsonNews.get("entities") != null;
				}
			});

			JavaPairRDD<String, String> newsEntityListMap = newsByRow
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

			JavaPairRDD<String, String> entityNewsMap = newsEntityListMap
					.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
						public Iterable<Tuple2<String, String>> call(
								Tuple2<String, String> news) throws Exception {
							List<String> entities = Arrays.asList(news._2
									.split(","));
							List<Tuple2<String, String>> newsList = new ArrayList<Tuple2<String, String>>();
							for (String entity : entities) {
								newsList.add(new Tuple2<String, String>(entity,
										news._1));
							}
							return newsList;
						}
					});

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

			JavaPairRDD<String, String> entityTweetMap = tweetEntityListMap
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

			// join entities and news with entities and tweets
			JavaPairRDD<String, Tuple2<String, String>> entityInfluenceMap = entityNewsMap
					.join(entityTweetMap);

			// filter entities without tweets or news
			entityInfluenceMap
					.filter(new Function<Tuple2<String, Tuple2<String, String>>, Boolean>() {
						@Override
						public Boolean call(
								Tuple2<String, Tuple2<String, String>> match)
								throws Exception {
							return match._2._1 != null && match._2._2 != null;
						}
					});

			// drop the entity, use url and timestamp as key and initialize the
			// counters
			JavaPairRDD<Tuple2<String, String>, Integer> countingInfluenceMap = entityInfluenceMap
					.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, Tuple2<String, String>, Integer>() {
						@Override
						public Tuple2<Tuple2<String, String>, Integer> call(
								Tuple2<String, Tuple2<String, String>> match)
								throws Exception {
							return new Tuple2<Tuple2<String, String>, Integer>(
									match._2, 1);
						}
					});

			// sum up all the entities matched in tweets
			JavaPairRDD<Tuple2<String, String>, Integer> summedInfluenceMap = countingInfluenceMap
					.reduceByKey(new Function2<Integer, Integer, Integer>() {
						@Override
						public Integer call(Integer first, Integer second)
								throws Exception {
							return first + second;
						}
					});

			// filter tweets with not enought entities matching
			summedInfluenceMap = summedInfluenceMap
					.filter(new Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>() {
						@Override
						public Boolean call(
								Tuple2<Tuple2<String, String>, Integer> influence)
								throws Exception {
							return influence._2 >= min_entities_matching;
						}
					});

			// drop entity counter
			JavaPairRDD<String, String> influenceMap = summedInfluenceMap
					.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, String>() {
						@Override
						public Tuple2<String, String> call(
								Tuple2<Tuple2<String, String>, Integer> matching)
								throws Exception {
							return matching._1;
						}
					});

			influenceMap.saveAsTextFile(outputFile);

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
