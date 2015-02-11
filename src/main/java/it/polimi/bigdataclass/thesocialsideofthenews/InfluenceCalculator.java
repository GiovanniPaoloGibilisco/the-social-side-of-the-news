package it.polimi.bigdataclass.thesocialsideofthenews;

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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InfluenceCalculator {
	static final Logger logger = LoggerFactory
			.getLogger(InfluenceCalculator.class);

	static private SparkConf conf = new SparkConf().setAppName(
			"The-social-side-of-the-news").setMaster("local[1]");
	static private JavaSparkContext sc = new JavaSparkContext(conf);
	static private FileSystem fs;

	public static void main(String[] args) {

		try {
			Config config = new Config();
			JCommander jcm = new JCommander(config, args);
			if (config.help) {
				jcm.usage();
				return;
			}
			final int min_entities_matching = 1;

			Configuration hadoopConf = new Configuration();
			fs = FileSystem.get(hadoopConf);

			Path inputTweets = new Path(config.tweetsPath);
			Path inputNews = new Path(config.newsPath);

			checkDataExists(inputTweets);
			checkDataExists(inputNews);

			JavaRDD<String> newsByRow = splitByRow(inputNews);
			newsByRow = filterOutEmptyEntities(newsByRow);
			JavaPairRDD<String, String> newsEntityListMap = extractPairs(newsByRow,"link","entities");
			JavaPairRDD<String, String> entityNewsMap = splitValuesAndSwapKeyValue(newsEntityListMap);

			JavaRDD<String> tweetsByRow = splitByRow(inputTweets);
			tweetsByRow = filterOutEmptyEntities(tweetsByRow);
			JavaPairRDD<String, String> tweetEntityListMap = extractPairs(tweetsByRow, "timestamp", "entities");
			JavaPairRDD<String, String> entityTweetMap = splitValuesAndSwapKeyValue(tweetEntityListMap);

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

			influenceMap.saveAsTextFile(config.outputPath);

		} catch (IOException e) {
			logger.error("Wrong Hadoop configuration", e);
		} catch (DataNotFoundException e) {
			logger.error("Input Data not found", e);
		} catch (ParameterException e) {
			logger.error("Wrong parameters. Required parameters: --newsPath, --tweetPath, --outputPath");
		} catch (Exception e) {
			logger.error("Unknown error", e);
		} finally {
			sc.stop();
		}
	}

	private static JavaPairRDD<String, String> splitValuesAndSwapKeyValue(
			JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> newData = data
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {
					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, String> pair) throws Exception {
						List<String> values = Arrays.asList(pair._2
								.split(","));
						List<Tuple2<String, String>> newsList = new ArrayList<Tuple2<String, String>>();
						for (String value : values) {
							newsList.add(new Tuple2<String, String>(value,
									pair._1));
						}
						return newsList;
					}
				});
		return newData;
	}

	private static JavaPairRDD<String, String> extractPairs(
			JavaRDD<String> data, String field1Name, String field2Name) {
		JavaPairRDD<String, String> dataPairs = data
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String row)
							throws Exception {
						JsonParser parser = new JsonParser();
						JsonObject json = parser.parse(row)
								.getAsJsonObject();
						String field1Value = json.get(field1Name).getAsString();
						String field2Value = json.get(field2Name)
								.getAsString();
						return new Tuple2<String, String>(field1Value, field2Value);
					}
				});
		return dataPairs;
	}

	private static JavaRDD<String> filterOutEmptyEntities(
			JavaRDD<String> data) {
		data = data.filter(new Function<String, Boolean>() {
			public Boolean call(String news) throws Exception {
				JsonParser parser = new JsonParser();
				JsonObject jsonNews = parser.parse(news).getAsJsonObject();
				return jsonNews.get("entities") != null;
			}
		});
		return data;
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
