package it.polimi.tssotn.dataprocessor;

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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InfluenceCalculator {
	static final Logger logger = LoggerFactory
			.getLogger(InfluenceCalculator.class);

	static Configuration hadoopConf;
	static FileSystem hadoopFileSystem;
	static SparkConf sparkConf;
	static JavaSparkContext sparkContext;		
	final static int min_entities_matching = 1;

	public static void main(String[] args) {

		try {
			Config config = new Config();
			JCommander jcm = new JCommander(config, args);
			if (config.help) {
				jcm.usage();
				return;
			}


			initHadoopFileSystem(new Configuration());
			initSpark(new SparkConf().setAppName("The-social-side-of-the-news"));



			Path inputTweets = new Path(config.tweetsPath);
			Path inputNews = new Path(config.newsPath);

			checkDataExists(inputTweets);
			checkDataExists(inputNews);

			JavaRDD<String> newsFile = sparkContext.textFile(inputNews.toString(), 1).cache();
			JavaRDD<String> newsByRow = splitByRow(newsFile);
			newsByRow = filterOutEmptyEntities(newsByRow);
			JavaPairRDD<String, String> newsEntityListMap = extractPairs(newsByRow, "link", "entities");
			JavaPairRDD<String, String> entityNewsMap = splitValuesAndSwapKeyValue(newsEntityListMap);

			JavaRDD<String> tweetFile = sparkContext.textFile(inputTweets.toString(), 1).cache();
			JavaRDD<String> tweetsByRow = splitByRow(tweetFile );
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
					.mapToPair(matching -> matching._1);

			influenceMap.saveAsTextFile(config.outputPath);

		} catch (IOException e) {
			logger.error("Wrong Hadoop configuration", e);
		} catch (DataNotFoundException e) {
			logger.error("Input Data not found", e);
		} catch (ParameterException e) {
			logger.error("Wrong configuration. Required parameters: --newsPath, --tweetPath, --outputPath");
		} catch (Exception e) {
			logger.error("Unknown error", e);
		} finally {
			if (sparkContext != null)
				sparkContext.stop();
		}
	}

	static void initSpark(SparkConf sparkConf) {
		InfluenceCalculator.sparkConf = sparkConf;
		sparkContext = new JavaSparkContext(sparkConf);		
	}

	static void initHadoopFileSystem(Configuration hadoopConf) throws IOException {			
		InfluenceCalculator.hadoopConf = hadoopConf;
		hadoopFileSystem = FileSystem.get(InfluenceCalculator.hadoopConf);
	}

	private static JavaPairRDD<String, String> splitValuesAndSwapKeyValue(
			JavaPairRDD<String, String> data) {
		JavaPairRDD<String, String> newData = data
				.flatMapToPair(pair -> {
					List<String> values = Arrays.asList(pair._2.split(","));
					List<Tuple2<String, String>> newsList = new ArrayList<Tuple2<String, String>>();
					for (String value : values) {
						newsList.add(new Tuple2<String, String>(value,
								pair._1));
					}
					return newsList;
				});
		return newData;
	}

	/**
	 * Generets a PairRDD using as key the element the element in the json corresponding with the first parameter and as value the element corresponding with the second element. In case elements are arrays all the elements of the arrays are used
	 * @param data an RDD containing one JSON object per string
	 * @param field1Name the name of the element to use as key  
	 * @param field2Name the name of thelement to use as value
	 * @return
	 */
	static JavaPairRDD<String, String> extractPairs(
			JavaRDD<String> data, String field1Name, String field2Name) {
		JavaPairRDD<String, String> dataPairs = data
				.mapToPair(new PairFunction<String, String, String>() {

					private String field1Name;
					private String field2Name;

					public Tuple2<String, String> call(String row)
							throws Exception {
						JsonParser parser = new JsonParser();
						JsonObject json = parser.parse(row).getAsJsonObject();						
						
						JsonElement keyElement = json.get(field1Name);
						String key;
						if(keyElement.isJsonArray())
							key = keyElement.getAsJsonArray().getAsString();
						key = keyElement.getAsString();
						
						JsonElement valueElement = json.get(field2Name);
						String value;
						if(valueElement.isJsonArray())
							value = valueElement.getAsJsonArray().getAsString();
						value = valueElement.getAsString();						
						return new Tuple2<String, String>(key,value);
					}

					public PairFunction<String, String, String> initialize(String field1Name, String field2Name){
						this.field1Name = field1Name;
						this.field2Name = field2Name;
						return this;
					}
				}.initialize(field1Name, field2Name));
		return dataPairs;
	}

	static JavaRDD<String> filterOutEmptyEntities(JavaRDD<String> data) {
		data = data.filter(news -> {
			JsonParser parser = new JsonParser();
			JsonObject jsonNews = parser.parse(news).getAsJsonObject();
			return jsonNews.get("entities") != null;
		});
		return data;
	}

	static JavaRDD<String> splitByRow(JavaRDD<String> data) {		
		return data.flatMap(s -> Arrays.asList(s.split("\n")));		
	}

	static void checkOutputGeneration(Path outputData) throws Exception{
		if(!hadoopFileSystem.exists(outputData)){
			throw new OutputNotProducedException(outputData.toString());
		}
	}

	static void checkDataExists(Path inputData) throws Exception {
		if (!hadoopFileSystem.exists(inputData)) {
			throw new DataNotFoundException(inputData.toString());
		}
	}

}
