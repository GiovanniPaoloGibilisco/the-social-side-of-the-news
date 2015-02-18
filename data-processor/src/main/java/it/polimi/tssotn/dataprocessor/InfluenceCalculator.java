package it.polimi.tssotn.dataprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.Tuple3;

import com.beust.jcommander.ParameterException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class InfluenceCalculator {

	static final Logger logger = LoggerFactory
			.getLogger(InfluenceCalculator.class);

	public static void main(String[] args) {
		JavaSparkContext sparkContext = null;
		try {
			Config.init(args);
			Config config = Config.getInstance();
			Configuration hadoopConf = new Configuration();
			FileSystem hadoopFileSystem = FileSystem.get(hadoopConf);
			SparkConf sparkConf = new SparkConf()
					.setAppName("tssotn-data-processor");
			if (config.runLocal)
				sparkConf.setMaster("local[1]");
			sparkContext = new JavaSparkContext(sparkConf);
			int parallelize = sparkConf.getInt("spark.default.parallelism", 4);

			logger.info("Broadcasting the configuration");
			Broadcast<Config> broadcastVar = sparkContext.broadcast(config);
			config = broadcastVar.value();

			if (!hadoopFileSystem.exists(new Path(config.newsEntitiesPath)))
				throw new IOException(config.newsEntitiesPath
						+ " does not exist");
			if (!hadoopFileSystem.exists(new Path(config.tweetsPath)))
				throw new IOException(config.tweetsPath + " does not exist");

			JavaPairRDD<String, String> newsEntityLinkPairs = sparkContext
					.textFile(config.newsEntitiesPath)
					.flatMap(n -> Commons.splitJsonObjects(n))
					.mapToPair(n -> deserialize(n))
					.filter(n -> Commons.hasEntities(n._2))
					.flatMapToPair(n -> flatEntitiesAndSwap(n));

			logger.info("computed newsEntityLinkPairs");

			JavaPairRDD<String, Tuple2<String, String>> tweetsEntityIDTimestampPairs = sparkContext
					.textFile(config.tweetsPath, parallelize)
					.map(t -> Commons.removeNewLines(t))
					.flatMap(t -> Commons.splitJsonObjects(t))
					.mapToPair(
							t -> new Tuple2<String, String>(UUID.randomUUID()
									.toString(), t))
					.mapToPair(t -> extractTimestampsAndEntities(t))
					.filter(t -> Commons.hasEntities(t._2))
					.flatMapToPair(t -> flatEntitiesAndSwap(t));
			logger.info("computed tweetsEntityIDTimestampPairs");

			JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> entityInfluenceMap = newsEntityLinkPairs
					.join(tweetsEntityIDTimestampPairs);
			logger.info("computed join");

			JavaRDD<Tuple3<Tuple3<String, String, String>, Iterable<String>, Integer>> raw = entityInfluenceMap
					.mapToPair(r -> prepareKeyWithLinkIdTimestamp(r))
					.groupByKey().map(r -> addEntitiesCount(r));

			raw.saveAsTextFile(config.outputPathBase);
			logger.info("computed raw");

		} catch (IOException e) {
			logger.error("Wrong Hadoop configuration", e);
		} catch (ParameterException e) {
			logger.error("Wrong configuration: {}", e.getMessage());
		} catch (Exception e) {
			logger.error("Unknown error", e);
		} finally {
			if (sparkContext != null)
				sparkContext.stop();
		}
	}

	private static Tuple3<Tuple3<String, String, String>, Iterable<String>, Integer> addEntitiesCount(
			Tuple2<Tuple3<String, String, String>, Iterable<String>> r) {
		return new Tuple3<Tuple3<String, String, String>, Iterable<String>, Integer>(
				r._1, r._2, size(r._2));
	}

	static Tuple2<String, String> removeIdAndCounter(
			Tuple2<Tuple3<String, String, String>, Integer> p) {
		return new Tuple2<String, String>(p._1._1(), p._1._3());
	}

	static Tuple2<Tuple3<String, String, String>, String> prepareKeyWithLinkIdTimestamp(
			Tuple2<String, Tuple2<String, Tuple2<String, String>>> p) {
		return new Tuple2<Tuple3<String, String, String>, String>(
				new Tuple3<String, String, String>(p._2._1, p._2._2._1,
						p._2._2._2), p._1);
	}

	static int size(Iterable<?> it) {
		if (it instanceof Collection)
			return ((Collection<?>) it).size();
		int i = 0;
		for (@SuppressWarnings("unused")
		Object obj : it)
			i++;
		return i;
	}

	static Tuple2<Tuple2<String, String>, Set<String>> extractTimestampsAndEntities(
			Tuple2<String, String> t) {
		Set<String> entities = new HashSet<String>();
		JsonObject jsonTweet = new JsonParser().parse(t._2).getAsJsonObject();
		String timestamp = jsonTweet.get("timestamp").getAsString();
		JsonElement entitiesArray = jsonTweet.get("entities");
		if (entitiesArray != null) {
			JsonArray jsonEntities = entitiesArray.getAsJsonArray();
			for (JsonElement jsonElement : jsonEntities) {
				entities.add(jsonElement.getAsString());
			}
		}
		return new Tuple2<Tuple2<String, String>, Set<String>>(
				new Tuple2<String, String>(t._1, timestamp), entities);
	}

	static <T> List<Tuple2<String, T>> flatEntitiesAndSwap(
			Tuple2<T, Set<String>> n) {
		List<Tuple2<String, T>> nOut = new ArrayList<Tuple2<String, T>>();
		for (String entity : n._2) {
			nOut.add(new Tuple2<String, T>(entity, n._1));
		}
		return nOut;
	}

	static String getNewsLink(String s) {
		return new JsonParser().parse(s).getAsJsonObject().get("link")
				.getAsString();
	}

	static Tuple2<String, Set<String>> deserialize(String newsEntity) {
		JsonObject json = new JsonParser().parse(newsEntity).getAsJsonObject();
		String link = json.get("link").getAsString();
		Set<String> entities = new HashSet<String>();

		JsonElement jsonElement = json.get("entities");
		if (jsonElement != null)
			for (JsonElement entityElement : jsonElement.getAsJsonArray()) {
				entities.add(entityElement.getAsString());
			}
		return new Tuple2<String, Set<String>>(link, entities);
	}

}
