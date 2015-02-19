package it.polimi.tssotn.dataprocessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import com.google.gson.JsonPrimitive;

public class InfluenceCalculator {

	static final Logger logger = LoggerFactory
			.getLogger(InfluenceCalculator.class);
	private static FileSystem hadoopFileSystem;

	public static void main(String[] args) {
		JavaSparkContext sparkContext = null;
		try {
			Config.init(args);
			Config config = Config.getInstance();
			Configuration hadoopConf = new Configuration();
			hadoopFileSystem = FileSystem.get(hadoopConf);
			SparkConf sparkConf = new SparkConf()
					.setAppName("tssotn-data-processor");
			if (config.runLocal)
				sparkConf.setMaster("local[1]");
			sparkContext = new JavaSparkContext(sparkConf);
			int parallelize = sparkConf.getInt("spark.default.parallelism", 4);

			logger.info("Broadcasting the configuration");
			Broadcast<Config> broadcastVar = sparkContext.broadcast(config);
			config = broadcastVar.value();
			final int minMatches = config.minMatches;

			if (!hadoopFileSystem.exists(new Path(config.newsEntitiesPath)))
				throw new IOException(config.newsEntitiesPath
						+ " does not exist");
			if (!hadoopFileSystem.exists(new Path(config.tweetsPath)))
				throw new IOException(config.tweetsPath + " does not exist");

			JavaPairRDD<String, Set<String>> newsLinkEntitiesPairs = sparkContext
					.textFile(config.newsEntitiesPath)
					.flatMap(n -> Commons.splitJsonObjects(n))
					.mapToPair(n -> deserialize(n))
					.filter(n -> Commons.hasEntities(n._2));

			JavaPairRDD<String, String> newsEntityLinkPairs = newsLinkEntitiesPairs
					.flatMapToPair(n -> flatEntitiesAndSwap(n));

			logger.info("computed newsEntityLinkPairs");

			JavaPairRDD<String, Tuple2<String, Integer>> tweetsEntityIDTimestampPairs = sparkContext
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

			JavaPairRDD<String, Tuple2<String, Tuple2<String, Integer>>> joinEntityLinkIDTimestamp = newsEntityLinkPairs
					.join(tweetsEntityIDTimestampPairs);
			logger.info("computed join");

			JavaRDD<Tuple3<Tuple3<String, String, Integer>, Iterable<String>, Integer>> newsTweetMatching = joinEntityLinkIDTimestamp
					.mapToPair(r -> prepareKeyWithLinkIdTimestamp(r))
					.groupByKey().map(r -> addEntitiesCount(r));

			JavaRDD<Tuple3<Tuple3<String, String, Integer>, Iterable<String>, Integer>> filteredNewsTweetMatching = newsTweetMatching
					.filter(r -> r._3() >= minMatches);

			JavaRDD<Tuple2<Tuple3<String, String, Integer>, String>> filteredNewsTweetEntity = filteredNewsTweetMatching
					.flatMap(m -> flatEntities(m));

			JavaPairRDD<String, Integer> newsTwees = filteredNewsTweetMatching
					.mapToPair(n -> new Tuple2<String, Integer>(n._1()._1(), n
							._1()._3()));

			JavaPairRDD<String, Iterable<Integer>> newsTweetTimes = newsTwees
					.groupByKey();

			JavaRDD<String> newsTweetsJson = newsTweetTimes
					.map(n -> serializeTimeserieToJson(n));

			saveToJson(newsTweetsJson.collect(), config.outputPathBase,
					"newsTimeSeries");

			Map<String, Object> nTweetByNews = sortByValue(filteredNewsTweetMatching
					.mapToPair(
							n -> new Tuple2<String, String>(n._1()._1(), n._1()
									._2())).countByKey());
			saveToCSV(nTweetByNews, config.outputPathBase, "nTweetByNews");

			Map<String, Object> nNewsByNewsEntity = sortByValue(newsEntityLinkPairs
					.countByKey());
			saveToCSV(nNewsByNewsEntity, config.outputPathBase,
					"nNewsByNewsEntity");

			Map<String, Object> nNewsByMatchedEntity = sortByValue(filteredNewsTweetEntity
					.mapToPair(j -> new Tuple2<String, String>(j._2, j._1._1()))
					.distinct().countByKey());
			saveToCSV(nNewsByMatchedEntity, config.outputPathBase,
					"nNewsByMatchedEntity");

			Map<String, Object> nTweetsByTweetsEntity = sortByValue(tweetsEntityIDTimestampPairs
					.countByKey());
			saveToCSV(nTweetsByTweetsEntity, config.outputPathBase,
					"nTweetsByTweetsEntity");

			Map<String, Object> nTweetsByMatchedEntity = sortByValue(filteredNewsTweetEntity
					.mapToPair(j -> new Tuple2<String, String>(j._2, j._1._2()))
					.distinct().countByKey());
			saveToCSV(nTweetsByMatchedEntity, config.outputPathBase,
					"nTweetsByMatchedEntity");

			Map<String, Object> nTweetsByNotMatchedEntities = subtractByKey(
					nTweetsByTweetsEntity, nTweetsByMatchedEntity);
			saveToCSV(nTweetsByNotMatchedEntities, config.outputPathBase,
					"nTweetsByNotMatchedEntities");

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

	private static void saveToJson(List<String> jsonObjects,
			String outputPathBase, String filename) throws IOException {
		Path outFile = new Path(outputPathBase, filename);
		FSDataOutputStream outStream = hadoopFileSystem.create(outFile);
		Writer writer = new BufferedWriter(new OutputStreamWriter(outStream,
				Charsets.UTF_8));
		writer.write("[");
		for (String jsonObject : jsonObjects) {
			writer.write(jsonObject);
			if (jsonObjects.size() - 1 != jsonObjects.lastIndexOf(jsonObject))
				writer.write(",");
		}

		writer.write("]");
		writer.flush();
		writer.close();
	}

	private static void saveToCSV(Map<String, Object> map,
			String outputPathBase, String filename) throws IOException {
		Path outFile = new Path(outputPathBase, filename);
		FSDataOutputStream outStream = hadoopFileSystem.create(outFile);
		Writer writer = new BufferedWriter(new OutputStreamWriter(outStream,
				Charsets.UTF_8));
		for (Entry<String, Object> entry : map.entrySet())
			writer.write(entry.getKey() + "," + entry.getValue() + "\n");
		writer.flush();
		writer.close();

	}

	private static List<Tuple2<Tuple3<String, String, Integer>, String>> flatEntities(
			Tuple3<Tuple3<String, String, Integer>, Iterable<String>, Integer> m) {
		List<Tuple2<Tuple3<String, String, Integer>, String>> linkIdTimestampEntity = new ArrayList<Tuple2<Tuple3<String, String, Integer>, String>>();
		for (String entity : m._2()) {
			linkIdTimestampEntity
					.add(new Tuple2<Tuple3<String, String, Integer>, String>(m
							._1(), entity));
		}
		return linkIdTimestampEntity;
	}

	private static Map<String, Object> subtractByKey(Map<String, Object> map1,
			Map<String, Object> map2) {
		LinkedHashMap<String, Object> newMap = new LinkedHashMap<String, Object>();
		for (String key1 : map1.keySet()) {
			if (!map2.containsKey(key1))
				newMap.put(key1, map1.get(key1));
		}
		return newMap;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> sortByValue(Map<String, Object> map) {
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		Stream<Entry<String, Object>> st = map.entrySet().stream();

		st.sorted(
				Comparator.comparing(e -> ((Entry<String, Long>) e).getValue())
						.reversed()).forEach(
				e -> result.put(e.getKey(), e.getValue()));

		return result;
	}

	static Tuple3<Tuple3<String, String, Integer>, Iterable<String>, Integer> addEntitiesCount(
			Tuple2<Tuple3<String, String, Integer>, Iterable<String>> r) {
		return new Tuple3<Tuple3<String, String, Integer>, Iterable<String>, Integer>(
				r._1, r._2, size(r._2));
	}

	static Tuple2<String, String> removeIdAndCounter(
			Tuple2<Tuple3<String, String, String>, Integer> p) {
		return new Tuple2<String, String>(p._1._1(), p._1._3());
	}

	static Tuple2<Tuple3<String, String, Integer>, String> prepareKeyWithLinkIdTimestamp(
			Tuple2<String, Tuple2<String, Tuple2<String, Integer>>> p) {
		return new Tuple2<Tuple3<String, String, Integer>, String>(
				new Tuple3<String, String, Integer>(p._2._1, p._2._2._1,
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

	static Tuple2<Tuple2<String, Integer>, Set<String>> extractTimestampsAndEntities(
			Tuple2<String, String> t) {
		Set<String> entities = new HashSet<String>();
		JsonObject jsonTweet = new JsonParser().parse(t._2).getAsJsonObject();
		int timestamp = jsonTweet.get("timestamp").getAsInt();
		JsonElement entitiesArray = jsonTweet.get("entities");
		if (entitiesArray != null) {
			JsonArray jsonEntities = entitiesArray.getAsJsonArray();
			for (JsonElement jsonElement : jsonEntities) {
				entities.add(jsonElement.getAsString());
			}
		}
		return new Tuple2<Tuple2<String, Integer>, Set<String>>(
				new Tuple2<String, Integer>(t._1, timestamp), entities);
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

	static String serializeTimeserieToJson(Tuple2<String, Iterable<Integer>> n) {
		JsonObject json = new JsonObject();
		json.addProperty("link", n._1);
		JsonArray timestamps = new JsonArray();
		json.add("timestamps", timestamps);
		for (Integer timestamp : n._2) {
			timestamps.add(new JsonPrimitive(timestamp));
		}
		return json.toString();
	}
}
