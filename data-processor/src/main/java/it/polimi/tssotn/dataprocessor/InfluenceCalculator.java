package it.polimi.tssotn.dataprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.Tuple3;

import com.beust.jcommander.ParameterException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

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

			if (!hadoopFileSystem.exists(new Path(config.newsPath)))
				throw new IOException(config.newsPath + " does not exist");
			if (!hadoopFileSystem.exists(new Path(config.tweetsPath)))
				throw new IOException(config.tweetsPath + " does not exist");

			String outputFileNameBase = config.outputPathBase + "/"
					+ new Date().getTime();

			JavaPairRDD<String, String> newsEntityLinkPairs = sparkContext
					.textFile(config.newsPath).map(n -> removeNewLines(n))
					.flatMap(n -> splitJsonObjects(n)).map(n -> getNewsLink(n))
					.mapToPair(n -> extractEntities(n))
					.filter(n -> hasEntities(n._2))
					.flatMapToPair(n -> flatEntitiesAndSwap(n));

			JavaPairRDD<String, Tuple2<String, String>> tweetsEntityIDTimestampPairs = sparkContext
					.textFile(config.tweetsPath)
					.map(t -> removeNewLines(t))
					.flatMap(t -> splitJsonObjects(t))
					.mapToPair(
							t -> new Tuple2<String, String>(UUID.randomUUID()
									.toString(), t))
					.mapToPair(t -> extractTimestampsAndEntities(t))
					.filter(t -> hasEntities(t._2))
					.flatMapToPair(t -> flatEntitiesAndSwap(t));

			JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> entityInfluenceMap = newsEntityLinkPairs
					.join(tweetsEntityIDTimestampPairs);

			JavaRDD<Tuple3<Tuple3<String, String, String>, Iterable<String>, Integer>> raw = entityInfluenceMap
					.mapToPair(r -> prepareKeyWithLinkIdTimestamp(r))
					.groupByKey().map(r -> addEntitiesCount(r));

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
		for (Object obj : it)
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
		return new Tuple2<Tuple2<String, String>, Set<String>>(t, entities);
	}

	static <T> List<Tuple2<String, T>> flatEntitiesAndSwap(
			Tuple2<T, Set<String>> n) {
		List<Tuple2<String, T>> nOut = new ArrayList<Tuple2<String, T>>();
		for (String entity : n._2) {
			nOut.add(new Tuple2<String, T>(entity, n._1));
		}
		return nOut;
	}

	static boolean hasEntities(Set<String> s) {
		return !s.isEmpty();
	}

	static String getNewsLink(String s) {
		return new JsonParser().parse(s).getAsJsonObject().get("link")
				.getAsString();
	}

	static Tuple2<String, Set<String>> extractEntities(String newsLink) {
		double minConfidence = 0.7;
		String dataTxtUrl = "https://api.dandelion.eu/datatxt/nex/v1";

		JsonObject json = new JsonObject();
		json.addProperty("link", newsLink);

		Set<String> entities = new HashSet<String>();

		Client client = Client.create();
		WebResource webResource = client.resource(dataTxtUrl)
				.queryParam("$app_id", Config.getInstance().app_id)
				.queryParam("$app_key", Config.getInstance().app_key)
				.queryParam("url", newsLink)
				.queryParam("min_confidence", Double.toString(minConfidence))
				.queryParam("lang", "it").queryParam("include", "lod")
				.queryParam("epsilon", "0.0");
		ClientResponse response = webResource.accept("application/json").get(
				ClientResponse.class);
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		JsonElement annotationElement = new JsonParser()
				.parse(response.getEntity(String.class)).getAsJsonObject()
				.get("annotations");
		if (annotationElement == null) {
			return new Tuple2<String, Set<String>>(newsLink, entities);
		}
		JsonArray annotations = annotationElement.getAsJsonArray();
		for (JsonElement annotation : annotations) {
			JsonObject annotationObject = annotation.getAsJsonObject();
			JsonElement lod = annotationObject.get("lod");
			if (lod == null)
				continue;
			String entity = lod.getAsJsonObject().get("dbpedia").getAsString();
			if (entity != null)
				entities.add(entity);
		}
		return new Tuple2<String, Set<String>>(newsLink, entities);
	}

	static List<String> splitJsonObjects(String string) {
		List<String> jsonObjects = new ArrayList<String>();
		int brackets = 0;
		int start = 0;

		for (int i = 0; i < string.length(); i++) {
			switch (string.charAt(i)) {
			case '{':
				brackets++;
				if (brackets == 1)
					start = i;
				break;
			case '}':
				brackets--;
				if (brackets < 0) {
					jsonObjects.clear(); // we started in the middle of a json
											// object
					brackets = 0;
				} else if (brackets == 0) {
					jsonObjects.add(string.substring(start, i + 1));
				}
				break;
			default:
				break;
			}
		}
		return jsonObjects;
	}

	static String removeNewLines(String s) {
		return s.replaceAll("\r\n|\r|\n", "");
	}

}
