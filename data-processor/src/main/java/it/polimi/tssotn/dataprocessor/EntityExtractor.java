package it.polimi.tssotn.dataprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class EntityExtractor {

	private static final String _NEWS_ENTITIES = "_newsEntities";
	static final Logger logger = LoggerFactory.getLogger(EntityExtractor.class);

	public static void main(String[] args) {
		JavaSparkContext sparkContext = null;
		try {
			ExtractorConfig.init(args);
			ExtractorConfig config = ExtractorConfig.getInstance();
			Configuration hadoopConf = new Configuration();
			FileSystem hadoopFileSystem = FileSystem.get(hadoopConf);
			SparkConf sparkConf = new SparkConf()
					.setAppName("tssotn-data-processor");
			if (config.runLocal)
				sparkConf.setMaster("local[1]");
			sparkContext = new JavaSparkContext(sparkConf);
			int parallelize = sparkConf.getInt("spark.default.parallelism", 4);
			logger.info("parallelize value: {}", parallelize);
			logger.info("ExtractorConfig instance: {} ", config);
			logger.info("Query parameters: app_id = {},  app_key = {}",
					new Object[] { config.app_id, config.app_key });
			logger.info("Broadcasting the configuration");

			Broadcast<ExtractorConfig> broadcastVar = sparkContext
					.broadcast(config);
			config = broadcastVar.value();
			final String app_id = config.app_id;
			final String app_key = config.app_key;

			if (!hadoopFileSystem.exists(new Path(config.newsPath)))
				throw new IOException(config.newsPath + " does not exist");

			logger.info("Config instance: {} ", config);
			logger.info("Query parameters: app_id = {},  app_key = {}",
					new Object[] { config.app_id, config.app_key });

			sparkContext.textFile(config.newsPath, parallelize)
					.map(n -> Commons.removeNewLines(n))
					.flatMap(n -> Commons.splitJsonObjects(n))
					.map(n -> getNewsLink(n))
					.map(n -> extractEntities(n, app_id, app_key))
					.saveAsTextFile(config.outputPathBase + _NEWS_ENTITIES);

			logger.info("retireved entitiesRetrieved");

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

	static String getNewsLink(String s) {
		return new JsonParser().parse(s).getAsJsonObject().get("link")
				.getAsString();
	}

	protected static String extractEntities(String newsLink, String app_id,
			String app_key) {
		double minConfidence = 0.7;
		String dataTxtUrl = "https://api.dandelion.eu/datatxt/nex/v1";

		JsonArray entities = new JsonArray();
		JsonObject json = new JsonObject();
		json.addProperty("link", newsLink);
		json.add("entities", entities);

		logger.info("Query parameters: app_id = {},  app_key = {}, url = {}",
				new Object[] { app_id, app_key, newsLink });
		Client client = Client.create();
		WebResource webResource = client.resource(dataTxtUrl)
				.queryParam("$app_id", app_id).queryParam("$app_key", app_key)
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
			return json.toString();
		}
		JsonArray annotations = annotationElement.getAsJsonArray();
		for (JsonElement annotation : annotations) {
			JsonObject annotationObject = annotation.getAsJsonObject();
			JsonElement lod = annotationObject.get("lod");
			if (lod == null)
				continue;
			JsonElement entity = lod.getAsJsonObject().get("dbpedia");
			if (entity != null)
				entities.add(entity);
		}
		return json.toString();
	}

}
