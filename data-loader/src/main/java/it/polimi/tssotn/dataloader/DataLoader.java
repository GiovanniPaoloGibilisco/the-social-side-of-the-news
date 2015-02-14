package it.polimi.tssotn.dataloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class DataLoader {

	private static final Logger logger = LoggerFactory
			.getLogger(DataLoader.class);

	private static final String twitterDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/social-pulse-milano/data";
	private static final String newsDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/milanotoday/data";

	public static void main(String[] args) {
		JavaSparkContext sparkContext = null;
		try {

			Config.init(args);
			Config config = Config.getInstance();

			SparkConf sparkConf = new SparkConf()
					.setAppName("tssotn-data-loader");
			if (config.runLocal)
				sparkConf.setMaster("local[1]");
			sparkContext = new JavaSparkContext(sparkConf);

			Map<String, String> commonPars = new HashMap<String, String>();
			commonPars.put("$app_id", config.app_id);
			commonPars.put("$app_key", config.app_key);

			JavaRDD<String> tweetsChunks = restToRDD(twitterDataURL, "$offset",
					"$limit", 1000, 0, 269290, commonPars, sparkContext);
			JavaRDD<String> newsChunks = restToRDD(newsDataURL, "$offset",
					"$limit", 1000, 0, 3014, commonPars, sparkContext);

			tweetsChunks.saveAsTextFile(config.tweetsPath);
			newsChunks.saveAsTextFile(config.newsPath);

		} catch (ParameterException e) {
			logger.error("Wrong configuration: {}", e.getMessage());
		} catch (Exception e) {
			logger.error("Unknown error", e);
		} finally {
			if (sparkContext != null)
				sparkContext.stop();
		}
	}

	public static JavaRDD<String> restToRDD(String url,
			String offsetParameterName, String limitParameterName,
			int chunkSize, int start, int end,
			Map<String, String> otherParameters, JavaSparkContext sparkContext) {

		int chunks = (int) Math.ceil((end - start) / chunkSize);
		List<String> urlsList = new ArrayList<String>(chunks);

		for (int i = 0; i < chunks; i++) {
			UriBuilder urlBuilder = UriBuilder
					.fromUri(url)
					.queryParam(offsetParameterName,
							Integer.toString(i * chunkSize))
					.queryParam(limitParameterName,
							Integer.toString(chunkSize - 1));
			for (Map.Entry<String, String> queryParameter : otherParameters
					.entrySet()) {
				if (queryParameter.getValue() != null) {
					urlBuilder.queryParam(queryParameter.getKey(),
							queryParameter.getValue());
				}
			}
			urlsList.add(urlBuilder.build().toString());
		}

		JavaRDD<String> urls = sparkContext.parallelize(urlsList);

		return urls.map(webResource -> {
			Client client = Client.create();
			ClientResponse response = client.resource(webResource)
					.accept("application/json").get(ClientResponse.class);
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatus());
			}
			return new JsonParser().parse(response.getEntity(String.class))
					.getAsJsonObject().get("items").toString();
		});
	}

}
