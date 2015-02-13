package it.polimi.tssotn.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class SparkUtils {

	public static JavaRDD<JsonObject> restToRDD(String url,
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
			JsonParser parser = new JsonParser();
			return parser.parse(response.getEntity(String.class))
					.getAsJsonObject();
		});
	}
}
