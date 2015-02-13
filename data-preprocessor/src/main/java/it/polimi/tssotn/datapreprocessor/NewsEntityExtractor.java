package it.polimi.tssotn.datapreprocessor;

import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class NewsEntityExtractor {

	static Set<String> extractEntities(String url) {
		
		final String endpoint = "https://api.dandelion.eu/datatxt/nex/v1";
		final String appID = "07ccdf25";
		final String appKey = "5981587e6a371d938a0fa50811f01acc";
		double min_confidence = 0.7;
		
		Set<String> entities = new HashSet<String>();
		
		Client client = Client.create();
		WebResource webResource = client.resource(endpoint)
				.queryParam("$app_id", appID)
				.queryParam("$app_key", appKey)
				.queryParam("url", url)
				.queryParam("min_confidence", Double.toString(min_confidence))
				.queryParam("lang", "it")
				.queryParam("include", "lod")
				.queryParam("epsilon", "0.0");
		
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
		
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
			     + response.getStatus());
		}
		JsonParser parser = new JsonParser();
		JsonObject jsonResponse = parser.parse(response.getEntity(String.class)).getAsJsonObject(); 
		JsonElement annotationElement = jsonResponse.get("annotations");
		if(annotationElement == null)
			return entities;
		JsonArray annotations = annotationElement.getAsJsonArray();		
		for (JsonElement annotation : annotations) {
			JsonObject annotationObject = annotation.getAsJsonObject();
			JsonElement lod = annotationObject.get("lod");
			if(lod == null)
				continue;
			String entity = lod.getAsJsonObject().get("dbpedia").getAsString();
			if(entity != null)
				entities.add(entity);
		}				
		return entities;
	}

}
