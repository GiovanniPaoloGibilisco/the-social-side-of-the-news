package thesocialsideofthenews;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class NewsEntityExtractor {

	public static List<String> extractEntities(String url) {
		
		final String endpoint = "https://api.dandelion.eu/datatxt/nex/v1";
		final String appID = "07ccdf25";
		final String appKey = "5981587e6a371d938a0fa50811f01acc";
		double min_confidence = 0.6;
		
		List<String> entities = new ArrayList<String>();
		
		Client client = Client.create();
		WebResource webResource = client.resource(endpoint)
				.queryParam("$app_id", appID)
				.queryParam("$app_key", appKey)
				.queryParam("url", url)
				.queryParam("min_confidence", Double.toString(min_confidence));
		
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
		
		if (response.getStatus() != 201) {
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
			String entity = annotationObject.get("lod").getAsJsonObject().get("dbpedia").getAsString();
			if(entity != null)
				entities.add(entity);
		}
				
		return entities;
	}

}
