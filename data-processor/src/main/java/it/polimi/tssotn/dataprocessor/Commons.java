package it.polimi.tssotn.dataprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Commons {

	

	static final Logger logger = LoggerFactory
			.getLogger(Commons.class);
	
	protected static boolean hasEntities(Set<String> s) {
		return !s.isEmpty();
	}

	
	protected static List<String> splitJsonObjects(String string) {
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

	protected static String removeNewLines(String s) {
		return s.replaceAll("\r\n|\r|\n", "");
	}

	public Commons() {
		super();
	}

}