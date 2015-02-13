package it.polimi.tssotn.datapreprocessor;

import static org.junit.Assert.assertFalse;
import it.polimi.tssotn.datapreprocessor.NewsEntityExtractor;

import java.util.Set;

import org.junit.Test;

public class NewsEntityExtractorTest {

	@Test
	public void entitiesInDerbyShouldNotBeEmpty() {
		Set<String> entities = NewsEntityExtractor.extractEntities("http://www.milanotoday.it/sport/inter-milan-1-0-palacio-22-dicembre-2013.html");
		assertFalse(entities.isEmpty());
		System.out.println("count: "+entities.size());
		for (String entity: entities) {
			System.out.println(entity);
		}
	}

}
