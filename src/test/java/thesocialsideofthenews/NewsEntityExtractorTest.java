package thesocialsideofthenews;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class NewsEntityExtractorTest {

	@Test
	public void test() {
		List<String> entities = NewsEntityExtractor.extractEntities("http://www.milanotoday.it/sport/inter-milan-1-0-palacio-22-dicembre-2013.html");
		assertFalse(entities.isEmpty());
	}

}
