package it.polimi.bigdataclass.thesocialsideofthenews;

import static org.junit.Assert.*;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InfluenceCalculatorTest {

	private SparkContext sc;


	@Before
	public void setUp(){
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");
		
		SparkConf conf = new SparkConf().setAppName(
				"The-social-side-of-the-news").setMaster("local[1]");
		sc = new SparkContext(conf);
	}
	
	
	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	@After
	public void tearDown() {
		sc.stop();
		sc = null;
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");
	}

}
