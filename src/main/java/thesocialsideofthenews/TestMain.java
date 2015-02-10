package thesocialsideofthenews;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class TestMain {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Grep Example By Gibbo").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);		

	}

}
