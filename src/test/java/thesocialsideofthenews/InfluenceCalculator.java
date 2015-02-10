package thesocialsideofthenews;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class InfluenceCalculator {
	static final Logger logger = LoggerFactory.getLogger(InfluenceCalculator.class);

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("The social side of the news").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);	
		try {

			Configuration hadoopConf = new Configuration();
			FileSystem fs = FileSystem.get(hadoopConf);
			
			//check that tweets exists
			Path inputTweets = new Path("tweets.json");			
			if(!fs.exists(inputTweets)){
				logger.error("{} does not exist", inputTweets);			
				sc.stop();
				return;
			}
			
			//check that news exist
			Path inputNews = new Path("tweets.json");			
			if(!fs.exists(inputNews)){
				logger.error("{} does not exist", inputNews);			
				sc.stop();
				return;
			}

			//split news
			JavaRDD<String> news =  sc.textFile(inputNews.toString(),1).flatMap(new FlatMapFunction<String, String>() {
				public Iterable<String> call(String s) throws Exception {													
					return Arrays.asList(s.split("\n"));
				}
			}).cache();			
			
			news = news.filter(new Function<String, Boolean>() {				
				public Boolean call(String news) throws Exception {
					JsonParser parser = new JsonParser();
					JsonObject jsonNews = parser.parse(news).getAsJsonObject();					
					return jsonNews.get("entities") != null;					
				}
			});
			
			JavaPairRDD<String,String> newsEntityMap = news.mapToPair(new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(String news) throws Exception {
					JsonParser parser = new JsonParser();
					JsonObject jsonNews = parser.parse(news).getAsJsonObject();
					String link = jsonNews.get("link").getAsString();
					String entities = jsonNews.get("entities").getAsString();					
					return new Tuple2<String, String>(link, entities);
				}
			});
			
			//split tweets
			JavaRDD<String> tweets =  sc.textFile(inputTweets.toString(),1).flatMap(new FlatMapFunction<String, String>() {
				public Iterable<String> call(String s) throws Exception {													
					return Arrays.asList(s.split("\n"));
				}
			}).cache();
			
			tweets = tweets.filter(new Function<String, Boolean>() {				
				public Boolean call(String tweet) throws Exception {
					JsonParser parser = new JsonParser();
					JsonObject jsonTweet = parser.parse(tweet).getAsJsonObject();					
					return jsonTweet.get("entities") != null;					
				}
			});
			
			JavaPairRDD<String,String> tweetEntityMap = tweets.mapToPair(new PairFunction<String, String, String>() {
				public Tuple2<String, String> call(String tweet) throws Exception {
					JsonParser parser = new JsonParser();
					JsonObject jsonTweet = parser.parse(tweet).getAsJsonObject();
					String link = jsonTweet.get("timestamp").getAsString();
					String entities = jsonTweet.get("entities").getAsString();					
					return new Tuple2<String, String>(link, entities);
				}
			});
			
			
			 
			
			

		} catch (IOException e) {
			logger.error("Wrong Hadoop configuration",e);
			return ;
		}
	}

}
