package it.polimi.tssotn.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.polimi.tssotn.dataloader.DataLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;

public class DataLoaderTest {

	private static final String twitterDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/social-pulse-milano/data";
	private static final String newsDataURL = "https://api.dandelion.eu/datagems/v2/SpazioDati/milanotoday/data";

	private JavaSparkContext sparkContext;
	private FileSystem hadoopFileSystem;

	@Before
	public void setUp() throws IOException {
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");
		Configuration hadoopConf = new Configuration();
		hadoopFileSystem = FileSystem.get(hadoopConf);
		SparkConf sparkConf = new SparkConf().setAppName("tssotn-data-loader")
				.setMaster("local[1]");
		sparkContext = new JavaSparkContext(sparkConf);
	}



	@After
	public void tearDown() {
		sparkContext.stop();
		sparkContext = null;
		System.clearProperty("spark.driver.port");
		System.clearProperty("spark.hostPort");
	}
}
