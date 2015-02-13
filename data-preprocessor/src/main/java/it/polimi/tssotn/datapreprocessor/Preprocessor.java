package it.polimi.tssotn.datapreprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Preprocessor {

	private static final Logger logger = LoggerFactory
			.getLogger(Preprocessor.class);

	private static final String app_id = "5bb0e37c";
	private static final String app_key = "593dc19bbaaa08aad799e6a57db362cf";

	private static boolean runLocal = true;

	public static void main(String[] args) {
		JavaSparkContext sparkContext = null;
		try {

			Configuration hadoopConf = new Configuration();
			FileSystem hadoopFileSystem = FileSystem.get(hadoopConf);
			SparkConf sparkConf = new SparkConf()
					.setAppName("tssotn-data-loader");
			if (runLocal)
				sparkConf.setMaster("local[1]");
			sparkContext = new JavaSparkContext(sparkConf);

			Path localTweetsPath = new Path(Preprocessor.class.getResource(
					"../data-loader/loaded-data/news/part-00000").toURI());
			Path hdfsTweetsPath = new Path("tweets.json");
			hadoopFileSystem.copyFromLocalFile(localTweetsPath, hdfsTweetsPath);

			sparkContext.textFile(hdfsTweetsPath.toString());

		} catch (Exception e) {
			logger.error("Unknown error", e);
		} finally {
			if (sparkContext != null)
				sparkContext.stop();
		}
	}
}
