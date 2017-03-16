
public class SparkSQL {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws InterruptedException {
		String testFile = "./testData/cts_out_58__summary_output2016-12-08-13-41-30.csv";

		SparkConf conf = new SparkConf().setAppName("testing");
		conf.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		SparkSession spark = SparkSession.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();
	}
}