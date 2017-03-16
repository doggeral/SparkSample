/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
  public static void main(String[] args) throws InterruptedException {
    String logFile = "/Users/longfang/Documents/AmazonWS/Spark/SparkTest/src/SimpleApp.java"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    conf.setMaster("local[4]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(p ->p.contains("a")).count();

    long numBs = logData.filter(p -> {
    	return p.contains("b");
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
    System.out.println("aa");
    Thread.sleep(100000);

  }
}