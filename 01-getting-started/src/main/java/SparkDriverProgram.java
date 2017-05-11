import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**
 * (c) geekmj.org All right reserved  
 * 
 * Main Spark Driver Program Class
 * It creates Resilient Distributed Dataset for a log file (external data source)
 * It does following operation on RDD
 * 1. Total no. of lines in log file
 * 2. Total characters in log file
 * 3. Total no. of URL in log file with HTML and GIF extension
 */
public class SparkDriverProgram {
    public static void main(String args[]) {
    	/* Define Spark Configuration */
		SparkConf conf = new SparkConf().setAppName("01-Getting-Started").setMaster(args[0]);
        
		/* Create Spark Context with configuration */
		JavaSparkContext sc = new JavaSparkContext(conf);
        
		/* Create a Resilient Distributed Dataset for a log file 
		 * Each line in log file become a record in RDD
		 * */
        JavaRDD<String> lines = sc.textFile(
        		"/Volumes/Drive2/projects/project_workspace/github/apache-spark-examples/data/apache-log04-aug-31-aug-2011-nasa.log");
        
		System.out.println("Total lines in log file " + lines.count());
        
		/* Map operation -> Mapping number of characters into each line as RDD */
		JavaRDD<Integer> lineCharacters = lines.map(s -> s.length());
		/* Reduce operation -> Calculating total characters */
		int totalCharacters = lineCharacters.reduce((a, b) -> a + b);
        
		System.out.println("Total characters in log file " + totalCharacters);
        
		/* Reduce operation -> checking each line for .html character pattern */
		System.out.println("Total URL with html extension in log file " 
				+ lines.filter(oneLine -> oneLine.contains(".html")).count());
		
		/* Reduce operation -> checking each line for .gif character pattern */
		System.out.println("Total URL with gif extension in log file "
				+ lines.filter(oneLine -> oneLine.contains(".gif")).count());
        
		sc.close();
    }
}
