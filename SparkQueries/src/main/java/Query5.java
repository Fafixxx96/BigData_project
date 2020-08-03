import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class Query5 {


    private static final String  LOCAL_DIR = "file:/D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query5/out/out5/";

    public static void main (String[] args) throws InterruptedException {


        SparkConf sparkConf = new SparkConf()
                // Spark Streaming needs at least two working thread
                .setMaster("local[*]")
                .setAppName("Query5");

        JavaSparkContext sc =
                new JavaSparkContext(sparkConf);



        JavaRDD<String> lines = sc.textFile("file:/D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query5/in/alldates.csv");

        JavaRDD<String[]> rows = lines.map(row -> row.split(","));
        

        JavaPairRDD<String, Double> samples = rows.mapToPair(value -> new Tuple2<>(value[0].toString(), 1.0));
        JavaPairRDD<String, Double> samples_countries = samples.reduceByKey((x,y)->x+y);

        JavaPairRDD<String, Double> percentage = rows.mapToPair(value -> new Tuple2<>(value[0].toString(), Double.parseDouble(value[5].toString())));
        JavaPairRDD<String, Double> countries = percentage.reduceByKey((x,y)->x+y);

        JavaPairRDD<String, Double> result = countries.union(samples_countries).reduceByKey((x,y)->x/y);

        result.saveAsTextFile("file:/D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query5/out/out5");
    }




}
