import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Query6 {

        private static final String  LOCAL_DIR = "file:/D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query6/out/out6";

        public static void main (String[] args) throws InterruptedException {



            SparkConf sparkConf = new SparkConf()
                    // Spark Streaming needs at least two working thread
                    .setMaster("local[2]")
                    .setAppName("Query6");

            JavaSparkContext sc =
                    new JavaSparkContext(sparkConf);

            JavaRDD<String> lines = sc.textFile("file:/D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query6/in/forniture.csv");

            JavaRDD<String[]> rows = lines.map(row -> row.split(","));

            JavaPairRDD<String, Double> money = rows.mapToPair(value -> new Tuple2<>(value[5].toString(), Double.parseDouble(value[19].toString())));
            JavaPairRDD<String, Double> total = money.reduceByKey((x,y)->x+y);

            total.saveAsTextFile(LOCAL_DIR);
            sc.close();
        }
}
