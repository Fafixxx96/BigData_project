import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;


public class Query6 {

    private static final String LOCAL_DIR = "out6/";
    private static final int WINDOW_TIME_UNIT_SECS = 5;


    private static class SaveAsLocalFile implements VoidFunction2<JavaPairRDD<String, Double>, Time> {

        @Override
        public void call(JavaPairRDD<String, Double> v1, Time v2) throws Exception {
            System.out.println(v1.count());
            v1.saveAsTextFile(LOCAL_DIR + v2.milliseconds());

        }

    }

    private static class Lenght implements VoidFunction2<JavaRDD<String>, Time> {

        @Override
        public void call(JavaRDD<String> v1, Time v2) throws Exception {
            System.out.println(v1.count());
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {



       SparkConf sparkConf = new SparkConf()
                // Spark Streaming needs at least two working thread
                .setMaster("local[*]")
                .setAppName("Query6");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));


        ssc.sparkContext().setLogLevel("ERROR");



        JavaDStream<String> lines = ssc.socketTextStream("localhost", 9999, StorageLevels.MEMORY_ONLY);

        //JavaDStream<String> lines = ssc.textFileStream(System.getProperty("user.dir"));
        //JavaDStream<String> lines = ssc.textFileStream("file:/\D:/\\Unical/Magistrale/FLESCA_BigDataManagement/project/query6/in/");
        //lines.foreachRDD(new Lenght());


        JavaDStream<String> linesInWindow =lines.window(Durations.seconds(30 * WINDOW_TIME_UNIT_SECS),Durations.seconds(2 * WINDOW_TIME_UNIT_SECS));
        JavaDStream<String[]> rows = linesInWindow.map(row -> row.split(","));


        JavaPairDStream<String, Double> money = rows.mapToPair(value -> new Tuple2<>(value[5].toString(), Double.parseDouble(value[19].toString())));
        JavaPairDStream<String, Double> total = money.reduceByKey((x, y) -> x + y);

        total.print();
        total.foreachRDD(new SaveAsLocalFile());
        ssc.start();

        ssc.awaitTermination();
    }


}