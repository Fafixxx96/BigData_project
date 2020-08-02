import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class Query6 {

    private static final String  LOCAL_DIR = "output2/";
    private static final int WINDOW_TIME_UNIT_SECS = 1;

    private static class SaveAsLocalFile implements VoidFunction2<JavaPairRDD<String,Double>, Time> {

        @Override
        public void call(JavaPairRDD<String, Double> v1, Time v2) throws Exception {
            v1.saveAsTextFile(LOCAL_DIR + v2.milliseconds());
        }

    }

    public static void main (String[] args) throws InterruptedException {


        SparkConf sparkConf = new SparkConf()
                // Spark Streaming needs at least two working thread
                .setMaster("local[*]")
                .setAppName("Query6");

        JavaStreamingContext ssc =
                new JavaStreamingContext(sparkConf, Durations.seconds(1));

        ssc.sparkContext().setLogLevel("ERROR");

        JavaDStream<String> lines = ssc.textFileStream("forniture.csv");

        JavaDStream<String> linesInWindow =
                lines.window(Durations.seconds(30* WINDOW_TIME_UNIT_SECS),
                        Durations.seconds(2 * WINDOW_TIME_UNIT_SECS));
        JavaDStream<String[]> rows = linesInWindow.map(row -> row.split(","));


        JavaPairDStream<String, Double> money = rows.mapToPair(value -> new Tuple2<>(value[5].toString(), Double.parseDouble(value[19].toString())));
        JavaPairDStream<String, Double> total = money.reduceByKey((x,y)->x+y);

        total.print();
        total.foreachRDD(new SaveAsLocalFile());
        ssc.start();
        ssc.awaitTermination();
    }


}
