import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;



public class Query3 {


    public static void main (String[] args) {


        SparkConf conf = new SparkConf()
                .setMaster("local[*]") //numbers of logical core of our machine
                .setAppName("Query 3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile("regioni.csv"); //un RDD di stringhe
        String header = data.first();
        JavaRDD<String> dataset = data.filter(row -> !row.equals(header));
        long giorni = dataset.count()/21; //21 regioni (Trentino suddivisa tra province autonome)

        JavaRDD<String[]> rows = dataset.map(row -> row.split(","));


        JavaPairRDD<String, Integer> hosplital = rows.mapToPair(value -> new Tuple2<>(value[4].toString(), Integer.parseInt(value[7].toString())));
        JavaPairRDD<String, Double> avg_hosplital = hosplital.reduceByKey((x, y)->x+y)
                                                             .mapToPair(val -> new Tuple2<>(val._1().toString(),  Double.parseDouble(val._2().toString())/giorni));

        JavaPairRDD<String, Integer> intensive = rows.mapToPair(value -> new Tuple2<>(value[4].toString(), Integer.parseInt(value[8].toString())));
        JavaPairRDD<String, Double> avg_intensive = intensive.reduceByKey((x, y)->x+y)
                                                             .mapToPair(val -> new Tuple2<>(val._1().toString(), Double.parseDouble(val._2().toString())/giorni));

        JavaPairRDD<String, Integer> home = rows.mapToPair(value -> new Tuple2<>(value[4].toString(), Integer.parseInt(value[10].toString())));
        JavaPairRDD<String, Double> avg_home = home.reduceByKey((x, y)->x+y)
                                                   .mapToPair(val -> new Tuple2<>(val._1().toString(),  Double.parseDouble(val._2().toString())/giorni));

        JavaPairRDD<String, Integer> newPositive = rows.mapToPair(value -> new Tuple2<>(value[4].toString(), Integer.parseInt(value[12].toString())));
        JavaPairRDD<String, Double> avg_newPositive = newPositive.reduceByKey((x, y)->x+y)
                                                                 .mapToPair(val -> new Tuple2<>(val._1().toString(), Double.parseDouble(val._2().toString())/giorni));

        JavaPairRDD<String, Integer> death = rows.mapToPair(value -> new Tuple2<>(value[4].toString(), Integer.parseInt(value[14].toString())));
        JavaPairRDD<String, Double> avg_death = death.reduceByKey((x, y)->(y-x)+x)
                                                     .mapToPair(val -> new Tuple2<>(val._1().toString(), Double.parseDouble(val._2().toString())/giorni));

        JavaPairRDD<String, Integer> recovered = rows.mapToPair(value -> new Tuple2<>(value[4].toString(), Integer.parseInt(value[13].toString())));
        JavaPairRDD<String, Double> avg_recovered = recovered.reduceByKey((x, y)->(y-x)+x)
                                                           .mapToPair(val -> new Tuple2<>(val._1().toString(), Double.parseDouble(val._2().toString())/giorni));



        JavaPairRDD<String, Iterable<Double>> result = avg_hosplital.union(avg_intensive)
                                                                  .union(avg_home)
                                                                  .union(avg_newPositive)
                                                                  .union(avg_death)
                                                                  .union(avg_recovered)
                                                                  .groupByKey()
                                                                  .sortByKey();

        result.saveAsTextFile("out3"); //contained in our server

        sc.stop();

    }

}
