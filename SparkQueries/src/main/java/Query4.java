import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Query4 {

    public static void  main (String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]") //numbers of logical core of our machine
                .setAppName("Query 4");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile("file:/D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query4/in/covid_mexico.csv"); //un RDD di stringhe
        String header = data.first();

        JavaRDD<String> dataset = data.filter(row -> !row.equals(header));

        JavaRDD<String[]> rows = dataset.map(row -> row.split(","));

        String recordDeath = "9999-99-99";
        JavaRDD<String[]> deaths = rows.filter(row -> !row[5].equals(recordDeath));

        JavaRDD<String[]> male = deaths.filter(row -> row[1].equals("1"));
        JavaRDD<String[]> female = deaths.filter(row -> row[1].equals("2"));

        JavaPairRDD<String, Iterable<Double>> ris = method(male, 1).union(method(female, 2));

        ris.saveAsTextFile("file:/D:/Unical/Magistrale/FLESCA_BigDataManagement/project/query4/out/out4");

        sc.close();
    }

    private static JavaPairRDD<String, Iterable<Double>> method( JavaRDD<String[]> data, int sex){

        long l = data.count();

        JavaPairRDD<String, Double> Intubed = data.filter(row -> row[6].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Pneumonia = data.filter(row -> row[7].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Age = data.mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", Long.parseLong(row[8])))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)));

        JavaPairRDD<String, Double> Diabet = data.filter(row -> row[10].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Bpco = data.filter(row -> row[11].equals("1"))  //broncopneumatia acuta
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Asthma = data.filter(row -> row[12].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Others = data.filter(row -> row[13].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Cardio = data.filter(row -> row[14].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Obesity = data.filter(row -> row[15].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Renal = data.filter(row -> row[16].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Tobacco = data.filter(row -> row[17].equals("1"))
                .mapToPair(row -> new Tuple2<>((sex==2)?"female":"male", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));

        JavaPairRDD<String, Double> Pregnant = data.filter(row -> row[9].equals("1"))
                .mapToPair(row -> new Tuple2<>("female", 1))
                .reduceByKey((x,y)->x+y)
                .mapToPair(row -> new Tuple2<>(row._1().toString(), (Double.parseDouble(row._2().toString())/l)*100));


        JavaPairRDD<String, Iterable<Double>> result = (sex == 2) ? Age.union(Intubed)
                                                                     .union(Pneumonia)
                                                                     .union(Diabet)
                                                                     .union(Bpco)
                                                                     .union(Asthma)
                                                                     .union(Others)
                                                                     .union(Cardio)
                                                                     .union(Obesity)
                                                                     .union(Renal)
                                                                     .union(Tobacco)
                                                                     .union(Pregnant)
                                                                     .groupByKey()
                                                                :
                                                                    Age.union(Intubed)
                                                                            .union(Pneumonia)
                                                                            .union(Diabet)
                                                                            .union(Bpco)
                                                                            .union(Asthma)
                                                                            .union(Others)
                                                                            .union(Cardio)
                                                                            .union(Obesity)
                                                                            .union(Renal)
                                                                            .union(Tobacco)
                                                                            .groupByKey();


        return  result;
    }





}
