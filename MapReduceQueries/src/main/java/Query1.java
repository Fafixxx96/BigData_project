import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Query1 {

    public static void main(String[] args) throws Exception {

        /*-----------------------------------JOB_1-------------------------------------------*/
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "forniture totali");
        job1.setJarByClass(Query1.class);

        /*Map function*/
        job1.setMapperClass(Query1.Mappers1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        /*intermediate Combiners*/
        job1.setCombinerClass(Query1.Reducers1.class);

        /* Reduce function */
        job1.setReducerClass(Query1.Reducers1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        /* Set input and output files/directories using command line arguments */
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job1 termination */
        job1.waitForCompletion(true);

        /*-------------------------------JOB_2*---------------------------------------------------*/
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "forniture MAX");
        job2.setJarByClass(Query1.class);

        /*map function*/
        job2.setMapperClass(Query1.Mappers2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        /* Reduce function 2*/
        job2.setReducerClass(Query1.Reducers2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        /* Set input and output files/directories using command line arguments */
        FileInputFormat.addInputPath(job2, new Path(args[1])); //the output of job1 is the input for job2
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        /* Wait for job termination */
        System.exit(job2.waitForCompletion(true)? 0: 1);


    }

    public static class Mappers1 extends Mapper <LongWritable, Text, Text, IntWritable> {

        private final static String SEPARATOR = ",";

        private Text azienda = new Text();
        private Text categoria = new Text();
        private Text chiave = new Text();
        private IntWritable quantita = new IntWritable(0);


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split the data with CSV_SEPARATOR
            String[] columnData = value.toString().split(SEPARATOR);

            azienda.set(columnData[0]);
            categoria.set(columnData[5]);
            chiave.set(categoria + "-" + azienda);
            quantita.set(Integer.parseInt(columnData[17]));

            context.write(chiave, quantita);

        }
    }

    public static class Reducers1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            result = new IntWritable(0);
            int c = 0;

            for(IntWritable v: values)
                c+=v.get();
            result.set(c);

            context.write(key,result);
        }
    }

    public static class Mappers2 extends Mapper <LongWritable, Text, Text, Text> {

        private final static String SEPARATOR = "\t";

        private Text categoria = new Text();
        private Text val = new Text(); //azienda + quantità

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split the data with space of output of first map reduce job
            String[] columnData2 = value.toString().split(SEPARATOR);

            categoria.set(columnData2[0].substring(0, columnData2[0].indexOf('-')));
            val.set(columnData2[0].substring(columnData2[0].indexOf('-')+1) + ","+  columnData2[1]);

            context.write(categoria, val);
        }
    }

    public static class Reducers2 extends Reducer<Text, Text, Text, Text> {

        private Text azienda, result;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            azienda = new Text();
            result = new Text();

            int c = 0;
            String s;
            String a;
            int q;
            for(Text v: values){
                s = v.toString();
                a = s.substring(0, s.indexOf(','));
                q = Integer.parseInt(s.substring(s.indexOf(',')+1));
                if (q > c){
                    azienda.set(a);
                    c = q;
                }
            }
            result.set("/" + azienda + "/" + c);

            context.write(key,result);
        }
    }
}