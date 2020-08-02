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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Query2 {

    private static String provincia;

    public static void main(String[] args) throws Exception {

        provincia = new String(args[2]);
        /*-----------------------------------JOB_1-------------------------------------------*/
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "andamento province");
        job.setJarByClass(Query2.class);

        /*Map function*/
        job.setMapperClass(Query2.Mappers.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        /*intermediate Combiners*/
        job.setCombinerClass(Query2.Reducers.class);

        /* Reduce function */
        job.setReducerClass(Query2.Reducers.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /* Set input and output files/directories using command line arguments */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        /* Wait for job1 termination */
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class Mappers extends Mapper <LongWritable, Text, Text, Text> {

        private final static String SEPARATOR = ",";

        private IntWritable casi = new IntWritable(0);
        private Text data = new Text();
        private Text prov = new Text();
        private Text testo = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split the data with CSV_SEPARATOR
            String[] columnData = value.toString().split(SEPARATOR);

            data.set(columnData[1].substring(0, columnData[1].indexOf('T')));
            prov.set(columnData[7]);
            casi.set(Integer.parseInt(columnData[10]));

            testo.set("/" + data + "/" + casi);

            context.write(prov, testo);

        }
    }

    public static class Reducers extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> tests, Context context) throws IOException, InterruptedException {

            List<String> valuesSorted = new ArrayList<String>();

            String k = key.toString();
            if(k.equals(provincia))
                for(Text t: tests)
                    valuesSorted.add(t.toString());

            Collections.sort(valuesSorted);
            for(int i = 0; i < valuesSorted.size(); i++)
                context.write(key, new Text(valuesSorted.get(i)));
        }
    }



}
