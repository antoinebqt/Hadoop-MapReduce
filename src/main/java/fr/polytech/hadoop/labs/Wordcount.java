package fr.polytech.hadoop.labs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Wordcount {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Utiliser une expression régulière pour extraire les mots composés uniquement de lettres
            String[] words = value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");

            for (String w : words) {
                // Ignorer les mots vides
                if (!w.isEmpty()) {
                    word.set(w);
                    context.write(word, one);
                }
            }
        }
    }


    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new Text("[" + sum + "]"));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Word count Job!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        // Set the Jar by class where Mapper and Reducer reside
        job.setJarByClass(Wordcount.class);

        // Set Mapper and Reducer class
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path("/input")); // Assuming input is in HDFS directory /input
        FileOutputFormat.setOutputPath(job, new Path("/output")); // Output will be in HDFS directory /output

        // Wait for the job to complete and print the result
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

