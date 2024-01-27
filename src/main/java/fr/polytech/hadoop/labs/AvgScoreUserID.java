package fr.polytech.hadoop.labs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AvgScoreUserID {

    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class AvgScorePerUserIDMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {

        private final LongWritable userId = new LongWritable();
        private final FloatWritable rating = new FloatWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignorer l'en-tête s'il y en a un
            if (key.get() == 0 && value.toString().contains("userId,movieId,rating,timestamp")) {
                return;
            }

            // Utiliser une expression régulière pour parser la ligne du CSV
            String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            userId.set(Long.parseLong(columns[0]));
            rating.set(Float.parseFloat(columns[2]));
            context.write(userId, rating);
        }
    }

    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class AvgScorePerUserIDReducer extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;
            for (FloatWritable value : values) {
                sum+= value.get();
                count++;
            }

            context.write(key, new FloatWritable(sum/count));
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("Average score per userID Job!");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Avg Score UserID");

        // Set the Jar by class where Mapper and Reducer reside
        job.setJarByClass(AvgScoreUserID.class);

        // Set Mapper and Reducer class
        job.setMapperClass(AvgScorePerUserIDMapper.class);
        job.setReducerClass(AvgScorePerUserIDReducer.class);

        // Set Mapper Output key and value classes
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        // Set Reducer Output key and value classes
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path("/input/ratings.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/output/avgScoreUserID"));

        // Wait for the job to complete and print the result
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

