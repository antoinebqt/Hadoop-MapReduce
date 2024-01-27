package fr.polytech.hadoop.step1;

import fr.polytech.hadoop.step1.job1.MovieIdRatingWritable;
import fr.polytech.hadoop.step1.job1.RatingMapper;
import fr.polytech.hadoop.step1.job1.RatingReducer;
import fr.polytech.hadoop.step1.job2.BestMovieIdMapper;
import fr.polytech.hadoop.step1.job2.MovieMapper;
import fr.polytech.hadoop.step1.job2.MovieUserIdWritable;
import fr.polytech.hadoop.step1.job2.UserIdMovieReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestMoviePerUserId {

    public static void main(String[] args) throws Exception {

        System.out.println("\u001B[32mSTEP 1: Get (one of) the best Movie per User Id\u001B[0m");

        /// JOB 1 ///

        System.out.println("\u001B[33mJOB 1: Best MovieID Per UserID\u001B[0m");
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Best MovieID Per UserID");

        job1.setJarByClass(BestMoviePerUserId.class);

        // Set Mapper and Reducer class
        job1.setMapperClass(RatingMapper.class);
        job1.setReducerClass(RatingReducer.class);

        // Set Mapper Output key and value classes
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(MovieIdRatingWritable.class);

        // Set Reducer Output key and value classes
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        Path input1 = new Path("/input/ratings.csv");
        Path output1 = new Path("/output/bestMovieIdPerUserId");

        FileInputFormat.addInputPath(job1, input1);
        FileOutputFormat.setOutputPath(job1, output1);

        // Delete the output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output1)) {
            fs.delete(output1, true);
        }

        // Wait for the job1 to complete and start the job2
        if (job1.waitForCompletion(true)) {

            /// JOB 2 ///

            System.out.println("\u001B[33mJOB 2: Best Movie Per UserID\u001B[0m");

            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Best Movie Per UserID");

            job2.setJarByClass(BestMoviePerUserId.class);

            // Set Reducer class
            job2.setReducerClass(UserIdMovieReducer.class);

            // Set Mapper Output key and value classes
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(MovieUserIdWritable.class);

            // Set Reducer Output key and value classes
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);

            // Set input and output paths
            Path input2 = new Path("/input/movies.csv");
            Path input3 = new Path("/output/bestMovieIdPerUserId/part-r-00000");
            Path output2 = new Path("/output/bestMoviePerUserId");

            MultipleInputs.addInputPath(job2, input2, TextInputFormat.class, MovieMapper.class);
            MultipleInputs.addInputPath(job2, input3, TextInputFormat.class, BestMovieIdMapper.class);
            FileOutputFormat.setOutputPath(job2, output2);

            // Delete the output directory if it exists
            if (fs.exists(output2)) {
                fs.delete(output2, true);
            }

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}

