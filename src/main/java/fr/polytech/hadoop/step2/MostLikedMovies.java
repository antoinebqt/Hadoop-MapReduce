package fr.polytech.hadoop.step2;

import fr.polytech.hadoop.step2.job1.BestMovieMapper;
import fr.polytech.hadoop.step2.job1.MovieCountReducer;
import fr.polytech.hadoop.step2.job2.GroupAndSortReducer;
import fr.polytech.hadoop.step2.job2.MovieCountMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostLikedMovies {

    public static void main(String[] args) throws Exception {

        System.out.println("\u001B[32mSTEP 2: Get number of users that have liked the provided list of movies\u001B[0m");

        /// JOB 1 ///

        System.out.println("\u001B[33mJOB 1: Count the number of like per movie name\u001B[0m");
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Number of like per Movie");

        job1.setJarByClass(MostLikedMovies.class);

        // Set Mapper and Reducer class
        job1.setMapperClass(BestMovieMapper.class);
        job1.setReducerClass(MovieCountReducer.class);

        // Set Mapper Output key and value classes
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // Set Reducer Output key and value classes
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        Path input1 = new Path("/output/bestMoviePerUserId/part-r-00000");
        Path output1 = new Path("/output/movieCount");

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

            System.out.println("\u001B[33mJOB 2: Group films by number of likes and sort in ascending order\u001B[0m");

            Job job2 = Job.getInstance(conf, "Movies grouped and sorted by number of likes");

            job2.setJarByClass(MostLikedMovies.class);

            // Set Mapper and Reducer class
            job2.setMapperClass(MovieCountMapper.class);
            job2.setReducerClass(GroupAndSortReducer.class);

            // Set Mapper Output key and value classes
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);

            // Set Reducer Output key and value classes
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);

            // Set input and output paths
            Path input2 = new Path("/output/movieCount/part-r-00000");
            Path output2 = new Path("/output/mostLikedMovies");

            FileInputFormat.addInputPath(job2, input2);
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

