package fr.polytech.hadoop.step1.job2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BestMovieIdMapper extends Mapper<LongWritable, Text, IntWritable, MovieUserIdWritable>{

    IntWritable movieId = new IntWritable();
    MovieUserIdWritable movieUserId = new MovieUserIdWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Parse the line
        String[] columns = value.toString().split("\\s+");

        if (columns.length >= 2) {

            // Set the movieId and the movieUserId
            movieId.set(Integer.parseInt(columns[1]));
            movieUserId.setUserId(Integer.parseInt(columns[0]));
        } else {
            return;
        }

        // Emit the movieId and the movieUserId containing the user id
        context.write(movieId, movieUserId);
    }
}

