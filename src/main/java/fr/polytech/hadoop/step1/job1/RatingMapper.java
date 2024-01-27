package fr.polytech.hadoop.step1.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<LongWritable, Text, IntWritable, MovieIdRatingWritable> {

    IntWritable userId = new IntWritable();
    MovieIdRatingWritable movieIdRating = new MovieIdRatingWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore the first line
        if (value.toString().contains("userId,movieId,rating,timestamp")) {
            return;
        }

        // Parse the line
        String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        // Set the userId and the movieIdRating
        userId.set(Integer.parseInt(columns[0]));
        movieIdRating.setMovieID(Integer.parseInt(columns[1]));
        movieIdRating.setRating(Float.parseFloat(columns[2]));

        // Emit the key-value pair
        context.write(userId, movieIdRating);
    }
}
