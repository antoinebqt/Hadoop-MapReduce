package fr.polytech.hadoop.step1.job2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, MovieUserIdWritable>{

    IntWritable movieId = new IntWritable();
    MovieUserIdWritable movieUserId = new MovieUserIdWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Ignore the first line
        if (value.toString().contains("movieId,title,genres")) {
            return;
        }

        // Parse the line
        String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        // Set the movieId and the movieUserId
        movieId.set(Integer.parseInt(columns[0]));
        movieUserId.setMovie(columns[1]);

        // Emit the movieId and the movieUserId containing the movie name
        context.write(movieId, movieUserId);
    }
}

