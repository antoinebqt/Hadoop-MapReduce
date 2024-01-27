package fr.polytech.hadoop.step1.job2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BestMovieIdMapper extends Mapper<LongWritable, Text, IntWritable, MovieUserIdWritable>{

    IntWritable movieIdWritable = new IntWritable();
    MovieUserIdWritable movieUserId = new MovieUserIdWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Parse the line
        Pattern pattern = Pattern.compile("(\\d+)\\s+(\\d+)$");
        Matcher matcher = pattern.matcher(value.toString());

        int movieId = -1;
        if (matcher.find()) {
            movieId = Integer.parseInt(matcher.group(2));
        }

        if (movieId == -1) {
            return;
        }

        // Set the movieId and the movieUserId
        movieIdWritable.set(movieId);
        movieUserId.setUserId(Integer.parseInt(matcher.group(1)));

        // Emit the key-value pair
        context.write(movieIdWritable, movieUserId);
    }
}

