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
        Pattern pattern = Pattern.compile("(\\d+),(\".+\"|[^\",]+),(.+)");
        Matcher matcher = pattern.matcher(value.toString());

        String movieIdStr = "";
        String movieNameStr = "";
        if (matcher.find()) {
            movieIdStr = matcher.group(1);
            movieNameStr = matcher.group(2);
        }

        if (movieIdStr.equals("movieId") || movieIdStr.isEmpty()) {
            return;
        }

        // Set the movieId and the movieUserId
        movieId.set(Integer.parseInt(movieIdStr));
        movieUserId.setMovie(movieNameStr);

        // Emit the movieId and the movieUserId containing the movie name
        context.write(movieId, movieUserId);
    }
}

