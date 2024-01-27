package fr.polytech.hadoop.step2.job1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BestMovieMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    Text movieName = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Parse the line
        Pattern pattern = Pattern.compile("(\\d+)\\s+(.+)$");
        Matcher matcher = pattern.matcher(value.toString());

        if (!matcher.find()) {
            return;
        }

        // Set the movie name
        movieName.set(matcher.group(2));

        // Emit the movie name and 1 (for the count in the reducer)
        context.write(movieName, new IntWritable(1));
    }
}

