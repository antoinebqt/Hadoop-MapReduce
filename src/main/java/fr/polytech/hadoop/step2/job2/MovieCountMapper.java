package fr.polytech.hadoop.step2.job2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieCountMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

    Text movieName = new Text();

    IntWritable count = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Parse the line
        Pattern pattern = Pattern.compile("(.+)\\s+(\\d+)$");
        Matcher matcher = pattern.matcher(value.toString());

        if (!matcher.find()) {
            return;
        }

        // Set the movie name and the count
        movieName.set(matcher.group(1));
        count.set(Integer.parseInt(matcher.group(2)));

        // Emit the key-value pair
        context.write(count, movieName);
    }
}

