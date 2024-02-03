package fr.polytech.hadoop.step2.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupAndSortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    Text movies = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Concatenate the movies
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            if (sb.length() == 0) {
                sb.append(value.toString());
            } else {
                sb.append(" ");
                sb.append(value.toString());
            }
        }

        // Set the movies
        movies.set(sb.toString());

        // Emit the number of occurrences and the list of movies associated
        context.write(key, movies);
    }
}
