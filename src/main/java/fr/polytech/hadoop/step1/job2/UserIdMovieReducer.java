package fr.polytech.hadoop.step1.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserIdMovieReducer extends Reducer<IntWritable, MovieUserIdWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<MovieUserIdWritable> values, Context context)
            throws IOException, InterruptedException {

        String movieName = "";

        for (MovieUserIdWritable value : values) {
            if (!value.getMovie().toString().isEmpty()) {
                movieName = value.getMovie().toString();
                break;
            }
        }

        for (MovieUserIdWritable value : values) {
            if (!movieName.isEmpty() && value.getMovie().toString().isEmpty()) {
                context.write(value.getUserId(), new Text(movieName));
            }
        }
    }
}
