package fr.polytech.hadoop.step1.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserIdMovieReducer extends Reducer<IntWritable, MovieUserIdWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<MovieUserIdWritable> values, Context context)
            throws IOException, InterruptedException {

        String movieName = "";
        List<Integer> userIds = new ArrayList<>();

        // Get the movie name and the list of user ids
        for (MovieUserIdWritable value : values) {
            if (!value.getMovie().toString().isEmpty()) {
                movieName = value.getMovie().toString();
            } else {
                userIds.add(value.getUserId().get());
            }
        }

        // Write for each user id its favorite movie name
        for (Integer userId : userIds) {
            context.write(new IntWritable(userId), new Text(movieName));
        }
    }
}
