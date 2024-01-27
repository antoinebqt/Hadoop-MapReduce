package fr.polytech.hadoop.step1.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RatingReducer extends Reducer<IntWritable, MovieIdRatingWritable, IntWritable, IntWritable> {

    IntWritable movieId = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<MovieIdRatingWritable> values, Context context)
            throws IOException, InterruptedException {

        // Find the movieId with the highest rating
        int max_movieid = -1;
        float max_rating = -1;
        for (MovieIdRatingWritable value : values) {
            if(value.getRating().get() > max_rating) {
                max_rating = value.getRating().get();
                max_movieid = value.getMovieId().get();
            }
        }

        // Set the movieId
        movieId.set(max_movieid);

        // Emit the key-value pair
        context.write(key, movieId);
    }
}
