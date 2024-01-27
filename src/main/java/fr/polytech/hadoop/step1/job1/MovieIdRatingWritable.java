package fr.polytech.hadoop.step1.job1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieIdRatingWritable implements Writable {

    IntWritable movieId;
    FloatWritable rating;

    public MovieIdRatingWritable() {
        this.movieId = new IntWritable();
        this.rating = new FloatWritable();
    }

    public void setMovieID(int movieId) {
        this.movieId.set(movieId);
    }

    public void setRating(float rating) {
        this.rating.set(rating);
    }

    public IntWritable getMovieId() {
        return movieId;
    }

    public FloatWritable getRating() {
        return rating;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        movieId.write(dataOutput);
        rating.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieId.readFields(dataInput);
        rating.readFields(dataInput);
    }

}
