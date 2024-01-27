package fr.polytech.hadoop.step1.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieUserIdWritable implements Writable {

    Text movie;
    IntWritable userId;

    public MovieUserIdWritable() {
        this.movie = new Text("");
        this.userId = new IntWritable();
    }

    public void setMovie(String movieId) {
        this.movie.set(movieId);
    }

    public void setUserId(int userId) {
        this.userId.set(userId);
    }

    public Text getMovie() {
        return movie;
    }

    public IntWritable getUserId() {
        return userId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        movie.write(dataOutput);
        userId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movie.readFields(dataInput);
        userId.readFields(dataInput);
    }

}
