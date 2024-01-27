package fr.polytech.hadoop.labs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopMovieIDByUserID {

    public static class UserMovieRating implements Writable {

        LongWritable movieId;
        FloatWritable rating;

        public UserMovieRating() {
            this.movieId = new LongWritable();
            this.rating = new FloatWritable();
        }

        public void setMovieID(long movieId) {
            this.movieId.set(movieId);
        }

        public void setRating(float rating) {
            this.rating.set(rating);
        }

        public LongWritable getMovieId() {
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

    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class RatingMovieMapper extends Mapper<LongWritable, Text, LongWritable, UserMovieRating> {

        LongWritable userId = new LongWritable();
        UserMovieRating movieRating = new UserMovieRating();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignorer l'en-tête s'il y en a un
            if (key.get() == 0 && value.toString().contains("userId,movieId,rating,timestamp")) {
                return;
            }

            // Utiliser une expression régulière pour parser la ligne du CSV
            String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            userId.set(Long.parseLong(columns[0]));
            movieRating.setMovieID(Long.parseLong(columns[1]));
            movieRating.setRating(Float.parseFloat(columns[2]));

            context.write(userId, movieRating);
        }
    }

    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class RatingMovieReducer extends Reducer<LongWritable, UserMovieRating, LongWritable, LongWritable> {

        LongWritable movieId = new LongWritable();

        @Override
        protected void reduce(LongWritable key, Iterable<UserMovieRating> values, Context context)
                throws IOException, InterruptedException {

            long max_movieid = 0;
            float max_rating = 0;
            for (UserMovieRating value : values) {
                if(value.getRating().get() > max_rating) {
                    max_rating = value.getRating().get();
                    max_movieid = value.getMovieId().get();
                }
            }

            movieId.set(max_movieid);

            context.write(key, movieId);
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("Top movieID per userID Job!");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Top MovieID By UserID");

        // Set the Jar by class where Mapper and Reducer reside
        job.setJarByClass(TopMovieIDByUserID.class);

        // Set Mapper and Reducer class
        job.setMapperClass(RatingMovieMapper.class);
        job.setReducerClass(RatingMovieReducer.class);

        // Set Mapper Output key and value classes
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(UserMovieRating.class);

        // Set Reducer Output key and value classes
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path("/input/ratings.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/output/topMovieIDByUserID"));

        // Wait for the job to complete and print the result
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

