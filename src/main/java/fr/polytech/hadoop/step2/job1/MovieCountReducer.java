package fr.polytech.hadoop.step2.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable count = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // Sum the values
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Set the count
        count.set(sum);

        // Emit the movie name and the number of times it has been the favorite movie of a userId
        context.write(key, count);
    }
}
