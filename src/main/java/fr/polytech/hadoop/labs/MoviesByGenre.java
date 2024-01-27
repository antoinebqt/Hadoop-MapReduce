package fr.polytech.hadoop.labs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MoviesByGenre {

    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class MoviesByGenreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text genre = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignorer l'en-tête s'il y en a un
            if (key.get() == 0 && value.toString().contains("movieId,title,genres")) {
                return;
            }

            // Utiliser une expression régulière pour parser la ligne du CSV
            String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            // Récupérer le genre depuis la colonne correspondante
            String[] genres = columns[2].split("\\|");

            // Émettre le genre avec la valeur 1 pour chaque genre de chaque film
            for (String genreValue : genres) {
                genre.set(genreValue);
                context.write(genre, one);
            }
        }
    }


    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class MoviesByGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Somme des films pour chaque genre
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            // Émettre le résultat : genre -> nombre de films
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Count the number of movies by genre Job!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movies By Genre");

        // Set the Jar by class where Mapper and Reducer reside
        job.setJarByClass(MoviesByGenre.class);

        // Set Mapper and Reducer class
        job.setMapperClass(MoviesByGenreMapper.class);
        job.setReducerClass(MoviesByGenreReducer.class);

        // Set Mapper Output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set Reducer Output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path("/input/movies.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/output/moviesByGenre"));

        // Wait for the job to complete and print the result
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

