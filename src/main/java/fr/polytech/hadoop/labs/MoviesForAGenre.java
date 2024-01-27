package fr.polytech.hadoop.labs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MoviesForAGenre {

    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class MoviesByGenreMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text movie = new Text();
        private final Text genre = new Text();
        private String genreFilter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Récupérer le genre à filtrer depuis la configuration
            genreFilter = context.getConfiguration().get("genre");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignorer l'en-tête s'il y en a un
            if (key.get() == 0 && value.toString().contains("movieId,title,genres")) {
                return;
            }

            // Utiliser une expression régulière pour parser la ligne du CSV
            String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (columns[2].contains(genreFilter)) {
                genre.set(genreFilter);
                movie.set(columns[1]);
                context.write(genre, movie);
            }
        }
    }


    // <InputKey, InputValue, OutputKey, OutputValue>
    public static class MoviesByGenreReducer extends Reducer<Text, Text, Text, Text> {

        Text movieList = new Text();
        Text genreText = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            StringBuilder valueList = new StringBuilder();
            for (Text value : values) {
                valueList.append(value.toString()).append("\n");
            }

            movieList.set("\n\nMovies:\n" + valueList);

            genreText.set("Genre: " + key + "\n");

            context.write(genreText, movieList);
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("List of movies for genre " + args[0] + " Job!");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movies For A Genre");

        // Set the Jar by class where Mapper and Reducer reside
        job.setJarByClass(MoviesForAGenre.class);

        // Set Mapper and Reducer class
        job.setMapperClass(MoviesByGenreMapper.class);
        job.setReducerClass(MoviesByGenreReducer.class);

        // Set Mapper Output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set Reducer Output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set configuration to the job
        job.getConfiguration().set("genre", args[0]);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path("/input/movies.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/output/moviesForAGenre"));

        // Wait for the job to complete and print the result
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

