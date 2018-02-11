/*
  Computes how many times the words “education”, “politics”, “sports”, and “agriculture”
  appear in each file. Then, the program outputs the number of states for which each of
  these words is dominant (i.e., appears more times than the other three words)
 */

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DominantTerms {

    private static final String OUTPUT_PATH = "/out/DominantTerms/temp";

    /*
     * TokenMapper - Will extrat and save each term from the document that meets the criteria
     * and is in the collection of terms we are trying to count.
     */
    public static class TokenMapper extends Mapper<Object, Text, Text, Text> {

        // Current Filepath of the inptu file.
        private Text filepath = new Text();
        // Current Filename of the input.
        private Text filename = new Text();

        // List of terms to match
        private List<String> collection = new ArrayList<String>();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {

            // Setup the terms we're looking for
            this.collection.add("agriculture");
            this.collection.add("politics");
            this.collection.add("education");
            this.collection.add("sports");

            // Capture the filename of the input so that we can related it back to the counts.

            filepath.set(((FileSplit) context.getInputSplit()).getPath().toString());

            String[] parts = filepath.toString().split(Pattern.quote(System.getProperty("file.separator")));
            filename.set(parts[parts.length-1].toString().trim());
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());

            // Current Text Token being checked.
            Text token = new Text();

            while (itr.hasMoreTokens()) {

                /*
                 * Set the token value.
                 * Clean the input and try to remove any none alphanumeric characters in the token.
                 */
                token.set(itr.nextToken().toString().toLowerCase().replaceAll("[^A-Za-z0-9]", "").trim());

                // Check that the item is in the collction of items that we're looking for.
                if (collection.contains(token.toString())) {

                    // Save the filename and the item.
                    context.write(filename, token);
                }
            }
        }
    }

    /*
     * Reducer will count how many times each term appers in the input collection
     * and determine the term that appears the most frequently.
     */
    public static class DominantTermReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            /*
             * Count and store the count of each term in the file.(key)
             * First we need to see how often each term appears in each file.
             */
            Map<String, Integer> count = new HashMap<String, Integer>();
            String current;

            // Iterate over the values and count how many times each term apprears in the list.
            for (Text term : values) {

                current = term.toString();

                // If the list does not already cotain the current term, create a new entry
                if (!count.containsKey(current)) {

                    // Initialize the count to 1
                    count.put(current, 1);

                } else {
                    // The key exists, increment the count by 1.
                    count.put(current, count.get(current) + 1);
                }
            }


            /*
             * Determine the term that appears most often in each key(state).
             */
            // Stores the most frequest word.
            int max = 0;
            int total = 0;

            String dominant = null;

            // Iterate over each term in the counts map.
            for (String term : count.keySet()) {

                // Get the count of the current term.
                total = count.get(term);

                // If the count is greater then the current max, reassign it.
                if (total > max) {
                    max = total;

                    // Save the current term.
                    dominant = term;
                }
            }

            // Save the current key(state) and the dominant term that appears in the input text.
            context.write(key, new Text(dominant));
        }
    }


    /*
     * Mapper will emit a count for each dominant term from the input.
     */
    public static class DominantTermCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // (State name/filename => Dominant term).
            String line = value.toString();

            // Filename.
            Text filename;

            // Dominant term.
            Text term;

            StringTokenizer itr = new StringTokenizer(line);

            while (itr.hasMoreTokens()) {

                // Filename is the first token.
                filename = new Text(itr.nextToken());

                // Dominant term is the second token.
                term = new Text(itr.nextToken());

                // Update the term count by 1.
                context.write(term, new IntWritable(1));
            }
        }
    }

    /*
     * Reducer will sum up the counts of dominant terms per filename(state).
     */
    public static class DominantTermCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int total = 0;

            // Sum up the term count
            for (IntWritable term : values) {

                // Get the count.
                total += term.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    /*
     * Will orchestrate the jobs and intermediate results.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*
          * Job 1:
          * 1. Extract and Count the number of terms that appear in each document based on the supplied list.
          * 2. Determine which term appears most frequently in each document(state)
          * output: (Document Name) => (Most Frequent Term from the list)
          */
        Job job = Job.getInstance(conf, "Job 1 - (State) => (Most Frequent Term)");

        // Tell the job the name of the .jar file
        // NOTE: (This works only if the .jar file is the same as the class name of the input class.)
        job.setJarByClass(DominantTerms.class);

        // Tell the job the name of the map class that implement the map method.
        job.setMapperClass(TokenMapper.class);

        // Tell the job the name of the combine function.
        job.setCombinerClass(DominantTermReducer.class);

        // tell the job the name of the reducer class that implements the reduce method.
        job.setReducerClass(DominantTermReducer.class);

        // Tell the job the output format for the key and the value.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Tell the job the input path to the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Store the results in a temporary path.
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.waitForCompletion(true);

        /*
          * Job 2:
          * 1. Count how many times each term appears in the input set:
          *     (Document Name) => (Most Frequent Term from the list)
          * 2. Total up the results per Term
          * output: (Term) => (total count of States(documents) in which its dominant)
          */
        Job job2 = Job.getInstance(conf, "Job 2 - (Term) => (Number of States its Dominant)");
        job2.setJarByClass(DominantTerms.class);
        job2.setMapperClass(DominantTermCountMapper.class);
        job2.setCombinerClass(DominantTermCountReducer.class);
        job2.setReducerClass(DominantTermCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Read the results from the temporary path.
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));

        // Output the results to the supplied path.
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}