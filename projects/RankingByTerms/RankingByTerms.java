/*
  Identify all states that have the same ranking of these four words. For example, NY, NJ, PA
  may have the ranking 1. Politics; 2. Sports. 3. Agriculture; 4. Education (meaning “politics”
  appears more times than “sports” in the Wikipedia file of the state, “sports” appears more
  times than “agriculture”, etc.)
 */

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RankingByTerms {

    private static final String TEMP = "/out/RankingByTerms/temp";

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

    public static class TokenRankReducer extends Reducer<Text, Text, Text, Text> {

        private static Map sortMapByValues(Map<String, Integer> aMap) {

            Set<Entry<String,Integer>> entries = aMap.entrySet();

            // used linked list to sort, because insertion of elements in linked list is faster than an array list.
            List<Entry<String,Integer>> temp = new LinkedList<Entry<String,Integer>>(entries);

            // sorting the List
            Collections.sort(temp, new Comparator<Entry<String,Integer>>() {

                @Override
                public int compare(Entry<String, Integer> ele1,
                                   Entry<String, Integer> ele2) {

                    return ele1.getValue().compareTo(ele2.getValue());
                }
            });

            // Storing the list into Linked HashMap to preserve the order of insertion.
            Map<String,Integer> result = new LinkedHashMap<String, Integer>();
            for(Entry<String,Integer> entry: temp) {
                result.put(entry.getKey(), entry.getValue());
            }

            return result;
        }
        //@key = filename
        //@values = terms
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

            // count = the number of each term in the filename.

            // Sort the list by the term count.
            // EX: 'sports' => 4, 'argiculture' => 3, 'entertainment' => 2
            Map<String, Integer> sorted = this.sortMapByValues(count);


            /*
             * Generate the order of terms as a string from higest order to lowest
             * This is going to be used by next job that will group the items by the
             * 'key'
             */
            String ordered = String.join("=>", sorted.keySet().toArray(new String[sorted.size()]));

            context.write(key, new Text(ordered));
        }
    }

    public static class RankToStateMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            /*
             * tokenize the string and the use the ranking as the key and state as the value
             * this will group the values by the ranking and pass it to the reducer.
             */
            StringTokenizer items = new StringTokenizer(line);

            while (items.hasMoreTokens()) {

                Text state = new Text(items.nextToken());
                Text ranking = new Text(items.nextToken());

                context.write(ranking, state);
            }
        }
    }

    public static class RankToStateReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Build a list of states from the vlues.
            String states = "";
            for (Text val : values) {
                states += val + ", ";
            }
            // Trim off the traiiling ', ,'
            context.write(key, new Text(states.substring(0, states.length() - 2)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*
          * Job 1:
          * 1. Extract the terms that fit the criterial and counts them.
          * 2. Orders the terms per state from most frequest to least frequent.
          */
        Job job = Job.getInstance(conf, "Job 1: (States) => (Term List with counts.)");
        job.setJarByClass(RankingByTerms.class);
        job.setMapperClass(TokenMapper.class);
        job.setCombinerClass(TokenRankReducer.class);
        job.setReducerClass(TokenRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Write intermediate data to the temp location.
        FileOutputFormat.setOutputPath(job, new Path(TEMP));
        job.waitForCompletion(true);

        /*
         * Job 2
         * 1. Creates groups of items based on the term frequency
         */
        Job job2 = Job.getInstance(conf, "Job 2: (Term Frequency) => (State List)");
        job2.setJarByClass(RankingByTerms.class);
        job2.setMapperClass(RankToStateMapper.class);
        job2.setCombinerClass(RankToStateReducer.class);
        job2.setReducerClass(RankToStateReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Read intermediate data from the temp location.
        FileInputFormat.addInputPath(job2, new Path(TEMP));

        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
