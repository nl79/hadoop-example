import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.*;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.*;

import org.apache.hadoop.fs.*;

public class RuleMiner extends Configured implements Tool
{
  private static String jobPrefix = " ";

  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

    if(args.length != 5) {
      System.err.println("Invalid Arguments Supplied");
      return -1;
    }

    String inputPath = args[0];
    String outputPath = args[1];

    int iterations = Integer.parseInt(args[2]);
    Double MIN_SUPPORT_PERCENT = Double.parseDouble(args[3]);
    Integer MAX_NUM_TXNS = Integer.parseInt(args[4]);

    for(int iteration=1; iteration <= iterations; iteration++) {

      boolean success = iterate(inputPath, outputPath, iteration, MIN_SUPPORT_PERCENT, MAX_NUM_TXNS);
      if(!success) {
        System.err.println("Job Failed");
        return -1;
      }
    }

    return 1;
  }

  private static boolean iterate(String hdfsInputDir, String hdfsOutputDirPrefix, int iteration, Double MIN_SUPPORT_PERCENT, Integer MAX_NUM_TXNS)
    throws IOException, InterruptedException, ClassNotFoundException
  {
    boolean success = false;

    Configuration config = new Configuration();
    config.setInt("iteration", iteration);
    config.set("support", Double.toString(MIN_SUPPORT_PERCENT));
    config.setInt("total", MAX_NUM_TXNS);

    Job job = new Job(config, jobPrefix + iteration);

    job.setJarByClass(RuleMiner.class);
    job.setReducerClass(RuleReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    if(iteration == 1) {
      job.setMapperClass(FirstPassMapper.class);
    }
    else {
      job.setMapperClass(kPassMapper.class);
    }

    FileInputFormat.addInputPath(job, new Path(hdfsInputDir));
    FileOutputFormat.setOutputPath(job, new Path(hdfsOutputDirPrefix + iteration));

    success = (job.waitForCompletion(true) ? true : false);

    return success;
  }

  public static class FirstPassMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] tokens = value.toString().split(",");

      Text token = new Text();

      int id;

      //first token is ID
      if(tokens.length > 0) {
        id = Integer.parseInt(tokens[0]);
      }

      for(int i = 1; i < tokens.length; ++i) {
        token.set(tokens[i].toString().toLowerCase().replaceAll("[^A-Za-z0-9]", "").trim());

        context.write(token, new IntWritable(1));
      }
    }
  }

  public static class RuleReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text terms, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


      int count = 0;


      // total up the number of occurances

      for (IntWritable value : values) {
        count += value.get();
      }

      String set = terms.toString();

      // Get the support value from the config object
      Double support = Double.parseDouble(context.getConfiguration().get("support"));

      Integer total = context.getConfiguration().getInt("total", 2);

      // calculate min support and add to the output
      if(Apriori.hasSupport(support, total, count)) {
        context.write(new Text(set), new IntWritable(count));
      }

      //
    }
  }

  public static class kPassMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text item = new Text();

    private List<Set> largeItemsetsPrevPass = new ArrayList<Set>();
    private List<Set> candidateItemsets     = null;

    @Override
    public void setup(Context context) throws IOException {
      //Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());

      int iteration = context.getConfiguration().getInt("iteration", 2);
      String opFileLastPass = context.getConfiguration().get("fs.default.name") + "/user/user/RuleMiner-out-" + (iteration-1) + "/part-r-00000";

      try
      {
        Path pt=new Path(opFileLastPass);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader fis=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String currLine = null;
        //System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        while ((currLine = fis.readLine()) != null) {
          currLine = currLine.trim();
          String[] words = currLine.split("[\\s\\t]+");
          if(words.length < 2) {
            continue;
          }

          List<Integer> items = new ArrayList<Integer>();
          for(int k=0; k < words.length -1 ; k++){
            String csvItemIds = words[k];
            String[] itemIds = csvItemIds.split(",");
            for(String itemId : itemIds) {
              items.add(Integer.parseInt(itemId));
            }
          }
          String finalWord = words[words.length-1];
          int supportCount = Integer.parseInt(finalWord);
          //System.out.println(items + " --> " + supportCount);
          largeItemsetsPrevPass.add(new Set(items, supportCount));
        }
      }
      catch(Exception e)
      {
      }

      candidateItemsets = Apriori.getCandidateSets(largeItemsetsPrevPass, (iteration-1));
    }

    public void map(Object key, Text txnRecord, Context context) throws IOException, InterruptedException {
      String txt = txnRecord.toString();
//      List<Set> candidateSetsInTxn = Apriori.findSets(hashTreeRootNode, txn, 0);
//      for(Set itemset : candidateItemsetsInTxn) {
//        item.set(itemset.getItems().toString());
//        context.write(item, one);
//      }

    }
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new RuleMiner(), args);
    System.exit(exitCode);
  }
}