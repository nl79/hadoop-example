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

    String hdfsInputDir = args[0];
    String hdfsOutputDirPrefix = args[1];

    int iterations = Integer.parseInt(args[2]);
    Double MIN_SUPPORT_PERCENT = Double.parseDouble(args[3]);
    Integer MAX_NUM_TXNS = Integer.parseInt(args[4]);

    for(int iteration=1; iteration <= iterations; iteration++) {

      boolean success = iterate(hdfsInputDir, hdfsOutputDirPrefix, iteration, MIN_SUPPORT_PERCENT, MAX_NUM_TXNS);
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
    config.setInt("numTxns", MAX_NUM_TXNS);

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
    private final static IntWritable one = new IntWritable(1);
    private Text item = new Text();

    public void map(Object key, Text transaction, Context context) throws IOException, InterruptedException {

      // split the transaction into terms
      String line = transaction.trim();
      String[] terms = line.split("[\\s\\t]+");

      // transaction ID is the first item in the list.
      int id = Integer.parseInt(words[0].trim());

      List<Integer> items = new ArrayList<Integer>();

      for(int i=1; i < terms.length; i++) {
        items.add(Integer.parseInt(terms[i].trim()));
      }

      // Emit a count for each term
      // context.write(term, new IntWritable(1));

    }
  }

  public static class RuleReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text itemset, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int countItemId = 0;
      for (IntWritable value : values) {
        countItemId += value.get();
      }

      String itemsetIds = itemset.toString();

      Double support = Double.parseDouble(context.getConfiguration().get("support"));
      Integer numTxns = context.getConfiguration().getInt("numTxns", 2);


      // calculate min support and add to the output
      //  context.write(new Text(itemsetIds), new IntWritable(countItemId));
    }
  }

  public static class kPassMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text item = new Text();

    private List<ItemSet> largeItemsetsPrevPass = new ArrayList<ItemSet>();
    private List<ItemSet> candidateItemsets     = null;
    private HashTreeNode hashTreeRootNode       = null;

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
          largeItemsetsPrevPass.add(new ItemSet(items, supportCount));
        }
      }
      catch(Exception e)
      {
      }

      candidateItemsets = AprioriUtils.getCandidateItemsets(largeItemsetsPrevPass, (iteration-1));
      hashTreeRootNode = HashTreeUtils.buildHashTree(candidateItemsets, iteration); // This would be changed later
    }

    public void map(Object key, Text txnRecord, Context context) throws IOException, InterruptedException {
      Transaction txn = AprioriUtils.getTransaction(txnRecord.toString());
      List<ItemSet> candidateItemsetsInTxn = HashTreeUtils.findItemsets(hashTreeRootNode, txn, 0);
      for(ItemSet itemset : candidateItemsetsInTxn) {
        item.set(itemset.getItems().toString());
        context.write(item, one);
      }

    }
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new RuleMiner(), args);
    System.exit(exitCode);
  }
}