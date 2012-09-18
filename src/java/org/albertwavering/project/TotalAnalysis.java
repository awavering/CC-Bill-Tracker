package org.albertwavering.project;

// Java classes
import java.lang.Math;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;

// Apache Project classes
import org.apache.log4j.Logger;

// Hadoop classes
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalAnalysis extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(TotalAnalysis.class);

  private static ArrayList<ArrayList<String>> billNames2D = new ArrayList<ArrayList<String>>();

  private static ArrayList<String> commonWords = Bills.getCommonWords();

  // set up bill names
  static {
    // bill name structure setup
    for(String sBillNicknames : Bills.getNewBills()){
      ArrayList<String> alBillNicknames = new ArrayList<String>();
      // normalize billName and add to list of nicknames
      for(String billNickname : sBillNicknames.split(","))
        alBillNicknames.add(billNickname.toLowerCase().replaceAll("[^a-zA-Z0-9 ]", "").trim());
      
      billNames2D.add(alBillNicknames);
    }
  }

  public static class TotalAnalysisMapper
      extends    MapReduceBase 
      implements Mapper<Text, Text, IntWritable, Text> {

    // create a counter group for Mapper-specific statistics
    private final String _counterGroup = "Custom Mapper Counters";

    public void map(Text key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {

      reporter.incrCounter(this._counterGroup, "Records In", 1);

      try {

        // Get the text content as a string
        String pageText = value.toString();

        // Removes all punctuation and normalizes whitespace to single spaces.
        pageText = pageText.replaceAll("[^a-zA-Z0-9 ]", "").replaceAll("\\s+", " ");

        if (pageText == null || pageText == "") {
          reporter.incrCounter(this._counterGroup, "Skipped - Empty Page Text", 1);
          return;
        }

        // Extract the domain
        URI uri = new URI(key.toString());
        String domain = uri.getHost();
        domain = domain.startsWith("www.") ? domain.substring(4) : domain;

        // Look for occurrence of bills by name
        for(int i = 0; i < billNames2D.size(); i++){
          // if any of the bill's name is mentioned at least once, count as one mention
          for(String billNickname : billNames2D.get(i)){
            if(pageText.indexOf(billNickname) >= 0){
              //emit domain
              output.collect(new IntWritable(i), new Text("d"+domain));

              //emit uncommon words from this page and associate them with bill
              for (String word : pageText.split(" ")) {
                word = word.toLowerCase().trim();
                if(commonWords.indexOf(word) == -1){
                  output.collect(new IntWritable(i), new Text("w"+word));
                }
              }
              break;
            }
          }
        }
      }
      catch (Exception ex) {
        LOG.error("Caught Exception", ex);
        reporter.incrCounter(this._counterGroup, "Exceptions", 1);
      }
    }
  }

  public static class TotalAnalysisReducer
    extends MapReduceBase
    implements Reducer<IntWritable, Text, Text, Text> {

    private static ArrayList<String> billNames = Bills.getNewBills();

    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
      
      try {
        // bill name lookup
	      String bill = billNames.get(key.get());

        HashMap<String, CountingLong> wordFreq = new HashMap<String, CountingLong>();
        HashMap<String, CountingLong> domainFreq = new HashMap<String, CountingLong>();
        long totalBillCount = 0;

        // assemble freq map
        while (values.hasNext()) {
          String fullVal = values.next().toString();
          String type = fullVal.substring(0,1);
          String content = fullVal.substring(1);

          //find out if we have a domain key or a word key
          if(type.equals("d")){
            if(domainFreq.get(content) == null)
              domainFreq.put(content, new CountingLong());
            else
              domainFreq.get(content).incr();

            totalBillCount++;
          }else{
            if(wordFreq.get(content) == null)
              wordFreq.put(content, new CountingLong());
            else
              wordFreq.get(content).incr();
          }
        }

        // WORDS

        // sort by key and emit one result with the top 50 words sorted in descending order

        // assemble reverse mapping -> count -> word
        TreeMap<Long, ArrayList<String>> revMap = new TreeMap<Long, ArrayList<String>>();
	       for(String word : wordFreq.keySet()){
	        Long freq = new Long(wordFreq.get(word).getVal());
          
          if(revMap.get(freq) == null)
            revMap.put(freq, new ArrayList<String>());
	  
          revMap.get(freq).add(word);
        }

        ArrayList<String> wordsToEmit = new ArrayList<String>();
        for(Long count : revMap.descendingKeySet()){
          
          if(wordsToEmit.size() >= 50)
            break;

          ArrayList<String> words = revMap.get(count);

          for(String word : words){
            if(wordsToEmit.size() >= 50)
              break;
            else
              wordsToEmit.add(word+":"+count.toString());
          }
        }

        String outputWords = "";
        for(String word : wordsToEmit){
	        outputWords += word+", ";
        }

        // emit word association information
        output.collect(new Text("WRD | "+bill+" | "), new Text(outputWords));

        // DOMAINS

        // sort by key and emit one result with the top 50 words sorted in descending order

        // assemble reverse mapping -> count -> domain
        TreeMap<Long, ArrayList<String>> revDomainMap = new TreeMap<Long, ArrayList<String>>();
         for(String domain : domainFreq.keySet()){
          Long freq = new Long(domainFreq.get(domain).getVal());
          
          if(revDomainMap.get(freq) == null)
            revDomainMap.put(freq, new ArrayList<String>());
    
          revDomainMap.get(freq).add(domain);
        }

        ArrayList<String> domainsToEmit = new ArrayList<String>();
        for(Long count : revDomainMap.descendingKeySet()){
          
          if(domainsToEmit.size() >= 50)
            break;

          ArrayList<String> domains = revDomainMap.get(count);

          for(String domain : domains){
            if(domainsToEmit.size() >= 50)
              break;
            else
              domainsToEmit.add(domain+":"+count.toString());
          }
        }

        String outputDomains = "";
        for(String domain : domainsToEmit){
          outputDomains += domain+", ";
        }

        // emit domain association information
        output.collect(new Text("DMN | "+bill+" | "), new Text(outputDomains));

        // emit count information
        output.collect(new Text("CNT | "+bill+" | "), new Text(Long.toString(totalBillCount)));
      }
      catch (Exception ex) {
        LOG.error("Caught Exception", ex);
        reporter.incrCounter("TotalAnalysis Reducer", "Exceptions", 1);
      }
    }

  }



  /**
   * Hadoop FileSystem PathFilter for ARC files, allowing users to limit the
   * number of files processed.
   *
   * @author Chris Stephens <chris@commoncrawl.org>
   */
  public static class SampleFilter
      implements PathFilter {

    private static int count =         0;
    private static int max   =      9999;

    public boolean accept(Path path) {

      if (!path.getName().startsWith("textData-"))
        return false;

      SampleFilter.count++;

      if (SampleFilter.count > SampleFilter.max)
        return false;

      return true;
    }
  }

  /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.  
   * @return      0 if the Hadoop job completes successfully, 1 if not. 
   */
  @Override
  public int run(String[] args)
      throws Exception {

    String outputPath = null;
    String configFile = null;

    // Read the command line arguments.
    if (args.length <  1)
      throw new IllegalArgumentException("Example JAR must be passed an output path.");

    outputPath = args[0];

    if (args.length >= 2)
      configFile = args[1];

    // For this example, only look at a single text file.
    //String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/1341690166822/textData-01666";
 
    // Switch to this if you'd like to look at all text files.  May take many minutes just to read the file listing.
    //String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/*/textData-*";

    // Creates a new job configuration for this Hadoop job.
    JobConf job = new JobConf(this.getConf());

    job.setJarByClass(TotalAnalysis.class);

    // fix from the google groups discussion
    String segmentListFile = "s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt";

    FileSystem fsInput = FileSystem.get(new URI(segmentListFile), job);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fsInput.open(new Path(segmentListFile))));

    String segmentId;

    while ((segmentId = reader.readLine()) != null) {
      String inputPath = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/"+segmentId+"/textData-*";
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }

    // Read in any additional config parameters.
    if (configFile != null) {
      LOG.info("adding config parameters from '"+ configFile + "'");
      this.getConf().addResource(configFile);
    }

    // Scan the provided input path for ARC files.
    //LOG.info("setting input path to '"+ inputPath + "'");
    //FileInputFormat.addInputPath(job, new Path(inputPath));
    //FileInputFormat.setInputPathFilter(job, SampleFilter.class);

    // Delete the output path directory if it already exists.
    LOG.info("clearing the output path at '" + outputPath + "'");

    FileSystem fs = FileSystem.get(new URI(outputPath), job);

    if (fs.exists(new Path(outputPath)))
      fs.delete(new Path(outputPath), true);

    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    // Set which InputFormat class to use.
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    // Set which OutputFormat class to use.
    job.setOutputFormat(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(TotalAnalysis.TotalAnalysisMapper.class);
    job.setReducerClass(TotalAnalysis.TotalAnalysisReducer.class);

    if (JobClient.runJob(job).isSuccessful())
      return 0;
    else
      return 1;
  }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {
    int res = ToolRunner.run(new Configuration(), new TotalAnalysis(), args);
    System.exit(res);
  }
}

