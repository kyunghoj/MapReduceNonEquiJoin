/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This program uses map/reduce to just run a distributed job where there is
 * no interaction between the tasks and each task write a large unsorted
 * random binary sequence file of BytesWritable.
 * In order for this program to generate data for terasort with 10-byte keys
 * and 90-byte values, have the following config:
 * <xmp>
 * <?xml version="1.0"?>
 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 * <configuration>
 *   <property>
 *     <name>test.randomwrite.min_key</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>test.randomwrite.max_key</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>test.randomwrite.min_value</name>
 *     <value>90</value>
 *   </property>
 *   <property>
 *     <name>test.randomwrite.max_value</name>
 *     <value>90</value>
 *   </property>
 *   <property>
 *     <name>test.randomwrite.total_bytes</name>
 *     <value>1099511627776</value>
 *   </property>
 * </configuration></xmp>
 * 
 * Equivalently, {@link RandomTextKeyValueWriter} also supports all the above options
 * and ones supported by {@link GenericOptionsParser} via the command-line.
 */
public class RandomTextKeyValueWriter extends Configured implements Tool {
  
  /**
   * User counters
   */
  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }
  
  /**
   * A custom input format that creates virtual inputs of a single string
   * for each map.
   */
  static class RandomInputFormat implements InputFormat<Text, Text> {

    /** 
     * Generate the requested number of file splits, with the filename
     * set to the filename of the output file.
     */
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) throws IOException {
      InputSplit[] result = new InputSplit[numSplits];
      Path outDir = FileOutputFormat.getOutputPath(job);
      for(int i=0; i < result.length; ++i) {
        result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, 
                                  (String[])null);
      }
      return result;
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class RandomRecordReader implements RecordReader<Text, Text> {
      Path name;
      public RandomRecordReader(Path p) {
        name = p;
      }
      public boolean next(Text key, Text value) {
        if (name != null) {
          key.set(name.getName());
          name = null;
          return true;
        }
        return false;
      }
      public Text createKey() {
        return new Text();
      }
      public Text createValue() {
        return new Text();
      }
      public long getPos() {
        return 0;
      }
      public void close() {}
      public float getProgress() {
        return 0.0f;
      }
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit split,
                                        JobConf job, 
                                        Reporter reporter) throws IOException {
      return new RandomRecordReader(((FileSplit) split).getPath());
    }
  }

  static class Map extends MapReduceBase
    implements Mapper<WritableComparable, Writable,
                      Text, Text> {
    
    private long numBytesToWrite;
    private int minKeySize;
    private int keySizeRange;
    private int minValueSize;
    private int valueSizeRange;
    private Random random = new Random();
    private Text randomKey = new Text();
    private Text randomValue = new Text();
    
    /**
     * Given an output filename, write a bunch of random records to it.
     */
    public void map(WritableComparable key, 
                    Writable value,
                    OutputCollector<Text, Text> output, 
                    Reporter reporter) throws IOException {
      int itemCount = 0;
      while (numBytesToWrite > 0) {
    	  
    	
        int keyLength = minKeySize + 
          (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
        randomKey.set(RandomStringUtils.random(keyLength, true, true));
        
        int valueLength = minValueSize +
          (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
        randomValue.set(RandomStringUtils.random(valueLength, true, true));
        
        output.collect(randomKey, randomValue);
        numBytesToWrite -= keyLength + valueLength;
        reporter.incrCounter(Counters.BYTES_WRITTEN, keyLength + valueLength);
        reporter.incrCounter(Counters.RECORDS_WRITTEN, 1);
        if (++itemCount % 200 == 0) {
          reporter.setStatus("wrote record " + itemCount + ". " + 
                             numBytesToWrite + " bytes left.");
        }
      }
      reporter.setStatus("done with " + itemCount + " records.");
    }
    
    /**
     * Save the values out of the configuration that we need to write
     * the data.
     */
    @Override
    public void configure(JobConf job) {
      numBytesToWrite = job.getLong("test.randomwrite.bytes_per_map",
                                   1*1024*1024*1024);
    		  						
      minKeySize = job.getInt("test.randomwrite.min_key", 3);
      keySizeRange = 
        job.getInt("test.randomwrite.max_key", 5) - minKeySize;
      minValueSize = job.getInt("test.randomwrite.min_value", 10);
      valueSizeRange = 
        job.getInt("test.randomwrite.max_value", 20) - minValueSize;
    }
    
  }
  
  /**
   * This is the main routine for launching a distributed random write job.
   * It runs 2 maps/node and each map writes 1 gig of data to a DFS file.
   * The reduce doesn't do anything.
   * 
   * @throws IOException 
   */
  public int run(String[] args) throws Exception {    
    if (args.length == 0) {
      System.out.println("Usage: writer <out-dir> [-conf jobconf.xml]");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    Path outDir = new Path(args[0]);
    JobConf job = new JobConf(getConf());
    
    job.setJarByClass(RandomTextKeyValueWriter.class);
    job.setJobName("random-text-kv-writer");
    FileOutputFormat.setOutputPath(job, outDir);
    
    for (int i = 1; i < args.length; i++) {
    	try {
    		if ("-conf".equals(args[i])) {
    			job.addResource(args[++i]);
    		}
    		
    	} catch (Exception e) {
    		System.out.println("Usage: writer <out-dir> [-conf jobconf.xml]");
    		ToolRunner.printGenericCommandUsage(System.out);
    		return -1;
    	}
    }
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setInputFormat(RandomInputFormat.class);
    job.setMapperClass(Map.class);        
    job.setReducerClass(IdentityReducer.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    
    for (Entry<String, String> entry: job) {
		System.err.printf("[Debug] %s=%s\n", entry.getKey(), entry.getValue());
	}
    
    JobClient client = new JobClient(job);
    ClusterStatus cluster = client.getClusterStatus();
    int numMapsPerHost = job.getInt("test.randomwriter.maps_per_host", 1);
    long numBytesToWritePerMap = job.getLong("test.randomwrite.bytes_per_map",
                                             1*1024*1024*1024);
    System.out.println(numMapsPerHost + " maps per host." );
    System.out.println("Writes " + numBytesToWritePerMap + " bytes per map." );
    
    if (numBytesToWritePerMap == 0) {
      System.err.println("Cannot have test.randomwrite.bytes_per_map set to 0");
      return -2;
    }
    long totalBytesToWrite = job.getLong("test.randomwrite.total_bytes", 
         numMapsPerHost*numBytesToWritePerMap*cluster.getTaskTrackers());
    int numMaps = (int) (totalBytesToWrite / numBytesToWritePerMap);
    if (numMaps == 0 && totalBytesToWrite > 0) {
      numMaps = 1;
      job.setLong("test.randomwrite.bytes_per_map", totalBytesToWrite);
    }
    
    job.setNumMapTasks(numMaps);
    System.out.println("Running " + numMaps + " maps.");
    
    // reducer NONE
    job.setNumReduceTasks(0);
    
    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(job);
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " + 
                       (endTime.getTime() - startTime.getTime()) /1000 + 
                       " seconds.");
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RandomTextKeyValueWriter(), args);
    System.exit(res);
  }

}
