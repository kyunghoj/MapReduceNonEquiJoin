/**
 * 
 */
package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author kyunghoj
 * 
 * A hash-join with some adaptations to Map-Reduce framework.
 */
public class HashJoin extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(HashJoin.class);
	private static String LEFT_TABLE = "LEFT_TABLE";
	private static String RIGHT_TABLE = "RIGHT_TABLE";
	private static String OUTPUT_TABLE = "OUTPUT_TABLE";
	
	private static boolean DEBUG = true;
	
	public static class JoinPartitioner 
		extends Partitioner<Text, Text> {
		
		public int getPartition(Text key, Text value, int numPartitions) {
			String joinKey = null;
			
			try {
				String [] strKeyArray = key.toString().split(":");
				joinKey = strKeyArray[1];
				LOG.debug("Composite key split: " + strKeyArray[0] + ", " + strKeyArray[1]);
			} catch (NullPointerException npe) {
				return 0;
			}
			
			// Because of the modulo operation (% numPartitions), 
			// it is possible that non-equal keys go to the same reducer. 
			return joinKey.hashCode() % numPartitions;
		}
	}
	
	// the default comparator for Text will lexicographically sort map output.
	// thus, it's okay to use the default one, since my map output keys are
	// int the form of {L|R}:join_key:attributes
	
	public static class HashJoinKeyComparator extends WritableComparator {
		public HashJoinKeyComparator() {
			super(Text.class);
		}
		
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return 0;
		}
	}
	
	public static class JoinMapper 
		extends Mapper<LongWritable, Text, Text, Text> {
		
		Path leftTableFilePath;
		Path rightTableFilePath;
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			leftTableFilePath = new Path(conf.get(LEFT_TABLE));
			rightTableFilePath = new Path(conf.get(RIGHT_TABLE));

			LOG.debug("Left: " + leftTableFilePath.toString() +
					" Right: " + rightTableFilePath.toString());
	
		}
		
		public void map(LongWritable key, Text values, Context context) 
			throws IOException, InterruptedException {
			
			String inputfile = null;
			Path inputFilePath = null;
			
			InputSplit split = context.getInputSplit();
			
			if (split instanceof FileSplit) {
				FileSplit fsplit = (FileSplit) split;
				inputFilePath = fsplit.getPath();
				inputfile = fsplit.getPath().getName();
			} else {
				LOG.debug("InputSplit is not FileSplit.");
				return;
			}
			
			String line = values.toString();
			LOG.debug("Input Line: " + line);
			
			String strKey = line.split(":")[0];
			String strVals = line.split(":")[1];
			
			LOG.debug("Map key = " + strKey + " Map val = " + strVals);
			
			String mapOutKey = null;
			
			LOG.debug("input file path: " + inputFilePath.toString());
			
			if (inputfile.endsWith(leftTableFilePath.getName())) {
				mapOutKey = "L:" + strKey;
			} else if (inputfile.endsWith(rightTableFilePath.getName())) {
				mapOutKey = "R:" + strKey;
			}
			
			// do not filter. 
			// assume we need all the columns from both tables.
			// values is in a form of "key:values"
			context.write(new Text(mapOutKey), values);
		}
	}
	
	public static class BlockNestedLoopJoinReducer 
		extends Reducer<Text, Text, Text, Text> {
		
		// need to change to other data structure
		private LinkedList<Text> L; 
		private LinkedList<Text> R;
		private int sizeBlockBuffer = 0;
		
		@Override
		protected void setup(Context context) {
			// 0. do some initialization
			Configuration conf = context.getConfiguration();
			// size of each block in megabytes
			sizeBlockBuffer = conf.getInt("BLOCK_SIZE_MB", 50);
			
			// 1. Initialize two block buffers, L and R.
			L = new LinkedList<Text>();
			R = new LinkedList<Text>();
		}
		
		public void reduce(Text key, Iterable<Text> rows, Context context) 
			throws IOException, InterruptedException {
			
			Iterator<Text> it = rows.iterator();
			
			for (Text row : rows) {
				String [] compositeKeyArray = key.toString().split(":");
				String tableTag = compositeKeyArray[0];
				String joinKey = compositeKeyArray[1];
				boolean isLTable = tableTag.equals("L");
				
				if (isLTable) {
					L.add(row);
				} else {
					R.add(row);
				}
				// it's a completely wrong design. to implement 
				// (block) nested loop join, two iterators are required
				// questions: 
				// 1) any other algorithm for equi-join?
				// 2) when use (block) nested loop join?
				
			}
		}
	}
	
	public static class HashJoinReducer 
		extends Reducer<Text, Text, Text, Text> {
		
		// Hashtable for matching tuples from relations
		private HashMap<String, LinkedList<String>> leftRowsBuffer = null;
		
		@Override
		protected void setup(Context context) {
			LOG.debug("Reducer setup() called.");
			leftRowsBuffer = new HashMap<String, LinkedList<String>>();
		}
		
		@Override
		protected void cleanup(Context context) {
			LOG.debug("Reducer cleanup() called.");
			leftRowsBuffer = null;
		}
		
		public void reduce(Text key, Iterable<Text> rows, Context context)
				throws IOException, InterruptedException {

			for (Text row : rows) {
				String srcTbl  = row.toString().split(":")[0];
				String joinKey = row.toString().split(":")[1];
				boolean isLTable = srcTbl.equals("L");
				LOG.debug("Table: " + srcTbl + " Key: " + joinKey);
				
				if (isLTable) {
					//String fullRowString = new String(joinKey + ":" + row.toString());
					if (leftRowsBuffer.containsKey(joinKey)) {
						leftRowsBuffer.get(joinKey).push(row.toString());
					} else {
						LinkedList<String> list = new LinkedList<String>();
						list.push(row.toString());
						leftRowsBuffer.put(joinKey, list);
						LOG.debug("Tuple: " + row.toString() + " is added to the hash table.");
					}
				} else {
					if (leftRowsBuffer.containsKey(joinKey)) {
						LinkedList<String> tuplesList = leftRowsBuffer.get(joinKey);

						for (String tuple : tuplesList) { 
							LOG.debug("Tuple: " + tuple + " Join key: " + joinKey + " matches? " + tuple.split(":")[0].equals(joinKey));
							if (tuple.split(":")[0].equals(joinKey)) {
								String result = tuple + ", " + row.toString();
								context.write(null, new Text(result));
							}
						}
					}
				}
			}
		}
		
	}
	
	

	public int run(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.printf("Usage: %s <Left_Table> <Right_Table> <Output_Table> [Configuration file]\n",
					getClass().getSimpleName());
			return -1;
		}
		
		int i = 0;
		Configuration conf = new Configuration();
		
		conf.set(LEFT_TABLE, args[i++]);
		conf.set(RIGHT_TABLE, args[i++]);
		conf.set(OUTPUT_TABLE, args[i++]);
		
		if (args.length > 3) {
			conf.addResource(args[i++]);
		}
		
		if (DEBUG) {
			for (Entry<String, String> entry: conf) {
				System.err.printf("[Debug] %s=%s\n", entry.getKey(), entry.getValue());
			}
		}
		
		Job job = new Job(conf, "HashJoin");
		
		job.setJarByClass(HashJoin.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(LEFT_TABLE)));
		FileInputFormat.addInputPath(job, new Path(conf.get(RIGHT_TABLE)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_TABLE)));
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(HashJoinReducer.class);
		job.setPartitionerClass(JoinPartitioner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HashJoin(), args);
		System.exit(exitCode);
	}
	
}
