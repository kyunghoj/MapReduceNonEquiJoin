/**
 * 
 */
package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
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
	
	public static class HashJoinPartitioner 
		extends Partitioner<Text, Text> {
		
		public int getPartition(Text key, Text value, int numPartitions) {
			String joinKey = null;
			
			try {
				String [] strKeyArray = key.toString().split(":");
				joinKey = strKeyArray[0];
				LOG.debug("[Partitioner] Composite key split: " + strKeyArray[0] + ", " + strKeyArray[1]);
			} catch (NullPointerException npe) {
				LOG.error("[Partitioner] Composite key is not in the valid form (tag:join_key).");
				return 0;
			}
			
			return joinKey.hashCode() % numPartitions;
		}
	}
	
	// the default comparator for Text will lexicographically sort map output.
	// thus, it's okay to use the default one, since my map output keys are
	// int the form of {L|R}:join_key:attributes
	// leave this method just in case we need to implement a compator
	
	public static class HashJoinGroupingComparator extends WritableComparator {
		
		private final DataInputBuffer buffer;
		private final Text key1;
		private final Text key2;
		
		public HashJoinGroupingComparator() {
			super(Text.class);
			buffer = new DataInputBuffer();
			key1 = new Text();
			key2 = new Text();
		}
		
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				buffer.reset(b1, s1, l1);                   // parse key1
				key1.readFields(buffer);

				buffer.reset(b2, s2, l2);                   // parse key2
				key2.readFields(buffer);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			String str1 = key1.toString().split(":")[0];
			String str2 = key2.toString().split(":")[0];
			LOG.debug("[GroupingComparator] Compare " + str1 + " to " + str2);
			return str1.compareTo(str2);
		}
	}
	
	public static class HashJoinMapper 
		extends Mapper<LongWritable, Text, Text, Text> {
		
		Path leftTableFilePath;
		Path rightTableFilePath;
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			leftTableFilePath = new Path(conf.get(LEFT_TABLE));
			rightTableFilePath = new Path(conf.get(RIGHT_TABLE));

			LOG.debug("[Setup] Left: " + leftTableFilePath.toString() +
					" Right: " + rightTableFilePath.toString());
	
		}
		
		public void map(LongWritable key, Text value, Context context) 
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
				
			// Extract the join attributes from value
			// let's assume key and value are separated by a comma
			String line = value.toString();
			String strKey = line.split(",")[0];
				
			LOG.debug("[Map] Input Line: " + line + " Map key = " + strKey);
			LOG.debug("[Map] input file path: " + inputFilePath.toString());
			
			Text taggedKey;
			Text taggedRecord;
			
			if (inputfile.endsWith(leftTableFilePath.getName())) {
				taggedKey = new Text(strKey + ":L");
				taggedRecord = new Text("L:" + line);
			} else if (inputfile.endsWith(rightTableFilePath.getName())) {
				taggedKey = new Text(strKey + ":R");
				taggedRecord = new Text("R:" + line);
			} else {
				taggedKey = null;
				taggedRecord = null;
				LOG.error("[Map] Input filename does not match.");
			}
			
			context.write(taggedKey, taggedRecord);
		}
	
		@Override
		protected void cleanup(Context context) {
			leftTableFilePath = null;
			rightTableFilePath = null;
		}
	}
	
	public static class HashJoinReducer 
		extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void setup(Context context) {
			LOG.debug("Reducer setup() called.");
		}

		@Override
		protected void cleanup(Context context) {
			LOG.debug("Reducer cleanup() called.");
		}

		public void reduce(Text key, Iterable<Text> records, Context context)
				throws IOException, InterruptedException {

			LinkedList<String> leftRecordBuf = new LinkedList<String>();
			
			for (Text taggedRecord : records) {
				LOG.debug("[Reduce] key = " + key + " record = " + taggedRecord);
				String tag = taggedRecord.toString().split(":")[0];
				String record = taggedRecord.toString().split(":")[1];
				if (tag.equals("L")) {
					leftRecordBuf.add(record);
				} else {
					for (String l_record : leftRecordBuf) { 
						context.write(null, new Text(l_record + "," + record));
					}
				}
			}
		}

	}
	public static class NLJoinMapper 
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
			LOG.debug("input file path: " + inputFilePath.toString());
			
			String mapOutVals = null;
			if (inputfile.endsWith(leftTableFilePath.getName())) {
				mapOutVals = "L:" + strVals;
			} else if (inputfile.endsWith(rightTableFilePath.getName())) {
				mapOutVals = "R:" + strKey;
			}
			

			// do not filter. 
			// assume we need all the columns from both tables.
			// values is in a form of "key:values"
			context.write(new Text(strKey), new Text(mapOutVals));
		}
	}
	
	public static class NLJoinReducer 
		extends Reducer<Text, Text, Text, Text> {
		
		//private static final Log LOG = LogFactory.getLog(BlockNestedLoopJoinReducer.class);
		// need to change to other data structure
		private LinkedList<Text> L; 
		private LinkedList<Text> R;
		//private int sizeBlockBuffer = 0;
		
		@Override
		protected void setup(Context context) {
			
			LOG.debug("setup() finished.");
		}
		
		@Override
		protected void cleanup(Context context) {

			
			L.clear(); L = null;
			R.clear(); R = null;

			LOG.debug("cleanup() finished.");
		}
		
		public void reduce(Text key, Iterable<Text> rows, Context context) 
			throws IOException, InterruptedException {
			
			LOG.debug("Reduce Key: " + key);
			
			// 1. Initialize two block buffers, L and R.
			L = new LinkedList<Text>();
			R = new LinkedList<Text>();
						
			// load all the tuples from both relations into memory
			// yes, it's not a good idea.

			
			for (Text row : rows) {	
				//String [] compositeKeyArray = row.toString().split(":");
				//String tableTag = compositeKeyArray[0];
				String tableTag = row.toString().split(":")[0];
				
				//String joinKey = compositeKeyArray[1];
				boolean isLTable = tableTag.equals("L");
				if (isLTable) {
					L.add(row);
					LOG.debug("L: " + row + " added.");
				} else {
					R.add(row);
					LOG.debug("R: " + row + " added.");
				}
			}
			
			for (Text l_tuple : L) {
				String l_row = l_tuple.toString();
				String [] l_row_array = l_row.split(":");
				String l_join_key = l_row_array[0];
				LOG.debug("L's tuple: " + l_row + " join key: " + l_join_key);
				
				for (Text r_tuple : R) {
					String r_row = r_tuple.toString();
					String [] r_row_array = r_row.split(":");
					String r_join_key = r_row_array[0];
					LOG.debug("R's tuple: " + r_row + " join key: " + r_join_key);
					if (l_join_key.equals(r_join_key)) {
						LOG.debug("Join key matches: " + l_tuple + ", " + r_tuple);
						Text result = new Text(l_tuple + ", " + r_tuple);
						
						try {
							context.write(null, result);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
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
		
		job.setMapperClass(HashJoinMapper.class);
		
		job.setReducerClass(HashJoinReducer.class);
		job.setPartitionerClass(HashJoinPartitioner.class);
		job.setGroupingComparatorClass(HashJoinGroupingComparator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HashJoin(), args);
		System.exit(exitCode);
	}
	
}
