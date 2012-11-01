/**
 * 
 */
package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;
import java.util.HashMap;
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
 * This is essential a hash-join with an adaptation to Map-Reduce framework.
 */
public class HashJoin extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(HashJoin.class);
	
	public static class HashJoinPartitioner 
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
		
		public static final Log LOG = LogFactory.getLog(JoinMapper.class);
		Path leftTableFilePath;
		Path rightTableFilePath;
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			leftTableFilePath = new Path(conf.get(LEFT_TABLE));
			rightTableFilePath = new Path(conf.get(RIGHT_TABLE));
			if (DEBUG) {
				System.err.printf("left: %s,  right: %s\n", leftTableFilePath.toString(), rightTableFilePath.toString());
			}
	
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
				LOG.info("InputSplit is not FileSplit.");
				return;
			}
			
			String line = values.toString();
			if (DEBUG) {
				System.err.printf("[Debug] Input line = %s\n", line);
			}
			
			String strKey = line.split(":")[0];
			String strVals = line.split(":")[1];
			
			if (DEBUG) {
				System.err.printf("[Debug] map key = %s, map val = %s\n", strKey, strVals);
			}
			
			String mapOutKey = null;
			
			LOG.info("input file path: " + inputFilePath.toString());
			
			
			if (inputfile.endsWith(leftTableFilePath.getName())) {
				LOG.info("Processing Table L");
				mapOutKey = "L:" + strKey;
			} else if (inputfile.endsWith(rightTableFilePath.getName())) {
				LOG.info("Processing Table R");
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
		
		private LinkedList<Text> L;
		private LinkedList<Text> R;
		
		@Override
		protected void setup(Context context) {
			// 0. do some initialization
			// 1. Initialize two block buffers, L and R.
		}
	}
	
	public static class HashJoinReducer 
		extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, String> leftRowsBuffer = null;
		
		@Override
		protected void setup(Context context) {
			leftRowsBuffer = new HashMap<String, String>();
		}
		
		public void reduce(Text key, Iterable<Text> rows, Context context)
			throws IOException, InterruptedException {
			String srcTbl  = key.toString().split(":")[0];
			String joinKey = key.toString().split(":")[1];
			boolean isLTable = srcTbl.matches("L");
			
			for (Text row : rows) {
				if (isLTable) {
					String fullRowString = new String(joinKey+":"+row.toString());
					leftRowsBuffer.put(joinKey, fullRowString);
				} else {
					if (leftRowsBuffer.containsKey(joinKey)) {
						String rowString = leftRowsBuffer.get(joinKey);
						
						String result = joinKey + ",<" + 
								leftRowsBuffer.get(joinKey) + ">, <" + 
								row.toString() + ">";
						
						context.write(null, new Text(result));
					}
				}
			}
			
		}
	}
	
	private static String LEFT_TABLE = "LEFT_TABLE";
	private static String RIGHT_TABLE = "RIGHT_TABLE";
	private static String OUTPUT_TABLE = "OUTPUT_TABLE";
	
	private static boolean DEBUG = true;

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
		
		Job job = new Job(conf, "Repartition Join");
		
		job.setJarByClass(HashJoin.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(LEFT_TABLE)));
		FileInputFormat.addInputPath(job, new Path(conf.get(RIGHT_TABLE)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_TABLE)));
		
		/*
		LocalFileSystem lfs = new LocalFileSystem();
		if (lfs.exists(new Path(conf.get(OUTPUT_TABLE))))
			lfs.delete(new Path(conf.get(OUTPUT_TABLE)), true);
		lfs.close();
		*/
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(HashJoinReducer.class);
		//job.setGroupingComparatorClass(cls);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HashJoin(), args);
		System.exit(exitCode);
	}
	
}