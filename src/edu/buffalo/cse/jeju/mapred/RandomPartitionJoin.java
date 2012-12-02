package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RandomPartitionJoin extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(RandomPartitionJoin.class);
	private static String LEFT_TABLE = "LEFT_TABLE";
	private static String RIGHT_TABLE = "RIGHT_TABLE";
	private static String OUTPUT_TABLE = "OUTPUT_TABLE";
	private static String LEFT_PARTITION = "LEFT_PARTITION";
	private static String RIGHT_PARTITION = "RIGHT_PARTITION";
	
	private static boolean DEBUG = true;
	
	public static class RandomPartitioner 
		extends Partitioner<Text, Text> {
		
		public int getPartition(Text key, Text value, int numPartitions) {
			String tblTag = null;
			String regionStrKey = null;
			
			try {
				String [] strKeyArray = key.toString().split(":");
				regionStrKey = strKeyArray[0];
				tblTag = strKeyArray[1];
				
				LOG.debug("[Partitioner] Composite key split: " + strKeyArray[0] + ", " + strKeyArray[1]);
			} catch (NullPointerException npe) {
				LOG.error("[Partitioner] Composite key is not in the valid form (tag:join_key).");
				return -1;
			}
			
			return new Integer(regionStrKey);
			
			/*
			if (tblTag.equals("L")) {
				String [] strKeyArray = key.toString().split(":");
				String reduceId = strKeyArray[2];
				return new Integer(reduceId);
			} else {
				// randomly generate a partition number from 0 to numPartitions - 1
				int partitionNum = rand.nextInt(numPartitions);
				LOG.info("[Partitioner] random number: " + partitionNum + " numPartitions: " + numPartitions);
				return partitionNum;
			}
			*/
		}
	}
	
	public static class RandomPartitionJoinMapper 
	extends Mapper<Text, Text, Text, Text> {

		Path leftTableFilePath;
		Path rightTableFilePath;
		
		int numReducers;
		int numRows, numCols;
		
		Text taggedKey, taggedVal;
		
		Random rand;
		
		@Override
		protected void setup(Context context) {
			rand = new Random();
			Configuration conf = context.getConfiguration();
			leftTableFilePath = new Path(conf.get(LEFT_TABLE));
			rightTableFilePath = new Path(conf.get(RIGHT_TABLE));

			numReducers = conf.getInt("mapred.reduce.tasks", 1);
			numRows = conf.getInt(LEFT_PARTITION, 1);
			numCols = conf.getInt(RIGHT_PARTITION, 1);
			
			taggedKey = new Text();
			taggedVal = new Text();
			
			LOG.debug("[Setup] Left: " + leftTableFilePath.toString() +
					" Right: " + rightTableFilePath.toString());
		}

		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException {

			String inputfile = null;
			String inputFileDir = null;
			Path inputFilePath = null;

			InputSplit split = context.getInputSplit();

			if (split instanceof FileSplit) {
				FileSplit fsplit = (FileSplit) split;
				inputFilePath = fsplit.getPath();
				inputfile = fsplit.getPath().getName();
				inputFileDir = inputFilePath.getParent().getName();
			} else {
				LOG.debug("InputSplit is not FileSplit.");
				return;
			}

			// Convert key and value into strings
			String strKey = key.toString();
			String strVal = value.toString();

			LOG.info("[Map] Input Key: " + strKey + " Value = " + strVal);
			LOG.info("[Map] input file path: " + inputFilePath.toString());

			if (numReducers > numRows * numCols) {
				LOG.error("The number of possible partitions are more than the number of reducers.");
				return;
			}
			
			// if tuple is from Left Table 
			if (inputfile.endsWith(leftTableFilePath.getName()) || 
				inputFileDir.endsWith(leftTableFilePath.getName())) {	
				
				int matrixRow = rand.nextInt(numRows);
				
				LOG.info("[map] numRows: " + numRows + " matrixRow: " + matrixRow);

				taggedVal.set("L:" + strKey + "," + strVal);
				
				for (int col = 0; col < numCols; col++) {
					int region = matrixRow * numRows + col;
					String regionKey = region + ":L";
					taggedKey.set(regionKey);
					if (DEBUG) {
						LOG.info("intermediate key: " + regionKey);
					}
					context.write(taggedKey, taggedVal);
				}

			} else if (inputfile.endsWith(rightTableFilePath.getName()) ||
					inputFileDir.endsWith(rightTableFilePath.getName())) {

				int matrixCol = rand.nextInt(numCols);
				
				LOG.info("[map] numCols: " + numCols + " matrixCol: " + matrixCol);
				
				taggedVal.set("R:" + strKey + "," + strVal);
							
				for (int row = 0; row < numRows; row++) {
					int region = row * numRows + matrixCol;
					String regionKey = region + ":R";
					taggedKey.set(regionKey);
					if (DEBUG) {
						LOG.info("intermediate key: " + regionKey);
					}
					context.write(taggedKey, taggedVal);
				}
			} else {
				taggedKey = null;
				taggedVal = null;
				LOG.error("[Map] Input filename does not match.");
			}
		}

		@Override
		protected void cleanup(Context context) {
			leftTableFilePath = null;
			rightTableFilePath = null;
			
			taggedKey = taggedVal = null;
		}
	}
	
	public static class RandomPartitionJoinReducer 
		extends Reducer<Text, Text, Text, Text> {
		
		LinkedList<String> leftRecordBuf;
		
		@Override
		protected void setup(Context context) {
			leftRecordBuf = new LinkedList<String>();
		}
		
		@Override
		protected void cleanup(Context context) {
			leftRecordBuf = null;
		}
		
		public void reduce(Text key, Iterable<Text> records, Context context)
			throws IOException, InterruptedException {
			
			for (Text taggedRecord : records) {
				LOG.info("[Reduce] key = " + key + " record = " + taggedRecord);
				String tag = taggedRecord.toString().split(":")[0];
				String tuple = taggedRecord.toString().split(":")[1];
				if (tag.equals("L")) {
					leftRecordBuf.add(tuple);
				} else {
					for (String l_tuple : leftRecordBuf) { 
						// new "if" condition for theta join
						String l_key = l_tuple.split(",")[0];
						String r_key = tuple.split(",")[0];
						if (l_key.compareTo(r_key) < 0) {
							context.write(null, new Text(l_tuple + "," + tuple));
						}
					}
				}
			}
		}
	}
	
	public static class RandomPartitionJoinGroupingComparator extends WritableComparator {
		
		private final DataInputBuffer buffer;
		private final Text key1;
		private final Text key2;
		
		public RandomPartitionJoinGroupingComparator() {
			super(Text.class);
			
			buffer = new DataInputBuffer();
			key1 = new Text();
			key2 = new Text();
			
		}
		
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			try {
				buffer.reset(b1, s1, l1);
				key1.readFields(buffer);

				buffer.reset(b2, s2, l2);
				key2.readFields(buffer);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			String str1 = key1.toString().split(":")[0];
			String str2 = key2.toString().split(":")[0];
			LOG.info("[GroupingComparator] Compare " + str1 + " to " + str2);
			return str1.compareTo(str2);
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.printf("Usage: %s <Left_Table> <Right_Table> <Output_Table> <Num_of_Reducers> <L_Partitions> <R_Partitions>\n",
					getClass().getSimpleName());
			return -1;
		}
		
		int i = 0;
		int numOfReducers = 1;
		int numOfLParts = 1;
		int numOfRParts = 1;
		
		Configuration conf = new Configuration();
		
		conf.set(LEFT_TABLE, args[i++]);
		conf.set(RIGHT_TABLE, args[i++]);
		conf.set(OUTPUT_TABLE, args[i++]);
		
		numOfReducers = new Integer(args[i++]);
		
		// divides left table into numOfLParts parts
		// numOfRParts * numOfLParts = numOfReducers
		numOfLParts = new Integer(args[i++]);
		conf.setInt(LEFT_PARTITION, numOfLParts);
		
		numOfRParts = new Integer(args[i++]);
		conf.setInt(RIGHT_PARTITION, numOfRParts);
		
		if (args.length > 6) {
			conf.addResource(args[i++]);
		}
		
		if (DEBUG) {
			for (Entry<String, String> entry: conf) {
				System.err.printf("[Debug] %s=%s\n", entry.getKey(), entry.getValue());
			}
		}
		
		Job job = new Job(conf, "Random-partitioning-Join");
		
		job.setJarByClass(RepartitionJoin.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(LEFT_TABLE)));
		FileInputFormat.addInputPath(job, new Path(conf.get(RIGHT_TABLE)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_TABLE)));
		
		job.setMapperClass(RandomPartitionJoinMapper.class);
		
		job.setReducerClass(RandomPartitionJoinReducer.class);
		job.setPartitionerClass(RandomPartitioner.class);
		job.setGroupingComparatorClass(RandomPartitionJoinGroupingComparator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		
		job.setNumReduceTasks(numOfReducers);
		
		Date startTime = new Date();
	    System.out.println("Job started: " + startTime);
	    boolean ret = job.waitForCompletion(true);
	    Date endTime = new Date();
	    System.out.println("Job ended: " + endTime);
	    System.out.println("The job took " + 
	                       (endTime.getTime() - startTime.getTime()) /1000 + 
	                       " seconds.");
		return ret ? 0 : 1;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new RandomPartitionJoin(), args);
		System.exit(exitCode);
	}

}
