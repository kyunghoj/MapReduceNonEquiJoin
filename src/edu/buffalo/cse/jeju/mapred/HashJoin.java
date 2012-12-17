
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author kyunghoj
 * 
 * A hash join, which is similar to sort-merge join
 * 
 */
public class HashJoin extends Configured implements Tool {

	private static Log LOG = LogFactory.getLog(HashJoin.class);
	
	private static String LEFT_TABLE = "LEFT_TABLE";
	private static String RIGHT_TABLE = "RIGHT_TABLE";
	private static String OUTPUT_TABLE = "OUTPUT_TABLE";

	private static boolean DEBUG = true;

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
				buffer.reset(b1, s1, l1);
				key1.readFields(buffer);

				buffer.reset(b2, s2, l2);
				key2.readFields(buffer);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			String str1 = key1.toString().split(":")[0];
			String str2 = key2.toString().split(":")[0];
			int hash1, hash2;
			hash1 = str1.hashCode();
			hash2 = str2.hashCode();

			LOG.debug("[GroupingComparator] Compare " + str1 + " to " + str2);
			return hash1 - hash2;
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

			String strKey = key.toString();
			String strVal = value.toString();

			LOG.debug("[Map] Input Line: " + line + " Map key = " + strKey);
			LOG.debug("[Map] input file path: " + inputFilePath.toString());

			Text taggedKey;
			Text taggedVal;
			
			if (inputfile.endsWith(leftTableFilePath.getName()) || 
			    inputFileDir.endsWith(leftTableFilePath.getName())) {
				taggedKey = new Text(strKey + ":L");
				taggedVal = new Text("L:" + strVal);
			} else if (inputfile.endsWith(rightTableFilePath.getName()) ||
					inputFileDir.endsWith(rightTableFilePath.getName())) {
				taggedKey = new Text(strKey + ":R");
				taggedVal = new Text("R:" + strVal);
			} else {
				taggedKey = null;
				taggedVal = null;
				LOG.error("[Map] Input filename does not match.");
			}
			
			context.write(taggedKey, taggedVal);
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
			LinkedList<String> rightRecordBuf = new LinkedList<String>();

			for (Text taggedRecord : records) {
				LOG.debug("[Reduce] key = " + key + " record = " + taggedRecord);
				String tag = taggedRecord.toString().split(":")[0];
				String record = taggedRecord.toString().split(":")[1];
				if (tag.equals("L")) {
					leftRecordBuf.add(record);
				} else {
					rightRecordBuf.add(record);
				}
			}

			for (String l_rec : leftRecordBuf) {
				for (String r_rec : rightRecordBuf) {
					context.write(null, new Text(l_rec + "," + r_rec));
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

		job.setJarByClass(RepartitionJoin.class);

		FileInputFormat.addInputPath(job, new Path(conf.get(LEFT_TABLE)));
		FileInputFormat.addInputPath(job, new Path(conf.get(RIGHT_TABLE)));
		FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_TABLE)));

		job.setMapperClass(HashJoinMapper.class);
		job.setReducerClass(HashJoinReducer.class);
		
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
