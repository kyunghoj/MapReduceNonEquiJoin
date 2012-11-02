
package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

	private static final Log LOG = LogFactory.getLog(HashJoin.class);
	private static String LEFT_TABLE = "LEFT_TABLE";
	private static String RIGHT_TABLE = "RIGHT_TABLE";
	private static String OUTPUT_TABLE = "OUTPUT_TABLE";

	private static boolean DEBUG = true;

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

			Text joinKey = new Text(strKey);
			Text taggedRecord;

			if (inputfile.endsWith(leftTableFilePath.getName())) {
				taggedRecord = new Text("L:" + line);
			} else if (inputfile.endsWith(rightTableFilePath.getName())) {
				taggedRecord = new Text("R:" + line);
			} else {
				taggedRecord = null;
				LOG.error("[Map] Input filename does not match.");
			}

			context.write(joinKey, taggedRecord);
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

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HashJoin(), args);
		System.exit(exitCode);
	}

}