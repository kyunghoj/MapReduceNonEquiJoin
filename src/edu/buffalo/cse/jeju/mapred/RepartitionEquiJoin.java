/**
 * 
 */
package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;


/**
 * @author kyunghoj
 *
 */
public class RepartitionEquiJoin extends Configured implements Tool {

	// left table "L"'s filename
	private static String L = "L";
	// right table "R"'s filename
	private static String R = "R";
    
	private static final Log LOG = LogFactory.getLog(RepartitionEquiJoin.class);
	
	public static class Repartitioner 
		extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int numPartitions) {
			String joinKey = null;
			try {
				String strKey = key.toString();
				joinKey = strKey.split(":")[1];
				LOG.debug("Composite key split: " + strKey.split(":")[0] + ", " + strKey.split(":")[1]);
			} catch (NullPointerException npe) {
				return 0;
			}
			return joinKey.hashCode() % numPartitions;
		}
	}
	
	public static class RepartitionEquiJoinMapper 
		extends Mapper<Text, Text, Text, Text> {
		
		public static final Log LOG = LogFactory.getLog(RepartitionEquiJoinMapper.class);
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
		}
		
		public void map(Text key, Text values, Context context) 
			throws IOException, InterruptedException {
			
			String inputfile = null;
			InputSplit split = context.getInputSplit();
			
			if (split instanceof FileSplit) {
				FileSplit fsplit = (FileSplit) split;
				inputfile = fsplit.getPath().getName();
			} else {
				LOG.info("InputSplit is not FileSplit.");
				return;
			}
			
			String stringKey = key.toString();
			Configuration conf = context.getConfiguration();
			
			String mapOutKey = null;
			
			if (inputfile.endsWith(L)) {
				LOG.debug("Table L");
				mapOutKey = "L:" + stringKey;
			} else if (inputfile.endsWith(R)) {
				LOG.debug("Table R");
				mapOutKey = "R:" + stringKey;
			}
			
			// do not filter. 
			// assume we need all the columns from both tables.
			context.write(new Text(mapOutKey), values);
		}
	}
	
	public static class RepartitionEquiJoinReducer 
		extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, String> leftRowsBuffer = null;
		
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			leftRowsBuffer = new HashMap<String, String>();
		}
		
		public void reduce(Text key, Iterable<Text> rows, Context context)
			throws IOException, InterruptedException {
			String srcTbl  = key.toString().split(":")[0];
			String joinKey = key.toString().split(":")[1];
			boolean isLTable = srcTbl.matches("L");
			
			for (Text row : rows) {
				if (isLTable) {
					leftRowsBuffer.put(joinKey, row.toString());
				} else {
					if (leftRowsBuffer.containsKey(joinKey)) {
						String result = joinKey + ",<" + 
								leftRowsBuffer.get(joinKey) + ">, <" + 
								row.toString() + ">";
						
						context.write(null, new Text(result));
					} else {
						String result = joinKey + ", <null>, " + "<" + row.toString() + ">"; 
						context.write(null, new Text(result));
					}
				}
			}
			
		}
	}
	
	private static String LEFT_TABLE = "LEFT_TABLE";
	private static String RIGHT_TABLE = "RIGHT_TABLE";
	
	private static boolean DEBUG = true;

	public int run(String[] args) throws Exception {
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
	
		
		if (args.length < 2) {
			System.err.println("Usage: RepartitionEquiJoin Left_Table Right_Table [Configuration file]");
			return;
		}
		
		int i = 0;
		Configuration conf = new Configuration();
		
		conf.set(LEFT_TABLE, args[i++]);
		conf.set(RIGHT_TABLE, args[i++]);
		if (args.length > 2) {
			conf.addResource(args[i++]);
		}
		
		if (DEBUG) {
			for (Entry<String, String> entry: conf) {
				System.err.printf("[Debug] %s=%s\n", entry.getKey(), entry.getValue());
			}
		}
		
		Job job = new Job(conf, "RepartitionJoin");
		
		job.setJarByClass(RepartitionEquiJoin.class);
		
	}
}