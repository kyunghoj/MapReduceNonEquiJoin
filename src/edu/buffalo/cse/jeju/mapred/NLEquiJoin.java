/**
 * 
 */
package edu.buffalo.cse.jeju.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author kyunghoj
 *
 */
public class NLEquiJoin {

	private static String L = "L";
	private static String R = "R";
	
	public static class NLEquiJoinMapper 
		extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
		}
		
		public void map(Text key, Text values, Context context) 
			throws IOException, InterruptedException{
			
			String stringKey = key.toString();
			Configuration conf = context.getConfiguration();
			String inputfile = conf.get("map.input.file");
			
			if (inputfile.endsWith(L)) {
				Log.debug("");
			} else {inputfile.endsWith(R)
				Log.debug("inog")
			}
		}
	}
	
	public static class NLEquiJoinReducer 
		extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
		}
	}
	
	public static void main(String[] args) throws Exception {
		
	}
	
}
