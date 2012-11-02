package edu.buffalo.cse.jeju.mapred;

import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

	@Override
	public int getPartition(KEY arg0, VALUE arg1, int numPartitions) {
		// 0. KEY must implement a method that splits its table tag part 
		//    and the others
		// 1. Separate a table tag from actual join attributes
		// 2. Compute a hash value from the actual join attributes
		// 3. Return (the hash value % numPartitions)
		return 0;
	}

}
