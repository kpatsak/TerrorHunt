package surfsara.java.hadoop.pfpgrowth;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import surfsara.java.hadoop.pfpgrowth.convertors.PatternPair;

public class NaturalKeyPartitioner extends Partitioner<PatternPair, Text> {

	@Override
	public int getPartition(PatternPair key, Text arg1, int numPartitions) {
		// int hash = key.getFirst().hashCode();
		// int partition = hash % numPartitions;
		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		// return partition;
	}

}
