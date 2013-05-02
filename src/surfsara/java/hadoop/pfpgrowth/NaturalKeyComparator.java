package surfsara.java.hadoop.pfpgrowth;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import surfsara.java.hadoop.pfpgrowth.convertors.PatternPair;

public class NaturalKeyComparator extends WritableComparator {

	protected NaturalKeyComparator() {
		super(PatternPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		PatternPair k1 = (PatternPair) w1;
		PatternPair k2 = (PatternPair) w2;
		return k1.getFirst().compareTo(k2.getFirst());
	}
}