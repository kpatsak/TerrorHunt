package surfsara.java.hadoop.pfpgrowth;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import surfsara.java.hadoop.pfpgrowth.convertors.PatternPair;

public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(PatternPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		PatternPair k1 = (PatternPair) w1;
		PatternPair k2 = (PatternPair) w2;
		int result = k1.getSecond().compareTo(k2.getSecond());
		if (0 == result) {
			result = -1 * k1.getSecond().compareTo(k2.getSecond());
		}
		return result;
	}
}