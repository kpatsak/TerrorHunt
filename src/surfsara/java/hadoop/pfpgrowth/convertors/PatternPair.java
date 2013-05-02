package surfsara.java.hadoop.pfpgrowth.convertors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PatternPair implements WritableComparable<PatternPair> {

	private Text key;
	private IntWritable value;

	public PatternPair(Text first, IntWritable second) {
		this.key = first;
		this.value = second;
	}

	public PatternPair() {
		this.key = new Text();
		this.value = new IntWritable();
	}

	public void set(Text first, IntWritable second) {
		this.key = first;
		this.value = second;
	}

	public Text getFirst() {
		return this.key;
	}

	public IntWritable getSecond() {
		return this.value;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		key.readFields(arg0);
		value.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		key.write(arg0);
		value.write(arg0);

	}

	@Override
	public int compareTo(PatternPair o) {
		int cmp = (value).compareTo(o.value);
		if (cmp != 0) {
			return cmp;
		}
		return key.compareTo(o.key);
	}

}
