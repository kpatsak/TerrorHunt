package surfsara.java.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordFrequencyReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	/**
	 * @param key
	 *            is the key of the mapper
	 * @param values
	 *            are all the values aggregated during the mapping phase
	 * @param context
	 *            contains the context of the job run
	 */
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		// write the key and the adjusted value (removing the last comma)
		context.write(key, new LongWritable(sum));
	}
}
