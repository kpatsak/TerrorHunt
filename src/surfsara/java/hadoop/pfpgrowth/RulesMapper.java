package surfsara.java.hadoop.pfpgrowth;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

import surfsara.java.hadoop.pfpgrowth.convertors.PatternPair;

public class RulesMapper extends
		Mapper<Text, TopKStringPatterns, PatternPair, Text> {

	public String ArraytoString(String[] array) {
		StringBuilder builder = new StringBuilder();
		int ctl = 0;
		for (String s : array) {
			if (ctl != 0) {
				builder.append(" ");
			}
			builder.append(s);
			ctl++;
		}
		return builder.toString();
	}

	@Override
	protected void map(Text feature, TopKStringPatterns patterns,
			Context context) throws IOException, InterruptedException {
		Iterator<Pair<List<String>, Long>> piterator = patterns.iterator();

		do {
			Pair<List<String>, Long> pair = piterator.next();
			String[] pattern_array = pair.getFirst().toArray(
					new String[pair.getFirst().size()]);
			context.write(
					new PatternPair(new Text(ArraytoString(pattern_array)),
							new IntWritable(0)), new Text(pair.getSecond()
							.toString()));
			for (int i = pattern_array.length - 1; i >= 0; i--) {
				if (pattern_array.length == 1) {
					break;
				}
				String[] subpattern = new String[pattern_array.length - 1];
				System.arraycopy(pattern_array, 0, subpattern, 0, i);
				System.arraycopy(pattern_array, i + 1, subpattern, i,
						pattern_array.length - i - 1);
				// System.out.println("Mapper++++++");
				// System.out.println(ArraytoString(subpattern));
				// System.out.println(ArraytoString(pattern_array));
				context.write(new PatternPair(new Text(
						ArraytoString(subpattern)), new IntWritable(1)),
						new Text(ArraytoString(pattern_array).concat("+")
								.concat(pair.getSecond().toString())));
			}
		} while (piterator.hasNext());

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

	}
}
