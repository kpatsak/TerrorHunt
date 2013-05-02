package surfsara.java.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

	public TFIDFMapper() {
	}

	/**
	 * @param key
	 *            is the byte offset of the current line in the file;
	 * @param value
	 *            is the line from the file
	 * @param output
	 *            has the method "collect()" to output the key,value pair
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] wordAndCounters = value.toString().split("\t");
		String[] wordAndDoc = wordAndCounters[0].split("@");
		context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "^"
				+ wordAndCounters[1]));
	}
}
