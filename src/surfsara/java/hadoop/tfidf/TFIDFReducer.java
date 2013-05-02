package surfsara.java.hadoop.tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import surfsara.java.hadoop.tfidf.TFIDFDriver.MAPPERCOUNTER;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat DF = new DecimalFormat("###.########");
	private long numberOfDocumentsInCorpus;

	public TFIDFReducer() {
	}

	/**
	 * @param key
	 *            is the key of the mapper
	 * @param values
	 *            are all the values aggregated during the mapping phase
	 * @param context
	 *            contains the context of the job run
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// get the number of documents indirectly from the file-system (stored
		// in the job name on purpose)
		// int numberOfDocumentsInCorpus =
		// Integer.parseInt(context.getJobName());
		// context.getConfiguration().get("numDocs");

		// int numberOfDocumentsInCorpus = (int) context.getConfiguration()
		// .getLong("mapreduce.input.num.files", 1);

		// int numberOfDocumentsInCorpus = (int) context.getCounter(
		// MAPPERCOUNTER.RECORDS_IN).getValue();

		context.getCounter(MAPPERCOUNTER.NUM_FILES).increment(
				numberOfDocumentsInCorpus);
		context.getCounter(MAPPERCOUNTER.REDUCE).increment(1);
		// total frequency of this word
		int numberOfDocumentsInCorpusWhereKeyAppears = 0;
		Map<String, String> tempFrequencies = new HashMap<String, String>();
		for (Text val : values) {
			String[] documentAndFrequencies = val.toString().split("\\^");
			// if (documentAndFrequencies.length < 2) {
			// context.write(
			// new Text(String.valueOf(documentAndFrequencies.length)),
			// new Text(val.toString()));
			// continue;
			// }
			numberOfDocumentsInCorpusWhereKeyAppears++;
			tempFrequencies.put(documentAndFrequencies[0],
					documentAndFrequencies[1]);
		}
		for (String document : tempFrequencies.keySet()) {
			String[] wordFrequenceAndTotalWords = tempFrequencies.get(document)
					.split("/");

			// Term frequency is the quocient of the number of terms in document
			// and the total number of terms in doc
			double tf = Double.valueOf(Double
					.valueOf(wordFrequenceAndTotalWords[0])
					/ Double.valueOf(wordFrequenceAndTotalWords[1]));

			// interse document frequency quocient between the number of docs in
			// corpus and number of docs the term appears
			double idf = (double) numberOfDocumentsInCorpus
					/ (double) numberOfDocumentsInCorpusWhereKeyAppears;

			// given that log(10) = 0, just consider the term frequency in
			// documents
			double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ? tf
					: tf * Math.log10(idf);

			context.write(new Text(key + "@" + document), new Text("["
					+ numberOfDocumentsInCorpusWhereKeyAppears + "/"
					+ numberOfDocumentsInCorpus + " , "
					+ wordFrequenceAndTotalWords[0] + "/"
					+ wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf)
					+ "]"));

		}
	}

	@Override
	protected void setup(Context context) {
		numberOfDocumentsInCorpus = context.getConfiguration().getLong(
				"numDocs", 1);
	}
}
