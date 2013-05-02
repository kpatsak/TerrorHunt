package surfsara.java.hadoop.tfidf;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Parameters;
import org.commoncrawl.hadoop.io.mapreduce.ARCFileItemInputFormat;

public class TFIDFDriver {

	protected static Logger log = Logger.getLogger(TFIDFDriver.class);;
	public static final String FILEFILTER = "arc.gz";
	public static final String WORD_FREQ = "word_freq";
	public static final String WORD_COUNT_DOC = "word_count_doc";
	public static final String TFIDF = "tfidf";
	public static final String RECORDS_IN = "RECORDS_IN";

	public static enum MAPPERCOUNTER {
		INTERESTING_PAGES, INTERESTING_PAGES_SIZE, PAGES_SIZE, RECORDS_IN, NULL_PAGES, HTML, NOTHTML, EXCEPTIONS, WORD_FREQ, NOEXCEPTION, EMPTYHANDLER, NUM_FILES, REDUCE
	}

	public void runTFIDF(Parameters params, org.slf4j.Logger log,
			Configuration conf) throws Exception {

		long numDocs = WordFrequency(params, conf);

		// log.info("Number of Docs" + numDocs);

		WordCountDoc(params, conf);

		HadoopUtil.delete(conf, new Path(params.get("output"), WORD_FREQ));

		TFIDFCalc(params, conf, numDocs);

		HadoopUtil.delete(conf, new Path(params.get("output"), WORD_COUNT_DOC));
	}

	public long WordFrequency(Parameters params, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {
		conf.setLong("mapred.task.timeout", 3600 * 60 * 60);
		// conf.setLong("numDocs", 0);
		Job job = new Job(conf);
		job.setJarByClass(TFIDFDriver.class);

		Path inputPath = new Path(params.get("input"));

		Path outputPath = new Path(params.get("output"), WORD_FREQ);

		HadoopUtil.delete(conf, outputPath);

		// Scan the provided input path for ARC files.
		log.info("setting input path to '" + inputPath + "'");
		SampleFilter.setFilter(FILEFILTER);
		FileInputFormat.addInputPath(job, inputPath);
		FileInputFormat.setInputPathFilter(job, SampleFilter.class);

		// Set the path where final output 'part' files will be saved.
		log.info("setting output path to '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, outputPath);

		// TODO create variable for compressed output
		FileOutputFormat.setCompressOutput(job, false);

		// Set which InputFormat class to use.
		job.setInputFormatClass(ARCFileItemInputFormat.class);

		// Set which OutputFormat class to use.
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the output data types.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(WordFrequencyMapper.class);
		job.setReducerClass(WordFrequencyReducer.class);
		job.setCombinerClass(WordFrequencyReducer.class);

		// job.getConfiguration().setInt("numDocs", numberofDocs);

		// Set the name of the job.
		job.setJobName("Sample Job");

		job.waitForCompletion(true);

		// FileSystem fs = inputPath.getFileSystem(conf);
		// FileStatus[] stat = fs.listStatus(inputPath);

		long numDocs = job.getCounters()
				.findCounter(MAPPERCOUNTER.INTERESTING_PAGES).getValue();

		return numDocs;
	}

	public void WordCountDoc(Parameters params, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException {

		conf.setLong("mapred.task.timeout", 3600 * 60 * 60);
		Job job = new Job(conf);
		job.setJarByClass(TFIDFDriver.class);

		Path inputPath = new Path(params.get("output"), WORD_FREQ);

		Path outputPath = new Path(params.get("output"), WORD_COUNT_DOC);

		// Scan the provided input path for ARC files.
		log.info("setting input path to '" + inputPath + "'");
		// SampleFilter.setFilter(FILEFILTER);
		FileInputFormat.addInputPath(job, inputPath);
		// FileInputFormat.setInputPathFilter(job, SampleFilter.class);

		// Set the path where final output 'part' files will be saved.
		log.info("setting output path to '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, outputPath);

		// TODO create variable for compressed output
		FileOutputFormat.setCompressOutput(job, false);

		// Set which InputFormat class to use.
		job.setInputFormatClass(TextInputFormat.class);

		// Set which OutputFormat class to use.
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the output data types.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(WordCountDocMapper.class);
		job.setReducerClass(WordCountDocReducer.class);

		// conf.getLong(arg0, arg1)

		// Set the name of the job.
		job.setJobName("Sample Job");

		job.waitForCompletion(true);

	}

	private void TFIDFCalc(Parameters params, Configuration conf, long numDocs)
			throws IOException, InterruptedException, ClassNotFoundException {
		conf.setLong("mapred.task.timeout", 3600 * 60 * 60);
		conf.setLong("numDocs", numDocs);
		Job job = new Job(conf);
		job.setJarByClass(TFIDFDriver.class);

		Path inputPath = new Path(params.get("output"), WORD_COUNT_DOC);

		Path outputPath = new Path(params.get("output"), TFIDF);

		HadoopUtil.delete(conf, outputPath);

		// Scan the provided input path for ARC files.
		log.info("setting input path to '" + inputPath + "'");
		FileInputFormat.addInputPath(job, inputPath);

		// Set the path where final output 'part' files will be saved.
		log.info("setting output path to '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, outputPath);

		// TODO create variable for compressed output
		FileOutputFormat.setCompressOutput(job, false);

		// Set which InputFormat class to use.
		job.setInputFormatClass(TextInputFormat.class);

		// Set which OutputFormat class to use.
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the output data types.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(TFIDFMapper.class);
		job.setReducerClass(TFIDFReducer.class);

		// conf.setInt("docsInCorpus", stat.length);
		// job.getConfiguration().setInt("numDocs", numberofDocs);
		//
		// log.info("NUMBER OF DOCS " + numberofDocs);

		// Set the name of the job.
		job.setJobName("Sample Job");

		job.waitForCompletion(true);

	}
}
