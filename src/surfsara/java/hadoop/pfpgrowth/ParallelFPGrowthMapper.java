package surfsara.java.hadoop.pfpgrowth;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.text.BreakIterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import surfsara.java.constants.Terror;
import surfsara.java.hadoop.tfidf.TFIDFDriver.MAPPERCOUNTER;
import surfsara.java.utils.DocumentUtils;
import surfsara.java.utils.Sentence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.ImmutableBuffer;
import org.mortbay.log.Log;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;

public class ParallelFPGrowthMapper extends
		Mapper<Text, ArcFileItem, IntWritable, TransactionTree> {

	private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
	@SuppressWarnings("unused")
	private Pattern splitter;
	private int maxPerGroup;

	private IntWritable wGroupID = new IntWritable();

	@Override
	public void run(Context context) throws InterruptedException, IOException {
		setup(context);
		try {
			while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
			}
		} catch (Exception e) {
			// TODO Counter
			// e.printStackTrace();
		}
		cleanup(context);
	}

	protected void WordFrequencyinDoc(String doc, String DocId, Context context)
			throws IOException, InterruptedException {
		context.getCounter(MAPPERCOUNTER.WORD_FREQ).increment(1);
		Analyzer analysis = new StandardAnalyzer(Version.LUCENE_36);
		PorterStemFilter stemfilter = null;
		Terror lists = new Terror();
		BreakIterator iterator = BreakIterator.getSentenceInstance(Locale.US);
		iterator.setText(doc);
		int start = iterator.first();
		// List<Sentence> sentences = DocumentUtils.stringToSentences(doc);
		// StringBuilder valueBuilder = new StringBuilder();
		for (int end = iterator.next(); end != BreakIterator.DONE; start = end, end = iterator
				.next()) {
			String sentence = doc.substring(start, end);
			if (sentence.length() < 320) {
				sentence = sentence.replace("\n", "").replace("\r", "")
						.replace("\t", "");
				TokenStream stream = analysis.tokenStream(null,
						new StringReader(sentence));

				StopFilter filter = new StopFilter(Version.LUCENE_36, stream,
						StopFilter.makeStopSet(Version.LUCENE_36,
								lists.getStopWords(), true));
				stemfilter = new PorterStemFilter(filter);
				stemfilter.reset();
				OpenIntHashSet itemSet = new OpenIntHashSet();
				while (stemfilter.incrementToken()) {
					String current_word = stream.getAttribute(
							CharTermAttribute.class).toString();
					if (current_word.matches("^[a-z].*$")
							&& !current_word.contains(".")
							&& !current_word.contains(":")) {
						CharsetEncoder encoder = Charset.forName("US-ASCII")
								.newEncoder();
						if (!encoder.canEncode(current_word)) {
							continue;
						}
						if (fMap.containsKey(current_word)
								&& !current_word.trim().isEmpty()) {
							itemSet.add(fMap.get(current_word));
						}
						IntArrayList itemArr = new IntArrayList(itemSet.size());
						itemSet.keys(itemArr);
						itemArr.sort();

						OpenIntHashSet groups = new OpenIntHashSet();
						for (int j = itemArr.size() - 1; j >= 0; j--) {
							// generate group dependent shards
							int item = itemArr.get(j);
							int groupID = PFPGrowth.getGroup(item, maxPerGroup);

							if (!groups.contains(groupID)) {
								IntArrayList tempItems = new IntArrayList(j + 1);
								tempItems.addAllOfFromTo(itemArr, 0, j);
								context.setStatus("Parallel FPGrowth: Generating Group Dependent transactions for: "
										+ item);
								wGroupID.set(groupID);
								context.write(wGroupID, new TransactionTree(
										tempItems, 1L));
							}
							groups.add(groupID);
						}
					}
				}
			}
		}
		stemfilter.close();
		analysis.close();
	}

	@Override
	public void map(Text key, ArcFileItem value, Context context)
			throws java.io.IOException, InterruptedException {
		context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);

		Terror lists = new Terror();
		String mime = value.getMimeType();
		InputStream stream = null;

		if (!mime.equals("text/html")) {
			context.getCounter(MAPPERCOUNTER.NOTHTML).increment(1);
		} else {
			ContentHandler contenthandler = new BodyContentHandler();
			ImmutableBuffer buffer = value.getContent();
			stream = new ByteArrayInputStream(buffer.getReadOnlyBytes());
			Metadata metadata = new Metadata();
			context.getCounter(MAPPERCOUNTER.HTML).increment(1);

			try {
				new HtmlParser().parse(stream, contenthandler, metadata,
						new ParseContext());
			} catch (SAXException e) {
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				return;
			} catch (TikaException e) {
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				return;
			}

			String noHTMLString = contenthandler.toString().replaceAll(
					"\\<.*?\\>", "");
			String tempstring = noHTMLString.toLowerCase();
			context.getCounter(MAPPERCOUNTER.NOEXCEPTION).increment(1);
			context.getCounter(MAPPERCOUNTER.PAGES_SIZE).increment(
					tempstring.getBytes().length);
			LanguageIdentifier identifier = new LanguageIdentifier(noHTMLString);
			if (identifier.getLanguage().equals("en")
					|| identifier.getLanguage().equals("eng")
					|| identifier.getLanguage().equals("engs")) {
				for (String word : lists.getTerrorWords()) {
					String finalString = noHTMLString.toLowerCase();
					if (finalString.contains(word.toLowerCase())) {
						context.getCounter(MAPPERCOUNTER.INTERESTING_PAGES_SIZE)
								.increment(finalString.getBytes().length);
						context.getCounter(MAPPERCOUNTER.INTERESTING_PAGES)
								.increment(1);
						WordFrequencyinDoc(finalString.trim(), value.getUri(),
								context);
						// context.write(new IntWritable(1),
						// new Text(finalString.trim()));
						break;
					}
				}
			} else {
				context.getCounter(MAPPERCOUNTER.NULL_PAGES).increment(1);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		int i = 0;
		for (Pair<String, Long> e : PFPGrowth.readFList(context
				.getConfiguration())) {
			fMap.put(e.getFirst(), i++);
		}

		Parameters params = new Parameters(context.getConfiguration().get(
				PFPGrowth.PFP_PARAMETERS, ""));

		splitter = Pattern.compile(params.get(PFPGrowth.SPLIT_PATTERN,
				PFPGrowth.SPLITTER.toString()));

		maxPerGroup = Integer
				.valueOf(params.getInt(PFPGrowth.MAX_PER_GROUP, 0));
	}
}
