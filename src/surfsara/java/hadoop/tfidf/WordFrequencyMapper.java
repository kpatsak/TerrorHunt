package surfsara.java.hadoop.tfidf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import surfsara.java.constants.Terror;
import surfsara.java.hadoop.tfidf.TFIDFDriver.MAPPERCOUNTER;

import org.apache.hadoop.io.LongWritable;
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
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class WordFrequencyMapper extends
		Mapper<Text, ArcFileItem, Text, LongWritable> {

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
		Terror lists = new Terror();
		StringBuilder valueBuilder = new StringBuilder();
		TokenStream stream = analysis.tokenStream(null, new StringReader(doc));

		StopFilter filter = new StopFilter(Version.LUCENE_36, stream,
				StopFilter.makeStopSet(Version.LUCENE_36, lists.getStopWords(),
						true));
		PorterStemFilter stemfilter = new PorterStemFilter(filter);
		stemfilter.reset();
		while (stemfilter.incrementToken()) {
			String current_word = stream.getAttribute(CharTermAttribute.class)
					.toString();
			if (current_word.matches("^[a-z].*$")
					&& !current_word.contains(".")
					&& !current_word.contains(":")) {
				CharsetEncoder encoder = Charset.forName("US-ASCII")
						.newEncoder();
				if (!encoder.canEncode(current_word)) {
					continue;
				}
				valueBuilder.append(current_word);
				valueBuilder.append("@");
				valueBuilder.append(DocId);
				context.write(new Text(valueBuilder.toString()),
						new LongWritable(1));
				valueBuilder.setLength(0);
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
						WordFrequencyinDoc(finalString, value.getUri(), context);
						break;
					}
				}
			} else {
				context.getCounter(MAPPERCOUNTER.NULL_PAGES).increment(1);
			}
		}
	}
}
