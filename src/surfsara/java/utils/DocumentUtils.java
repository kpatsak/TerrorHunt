package surfsara.java.utils;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

public class DocumentUtils {

	public static List<Sentence> stringToSentences(String documentLiteral) {
		String[] sentenceLiterals = documentLiteral
				.split("\\\n|(?=[a-zA-Z0-9]*)[\\.\\?\\!]+[ \r\n\t]+(?=[A-Z0-9])");
		List<Sentence> sentences = new ArrayList<Sentence>(
				sentenceLiterals.length);
		for (String sentenceLiteral : sentenceLiterals) {
			// Skip sentences that are code
			if (sentenceLiteral.contains("{") || sentenceLiteral.contains("@")
					|| sentenceLiteral.contains(".")) {
				continue;
			}

			// Remove accents
			sentenceLiteral = Normalizer.normalize(sentenceLiteral,
					Normalizer.Form.NFD);
			sentenceLiteral = sentenceLiteral.replaceAll("[^\\p{ASCII}]", "");

			// Remove mark-up
			sentenceLiteral.replaceAll("[^a-zA-Z']+", " ");

			// Add
			sentences.add(new Sentence(sentenceLiteral));
		}
		return sentences;
	}

}
