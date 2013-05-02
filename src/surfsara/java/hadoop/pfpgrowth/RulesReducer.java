/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package surfsara.java.hadoop.pfpgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import surfsara.java.hadoop.pfpgrowth.convertors.PatternPair;

public class RulesReducer extends
		Reducer<PatternPair, Text, Text, FloatWritable> {

	private List<Pair<String, TopKStringPatterns>> freq_patterns = new ArrayList<Pair<String, TopKStringPatterns>>();
	private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
	String[] temp;
	String[] value;
	String[] temp2;
	StringBuilder rule = new StringBuilder();
	Text ruleR = new Text();
	FloatWritable rassociation_probR = new FloatWritable();
	long lsupport = 0;
	boolean found = false;
	String rassociation = "";
	float rassociation_prob;
	int i;

	public String ListtoString(List<String> array) {
		StringBuilder builder = new StringBuilder();
		for (String s : array) {
			builder.append(s);
			builder.append(" ");
		}
		return builder.toString();
	}

	@Override
	protected void reduce(PatternPair key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		for (Text iter : values) {
			temp = iter.toString().split("\\+");
			temp2 = temp[0].split(" ");
			if (temp2.length > 1) {
				value = iter.toString().split("\\+");
				if (key.getFirst().toString().split(" ").length == 1) {
					lsupport = fMap.get(key.getFirst().toString());
					if (lsupport != 0 && lsupport != -1) {
						System.out.println("Found pattern111");
						found = true;
					}
				} else {
					outerloop: for (Pair<String, TopKStringPatterns> pair : freq_patterns) {
						for (Pair<List<String>, Long> pattern : pair
								.getSecond().getPatterns()) {
							if (ListtoString(pattern.getFirst()).equals(
									key.getFirst().toString() + " ")) {
								System.out.println("Found pattern222");
								lsupport = pattern.getSecond();
								found = true;
								break outerloop;
							}
						}
					}
				}
			} else {
				lsupport = Long.parseLong(iter.toString());
				found = true;
				continue;
			}

			if (!found) {
				return;
			}
			temp = value[0].split(" ");
			if (key.getFirst().toString().split(" ").length == 1) {
				temp2 = new String[1];
				temp2[0] = key.getFirst().toString();
			} else {
				temp2 = key.getFirst().toString().split(" ");
			}

			for (i = 0; i < temp.length - 1; i++) {
				System.out.println("Found pattern333");
				if (!temp[i].equals(temp2[i])) {
					rassociation = temp[i];
					break;
				}
			}
			if (rassociation.equals("")) {
				rassociation = temp[i];
			}
			long pattern_support = Long.parseLong(value[1]);
			rassociation_prob = (float) pattern_support / lsupport;
			rule.append(key.getFirst().toString());
			rule.append("->");
			rule.append(rassociation);
			ruleR.set(rule.toString());
			rassociation_probR.set(rassociation_prob);
			context.write(ruleR, rassociation_probR);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Parameters params = new Parameters(context.getConfiguration().get(
				PFPGrowth.PFP_PARAMETERS, ""));
		freq_patterns = PFPGrowth.readFrequentPattern(params);
		for (Pair<String, Long> e : PFPGrowth.readFList(context
				.getConfiguration())) {
			fMap.put(e.getFirst(), e.getSecond().intValue());
		}
	}
}
