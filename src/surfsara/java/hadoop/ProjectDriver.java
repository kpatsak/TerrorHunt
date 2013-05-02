/**
s * Licensed to the Apache Software Foundation (ASF) under one or more
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

package surfsara.java.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import surfsara.java.hadoop.pfpgrowth.PFPGrowth;
import surfsara.java.hadoop.tfidf.TFIDFDriver;

public final class ProjectDriver extends AbstractJob {

	private static final Logger log = LoggerFactory
			.getLogger(ProjectDriver.class);

	private ProjectDriver() {
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ProjectDriver(), args);
	}

	/**
	 * Run TopK FPGrowth given the input file,
	 */
	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();

		addOption("minSupport", "s",
				"(Optional) The minimum number of times a co-occurrence must be present."
						+ " Default Value: 3", "3");
		addOption("minConfidence", "c",
				"(Optional) The minimum number of confidence."
						+ " Default Value: 3", "3");
		addOption("mintf-idf", "t",
				"(Optional) The minimum number of tf-idf value"
						+ " Default Value: 3", "3");
		addOption(
				"maxHeapSize",
				"k",
				"(Optional) Maximum Heap Size k, to denote the requirement to mine top K items."
						+ " Default value: 50", "50");

		if (parseArguments(args) == null) {
			return -1;
		}

		Parameters params = new Parameters();

		if (hasOption("minSupport")) {
			String minSupportString = getOption("minSupport");
			params.set("minSupport", minSupportString);
		}

		if (hasOption("maxHeapSize")) {
			String maxHeapSizeString = getOption("maxHeapSize");
			params.set("maxHeapSize", maxHeapSizeString);
		}

		Path inputDir = getInputPath();
		Path outputDir = getOutputPath();

		params.set("input", inputDir.toString());
		params.set("output", outputDir.toString());

		Configuration conf = new Configuration();
		HadoopUtil.delete(conf, outputDir);
		// PFPGrowth.runPFPGrowth(params);
		TFIDFDriver tfidf = new TFIDFDriver();
		// tfidf.runTFIDF(params, log, conf);

		PFPGrowth.runPFPGrowth(params);

		return 0;
	}
}
