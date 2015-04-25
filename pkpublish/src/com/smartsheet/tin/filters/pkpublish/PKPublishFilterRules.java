/**
* Copyright 2014-2015 Smartsheet.com, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

/**
 * Manage a set of PKPublish filter rules.
 * Load rules specified in JSON (from a string or a file).
 * Perform comparisons of ReplDBMSEvent objects against the set of rules.
 */
package com.smartsheet.tin.filters.pkpublish;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.smartsheet.tin.filters.common.JsonFilterException;

import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildByName;
import static com.smartsheet.tin.filters.common.JsonFilterTools.confirmNodeType;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;


import org.apache.log4j.Logger;

/**
 * @author scott.wimer@smartsheet.com
 * 
 */
public class PKPublishFilterRules {
	private static Logger logger = Logger.getLogger(PKPublishFilterRules.class);
	private List<TransactionFilter> transaction_filters;

	public PKPublishFilterRules() {
		this.transaction_filters = new ArrayList<TransactionFilter>();
	}


	/**
	 * Load the filter rules from a JSON-formatted string.
	 * 
	 * @param rules_json JSON formatted string of the filter rules.
	 * @throws PKPublishFilterException
	 * @throws IOException
	 * @throws JsonFilterException 
	 * @throws JsonProcessingException
	 */
	public void loadRules(String rules_json)
			throws IOException, JsonFilterException {
		try {
			JsonNode rules_jn = new ObjectMapper().readTree(rules_json);
			confirmNodeType(rules_jn, JsonNodeType.OBJECT, "TransactionRules",
					logger);
			JsonNode transaction_filters_jn = fetchChildByName(rules_jn,
					"transaction_filters", "array");

			for (JsonNode tfilter_jn : transaction_filters_jn) {
				confirmNodeType(tfilter_jn, JsonNodeType.OBJECT,
						"TransactionFilter", logger);
				TransactionFilter tfilter = TransactionFilter.newFromJson(
						tfilter_jn);
				this.transaction_filters.add(tfilter);
				logger.debug("Added Transaction filter");
			}
		} catch (JsonProcessingException e) {
			throw new JsonFilterException(e);
		}
	}


	/**
	 * Load the filter rules from a file.
	 * 
	 * @param path
	 * @throws IOException
	 * @throws JsonFilterException 
	 * @throws PKPublishFilterException
	 * 
	 * 
	 */
	public void loadRulesFromFile(String path)
			throws IOException, JsonFilterException {
		String rules = this.readFile(path);
		this.loadRules(rules);
	}


	/**
	 * Apply the filters to a replication event (at transaction).
	 * 
	 * The returned list of TransactionMatchResult objects should be walked
	 * and any messages from each match result should be handled
	 * appropriately.
	 * 
	 * @param event The Event to apply the filters to.
	 * @return A list of TransactionMatchResult objects.
	 */
	public List<TransactionMatchResultAccumulator>  apply(ReplDBMSEvent event) {
		List<TransactionMatchResultAccumulator> results = 
				new ArrayList<TransactionMatchResultAccumulator>();
		for (TransactionFilter tf : this.transaction_filters) {
			results.add(tf.match(event));
		}
		return results;
	}


	/**
	 * Read the specified file into a String.
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private String readFile(String path) throws IOException {
		Path p = FileSystems.getDefault().getPath(path);
		String contents = new String(Files.readAllBytes(p));
		return contents;
	}

}
