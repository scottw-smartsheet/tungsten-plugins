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
 * Bundle up and provide easy access to the results of comparing 
 * a Transaction (ReplDBMSEvent) against a TransactionFilter.
 */
package com.smartsheet.tin.filters.pkpublish;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.smartsheet.tin.filters.common.ORCFormatter;
import com.smartsheet.tin.filters.common.Pair;
import com.smartsheet.tin.filters.common.TransactionInfo;

public class TransactionMatchResultAccumulator {
	private static Logger logger = Logger.getLogger(TransactionMatchResultAccumulator.class);
	private TransactionFilter tfilter;
	private ReplDBMSEvent event;
	// We accumulate state info (about whether matched or not) to these.
	private List<Pair<OneRowChange, RowFilter>> matched_orcs_and_their_filters;
	private Map<RowFilter, Boolean> row_filter_match_results;
	private Map<OneRowChange, Boolean> orc_match_results;

	public TransactionMatchResultAccumulator(TransactionFilter tf,
			ReplDBMSEvent event) {
		this.event = event;
		this.tfilter = tf;
		this.matched_orcs_and_their_filters = new
				ArrayList<Pair<OneRowChange, RowFilter>>();
		this.row_filter_match_results = new HashMap<RowFilter, Boolean>();
		this.orc_match_results = new HashMap<OneRowChange, Boolean>();
	}


	/**
	 * Accumulate the result of comparing a RowFilter against a OneRowChange.
	 * 
	 * @param rf The RowFilter that was used.
	 * @param orc The OneRowChange that was compared.
	 * @param matched The result of the comparison.
	 */
	public void recordRowFilterOrcCompare(RowFilter rf, OneRowChange orc,
			boolean matched ) {
		if (matched) {
			this.orc_match_results.put(orc,  true);
			this.row_filter_match_results.put(rf, true);
			this.matched_orcs_and_their_filters.add(Pair.of(orc, rf));
		} else  {
			if (! this.orc_match_results.containsKey(orc)) {
				this.orc_match_results.put(orc, false);
			}
			if (! this.row_filter_match_results.containsKey(rf)) {
				this.row_filter_match_results.put(rf, false);
			}
		}
	}


	/**
	 * Return true if, and only if, all of the RowFilters matched. 
	 * 
	 * @return true if all RowFilters matched, false otherwise.
	 */
	public boolean allRowFiltersMatched() {
		return ! this.row_filter_match_results.values().contains(false);
	}


	/**
	 * Return true, if and only if, any of the RowFilters matched.
	 * 
	 * @return true if at least one RowFilter matched, false otherwise.
	 */
	public boolean anyRowFiltersMatched() {
		return this.row_filter_match_results.values().contains(true);
	}


	/**
	 * Return true, if and only if, none of the RowFilters matched.
	 * 
	 * @return if no RowFilters matched, false otherwise
	 */
	public boolean noRowFiltersMatched() {
		return ! anyRowFiltersMatched();
	}


	/**
	 * Return true, if and only if, all of the OneRowChanges matched.
	 * 
	 * @return true if all the OneRowChanges were matched by a RowFilter.
	 */
	public boolean allOrcsMatched() {
		return ! this.orc_match_results.values().contains(false);
	}


	/**
	 * Return true, if and only if, at least one OneRowChange matched.
	 * 
	 * @return true if any of the OneRowChanges were matched by a RowFilter.
	 */
	public boolean anyOrcsMatched() {
		return this.orc_match_results.values().contains(true);
	}


	/**
	 * Return true, if and only if, none of the OneRowChanges matched.
	 * 
	 * @return true if none of the OneRowChanges were matched by a RowFilter.
	 */
	public boolean noOrcsMatched() {
		return ! anyOrcsMatched();
	}


	/**
	 * Return whether or not the underlying event matched the filter.
	 * 
	 * @return
	 */
	public boolean matched() {
		if (this.tfilter.mustMatchAllFilters()) {
			if (! this.allRowFiltersMatched()) {
				return false;
			}
		} else if (this.tfilter.mustMatchAnyFilters()) {
			if (! this.anyRowFiltersMatched()) {
				return false;
			}
		} else if (this.tfilter.mustMatchNoFilters()) {
			if (! this.noRowFiltersMatched()) {
				return false;
			}
		}
		
		if (this.tfilter.mustMatchAllRows()) {
			if (! this.allRowFiltersMatched()) {
				return false;
			}
		} else if (this.tfilter.mustMatchAnyRows()) {
			if (! this.anyRowFiltersMatched()) {
				return false;
			}
		} else if (this.tfilter.mustMatchNoRows()) {
			if (! this.noOrcsMatched()) {
				return false;
			}
		}

		return true;
	}


	/**
	 * Get the messages to publish for matching RowFilters set to publish.
	 * 
	 * If a RowFilter in a TransactionFilter is set to publish, it will
	 * publish if matched -- whether or not the TransactionFilter is is
	 * part of matches.
	 * 
	 * @throws PKPublishException
	 * @return List of Pairs of Strings (routing_key, msg) to publish.
	 */
	public List<Pair<String,String>> getRowFilterMessagesToPublish(
			ORCFormatter orc_formatter) throws PKPublishException {
		List<Pair<String,String>> routing_key_msg_tuples =
				new ArrayList<Pair<String,String>>();
		for (Pair<OneRowChange, RowFilter> orc_rf :
			this.matched_orcs_and_their_filters) {
			OneRowChange orc = orc_rf.first;
			RowFilter rf = orc_rf.second;
			if (rf.shouldPublish()) {
				String routing_key = rf.getRoutingKeyForMatch(orc,
						orc_formatter);
				for (String msg : rf.getMessagesForMatch(orc, this.event,
						orc_formatter)) {
					routing_key_msg_tuples.add(Pair.of(routing_key, msg));
				}
			}
		}
		return routing_key_msg_tuples;
	}


	/**
	 * If matched and should publish, get message its routing key.
	 * 
	 * If there is no message to publish, returns null (the caller should
	 * have checked whether or not the transaction filter matched and if
	 * it "intends" to publish).
	 * 
	 * @param orc_formatter
	 * @return Pair of Strings: routing key, message; or null if no message
	 * @throws PKPublishException
	 */
	public Pair<String,String> getTransactionFilterMessageToPublish(
			ORCFormatter orc_formatter) throws PKPublishException {
		if (! this.matched() ) {
			return null;
		}
		if (! this.tfilter.shouldPublish()) {
			return null;
		}
		TransactionInfo ti = new TransactionInfo(this.tfilter.getName(),
				this.event.getEventId(),
				this.event.getExtractedTstamp().getTime());

		String msg = "";
		
		// TODO: Cleaner handling of the two different message approaches.

		if (this.tfilter.hasMessage()) {
			ti.message = this.tfilter.getMessage();
			try {
				msg = orc_formatter.getMapper().writeValueAsString(ti);
			} catch (JsonProcessingException e) {
				String err = String.format("Failed to make JSON from " +
						"TransactionInfo for filter: %s against " +
						"eventId: %s, error: %s", this.tfilter.getName(),
						this.event.getEventId(), e);
				logger.error(err);
				msg = ti.toBasicJSON();
			}
		} else {
			for (Pair<OneRowChange,RowFilter> orc_rf :
				this.matched_orcs_and_their_filters) {
				OneRowChange orc = orc_rf.first;
				RowFilter rf = orc_rf.second;
				if (rf.shouldInclude())
				{
					for (String row_msg : rf.getMessagesForMatch(orc, event,
							orc_formatter)) {
						ti.addRow(row_msg);
					}
				}
			}
			try {
				msg = orc_formatter.getMapper().writeValueAsString(ti);
			} catch (JsonProcessingException e) {
				String err = String.format("Failed getting message for " +
						"filter: %s against eventId: %s: %s",
						this.tfilter.getName(), this.event.getEventId(), e);
				logger.error(err, e);
				msg = ti.toBasicJSON();
			}
		}

		return Pair.of(this.tfilter.getRoutingKey(), msg);
	}
}
