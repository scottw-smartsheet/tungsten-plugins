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

public class TransactionMatchResult {
	private static Logger logger = Logger.getLogger(TransactionMatchResult.class);
	private List<Pair<OneRowChange, RowFilter>> matched_orcs_and_their_filters;
	private Map<RowFilter, Boolean> row_filter_match_state;
	private TransactionFilter tfilter;
	private ReplDBMSEvent event;
	private boolean all_orcs_matched;
	private boolean transaction_filter_matched;
	private boolean match_state_set;

	public TransactionMatchResult(TransactionFilter tf, ReplDBMSEvent event) {
		this.matched_orcs_and_their_filters = new ArrayList<Pair<OneRowChange, RowFilter>>();
		this.row_filter_match_state = new HashMap<RowFilter, Boolean>();
		this.event = event;
		this.tfilter = tf;
		this.all_orcs_matched = false;
		this.all_orcs_matched = false;
		this.transaction_filter_matched = false;
		this.match_state_set = false;

		for (RowFilter rf : tf.getRowFilters()) {
			this.row_filter_match_state.put(rf,  false);
		}
	}

	// FIXME:  Handle No ORCs matching differently than at least one ORC matching. 
	// Right now, they are effectively equivalent.


	public void setAllORCsMatched(boolean all_orcs_matched) {
		this.all_orcs_matched = all_orcs_matched;
	}

	public boolean allORCsMatched() {
		return this.all_orcs_matched;
	}

	/**
	 * Record that a RowFilter matched a particular OneRowChange.
	 * 
	 * @param orc The OneRowChange that was matched by the RowFilter.
	 * @param rf The RowFilter that matched.
	 */
	public void recordRowFilterMatchedOrc(RowFilter rf, OneRowChange orc) {
		this.matched_orcs_and_their_filters.add(Pair.of(orc, rf));
		this.row_filter_match_state.put(rf,  true);
	}


	/**
	 * Return true if, and only if, all of the RowFilters matched. 
	 * @return true if all RowFilters matched, false otherwise.
	 */
	public boolean allRowFiltersMatched() {
		for (Map.Entry<RowFilter, Boolean> entry :
			this.row_filter_match_state.entrySet()) {
			if (! entry.getValue()) {
				logger.debug("Row filter: " + entry.getKey().toString() + " did not match");
				return false;
			} else {
				logger.debug("Row filter: " + entry.getKey().toString() + " matched");
			}
		}
		logger.debug("All Row filters matched.");
		return true;
	}

	/**
	 * Return true, if and only if, any of the RowFilters matched.
	 * @return true if at least one RowFilter matched, false otherwise.
	 */
	public boolean anyRowFiltersMatched() {
		for (Map.Entry<RowFilter, Boolean> entry :
			this.row_filter_match_state.entrySet()) {
			if (entry.getValue()) {
				return true;
			}
		}
		return false;
	}


	public void setTransactionFilterMatched(boolean matched) {
		this.transaction_filter_matched = matched;
		this.match_state_set = true;
	}


	public boolean transactionFilterDidMatch() {
		return this.transaction_filter_matched;
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
		if (! this.match_state_set) {
			logger.error("Must call " +
					"TransactionMatchResult.setTransactionFilterMatched()" +
					"before getting messages from the match result.");
			throw new PKPublishException("TransactionMatchResult not ready");
		}
		if (! this.transaction_filter_matched) {
			return null;
		}
		if (! this.tfilter.shouldPublish()) {
			return null;
		}
		TransactionInfo ti = new TransactionInfo(this.tfilter.getName(),
				this.event.getEventId(),
				this.event.getExtractedTstamp().getTime());

		String msg = "";

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
