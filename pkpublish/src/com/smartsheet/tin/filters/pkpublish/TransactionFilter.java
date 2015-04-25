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


package com.smartsheet.tin.filters.pkpublish;

import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildByName;
import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildString;
import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildBoolean;
import static com.smartsheet.tin.filters.common.JsonFilterTools.confirmNodeType;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.smartsheet.tin.filters.common.JsonFilterChildNotFound;
import com.smartsheet.tin.filters.common.JsonFilterException;


public class TransactionFilter {
	private static Logger logger = Logger.getLogger(TransactionFilter.class); 
	private static int counter = 1; 	// Reset on each load from JSON.

	private int current_counter;
	private String name;
	private String cached_name;
	private boolean must_match_all_filters;
	private boolean must_match_any_filters;
	private boolean must_match_no_filters;
	private boolean must_match_all_rows;
	private boolean must_match_any_rows;
	private boolean must_match_no_rows;
	private List<RowFilter> row_filters;
	private List<MatchAction> actions;

	// FIXME:  Get rid of this coupling with specific actions.
	private boolean publish;
	private String routing_key;
	private String msg;

	// If this TransactionFilter is publishing and not using a hard-coded
	// message, then its message will be built from the messages for each
	// matched and included RowFilter.  This accumulator is filled when
	// processing a ReplDBMSEvent.
	private List<String> row_messages;

	// If any of the constituent RowFilters match and choose to publish,
	// messages will be accumulated here.  This accumulator is filled when
	// processing a ReplDBMSEvent.
	private List<String> row_publish_messages;

	public TransactionFilter() {
		this.name = "";
		this.cached_name = null;
		this.current_counter = TransactionFilter.counter;
		TransactionFilter.counter++;
		this.must_match_all_filters = true;
		this.must_match_any_filters = true;
		this.must_match_no_filters = false;
		this.must_match_all_rows = true;
		this.must_match_any_rows = true;
		this.must_match_no_rows = false;
		this.row_filters = new ArrayList<RowFilter>();
		this.actions = new ArrayList<MatchAction>();
		this.publish = false;
		this.routing_key = null;
		this.msg = null;
		this.row_messages = new ArrayList<String>();
		this.row_publish_messages = new ArrayList<String>();

	}


	static public TransactionFilter newFromJson(JsonNode node)
			throws JsonFilterException, JsonProcessingException {
		confirmNodeType(node, JsonNodeType.OBJECT, "TransactionFilter", logger);

		TransactionFilter tf = new TransactionFilter();
		tf.setName(fetchChildString(node, "name", false));
		tf.setFilterMatchRule(fetchChildString(node, "filter_match_rule", false));
		tf.setRowMatchRule(fetchChildString(node, "row_match_rule", false));

		JsonNode row_filters_jn = fetchChildByName(node, "row_filters",
				"array");
		for (JsonNode row_filter_jn : row_filters_jn) {
			confirmNodeType(row_filter_jn, JsonNodeType.OBJECT, "row_filter",
					logger);
			RowFilter rf = RowFilter.newFromJson(row_filter_jn);
			tf.row_filters.add(rf);
		}

		JsonNode actions_jn = null;
		try {
			actions_jn = fetchChildByName(node, "actions", "array");
		} catch (JsonFilterChildNotFound e) {
			// Do nothing.
		}
		if (actions_jn != null) {
			for (JsonNode action_jn :  actions_jn) {
				confirmNodeType(action_jn, JsonNodeType.OBJECT, "action", logger);
				String action_type = fetchChildString(action_jn, "type", true);
				if (action_type.equals("publish")) {
					PublishAction pa = PublishAction.newFromJson(action_jn);
					tf.setPublish(true);
					tf.setRoutingKey(pa.getRoutingKey());
					tf.setMessage(pa.getMessage());
					tf.actions.add(pa);
				} else {
					String err = String.format("Unknown action type: '%s', " +
							"in node: '%s'", action_type, action_jn.toString());
					logger.error(err);
					throw new JsonFilterException(err);	
				}
			}
		}
		return tf;
	}


	/**
	 * Set whether all, any, or none of the filter rules must match.
	 * 
	 * @param rule The string (from the JSON rules).
	 * @throws JsonFilterException
	 */
	private void setFilterMatchRule(String rule) throws JsonFilterException {
		if (rule == null) {
			String err = "filter_match_rule had no value, " +
					"valid values are:  'ALL', 'ANY', or 'NONE'";
			logger.error(err);
			throw new JsonFilterException(err);
		}

		resetFilterMatchRules();

		if (rule.equalsIgnoreCase("ALL")) {
			this.must_match_all_filters = true;
		} else if (rule.equalsIgnoreCase("ANY")) {
			this.must_match_any_filters = true;
		} else if (rule.equalsIgnoreCase("NONE")) {
			this.must_match_no_filters = true;
		} else {
			String err = String.format("filter_match_rule '%s' not valid, " +
					"valid values are:  'ALL', 'ANY', or 'NONE'", rule);
			logger.error(err);
			throw new JsonFilterException(err);
		}
	}


	private void resetFilterMatchRules() {
		this.must_match_all_filters = false;
		this.must_match_any_filters = false;
		this.must_match_no_filters = false;
	}


	/**
	 * Set whether all, any, or none of the OneRowChanges must match.
	 * 
	 * @param rule The string (from the JSON rules).
	 * @throws JsonFilterException
	 */
	private void setRowMatchRule(String rule) throws JsonFilterException {
		if (rule == null) {
			String err = "row_match_rule had no value, " +
					"valid values are:  'ALL', 'ANY', or 'NONE'";
			logger.error(err);
			throw new JsonFilterException(err);
		}

		resetRowMatchRules();

		if (rule.equalsIgnoreCase("ALL")) {
			this.must_match_all_rows = true;
		} else if (rule.equalsIgnoreCase("ANY")) {
			this.must_match_any_rows = true;
		} else if (rule.equalsIgnoreCase("NONE")) {
			this.must_match_no_rows = true;
		} else {
			String err = String.format("row_match_rule '%s' not valid, " +
					"valid values are:  'ALL', 'ANY', or 'NONE'", rule);
			logger.error(err);
			throw new JsonFilterException(err);
		}
	}


	private void resetRowMatchRules() {
		this.must_match_all_rows = false;
		this.must_match_any_rows = false;
		this.must_match_no_rows = false;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getName() {
		if (this.name != null) {
			return this.name;
		}
		if (this.cached_name == null) {
			this.setCachedName(String.format("TransactionFilter-%d",
					this.current_counter));
		}
		return this.cached_name;
	}


	private void setCachedName(String name) {
		this.cached_name = name;
	}


	public boolean mustMatchAllFilters() {
		return this.must_match_all_filters;
	}


	public boolean mustMatchAnyFilters() {
		return this.must_match_any_filters;
	}


	public boolean mustMatchNoFilters() {
		return this.must_match_no_filters;
	}


	public boolean mustMatchAllRows() {
		return this.must_match_all_rows;
	}


	public boolean mustMatchAnyRows() {
		return this.must_match_any_rows;
	}


	public boolean mustMatchNoRows() {
		return this.must_match_no_rows;
	}

	public void setPublish(boolean should_publish) {
		this.publish = should_publish;
	}


	public boolean shouldPublish() {
		return this.publish;
	}


	public void setRoutingKey(String routing_key) {
		this.routing_key = routing_key;
	}


	/**
	 * Get the routing key to use when publishing the transaction.
	 * 
	 * The routing key is the TransactionFilter's name unless otherwise
	 * specified.
	 * @return The routing key.
	 */
	public String getRoutingKey() {
		if (this.routing_key != null) {
			return this.routing_key;
		} else {
			return this.getName();
		}
	}


	public void setMessage(String msg) {
		this.msg = msg;
	}


	public boolean hasMessage() {
		return (this.msg != null);
	}


	public String getMessage() {
		return this.msg;
	}


	public List<RowFilter> getRowFilters() {
		return this.row_filters;
	}


	/**
	 * Compare this TransactionFilter against a transaction (ReplDBMSEvent).
	 *
	 * The TransactionMatchResult that is returned can be used to obtain
	 * any messages that should be published as a result of this match attempt.
	 * There are two sorts of messages possible.  The first are messages from
	 * the constituent RowFilters of the TransactionFilter.   These may be
	 * set to publish, even if the TransactionFilter does not meet its overall
	 * matching criteria.  These filters are obtained via the method:
	 *   getRowFilterMessagesToPublish()
	 * on the returned object which returns a list of (routing_key, message)
	 * Pairs.  If no RowFilters had a published match, the returned list will
	 * be empty.
	 *
	 * In addition, the TransactionFilter itself may have matched and therefore
	 * have a message to publish.  This message is obtained via the method:
	 *   getTransactionFilterMessageToPublish()
	 * on the returned object which returns a Pair of (routing key, message).
	 * The returned object's transactionFilterDidMatch() method can be used to
	 * see if there is reason to fetch the TransactionFilter's message.
	 * 
	 * The caller is obligated to give us a good event (not null, or empty,
	 * or otherwise goofy).
	 * 
	 * The caller is responsible for ensuring the updating of any table state
	 * information -- we will ignore all but RowChangeData entries in the
	 * ReplDBMSEvent.
	 * 
	 * @param event The transaction (or, potentially, transaction fragment).
	 * @return
	 */
	public TransactionMatchResultAccumulator match(ReplDBMSEvent event) {
		this.row_messages.clear();
		this.row_publish_messages.clear();

		// Compare this TransactionFilter's RowFilters against each of the
		// OneRowChange objects in this transaction.  Along the way, we keep
		// track of whether or not each OneRowChange has been matched by at
		// least one RowFilter and whether or not each RowFilter has matched
		// at least one OneRowChange.
		TransactionMatchResultAccumulator result = 
				new TransactionMatchResultAccumulator(this, event);

		for (DBMSData edata : event.getData()) {
			if (! (edata instanceof RowChangeData)) {
				continue;
			}
			RowChangeData rcdata = (RowChangeData) edata;
			for (OneRowChange orc : rcdata.getRowChanges()) {
				for (RowFilter rf : this.row_filters) {
					result.recordRowFilterOrcCompare(rf, orc, rf.match(orc));
				}
			}
		}
		return result;
	}


	public String toString() {
		return String.format("<TransactionFilter name: %s with %d RowFilters>",
				this.getName(), this.row_filters.size());
	}

}
