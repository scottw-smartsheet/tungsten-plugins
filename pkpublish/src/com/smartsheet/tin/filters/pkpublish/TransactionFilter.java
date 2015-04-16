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
	private boolean must_match_all_rows;
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
		this.must_match_all_filters = false;
		this.must_match_all_rows = false;
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
		tf.setMustMatchAllFilters(fetchChildBoolean(node,
				"must_match_all_filters", tf.must_match_all_filters));
		tf.setMustMatchAllRows(fetchChildBoolean(node,
				"must_match_all_rows", tf.must_match_all_rows));


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


	public void setMustMatchAllFilters(boolean match_all) {
		this.must_match_all_filters = match_all;
	}


	public boolean mustMatchAllFilters() {
		return this.must_match_all_filters;
	}


	public void setMustMatchAllRows(boolean match_all) {
		this.must_match_all_rows = match_all;
	}


	public boolean mustMatchAllRows() {
		return this.must_match_all_rows;
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
	public TransactionMatchResult match(ReplDBMSEvent event) {
		// TODO:  Switch to a more efficient rule matching algorithm.
		// Some sort of tree would probably be better than the current
		// nested loop approach.

		TransactionMatchResult result = new TransactionMatchResult(this, event);
		this.row_messages.clear();
		this.row_publish_messages.clear();

		// Flip this as soon as an OneRowChange fails to match.
		result.setAllORCsMatched(true);

		for (DBMSData edata : event.getData()) {
			if (edata.getClass() == RowChangeData.class){
				RowChangeData rcdata = (RowChangeData) edata;
				for (OneRowChange orc : rcdata.getRowChanges()) {
					boolean orc_matched = false;
					for (RowFilter rf : this.row_filters) {
						if (rf.match(orc)) {
							result.recordRowFilterMatchedOrc(rf, orc);
							orc_matched = true;
							logger.debug("orc matched rf: " + rf.toString());
						}
					}
					if (! orc_matched) {
						result.setAllORCsMatched(false);
						logger.debug("ORC: " + orc.toString() + " was not matched.");
					}
				}
			} 
		}


		if (this.must_match_all_rows && ! result.allORCsMatched()) {
			result.setTransactionFilterMatched(false);
		}
		if (this.must_match_all_filters && ! result.allRowFiltersMatched()) {
			result.setTransactionFilterMatched(false);
		}

		return result;
	}


	public String toString() {
		return String.format("<TransactionFilter name: %s with %d RowFilters>",
				this.getName(), this.row_filters.size());
	}



}
