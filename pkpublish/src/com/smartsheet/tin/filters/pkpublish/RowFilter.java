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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.smartsheet.tin.filters.common.JsonFilterChildNotFound;
import com.smartsheet.tin.filters.common.JsonFilterException;
import com.smartsheet.tin.filters.common.ORCFormatter;

import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildByName;
import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildString;
import static com.smartsheet.tin.filters.common.JsonFilterTools.confirmNodeType;
import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildBoolean;
import static com.smartsheet.tin.filters.common.StringUtils.joinList;


public class RowFilter {
	private static Logger logger = Logger.getLogger(RowFilter.class);
	private String name;
	private RowPattern pattern;
	private boolean include;
	private List<MatchAction> actions;
	// FIXME:  Get rid of this coupling with specific actions.
	private boolean publish;
	private String routing_key;
	private String msg;

	public RowFilter() {
		this.name = "";
		this.pattern = null;
		this.include = true;
		this.actions = new ArrayList<MatchAction>();
		this.publish = false;
		this.routing_key = null;
		this.msg = null;
	}

	public static RowFilter newFromJson(JsonNode node) 
			throws JsonFilterException, JsonProcessingException {
		confirmNodeType(node, JsonNodeType.OBJECT, "RowFilter", logger);

		RowFilter rf = new RowFilter();
		rf.setName(fetchChildString(node, "name", false));
		rf.setPublish(fetchChildBoolean(node, "include", true));
		try {
			rf.setRowPattern(RowPattern.newFromJson(
					fetchChildByName(node, "row_pattern", "object")));
		} catch (RowPatternException e) {
			e.printStackTrace();
			throw new JsonFilterException(e);
		}

		// FIXME:  Handle actions without the excess of coupling here now.
		// We really need a way to have the actions support an efficient
		// visitor pattern.
		JsonNode actions_jn = null;
		try {
			actions_jn = fetchChildByName(node, "actions", "array");
		} catch (JsonFilterChildNotFound e) {
			// Do nothing.
		}
		if (actions_jn != null) {
			for (JsonNode action_jn : actions_jn) {
				confirmNodeType(action_jn, JsonNodeType.OBJECT, "action", logger);
				String action_type = fetchChildString(action_jn, "type", true);
				if (action_type.equals("publish")) {
					PublishAction pa = PublishAction.newFromJson(action_jn);
					rf.setPublish(true);
					rf.setRoutingKey(pa.getRoutingKey());
					rf.setMessage(pa.getMessage());
					rf.actions.add(pa);
				} else {
					String err = "Unknown action type: '" + action_type + "'";
					logger.error(err);
					throw new JsonFilterException(err);
				}
			}
		}
		return rf;
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


	public boolean hasRoutingKey() {
		return (this.routing_key != null);
	}


	public String getRoutingKey() {
		return this.routing_key;
	}


	public String getRoutingKeyForMatch(OneRowChange orc,
			ORCFormatter orc_formatter) {
		if (this.hasRoutingKey()) {
			return this.getRoutingKey();
		}
		if (orc == null || orc_formatter == null) {
			return "DEFAULT.DEFAULT";
		}
		return orc_formatter.getFullyQualifiedTableName(orc) + "." + 
		orc.getAction().toString();
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


	/**
	 * Get the messages for this RowFilter for a given OneRowChange.
	 * 
	 * If the RowFilter doesn't define a fixed message, a message is created
	 * for each of the rows in the OneRowChange.
	 * 
	 * @param orc The OneRowChange that the messages are for.
	 * @param event The transaction the OneRowChange is in.
	 * @param orc_formatter The formatter to build the messages.
	 * @return List of messages as strings.
	 */
	public List<String> getMessagesForMatch(OneRowChange orc,
			ReplDBMSEvent event, ORCFormatter orc_formatter) {
		if (this.hasMessage()) {
			ArrayList<String> msgs = new ArrayList<String>();
			msgs.add(this.getMessage());
			return msgs;
		}
		return orc_formatter.makeJSONStringsFromORC(orc, event);
	}


	public void setInclude(boolean should_enclude) {
		this.include = should_enclude;
	}

	public boolean shouldInclude() {
		return this.include;
	}


	public String toString() {
		ArrayList<String> action_list = new ArrayList<String>();
		for (MatchAction action : this.actions) {
			action_list.add(action.getClass().toString());
		}
		return String.format("<RowFilter name: '%s'  pattern: '%s'  " +
				"actions: [%s]>", this.getName(), this.pattern.toString(),
				joinList(action_list, ","));
	}


	/**
	 * Check if this RowFilter matches a OneRowChange instance.
	 * @param orc The OneRowChange instance to check for a match.
	 * @return true on match, false otherwise.
	 */
	public boolean match(OneRowChange orc) {
		return this.pattern.match(orc);
	}

	public String getName() {
		if (this.name != null) {
			return this.name;
		}
		return String.format("%s.%s.%s", this.pattern.schema,
				this.pattern.table, joinList(this.pattern.changeTypes, ","));
	}


	public void setName(String name) {
		if (name != null) {
			this.name = name;
		}
	}

	public void setRowPattern(RowPattern pattern) {
		if (pattern != null) {
			this.pattern = pattern;
		}
	}

	public void addMatchAction(MatchAction action) {
		if (action != null) {
			this.actions.add(action);
		}
	}

}
