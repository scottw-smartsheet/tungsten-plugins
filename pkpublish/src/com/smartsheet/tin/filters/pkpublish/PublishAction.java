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

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.smartsheet.tin.filters.common.JsonFilterException;

import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildString;
import static com.smartsheet.tin.filters.common.JsonFilterTools.confirmNodeType;

public class PublishAction extends MatchAction {
	private static Logger logger = Logger.getLogger(PublishAction.class);
	private String routingKey;
	private String message;

	static public PublishAction newFromJson(JsonNode node)
			throws JsonFilterException, JsonProcessingException {
		confirmNodeType(node, JsonNodeType.OBJECT, "action", logger);

		PublishAction pa = new PublishAction();
		pa.setRoutingKey(fetchChildString(node, "routing_key", false));
		pa.setMessage(fetchChildString(node, "message", false));
		return pa;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	public void setMessage(String msg) {
		this.message = msg;
	}

	public String getMessage() {
		return this.message;
	}

	public PublishAction() {
		this.routingKey = null;
		this.message = null;
	}

}
