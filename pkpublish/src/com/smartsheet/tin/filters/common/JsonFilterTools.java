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

package com.smartsheet.tin.filters.common;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

public class JsonFilterTools {
	private static Logger logger = Logger.getLogger(JsonFilterTools.class);

	/**
	 * Fetch a child node from a JSON object node.
	 * 
	 * @param parent The parent JSON object node.
	 * @param node_name The name of the child node.
	 * @param node_type array, object, string, number, container, value, or bool
	 * @return The child node.
	 * @throws JsonFilterException
	 */
	public static JsonNode fetchChildByName(JsonNode parent, String node_name,
			String node_type) throws JsonFilterException {
		if (JsonNodeType.OBJECT != parent.getNodeType()) {
			String err = String.format("parent must be an Object/{}/hash type");
			throw new JsonFilterException(err);
		}

		if (parent.hasNonNull(node_name)) {
			JsonNode child = parent.get(node_name);
			boolean type_error = false;

			if (node_type == null || node_type.isEmpty()) {
				// Do no validation on the child type.
				type_error = false;
			} else {
				if (node_type.equals("array")) {
					if (! child.isArray()) {
						type_error = true;
					}
				} else if (node_type.equals("object")) {
					if (! child.isObject()) {
						type_error = true;
					}
				} else if (node_type.equals("string")) {
					if (! child.isTextual()) {
						type_error = true;
					}
				} else if (node_type.equals("number")) {
					if (! child.isNumber()) {
						type_error = true;
					}
				} else if (node_type.equals("container")) {
					if (! child.isContainerNode()) {
						type_error = true;
					}
				} else if (node_type.equals("value")) {
					if (! child.isValueNode()) {
						type_error = true;
					}
				} else if (node_type.startsWith("bool")) {
					if (! child.isBoolean()) {
						type_error = true;
					}
				}
			}
			if (type_error == true) {
				String err = String.format("Type error: child node '%s' " +
						"is not a %s", node_name, node_type);
				throw new JsonFilterException(err);
			}
			return child;
		} else {
			String err = String.format("Child node '%s', type: '%s' not found",
					node_name, node_type);
			throw new JsonFilterChildNotFound(err);
		}
	}


	public static String fetchChildString(JsonNode parent, String node_name,
			boolean required) throws JsonFilterException {
		String result = null;
		try {
			JsonNode child = fetchChildByName(parent, node_name, "string");
			result = child.asText();
		} catch (JsonFilterException e) {
			if (required) {
				throw e;
			}
		}
		return result;
	}


	public static boolean fetchChildBoolean(JsonNode parent, String node_name,
			boolean default_value) {
		try {
			JsonNode child = fetchChildByName(parent, node_name, "boolean");
			if (child.isBoolean()) {
				return child.asBoolean();
			} else {
				return default_value;
			}
		} catch (JsonFilterException e) {
			return default_value;
		}
	}


	/**
	 * Wrap up boilerplate JsonNode type checking and error reporting.
	 * 
	 * @param node The node to confirm the type of.
	 * @param need_type	The type that the node needs to be.
	 * @param node_name	The name of the node (for error reporting purposes).
	 * @param logger The logger to use if node has the wrong type.
	 * @throws JsonFilterException
	 */
	public static void confirmNodeType(JsonNode node, JsonNodeType need_type,
			String node_name, Logger logger) throws JsonFilterException {
		if (need_type != node.getNodeType()) {
			String err = String.format("'%s' must be an %s, it is '%s'",
					node_name, need_type, node.getNodeType());
			if (logger != null) {
				logger.error(err);
			}
			throw new JsonFilterException(err);
		}
	}

}
