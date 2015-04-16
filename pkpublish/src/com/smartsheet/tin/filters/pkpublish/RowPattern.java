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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.smartsheet.tin.filters.common.JsonFilterException;

import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildByName;
import static com.smartsheet.tin.filters.common.JsonFilterTools.fetchChildString;

import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;

public class RowPattern {
	private static Logger logger = Logger.getLogger(RowPattern.class);
	protected String schema;
	protected String table;
	protected List<String> changeTypes;
	protected boolean matchInsert;
	protected boolean matchUpdate;
	protected boolean matchDelete;
	protected boolean anyChange;
	protected boolean anySchema;
	protected boolean anyTable;

	public RowPattern() {
		this.schema = "";
		this.table = "";
		this.changeTypes = new ArrayList<String>();
		this.matchInsert = false;
		this.matchUpdate = false;
		this.matchDelete = false;
		this.anyChange = false;
		this.anySchema = false;
		this.anyTable = false;
	}


	public static RowPattern newFromJson(JsonNode node) 
			throws JsonProcessingException, JsonFilterException,
			RowPatternException {
		if (JsonNodeType.OBJECT != node.getNodeType()) {
			String err = String.format("RowPattern node must be an Object, " +
					"it is '%s'", node.getNodeType());
			logger.error(err);
			throw new JsonFilterException(err);
		}

		RowPattern rp = new RowPattern();

		rp.setSchema(fetchChildString(node, "schema", true));
		rp.setTable(fetchChildString(node,  "table", true));

		JsonNode change_types = fetchChildByName(node, "change_types", "array");
		for (JsonNode ctype : change_types) {
			rp.addChangeType(ctype.asText());
		}
		return rp;
	}


	public boolean match(OneRowChange orc) {
		return (this.matchChangeType(orc) &&
				this.matchSchema(orc) &&
				this.matchTable(orc));
	}


	/**
	 * Set the schema to match.
	 * 
	 * If the schemaName is "*", then this pattern will match any schema.
	 * @param schemaName The name of the schema to match, or '*'.
	 */
	public void setSchema(String schemaName) {

		if (schemaName != null) {
			this.schema = schemaName;
			if (schemaName.equals("*")) {
				this.anySchema = true;
			}
		}
	}


	/**
	 * Set the table to match.
	 * 
	 * If the tableName is "*", then this pattern will match any table.
	 * @param tableName The name of the table to match, or '*'.
	 */
	public void setTable(String tableName) {
		if (tableName != null) {
			this.table = tableName;
			if (tableName.equals("*")) {
				this.anyTable = true;
			}
		}
	}


	/**
	 * Add a change type to match.
	 *
	 * If the change type is "*", the pattern will match any change.
	 * 
	 * @param changeType "INSERT", "UPDATE", "DELETE" or "*".
	 * @throws RowPatternException
	 */
	public void addChangeType(String changeType) throws RowPatternException {
		if (changeType == null) {
			String err = "Null changeType";
			logger.error(err);
			throw new RowPatternException(err);
		}
		if (changeType.equalsIgnoreCase("INSERT")) {
			this.matchInsert = true;
		} else if (changeType.equalsIgnoreCase("UPDATE")) {
			this.matchUpdate = true;
		} else if (changeType.equalsIgnoreCase("DELETE")) {
			this.matchDelete = true;
		} else if (changeType.equals("*")) {
			this.anyChange = true;
		} else {
			String err = String.format("Unknown change type: '%s', " +
					"valid change types are: " +
					"['INSERT', 'UPDATE', 'DELETE', '*']", changeType);
			logger.error(err);
			throw new RowPatternException(err);
		}
		this.changeTypes.add(changeType);
	}


	public boolean matchChangeType(OneRowChange orc) {
		if (this.anyChange) {
			return true;
		}

		ActionType action = orc.getAction();
		if (this.matchInsert && action == ActionType.INSERT) {
			return true;
		}
		if (this.matchUpdate && action == ActionType.UPDATE) {
			return true;
		}
		if (this.matchDelete && action == ActionType.DELETE) {
			return true;
		}
		return false;
	}

	public boolean matchSchema(OneRowChange orc) {
		if (this.anySchema) {
			return true;
		}
		return orc.getSchemaName().equalsIgnoreCase(this.schema);
	}


	public boolean matchTable(OneRowChange orc) {
		if (this.anyTable) {
			return true;
		}
		return orc.getTableName().equalsIgnoreCase(this.table);
	}




	public String toString() {
		return String.format("<RowPattern schema: '%s'  table: '%s'  " +
				"matchInsert: %s  matchUpdate: %s  matchDelete: %s  " +
				"anyChange: %s  anySchema: %s  anyTable: %s>",
				this.schema, this.table, this.matchInsert, this.matchUpdate,
				this.matchDelete, this.anyChange, this.anySchema,
				this.anyTable);
	}

}
