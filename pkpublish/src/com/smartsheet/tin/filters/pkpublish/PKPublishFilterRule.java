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
 * 
 */
package com.smartsheet.tin.filters.pkpublish;

import org.apache.log4j.Logger;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;

/**
 * A filter rule for OneRowChange objects.
 * 
 * The filter matches OneRowChange objects according to schema name, table name,
 * and change type.
 * 
 * @author scott.wimer@smartsheet.com
 */
public class PKPublishFilterRule {

	private static Logger logger = Logger.getLogger(PKPublishFilterRule.class);
	private String schemaName;
	private String tableName;
	private String routingKey;
	private boolean changeInsert;
	private boolean changeUpdate;
	private boolean changeDelete;

	// These are used for wildcard rule matches.
	private boolean anyChange;
	private boolean anySchema;
	private boolean anyTable;

	public PKPublishFilterRule() {
		this.schemaName = "";
		this.tableName = "";
		this.routingKey = null;
		this.changeInsert = false;
		this.changeUpdate = false;
		this.changeDelete = false;
		this.anyChange = false;
		this.anySchema = false;
		this.anyTable = false;
	}

	public String toString() {
		return String.format("<PKPublishFilterRule "
				+ "schemaName: '%s'  tableName: '%s'  "
				+ "routingKey: '%s'  changeInsert: '%s'  "
				+ "changeUpdate: '%s'  changeDelete: '%s'  "
				+ "anyChange: '%s'  anySchema: '%s'  anyTable: '%s'>",
				this.schemaName, this.tableName, this.routingKey,
				this.changeInsert, changeUpdate, this.changeDelete,
				this.anyChange, this.anySchema, this.anyTable);
	}

	/**
	 * Compare a OneRowChange object against a filter rule.
	 * 
	 * @param orc
	 *            The OneRowChange instance to compare this filter against.
	 * @return true if this filter matches orc, false otherwise.
	 */
	public boolean match(OneRowChange orc) {

		return (this.matchChangeType(orc) && 
				this.matchSchemaName(orc) &&
				this.matchTableName(orc));
	}

	private boolean matchChangeType(OneRowChange orc) {
		if (this.anyChange)
			return true;
		ActionType action = orc.getAction();
		if (this.changeInsert && action == ActionType.INSERT)
			return true;
		if (this.changeUpdate && action == ActionType.UPDATE)
			return true;
		if (this.changeDelete && action == ActionType.DELETE)
			return true;
		return false;
	}

	private boolean matchSchemaName(OneRowChange orc) {
		if (this.anySchema)
			return true;
		return orc.getSchemaName().equals(this.schemaName);
	}

	private boolean matchTableName(OneRowChange orc) {
		if (this.anyTable)
			return true;
		return orc.getTableName().equals(this.tableName);
	}

	/**
	 * @param schemaName
	 *            the schemaName to set
	 */
	public void setSchemaName(String schemaName) {
		if (schemaName != null) {
			this.schemaName = schemaName;
			if (schemaName.equals("*")) {
				this.anySchema = true;
			}
		}
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String tableName) {
		if (tableName != null) {
			this.tableName = tableName;
			if (tableName.equals("*")) {
				this.anyTable = true;
			}
		}
	}

	/**
	 * @param routingKey
	 *            The routing key for this rule.
	 */
	public void setRoutingKey(String routingKey) {
		if (routingKey != null) {
			this.routingKey = routingKey;
		}
	}

	public String getRoutingKey() {
		return this.routingKey;
	}

	/**
	 * @param changeType
	 *            The change type to add.
	 */
	public void addChangeType(String changeType) throws PKPublishException {
		if (changeType.equals("INSERT")) {
			this.changeInsert = true;
		} else if (changeType.equals("UPDATE")) {
			this.changeUpdate = true;
		} else if (changeType.equals("DELETE")) {
			this.changeDelete = true;
		} else if (changeType.equals("*"))
			this.anyChange = true;
		else {
			logger.error("Unknown change type: '" + changeType + "'.");
			throw new PKPublishException("Unknown change type: ");
		}
	}
}
