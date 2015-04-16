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
 * POJO used to create a JSON string about a OneRowChange and its PK.
 */
package com.smartsheet.tin.filters.common;

import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author scott.wimer@smartsheet.com
 *
 */
public class ORCPrimaryKeyInfo {
	private class KeyInfo {
		@SuppressWarnings("unused")
		public String columnName;
		@SuppressWarnings("unused")
		public int columnType;		// From java.sql.types.
		@SuppressWarnings("unused")
		public String columnTypeName;
		@SuppressWarnings("unused")
		public Object value;
		
		public KeyInfo(String columnName, int columnType, 
				String columnTypeName, Object value) {
			this.columnName = columnName;
			this.columnType = columnType;
			this.columnTypeName = columnTypeName;
			this.value = value;
		}
	}
	
	public String sourceID;
	public String shardID;
	public String schema;
	public String table;
	public String changeType;
	public String eventID;
	public long eventTimestamp;
	public ArrayList<KeyInfo> primaryKey;
	
	public ORCPrimaryKeyInfo(String sourceID, String shardID,
			String schema, String table, String changeType,
			String eventID, long eventTimestamp) {
		this.sourceID = sourceID;
		this.shardID = shardID;
		this.schema = schema;
		this.table = table;
		this.changeType = changeType;
		this.eventID = eventID;
		this.eventTimestamp = eventTimestamp;
		this.primaryKey = new ArrayList<KeyInfo>();
	}
	
	/**
	 * Add a primary key column and its value.
	 * 
	 * @param columnName
	 * @param columnType
	 * @param columnTypeName
	 * @param value
	 */
	@JsonIgnore
	public void addKey(String columnName, int columnType,
			String columnTypeName, Object value) {
		this.primaryKey.add(new KeyInfo(columnName, columnType, 
				columnTypeName, value));
	}
	
	/**
	 * Clear the array of primary key parts.
	 */
	@JsonIgnore
	public void resetKey() {
		this.primaryKey.clear();
	}
	
	/**
	 * This is a fallback, it produces a very basic JSON string.
	 * Notably absent is any listing of the primary key -- too much work.
	 * 
	 * @return
	 */
	@JsonIgnore
	public String toBasicJSON()
	{
		// In the format string, '#', are replaced with
		// double-quotes ("\"") to avoid lots of slash-escaping nonsense.
		String fmt_string = "{#RowFallback#: true,  #schema#: #%s#,  " + 
				"#table#: #%s#,  #changeType#: #%s#, " +
				"#eventID#: #%s#,  #eventTimestamp#: %d}".replace("#",  "\"");
		return String.format(fmt_string, this.schema, this.table,
				this.changeType, this.eventID, this.eventTimestamp);
	}
}
	
