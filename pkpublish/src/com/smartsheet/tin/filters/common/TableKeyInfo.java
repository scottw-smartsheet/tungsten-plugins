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
 * Identifies the primary key column(s) for a table.
 */
package com.smartsheet.tin.filters.common;

import java.util.ArrayList;

import com.continuent.tungsten.replicator.database.Column;

/**
 * @author scottw
 * 
 */
public class TableKeyInfo {
	public class KeyPair {
		private String columnName;
		private int index;
		private int columnType;
		private String columnTypeDesc;

		public String getColumnName() {
			return this.columnName;
		}

		public int getIndex() {
			return this.index;
		}

		public int getColumnType() {
			return this.columnType;
		}

		public String getColumnTypeDesc() {
			return this.columnTypeDesc;
		}
	}

	private ArrayList<KeyPair> keys;

	public TableKeyInfo() {
		this.keys = new ArrayList<KeyPair>();
	}

	public void addKey(Column col) {
		KeyPair kp = new KeyPair();
		kp.columnName = col.getName();
		kp.index = col.getPosition();
		kp.columnType = col.getType();
		kp.columnTypeDesc = col.getTypeDescription();
		this.keys.add(kp);
	}

	public ArrayList<KeyPair> getKeys() {
		return this.keys;
	}

	public KeyPair getColumnKeyInfo(int col_index) {
		for (KeyPair kp : this.keys) {
			if (kp.getIndex() == col_index) {
				return kp;
			}
		}
		return null;
	}
}
