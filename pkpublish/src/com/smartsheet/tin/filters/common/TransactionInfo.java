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
 * POJO used to create a JSON string about a transaction, including any
 * messages for rows in the transaction that have matched and are supposed
 * to have their messages included in the transaction message.
 */
package com.smartsheet.tin.filters.common;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TransactionInfo {
	public String name;
	public String eventId;
	public long eventTimestamp;
	public String message;
	public ArrayList<String> rows;

	public TransactionInfo(String name, String eventId, long eventTimestamp) {
		this.name = name;
		this.eventId = eventId;
		this.eventTimestamp = eventTimestamp;
		this.message = "";
		this.rows = new ArrayList<String>();
	}


	/**
	 * Reset the list of rows in this transaction.
	 * Call this before each subsequent use (if reusing this object).
	 */
	@JsonIgnore
	public void resetRows() {
		this.rows.clear();
	}


	/**
	 * Add a Row's message/info to the transaction's information.
	 * 
	 * @param row The string of the row's message/information.
	 */
	@JsonIgnore
	public void addRow(String row) {
		this.rows.add(row);
	}


	/**
	 * Add the messages from multiple rows to the transactions information.
	 * 
	 * @param rows The list of info about the matched rows.
	 */
	@JsonIgnore
	public void addRows(List<String> rows) {
		this.rows.addAll(rows);
	}


	/**
	 * This is a fallback, it produces a very basic JSON string.
	 * @return
	 */
	@JsonIgnore
	public String toBasicJSON() {
		// In the format string, '#', are replaced with
		// double-quotes ("\"") to avoid lots of slash-escaping nonsense.
		String fmt_string = 
				"{#FallbackTransactionMessage#: true,  "
						+ "#name#: #%s#,  #eventId#: #%s#,  " +
						"#eventTimestamp#: %d,  #rowCount#: %d,  " +
						"#message#: #%s#}".replace("#",  "\"");
		String msg = String.format(fmt_string, this.name, this.eventId,
				this.eventTimestamp, this.rows.size(), this.message);
		return msg;
	}

}
