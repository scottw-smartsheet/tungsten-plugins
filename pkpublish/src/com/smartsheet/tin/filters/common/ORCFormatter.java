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
 * Format OneRowChange records for use by the event filter.
 * 
 * TODO: This package probably needs to be subclassed into children
 * that handle specific types of formatting.
 */
package com.smartsheet.tin.filters.common;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartsheet.tin.filters.common.TableKeyInfo.KeyPair;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

public class ORCFormatter {

	/**
	 * This class maps the indexes from an array of key columns to the indexes
	 * of an array of row values.
	 * This is a helper to deal with the fact that we can't guarantee that the
	 * order of rows in a table is always consistent.
	 * 
	 * @author scott.wimer@smartsheet.com
	 *
	 */
	private class Position2Index {
		HashMap<Integer, Integer> pos2idx;

		public Position2Index(OneRowChange orc, boolean useColumns) {
			this.pos2idx = new HashMap<Integer, Integer>();
			ArrayList<OneRowChange.ColumnSpec> col_specs;
			if (useColumns) {
				col_specs = orc.getColumnSpec();
			} else {
				col_specs = orc.getKeySpec();
			}
			for (int i = 0; i < col_specs.size() - 1; ++i) {
				this.pos2idx.put(col_specs.get(i).getIndex(), i);
			}
		}

		public int getIndex(int pos) {
			return this.pos2idx.get(pos);
		}
	}

	private static Logger logger = Logger.getLogger(ORCFormatter.class);
	private ObjectMapper mapper;
	private TableKeyTracker keyTracker;
	private FilterMetrics metrics;

	public ORCFormatter(TableKeyTracker key_tracker, FilterMetrics metrics) {
		this.mapper = new ObjectMapper();
		this.keyTracker = key_tracker;
		this.metrics = metrics;
	}

	/**
	 * Return schemaName.tableName for a OneRowChange.
	 * 
	 * @param orc
	 * @return
	 */
	public String getFullyQualifiedTableName(OneRowChange orc) {
		return orc.getSchemaName() + "." + orc.getTableName();
	}

	/**
	 * Create an array of JSON strings for each row in a OneRowChange.
	 * 
	 * @param orc The OneRowChange
	 * @param event The DB event/transaction.
	 * @return List of JSON-formatted Strings.
	 */
	public ArrayList<String> makeJSONStringsFromORC(OneRowChange orc,
			ReplDBMSEvent event) {
		ArrayList<String> messages = new ArrayList<String>();

		TableKeyInfo tki = keyTracker.lookupTableKey(orc);
		if (tki == null) {
			// Skip tables that have no primary key.
			return messages;
		}

		ORCPrimaryKeyInfo msg_pojo = new ORCPrimaryKeyInfo(
				event.getSourceId(), event.getShardId(),
				orc.getSchemaName(), orc.getTableName(),
				orc.getAction().toString(), event.getEventId(),
				event.getExtractedTstamp().getTime());

		if (orc.getAction() != ActionType.DELETE) {
			ArrayList<ArrayList<OneRowChange.ColumnVal>> columns_values =
					orc.getColumnValues();
			Position2Index pos2idx = new Position2Index(orc, true);
			for (ArrayList<OneRowChange.ColumnVal> row_vals : columns_values) {
				messages.add(makeRowEntry(msg_pojo, tki, pos2idx, row_vals));
			}
		} else {
			// NOTE: for DELETE events, the values are in the keys array.
			ArrayList<ArrayList<OneRowChange.ColumnVal>> keys_values = orc
					.getKeyValues();
			Position2Index pos2idx = new Position2Index(orc, false);
			for (ArrayList<OneRowChange.ColumnVal> row_vals : keys_values) {
				messages.add(makeRowEntry(msg_pojo, tki, pos2idx, row_vals));
				// rowMeter.mark();
			}
		}
		return messages;
	}

	/**
	 * Create the JSON entry for a row from a OneRowChange.
	 * Whether the row_vals are ColumnVals or KeyVals depends on whether the
	 * OneRowChange was for an INSERT/UPDATE (Column values) or a DELETE (Key
	 * values).
	 * 
	 * @param msg_pojo The partially filled out object to add the key to.
	 * @param tki Specifies which columns are keys.
	 * @param pos2idx Mapping between key columns and the indexes of row_vals.
	 * @param row_vals These can be the Column or Key values.
	 * @return The JSON string representation of the msg_pojo.
	 */
	private String makeRowEntry(ORCPrimaryKeyInfo msg_pojo,
			TableKeyInfo tki, Position2Index pos2idx,
			ArrayList<ColumnVal> row_vals) {
		try {
			// Discard the key values for any previous row.
			msg_pojo.resetKey();

			for (TableKeyInfo.KeyPair kp : tki.getKeys()) {
				int idx = kp.getIndex();
				logger.debug(String.format(
						"Key column '%s' at idx: %d pos2idx.get(%d)=%d",
						kp.getColumnName(), idx, idx, pos2idx.getIndex(idx)));
				msg_pojo.addKey(kp.getColumnName(), kp.getColumnType(),
						kp.getColumnTypeDesc(),
						row_vals.get(pos2idx.getIndex(idx)).getValue());
			}
			try {
				return this.mapper.writeValueAsString(msg_pojo);
			} catch (JsonProcessingException e) {
				logger.error("Failed using mapper to write JSON:", e);
				return msg_pojo.toBasicJSON();
			}
		} catch (Exception e) {
			logger.error("Failed creating JSON row entry", e);
			// NOTE: This might not be valid JSON, depends on the error msg.
			return "{\"ERROR\": \"" + e.toString() + "\" }";
		}

	}

	public TableKeyTracker getKeyTracker() {
		return this.keyTracker;
	}

	public ObjectMapper getMapper() {
		return this.mapper;
	}


	public void release() {
		if (this.keyTracker != null) {
			this.keyTracker.release();
			this.keyTracker = null;
		}
	}

}