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
 * Track the primary key columns of tables.
 */
package com.smartsheet.tin.filters.common;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.StatementData;

import org.apache.log4j.Logger;

public class TableKeyTracker {
	private Logger logger = Logger.getLogger(TableKeyTracker.class);

	private HashMap<String, HashMap<String, TableKeyInfo>> keyCache;
	private Database dbConn;
	private long lastConnectionTime;
	private long reconnectTimeoutSeconds;
	private String dbUser;
	private String dbPassword;
	private String dbUrl;
	private FilterMetrics metrics;

	public TableKeyTracker(String dbUrl, String dbUser, String dbPassword,
			FilterMetrics metrics) {
		this.dbUrl = dbUrl;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.keyCache = new HashMap<String, HashMap<String, TableKeyInfo>>();
		this.reconnectTimeoutSeconds = 10;
		this.metrics = metrics;
	}

	public void init() throws TableKeyTrackerException {
		try {
			this.logger.debug("creating db with url: '" + this.dbUrl +
					"', user: '" + this.dbUser + "'");
			this.dbConn = DatabaseFactory.createDatabase(this.dbUrl,
					this.dbUser, this.dbPassword);
			this.dbConn.connect();
			this.lastConnectionTime = System.currentTimeMillis();
		} catch (SQLException e) {
			logger.error("Unable to connect to database", e);
			this.metrics.dbConnectError();
			throw new TableKeyTrackerException("Unable to connect to database");
		}
		logger.debug("Initted TableKeyTracker to: " + dbUrl);
	}

	/**
	 * Statement events can include SQL to change the table(s) or schema(s)
	 * whose keys we are caching. This method handles the invalidation of cached
	 * data.
	 * 
	 * @param sdata
	 *            The DBMSData that is a StatementData instance.
	 */
	public void maybeUpdateFromStatement(StatementData sdata) {
		String query = sdata.getQuery();
		if (query == null) {
			logger.info("Query was null, trying harder.");
			query = new String(sdata.getQueryAsBytes());
			if (query == null || query.isEmpty()) {
				logger.info("Query was well and truly null/empty skipping it.");
				return;
			}
			logger.debug("Query is: " + query);
		}
		try {
			SqlOperationMatcher sqlMatcher = new MySQLOperationMatcher();
			SqlOperation sqlOp = sqlMatcher.match(query);
			if (sqlOp == null) {
				logger.warn("Couldn't understand DDL, resetting keyCache.");
				this.keyCache.clear();
				this.metrics.ddlParseError();
				return;
			}

			if (sqlOp.getOperation() == SqlOperation.DROP) {
				String dbName = sqlOp.getSchema();
				if (dbName == null) {
					logger.warn("DROP with no schema, resetting keyCache.");
					this.keyCache.clear();
					this.metrics.ddlParseError();
					return;
				}
				if (sqlOp.getObjectType() == SqlOperation.SCHEMA) {
					this.keyCache.remove(dbName.toUpperCase());
					logger.debug("Removed schema entry from key cache.");
				} else if (sqlOp.getObjectType() == SqlOperation.TABLE) {
					String defaultDB = sdata.getDefaultSchema();
					this.removeTableEntry(dbName, defaultDB, sqlOp.getName());
				}
			} else if (sqlOp.getOperation() == SqlOperation.ALTER) {
				this.removeTableEntry(sqlOp.getSchema(),
						sdata.getDefaultSchema(), sqlOp.getName());
			} else {
				// Do nothing, nobody cares.
			}
		} catch (Throwable e) {
			logger.warn("Error processing statement: '" + query +
					"' ignoring it:", e);
			this.metrics.ddlParseError();
		}
	}

	/**
	 * Remove a table's entry from the key cache.
	 * 
	 * @param schema
	 * @param default_schema
	 * @param table_name
	 */
	private void removeTableEntry(String schema, String default_schema,
			String table_name) {
		if (schema != null) {
			if (this.keyCache.containsKey(schema.toUpperCase())) {
				this.keyCache.get(schema.toUpperCase()).remove(
						table_name.toUpperCase());
				logger.debug("Removed table's entry from key cache.");
			}
		} else if (default_schema != null) {
			if (this.keyCache.containsKey(default_schema.toUpperCase())) {
				this.keyCache.get(default_schema.toUpperCase()).remove(
						table_name.toUpperCase());
				logger.debug("Removed table's entry from key cache.");
			}
		}
	}

	/**
	 * Look up the primary key column(s) for the table in a OneRowChange.
	 * 
	 * @return The TableKeyInfo for the table.
	 */
	public TableKeyInfo lookupTableKey(OneRowChange orc) {
		String schema_name = orc.getSchemaName().toUpperCase();
		String table_name = orc.getTableName().toUpperCase();

		if (! this.keyCache.containsKey(schema_name)) {
			this.keyCache.put(schema_name, 
					new HashMap<String, TableKeyInfo>());
		}
		HashMap<String, TableKeyInfo> dbCache = this.keyCache.get(schema_name);

		if (!dbCache.containsKey(table_name) || orc.getTableId() == -1) {
			// This table has not yet been cached, or the cache is out of date.
			dbCache.remove(table_name);
			boolean has_pk = true;

			this.reconnectIfNeeded();
			Table orc_table;
			try {
				orc_table = this.dbConn.findTable(orc.getSchemaName(),
						orc.getTableName());
			} catch (SQLException e) {
				String err = String.format("Could not find table info " +
						"for '%s'.'%s' from OneRowChange",
						orc.getSchemaName(), orc.getTableName());
				logger.error(err, e);
				this.metrics.dbLookupError();
				return null;
			}
			if (orc_table == null) {
				dbCache.remove(table_name);
				logger.error("Unable to lookup table '" + table_name +
						"' in schema '" + schema_name + "'.");
				this.metrics.dbLookupError();
				return null;
			}

			Key primary_key = orc_table.getPrimaryKey();
			List<Column> keys = null;
			if (primary_key != null) {
				keys = primary_key.getColumns();
			}

			if (primary_key == null || keys == null || keys.isEmpty()) {
				logger.warn("Table '" + schema_name + "." + table_name + 
						"' has no primary key.");
				this.metrics.dbNoPrimaryKey();
				has_pk = false;
			}

			if (has_pk) {
				TableKeyInfo tki = new TableKeyInfo();
				for (Column col : keys) {
					tki.addKey(col);
				}
				dbCache.put(table_name, tki);
			} else {
				dbCache.put(table_name, null);
			}

			logger.info("Added TableKeyInfo for '" + schema_name + "."
					+ table_name + "'.");
		}

		TableKeyInfo tki = dbCache.get(table_name);
		return tki;
	}

	/**
	 * @return the reconnectTimeoutSeconds
	 */
	public long getReconnectTimeoutSeconds() {
		return this.reconnectTimeoutSeconds;
	}

	/**
	 * @param reconnectTimeoutSeconds
	 *            the reconnectTimeoutSeconds to set
	 */
	public void setReconnectTimeoutSeconds(long reconnectTimeoutSeconds) {
		this.reconnectTimeoutSeconds = reconnectTimeoutSeconds;
	}

	private void reconnectIfNeeded() {
		long current_time = System.currentTimeMillis();
		if (this.reconnectTimeoutSeconds > 0
				&& current_time - this.lastConnectionTime > 
		(this.reconnectTimeoutSeconds * 1000)) {
			this.dbConn.close();
			try {
				this.dbConn.connect();
				this.lastConnectionTime = current_time;
			} catch (SQLException e) {
				logger.error("Unable to connect to database:", e);
				this.metrics.dbConnectError();
				// Don't update the last connect time -- this way we retry next
				// time.
			}
		}
	}

	public void release() {
		try {
			if (this.dbConn != null) {
				this.dbConn.close();
				this.dbConn = null;
			}
		} catch (Exception e) {
			logger.warn("Error closing database connection:", e);
		}

		if (this.keyCache != null) {
			this.keyCache.clear();
			this.keyCache = null;
		}
	}

}
