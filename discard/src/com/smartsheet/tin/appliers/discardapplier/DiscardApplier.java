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
package com.smartsheet.tin.appliers.discardapplier;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.applier.MySQLApplier;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Discard all DBMS transactions, keeping track of the position in the input stream.
 * Works with Tungsten Replicator 2.2.1.  Does not work with 3.x.x versions
 * @author <a href="mailto:scott.wimer@smartsheet.com">Scott Wimer</a>
 * @version 0.0.1
 */
public class DiscardApplier extends MySQLApplier {
	static Logger logger = Logger.getLogger(DiscardApplier.class);
	private ReplDBMSHeader lastProcessedEvent = null;

	/**
	 * Apply a DBMS event, this discards the event, but records the updated position.
	 */
	public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
			boolean doRollback) throws ReplicatorException,  ConsistencyException {
		// Skip previously discarded events.  This can happen during restart.
		if (lastProcessedEvent != null &&
				(lastProcessedEvent.getLastFrag() &&
				lastProcessedEvent.getSeqno() >= header.getSeqno()) &&
				! (event instanceof DBMSEmptyEvent)) {
			return;
		}

		long appliedLatency = (System.currentTimeMillis() -
				event.getSourceTstamp().getTime()) / 1000;
		try {
			updateCommitSeqno(header, appliedLatency);
		} catch (SQLException e) {
			logger.error("Error updating sequence tracking table for seqno: " +
					header.getSeqno(), e);
			e.printStackTrace();
			throw new ApplierException("Failed to update sequence tracking table", e);
		}

        logger.debug("Discarded event: " + header.getSeqno());
		lastProcessedEvent = header;
		
		// Since we are dropping every event, there's no point in a consistency check
		// and no target that it makes sense to conduct a heartbeat with/to.
	}

	private void updateCommitSeqno(ReplDBMSHeader header, long appliedLatency)
			throws SQLException {
		commitSeqnoTable.updateLastCommitSeqno(taskId, header, appliedLatency);
	}

	 
}
