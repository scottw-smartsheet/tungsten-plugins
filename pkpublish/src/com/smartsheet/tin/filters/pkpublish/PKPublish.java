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
 * Examine transactions, comparing them against filters, and publishing
 * the primary keys of the matching ones to a message queue.
 * 
 */

package com.smartsheet.tin.filters.pkpublish;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.smartsheet.tin.filters.common.FilterMetrics;
import com.smartsheet.tin.filters.common.JsonFilterException;
import com.smartsheet.tin.filters.common.ORCFormatter;
import com.smartsheet.tin.filters.common.Pair;
import com.smartsheet.tin.filters.common.TableKeyTracker;
import com.smartsheet.tin.filters.pkpublish.MQPublishWrapper.MQError;

/**
 * PKPublish is a Filter that uses a set of rules to identify "interesting"
 * transactions and/or row changes and then publishes messages about those
 * transactions and/or rows to a message queue.
 * 
 * @author scott.wimer@smartsheet.com
 * 
 */

public class PKPublish implements Filter {
	private static Logger logger = Logger.getLogger(PKPublish.class);
	private FilterMetrics metrics = new FilterMetrics();
	private String metricsRoutingKey = "pkpublish.stats";
	private PKPublishFilterRules rules;
	private String ruleFile;
	private long nextRuleFileLoadCheckTime;
	private long ruleFileLastModified;
	private long ruleFileCheckInterval;
	private String dbUrl;
	private String dbUser;
	private String dbPassword;

	private Integer statusMessageInterval;

	private MQPublishWrapper mq;

	private ORCFormatter orcFormatter;

	public PKPublish() {
		this.mq = new MQPublishWrapper();
		this.ruleFileCheckInterval = 30;
		this.statusMessageInterval = 5;
		this.rules = new PKPublishFilterRules();
	}

	public void setRuleFile(String ruleFile) throws ReplicatorException {
		this.ruleFile = ruleFile;
	}

	/**
	 * Set the time between rule file checks.
	 * 
	 * @param ruleFileCheckInterval
	 *            Seconds between rule file checks.
	 */
	public void setRuleFileCheckInterval(long ruleFileCheckInterval) {
		// Internally, we track timestamps with millisecond precision.
		this.ruleFileCheckInterval = 1000 * ruleFileCheckInterval;
	}

	public void setMessageQueueExchangeName(String exchangeName) {
		this.mq.config.exchangeName = exchangeName;;
	}

	public void setMessageQueueExchangeType(String exchangeType) {
		this.mq.config.exchangeType = exchangeType;
	}

	public void setMessageQueueServerName(String messageQueueServerName) {
		this.mq.config.setHost(messageQueueServerName);
	}

	public void setMessageQueueServerPort(int messageQueueServerPort) {
		this.mq.config.setPort(messageQueueServerPort);
	}

	public void setMessageQueueVHost(String messageQueueVHost) {
		this.mq.config.setVirtualHost(messageQueueVHost);
	}

	public void setMessageQueueExchangeIsDurable(boolean messageQueueExchangeIsDurable) {
		this.mq.config.exchangeIsDurable = messageQueueExchangeIsDurable;
	}

	/**
	 * @param messageQueueCloseTimeout
	 *            Timeout, in seconds.
	 */
	public void setMessageQueueCloseTimeout(int messageQueueCloseTimeout) {
		this.mq.config.closeTimeout = messageQueueCloseTimeout;
	}

	public void setMessageQueueUserName(String messageQueueUserName) {
		this.mq.config.setUserName(messageQueueUserName);
	}

	public void setMessageQueuePassword(String messageQueuePassword) {
		this.mq.config.setPassword(messageQueuePassword);
	}

	public void setMessageQueueHeartbeatInterval(
			int messageQueueHeartbeatInterval) {
		this.mq.config.setHeartBeatInterval(messageQueueHeartbeatInterval);
	}

	public void setMessageQueueEnableAutoRecovery(
			boolean messageQueueEnableAutoRecovery) {
		this.mq.config.setAutomaticRecoveryEnabled(
				messageQueueEnableAutoRecovery);
	}

	public void setMessageQueueRecoveryInterval(int recoveryIntevalSeconds) {
		this.mq.config.setNetworkRecoveryInterval(recoveryIntevalSeconds);
	}

	public void setMessageQueueConnectRetryLimit(
			int messageQueueConnectRetryLimit) {
		this.mq.config.retryLimit = messageQueueConnectRetryLimit;
	}

	public void setStatusMessageInterval(Integer interval) {
		this.statusMessageInterval = interval;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}


	/**
	 * This is called after all of the property setters have been called. The
	 * plug-in *should* be fully configured by this point.
	 */
	@Override
	public void configure(PluginContext context) throws ReplicatorException,
	InterruptedException {
		boolean ok = true;
		if (! this.mq.config.haveHost) {
			logger.error("Must specify messageQueueServerName");
			ok = false;
		}
		this.mq.markConfigComplete();

		if (this.dbUrl == null || this.dbUrl.isEmpty()) {
			logger.error("Must specify dbUrl");
			ok = false;
		}

		if (this.dbUser == null || this.dbPassword == null) {
			logger.error("Must specify dbUser and dbPassword");
			ok = false;
		}

		if (this.ruleFile == null) {
			logger.error("Must specify ruleFile");
			ok = false;
		}

		if (! ok) {
			throw new ReplicatorException(
					"PKPublish not properly configured.");
		}
		this.metrics = new FilterMetrics(this.statusMessageInterval, 0);
		this.orcFormatter = new ORCFormatter(this.metrics);
	}

	/**
	 * Do any resource allocation for the plug-in. We load the filtering rules
	 * and connect to the message queue.
	 */
	@Override
	public void prepare(PluginContext context) throws ReplicatorException,
	InterruptedException {
		// First, load our filtering rules.
		// Any errors in the rules are fatal.
		this.maybeReloadRulesFile(null);

		// Next, connect to the message queue.
		try {
			this.mq.connect();
		} catch (MQError e) {
			logger.error("Unable to connect to message queue, giving up:", e);
			throw new ReplicatorException(
					"Unable to connect to message queue");
		}

		// Finally, prepare the formatter.
		this.orcFormatter.prepare(this.dbUrl, this.dbUser, this.dbPassword);
	}

	@Override
	public void release(PluginContext context) throws ReplicatorException,
	InterruptedException {
		try {
			this.mq.releaseMQ();
		} catch (Throwable e) {
			logger.warn("PKPublish shutdown was not clean:", e);
		} finally {
			this.mq = null;
			this.rules = null;
			if (this.orcFormatter != null) {
				this.orcFormatter.release();
				this.orcFormatter = null;
			}
			logger.info("PKPublish shutdown");
		}
	}

	/**
	 * Process a replication event. Returns null to drop the event, otherwise
	 * the event continues through the replication pipeline.
	 */
	@Override
	public ReplDBMSEvent filter(ReplDBMSEvent event)
			throws ReplicatorException, InterruptedException {
		this.metrics.event();

		if (event == null) {
			this.metrics.nullEvent();
			return event;
		}
		if (event.getDBMSEvent().getClass() == DBMSEmptyEvent.class) {
			this.metrics.emptyEvent();
			return event;
		}

		ArrayList<DBMSData> event_data_list = event.getData();
		if (event_data_list == null || event_data_list.isEmpty()) {
			this.metrics.emptyDataList();
			return event;
		}

		this.maybeReloadRulesFile(event);
		this.metrics.eventTimestamp(event.getExtractedTstamp());

		/* Update the primary key tracker and the DML/DDL counters. */
		for (DBMSData edata : event_data_list) {
			if (edata.getClass() == RowChangeData.class) {
				this.metrics.dmlEvent();
			} else if (edata.getClass() == StatementData.class) {
				this.metrics.ddlEvent();
				TableKeyTracker kt = orcFormatter.getKeyTracker();
				StatementData sdata = (StatementData) edata;
				kt.maybeUpdateFromStatement(sdata);
			} else {
				logger.warn("Unknown DBMSData type: " + edata.getClass() +
						" event id: " + event.getEventId());
			}
		}

		// Have each of the filter rules try to match this transaction.
		List<TransactionMatchResult> results = this.rules.apply(event);

		// Publish any messages from the filter results.
		// If there are errors, we throw a ReplicatorException.
		// This way, the replicator will stop and we can restart it
		// without losing events.
		// FIXME:  Whether or not publishing errors are fatal should be configurable.
		for (TransactionMatchResult result : results) {
			logger.debug("publishing results for result: " + result.toString());
			publishResultRowFilterMessages(result, event);
			if (result.transactionFilterDidMatch()) {
				logger.debug("Result:  " + result.toString() + " Transaction filter did match");
				publishResultTransactionFilterMessage(result, event);
			} else {
				logger.debug("Result:  " + result.toString() + " Transaction filter did not match");
			}
		}

		try {
			this.maybeReportMetrics();
		} catch (Throwable e) {
			/* We refuse to fail just because we couldn't report our metrics. */
			logger.error("Unable to report metrics:", e);
			this.metrics.publishingError();
		}
		return event;
	}

	private void publishResultTransactionFilterMessage(
			TransactionMatchResult result, ReplDBMSEvent event)
					throws ReplicatorException {
		try {
			Pair<String, String> rk_msg = 
					result.getTransactionFilterMessageToPublish(this.orcFormatter);
			if (rk_msg != null) {
				this.mq.publishMessage(rk_msg.first, rk_msg.second);
			} else {
				logger.debug("Tfilter's msg to publish is null.");
			}
		} catch (PKPublishException e) {
			this.metrics.error();
			String err = "Failed to create TransactionFilter message for event id: " +
					event.getEventId() + " error: " + e.toString();
			logger.error(err, e);
		} catch (MQError e) {
			this.metrics.error();
			String err = "Unable to publish TransactionFilter message, event id: " +
					event.getEventId() + " error: " + e.toString();
			logger.error(err, e);
			throw new ReplicatorException(err + e.toString());
		}
	}


	private void publishResultRowFilterMessages(TransactionMatchResult result,
			ReplDBMSEvent event) throws ReplicatorException {
		try {
			for (Pair<String, String> rk_msg :
				result.getRowFilterMessagesToPublish(this.orcFormatter))
			{
				this.mq.publishMessage(rk_msg.first, rk_msg.second);
			}
		} catch (PKPublishException e) {
			this.metrics.error();
			String err = "Failed to create RowFilter message for event id: " +
					event.getEventId() + " error: " + e.toString();
			logger.error(err, e);
		} catch (MQError e) {
			this.metrics.error();
			String err = "Unable to publish RowFilter message, event id: " +
					event.getEventId() + " error: " + e.toString();
			logger.error(err, e);
			throw new ReplicatorException(err + e.toString());
		}
	}


	private void maybeReportMetrics() throws MQError {
		if (! this.metrics.shouldReport()) {
			return;
		}
		String report = this.metrics.makeReport();
		this.mq.publishMessage(this.metricsRoutingKey, report);
		logger.info(report);	// This should probably go to a custom logger.
	}


	/**
	 * Check if the rule file was changed periodically and reload if it was. We
	 * use the timestamps in the event stream as our "clock". This will be just
	 * fine so long as the clock doesn't go "backwards". That's problematic,
	 * since we *know* that clocks can go backwards from time to time. But, if
	 * the clock that is used for the events is using UTC, we shouldn't have too
	 * large of shift back in time. And that's good enough for me.
	 * 
	 * If we are unable to replace the existing rules for any reason, they are
	 * left unchanged.
	 * 
	 * @param event
	 *            The event used for a time source, or null.
	 * @throws PKPublishException
	 * @throws IOException
	 */
	private void maybeReloadRulesFile(ReplDBMSEvent event) {
		if (event != null &&
				(event.getExtractedTstamp().getTime() <
						this.nextRuleFileLoadCheckTime)) {
			// It's not time to check yet.
			return;
		}

		// Set the time for the next check.
		if (event == null) {
			this.nextRuleFileLoadCheckTime = this.ruleFileCheckInterval;
		} else {
			this.nextRuleFileLoadCheckTime = event.getExtractedTstamp()
					.getTime() + this.ruleFileCheckInterval;
		}

		// See if the file modification time has changed.
		// One down side of our strategy is that we will check a bad file
		// over and over and over and over until it is fixed. That will
		// slow us down.
		// 
		try {
			File fh = new File(this.ruleFile);
			long last_modified = fh.lastModified();
			if (last_modified != this.ruleFileLastModified)
			{
				this.metrics.ruleFileReload();
				PKPublishFilterRules new_rules = new PKPublishFilterRules();
				new_rules.loadRulesFromFile(this.ruleFile);
				ruleFileLastModified = last_modified;
				this.rules = new_rules;
			}
		} catch (IOException e) {
			this.metrics.ruleFileReloadError();
			logger.warn(String.format("Reload of updated rule file '%s' " +
					"failed, using prior rules", this.ruleFile), e);
		} catch (JsonFilterException e) {
			this.metrics.ruleFileReloadError();
			logger.warn(String.format("Reload of updated rule file '%s' " +
					"failed, using prior rules", this.ruleFile), e);
		}
	}

}
