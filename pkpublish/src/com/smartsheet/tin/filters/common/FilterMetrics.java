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
/**
 * Keep track of statistics about the event stream and the filter's 
 * behavior (errors and "surprises").
 */

import java.sql.Timestamp;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FilterMetrics {
	private static Logger logger = Logger.getLogger(FilterMetrics.class);
	/**
	 * These are the actual metrics collected.
	 * This POJO will be serialized using jackson.
	 */
	public class BareMetrics {
		public long nullEventCount = 0;
		public long emptyEventCount = 0;
		public long publishingErrorCount = 0;
		public long formattingErrorCount = 0;
		public long totalEventCount = 0;
		public long tableLookupErrorCount = 0;
		public long ruleFileReloadCount = 0;
		public long ruleFileReloadErrorCount = 0;
		public long dmlEventCount = 0;
		public long emptyDataListCount = 0;
		public long errorCount = 0;
		public long ddlEventCount = 0;
		public long ddlParseErrorCount = 0;
		public long dbConnectErrorCount = 0;
		public long dbLookupErrorCount = 0;
		public long dbLoookupNoPrimaryKeyCount = 0;

		public long reportStartTime = 0;
		public long reportEndTime = 0;
		public long totalEventsThisReport = 0;
		public long totalErrorsThisReport = 0;
	}

	private BareMetrics metrics;
	private ObjectMapper mapper;
	private static long defaultReportFrequency = 10;	// Seconds
	private static long defaultMinReqportInterval = 1;	// Seconds
	private long currentEventTS = 0;
	private long lastReportEventTS = 0;
	private long reportFrequency;	// In milliseconds
	private long lastReportTime = 0;
	private long minReportInterval; // In milliseconds
	private long reportErrorFrequency;
	private long lastReportErrorCount = 0;
	private long lastReportEventCount = 0;


	public FilterMetrics() {
		this(FilterMetrics.defaultReportFrequency,
				FilterMetrics.defaultMinReqportInterval);
	}

	/**
	 * 
	 * @param report_frequency 
	 */
	public FilterMetrics(long report_frequency, long min_report_interval) {
		this.mapper = new ObjectMapper();
		this.metrics = new BareMetrics();
		if (report_frequency == 0) {
			this.reportFrequency = FilterMetrics.defaultReportFrequency * 1000;
		} else {
			this.reportFrequency = report_frequency * 1000;
		}
		if (min_report_interval == 0) {
			this.minReportInterval = FilterMetrics.defaultMinReqportInterval * 1000;
		} else {
			this.minReportInterval = min_report_interval * 1000;
		}
		logger.info("FilterMetrics reportFrequency: " + this.reportFrequency + 
				", minReportInterval: " + this.minReportInterval);
	}

	public void event() {
		++this.metrics.totalEventCount;
	}

	public void error() {
		++this.metrics.errorCount;
	}


	public void nullEvent() {
		++this.metrics.nullEventCount;
		error();
	}

	public void emptyEvent() {
		++this.metrics.emptyEventCount;
	}

	public void emptyDataList() {
		++this.metrics.emptyDataListCount;
	}

	public void publishingError() {
		++this.metrics.publishingErrorCount;
		error();
	}

	public void formattingError() {
		++this.metrics.formattingErrorCount;
		error();
	}

	public void ruleFileReload() {
		++this.metrics.ruleFileReloadCount;
	}

	public void ruleFileReloadError() {
		++this.metrics.ruleFileReloadErrorCount;
		error();
	}

	public void ddlEvent() {
		++this.metrics.ddlEventCount;
	}

	public void ddlParseError() {
		++this.metrics.ddlParseErrorCount;
		error();
	}

	public void dmlEvent() {
		++this.metrics.dmlEventCount;
	}

	public void dbConnectError() {
		++this.metrics.dbConnectErrorCount;
		error();
	}

	public void dbLookupError() {
		++this.metrics.dbLookupErrorCount;
		error();
	}

	public void dbNoPrimaryKey() {
		++this.metrics.dbLoookupNoPrimaryKeyCount;
	}

	/**
	 * Register the timestamp of the event.
	 * @param event_ts
	 */
	public void eventTimestamp(Timestamp event_ts) {
		this.currentEventTS = event_ts.getTime();
	}

	/**
	 * Return true if the statistics should be reported.
	 * Statistics are reported if:
	 *    - Enough time (measured by event TS) has elapsed (reportFrequency)
	 *    - Enough error-type events have occurred (reportErrorFrequency)
	 *    - Enough wallclock time has elapsed to avoid a flood of reports.
	 * @return
	 */
	public boolean shouldReport() {
		boolean should_report = false;
		if ((this.currentEventTS - this.lastReportEventTS) > 
		this.reportFrequency) {
			should_report = true;
		} else if ((this.metrics.errorCount - this.lastReportErrorCount) > 
		this.reportErrorFrequency) {
			should_report = true;
		}

		if (should_report) {
			long current_time = System.currentTimeMillis();
			if ((current_time - this.lastReportTime) < this.minReportInterval) {
				should_report = false;
			}
		}
		return should_report;
	}

	public String makeReport() {
		String report = null;
		try {
			report = this.toJSON();
			this.lastReportTime = System.currentTimeMillis();
			this.metrics.totalEventsThisReport = 
					this.metrics.totalEventCount - this.lastReportEventCount;
			this.metrics.totalErrorsThisReport =
					this.metrics.errorCount - this.lastReportErrorCount;
			this.lastReportEventCount = this.metrics.totalEventCount;
			this.lastReportErrorCount = this.metrics.errorCount;
			this.lastReportEventTS = this.currentEventTS;
			this.metrics.reportStartTime = this.metrics.reportEndTime;
			this.metrics.reportEndTime = this.lastReportTime;
		} catch (JsonProcessingException e) {
			logger.warn("Error making report:", e);
			report = "{\"FilterMetrics\": { \"Error\": \"" + e + "\"} }";
		}
		return report;
	}


	public String toJSON() throws JsonProcessingException {
		return this.mapper.writeValueAsString(this.metrics);
	}

}
