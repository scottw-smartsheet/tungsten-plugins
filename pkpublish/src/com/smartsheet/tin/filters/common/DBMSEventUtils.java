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

import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

public class DBMSEventUtils {

	public DBMSEventUtils() {
	}
	
	public static String makeEventPrefix(DBMSEvent event) {
		return String.format("<DBMSEvent eventId: %s>", event.getEventId());
	}
	
	public static String makeEventPrefix(ReplDBMSEvent event) {
		return String.format("<DBMSEvent eventId: %s>", event.getEventId());
	}
	

}
