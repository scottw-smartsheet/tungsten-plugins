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

import java.util.List;

public class StringUtils {


	/**
	 * Join a List of Strings with a separator.
	 * 
	 * @param parts The Strings to join.
	 * @param sep The separator to use between parts members.
	 * @return The resulting string, or "" (empty or null parts list).
	 */
	public static String joinList(List<String> parts, String sep) {
		if (parts == null || parts.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String part : parts) {
			if (first) {
				first = false;
			} else {
				sb.append(sep);
			}
			sb.append(part);
		}
		return sb.toString();
	}

}