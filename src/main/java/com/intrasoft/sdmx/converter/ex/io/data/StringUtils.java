/**
 *
 * Copyright 2015 EUROSTAT
 *
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * 	https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.ex.io.data;

/**
 * Utility class for String processing.
 * 
 * @author Mihaela Munteanu
 *
 */
public class StringUtils {
	
	/**
	 * Method for determining if a String is empty (null, or empty String)
	 * 
	 * @param item - the String to be checked
	 * @return true if the String is null or Empty String
	 */
	protected static boolean isEmpty(String item) {
		if (item == null || "".equals(item)) {
			return true;
		} else {
			return false;
		}
	}
}
