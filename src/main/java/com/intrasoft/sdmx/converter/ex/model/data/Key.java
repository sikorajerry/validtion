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
package com.intrasoft.sdmx.converter.ex.model.data;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @created 2008 4:52:39
 * @version 1.0
 */
public abstract class Key extends AttachableArtefact {

	private Map<String, String> keyValues = new LinkedHashMap<String, String>();

	public Key() {
		super();
	}

	public java.util.Map<String, String> getKeyValues() {
		return keyValues;
	}

	public void setKeyValues(final java.util.Map<String, String> keyValues) {
		this.keyValues = keyValues;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (String key: keyValues.keySet()) {
			sb.append(key + ":" + keyValues.get(key) + " ");
		}
		
		return sb.toString();
	}

}