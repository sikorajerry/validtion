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

import org.sdmxsource.sdmx.api.model.data.KeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @created 2008 4:52:39
 * @version 1.0
 */
public class Observation extends AttachableArtefact {

	private String timeValue;
	private String value;

	public Observation() {
		super();
	}

	public Observation(org.sdmxsource.sdmx.api.model.data.Observation obs) {
		super();
		this.timeValue = obs.getObsTime();
		this.value = obs.getObservationValue();
		
		List<KeyValue> keyValues = obs.getAttributes();
		Map<String, String> attributesMap = new HashMap<String, String>();
		for (KeyValue key : keyValues) {
			attributesMap.put(key.getConcept(), key.getCode());
		}
		this.setAttributeValues(attributesMap);
	}
	
	public String getTimeValue() {
		return timeValue;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setTimeValue(final String newVal) {
		timeValue = newVal;
	}

	/**
	 * Getter for the value of the observation.<br>
	 * NOTE: Null for missing values!!!
	 * 
	 * @return
	 */
	public String getValue() {
		return value;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setValue(final String newVal) {
		value = newVal;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(timeValue + "+" + value);
		return sb.toString();
	}

}