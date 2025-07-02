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
package com.intrasoft.sdmx.converter.ex.model.data.cross;

import com.intrasoft.sdmx.converter.ex.model.data.Key;

import java.util.ArrayList;
import java.util.List;

/**
 * @created 2008 4:52:38
 * @version 1.0
 */
public class XSSection extends Key {

	private List<XSObservation> observations = new ArrayList<XSObservation>();

	public XSSection() {
		super();
	}

	public List<XSObservation> getObservations() {
		return observations;
	}

	public void setObservations(final List<XSObservation> observations) {
		this.observations = observations;
	}

}