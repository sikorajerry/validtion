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

import org.sdmxsource.sdmx.api.model.header.HeaderBean;

public class MessageBean {
    
	/** The Header. */
	private HeaderBean header = null;

	/**
	 * Getter for the header.
	 * 
	 * @return the header
	 */
	public HeaderBean getHeader() {
		return this.header;
	}

	/**
	 * Setter for the header.
	 * @param header the header to set
	 */
	public void setHeader(final HeaderBean header) {
		this.header = header;
	}
}
