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
 * An interface for transcoding time values. A class can implement this interface to transcode native values
 * to SDMX or vice-verca
 * 
 * @author fch
 *
 */
public interface TimeTranscoder {
	
	/**
	 * gets the time value and its native format and return an SDMX compliant time value
	 * @param timeValue
	 * @param format
	 * @return
	 */
	public String timeToSDMXTranscode(String timeValue);
	/**
	 * gets the SDMX time value and return a DSPL compliant time value (joda time format)
	 * @param timeValue
	 * @param format
	 * @return
	 */
	public String timeToDSPLTranscode(String timeValue);
	/**
	 * sets the frequency for the time
	 * @param freq
	 */
	public void setFrequency(String freq);
	/**
	 * sets the format that the time should be parsed
	 * @param format
	 */
	public void setPatternFormat(String format);
}
