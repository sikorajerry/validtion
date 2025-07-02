/**
 * Copyright 2015 EUROSTAT
 * <p>
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter;

import org.estat.sdmxsource.config.InputConfig;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;

import com.intrasoft.sdmx.converter.io.data.Formats;

/**
 * Class that groups converter input objects. 
 * 
 * @author Mihaela Munteanu
 * @since 10th of July 2017
 *
 */
public class ConverterInput {
	
	private Formats inputFormat;
	
	private ReadableDataLocation inputDataLocation;
	
	private InputConfig inputConfig;

	/**
	 * Default public constructor
	 */
	public ConverterInput() {
		
	}
	
	/**
	 * Full constructor.
	 * 
	 * @param inputFormat
	 * @param inputDataLocation
	 * @param inputConfig
	 */
	public ConverterInput(Formats inputFormat, ReadableDataLocation inputDataLocation, InputConfig inputConfig) {
		this.inputFormat = inputFormat; 
		this.inputDataLocation = inputDataLocation;
		this.inputConfig = inputConfig;
	}
	
	/**
	 * @return the inputFormat
	 */
	public Formats getInputFormat() {
		return inputFormat;
	}

	/**
	 * @param inputFormat the inputFormat to set
	 */
	public void setInputFormat(Formats inputFormat) {
		this.inputFormat = inputFormat;
	}

	/**
	 * @return the inputDataLocation
	 */
	public ReadableDataLocation getInputDataLocation() {
		return inputDataLocation;
	}

	/**
	 * @param inputDataLocation the inputDataLocation to set
	 */
	public void setInputDataLocation(ReadableDataLocation inputDataLocation) {
		this.inputDataLocation = inputDataLocation;
	}

	/**
	 * @return the inputConfig
	 */
	public InputConfig getInputConfig() {
		return inputConfig;
	}

	/**
	 * @param inputConfig the inputConfig to set
	 */
	public void setInputConfig(InputConfig inputConfig) {
		this.inputConfig = inputConfig;
	}


    @Override
    public String toString() {
        return "ConverterInput{inputFormat=" + inputFormat +
                ", inputConfig=" + inputConfig +
                ", inputDataLocation=" + inputDataLocation +
                '}';
    }
}
