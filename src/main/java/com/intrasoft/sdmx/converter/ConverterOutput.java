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

import java.io.OutputStream;

import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;

import com.intrasoft.sdmx.converter.io.data.Formats;

/**
 * Class that groups converter output objects. 
 * 
 * @author Mihaela Munteanu
 * @since 10th of July 2017
 *
 */
public class ConverterOutput {
	
	private Formats outputFormat;
	
	private OutputStream outputStream; 
	
	private OutputConfig outputConfig;
	
	/** 
	 * Default public constructor 
	 */
	public ConverterOutput() {
		
	}

	/** 
	 * Full constructor.
	 * 
	 * @param outputFormat
	 * @param outputStream
	 * @param outputConfig
	 */
	public ConverterOutput(Formats outputFormat, OutputStream outputStream, OutputConfig outputConfig) {
		this.outputFormat = outputFormat;
		this.outputStream = outputStream;
		this.outputConfig = outputConfig;
	}

	/**
	 * @return the outputFormat
	 */
	public Formats getOutputFormat() {
		return outputFormat;
	}

	/**
	 * @param outputFormat the outputFormat to set
	 */
	public void setOutputFormat(Formats outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * @return the outputStream
	 */
	public OutputStream getOutputStream() {
		return outputStream;
	}

	/**
	 * @param outputStream the outputStream to set
	 */
	public void setOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	/**
	 * @return the outputConfig
	 */
	public OutputConfig getOutputConfig() {
		return outputConfig;
	}

	/**
	 * @param outputConfig the outputConfig to set
	 */
	public void setOutputConfig(OutputConfig outputConfig) {
		this.outputConfig = outputConfig;
	}

	@Override
	public String toString() {
		return "ConverterOutput{" +
				"outputFormat=" + outputFormat +
				", outputConfig=" + outputConfig +
                ", outputStream=" + outputStream +
                '}';
	}
}
