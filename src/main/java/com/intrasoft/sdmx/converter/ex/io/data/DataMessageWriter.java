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

import com.intrasoft.sdmx.converter.ex.model.data.DataMessage;
import com.intrasoft.sdmx.converter.ex.model.data.DataSet;
import com.intrasoft.sdmx.converter.ex.model.data.GroupKey;
import com.intrasoft.sdmx.converter.ex.model.data.TimeseriesKey;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.OutputStream;

/**
 * Implementation of the Writer interface for writing DataMessage instances.
 * This implementation has the particularity that instead of writing a data
 * message to an output stream, it constructs directly a DataMessage instance.
 */
public class DataMessageWriter implements Writer {

	private DataMessage message = new DataMessage();
	private DataSet dataSet;

	/**
	 * Implements Writer interface method.
	 */
	public void writeData(final DataMessage message, final OutputStream os, final InputParams params) throws Exception {
	    final DataMessageReader dreader = new DataMessageReader();
		dreader.setDataMessage(message);
		dreader.readData(null, params, this);
	}

	/**
	 * Implements Writer interface method.
	 */
	public void setOutputStream(final OutputStream os) {
		// nothing to do, OutputStream is not used by this Writer
	}

	/**
	 * Implements Writer interface method.
	 */
	public void setInputParams(final InputParams params) {
		// nothing to do, InputParams is not used by this Writer
	}

	/**
	 * Implements Writer interface method.
	 */
	public boolean isReady() throws Exception {
		return true;
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeHeader(final HeaderBean header) throws Exception {
		message.setHeader(header);
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeEmptyDataSet(final DataSet dataSet) throws Exception {
		this.dataSet = dataSet;
		message.setDataSet(this.dataSet);
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeGroupKey(final GroupKey grKey) throws Exception {
		dataSet.getGroups().add(grKey);
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeTimeseriesKey(final TimeseriesKey tsKey) throws Exception {
		dataSet.getSeries().add(tsKey);
	}

	/**
	 * Implements Writer interface method.
	 */
	public void closeWriter() {
		// nothing to do, no closing actions for this Writer
	}

	public DataMessage getDataMessage() {
		return message;
	}

	public void releaseResources() throws Exception {		
		// nothing to do for this Writer
	}

}
