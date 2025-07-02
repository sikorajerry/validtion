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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;

/**
 * Implementation of the Reader interface for reading DataMessage instances.
 * This implementation has the particularity that instead of reading a data
 * message from an input stream, it reads directly from a DataMessage instance.
 */
public class DataMessageReader implements Reader {

	private static Logger logger = LogManager.getLogger(DataMessageReader.class);

	private String lineEnd = System.getProperty("line.separator");

	/* properties required for implementing the Reader interface */
	private Writer writer;
	private DataMessage message;

	public void setDataMessage(final DataMessage message) {
		this.message = message;
	}

	/**
	 * Implements Reader interface method.
	 */
	public DataMessage readData(final InputStream is, final InputParams params) throws Exception {
		return this.message;
	}

	/**
	 * Implements Reader interface method.
	 */
	public void setInputStream(final InputStream is) {
		// nothing to do, InputStream is not used by this Reader
	}

	/**
	 * Implements Reader interface method.
	 */
	public void setInputParams(final InputParams params) {
		// nothing to do, InputParams is not used by this Reader
	}

	/**
	 * Implements Reader interface method.
	 */
	public void setWriter(final Writer writer) {
		this.writer = writer;
	}

	/**
	 * Implements Reader interface method.
	 */
	public boolean isReady() throws Exception {
		boolean ready = true;
		String errorMessage = "Reader is not ready. Missing:";
		if (message == null) {
			ready = false;
			errorMessage += "data message, ";
		}
		if (writer == null) {
			ready = false;
			errorMessage += "writer, ";
		}
		if (!ready) {
			errorMessage = errorMessage.substring(0, errorMessage.length() - 2) + ".";
			throw new Exception(errorMessage);
		}
		return ready;
	}

	/**
	 * Implements Reader interface method.
	 */
	public void readData(final InputStream is, final InputParams params, final Writer writer) throws Exception {
		setInputStream(is);
		setInputParams(params);
		setWriter(writer);
		readData();
	}

	/**
	 * Implements Reader interface method.
	 */
	public void readData() throws Exception {
		logger.info(this.getClass().getName() + ".readData");
		try {
			isReady();
			parseDataMessage(message);
		} catch (Exception e) {
		    final String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	private void parseDataMessage(final DataMessage message) throws Exception {
		writer.writeHeader(message.getHeader());
		final DataSet dataset = message.getDataSet();
		writer.writeEmptyDataSet(dataset);
		for (final GroupKey gk : dataset.getGroups()) {
			writer.writeGroupKey(gk);
		}
		for (final TimeseriesKey tk : dataset.getSeries()) {
			writer.writeTimeseriesKey(tk);
		}
		writer.closeWriter();
	}

}
