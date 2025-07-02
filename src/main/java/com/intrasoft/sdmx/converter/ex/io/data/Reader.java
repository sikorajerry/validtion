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

import java.io.InputStream;

/**
 * Public interface that every reader class must implement.
 */
public interface Reader {

	/**
	 * Sets the data input stream for this reader.
	 * @param is - the data input stream.
	 */
	public void setInputStream(InputStream is);

	/**
	 * Sets the InputParams instance specifying the format of the data stream as
	 * well as any additional required properties.
	 * @param params - the properties for reading data of various formats.
	 */
	public void setInputParams(InputParams params);

	/**
	 * Associates a writer with this reader.
	 * @param writer - the associated writer instance that implements the Writer
	 *            interface.
	 */
	public void setWriter(Writer writer);

	/**
	 * Basic method that every reader must implement. Each implementation must
	 * ensure that all required parameters for reading the data input stream are
	 * set. Note : Each implementation of method 'readData' should internally
	 * call this method before reading the data input stream.
	 * @return true if all required parameters for reading the data input stream
	 *         are set.
	 * @throws Exception
	 */
	public boolean isReady() throws Exception;

	/**
	 * Reads a data input stream and passes the constructed sdmx data beans to
	 * the specified writer. The format of the data stream as well as any
	 * additional required properties is specified in a InputParams instance.
	 * This method signature was introduced since verson 1.6 of the SDMX
	 * Converter. It has the advantage of being able to handle large datasets,
	 * since the constructed data beans are passed to the writer instance and
	 * therefore do not stay resident on system memory. Note : Each
	 * implementation should internally call method 'isReady' before reading the
	 * data input stream.
	 * @param is - the data input stream.
	 * @param params - the properties for reading data of various formats.
	 * @param writer - the associated writer instance that implements the Writer
	 *            interface.
	 * @throws Exception
	 */
	public void readData(InputStream is, InputParams params, Writer writer) throws Exception;

	/**
	 * Reads a data input stream and passes the constructed sdmx data beans to
	 * the associated writer. The format of the data stream as well as any
	 * additional required properties is specified in a InputParams instance.
	 * This method signature was introduced since verson 1.6 of the SDMX
	 * Converter. It has the advantage of being able to handle large datasets,
	 * since the constructed data beans are passed to the writer instance and
	 * therefore do not stay resident on system memory. Note : Each
	 * implementation should internally call method 'isReady' before reading the
	 * data input stream.
	 * @param is - the data input stream.
	 * @param params - the properties for reading data of various formats.
	 * @param writer - the associated writer instance that implements the Writer
	 *            interface.
	 * @throws Exception
	 */
	public void readData() throws Exception;

}
