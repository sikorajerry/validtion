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

import com.intrasoft.sdmx.converter.ex.model.data.DataSet;
import com.intrasoft.sdmx.converter.ex.model.data.GroupKey;
import com.intrasoft.sdmx.converter.ex.model.data.TimeseriesKey;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.OutputStream;

/**
 * 
 * Public interface that every writer class must implement.
 * 
 */
public interface Writer {

	
	/**
	 * Sets the data output stream for this writer.
	 * @param os - the data output stream.
	 */
	public void setOutputStream(OutputStream os);

	/**
	 * Sets the InputParams instance specifying the format of the output data stream as well as any additional required
	 * properties.
	 * @param params - the properties for writing data of various formats.
	 */
	public void setInputParams(InputParams params);
	
	/**
	 * Ensure that all required parameters for writing the data output stream are set. 
	 * Note : Each implementation of methods 'writeHeader', 'writeEmptyDataSet',
	 * 'writeGroupKey', 'writeTimeseriesKey' and 'closeWriter' should internally call this method before writing to the
	 * data output stream.
	 * @return true if all required parameters for reading the data input stream are set.
	 * @throws Exception
	 */
	public boolean isReady() throws Exception;

	/**
	 * Using the given HeaderBean writes the header part of a data message on the writer's data output stream. Note :
	 * Each implementation should internally call method 'isReady' before writing to the data output stream.
	 * @param header - the given HeaderBean
	 * @throws Exception
	 */
	public void writeHeader(HeaderBean header) throws Exception;

	/**
	 * Writes the dataset-level informaton of a data message, passed in the given DataSet, on the writer's data output
	 * stream. The given DataSet is an 'empty' DataSet bean, meaning that it does not contain any TimeseriesKey or
	 * GroupKey beans. Note : Each implementation should internally call method 'isReady' before writing to the data
	 * output stream.
	 * @param dataSet - the given DataSet
	 * @throws Exception
	 */
	public void writeEmptyDataSet(DataSet dataSet) throws Exception;

	/**
	 * Writes the group-level information, passed in the given GroupKey, on the writer's data output stream. The given
	 * GroupKey is an 'empty' GroupKey bean, meaning that it does not contain any TimeseriesKey beans. Note : Each
	 * implementation should internally call method 'isReady' before writing to the data output stream.
	 * @param groupKey - the given GroupKey
	 * @throws Exception
	 */
	public void writeGroupKey(GroupKey grKey) throws Exception;

	/**
	 * Writes the series-level and observation-level information, passed in the given TimeseriesKey, on the writer's data
	 * output stream. The given TimeseriesKey contains all its Observation beans. Note : Each implementation should
	 * internally call method 'isReady' before writing to the data output stream.
	 * @param tsKey - the given TimeseriesKey
	 * @throws Exception
	 */
	public void writeTimeseriesKey(TimeseriesKey tsKey) throws Exception;

	/**
	 * Writes any closing information required by the data message and closes the writer's data output stream. Note :
	 * Each implementation should internally call method 'isReady' before writing to the data output stream.
	 * @throws Exception
	 */
	public void closeWriter() throws Exception;
	
	/**
	 * Close all allocated resources (streams, temp files etc.) during writing 
	 * @throws Exception
	 */
	public void releaseResources() throws Exception;

}
