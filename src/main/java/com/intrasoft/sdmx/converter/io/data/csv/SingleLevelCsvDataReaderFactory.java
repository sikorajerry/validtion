/**
 * Copyright (c) 2015 European Commission.
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl5
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.io.data.csv;

import org.estat.struval.engine.impl.SingleLevelCsvReadableDataLocation;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.factory.DataReaderFactory;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.stereotype.Service;

@Service
public class SingleLevelCsvDataReaderFactory implements DataReaderFactory {

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData, DataStructureBean dsd,
			DataflowBean dataflowBean) {
		if(sourceData instanceof SingleLevelCsvReadableDataLocation){
			return new SingleLevelCsvDataReaderEngine(sourceData, dsd, dataflowBean, null, ((SingleLevelCsvReadableDataLocation)sourceData).getCsvInputConfig(), new FirstFailureExceptionHandler());
		}   		 
		return null;
	}

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData,
			SdmxBeanRetrievalManager retrievalManager) {
		if(sourceData instanceof SingleLevelCsvReadableDataLocation){
			return new SingleLevelCsvDataReaderEngine(sourceData, null, null, retrievalManager, ((SingleLevelCsvReadableDataLocation)sourceData).getCsvInputConfig(), new FirstFailureExceptionHandler());
		}
		return null;
	}
}
