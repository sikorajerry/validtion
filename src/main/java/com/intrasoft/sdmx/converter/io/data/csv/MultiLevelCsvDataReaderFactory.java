package com.intrasoft.sdmx.converter.io.data.csv;

import org.estat.struval.engine.impl.MultiLevelCsvReadableDataLocation;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.factory.DataReaderFactory;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.stereotype.Service;

@Service
public class MultiLevelCsvDataReaderFactory implements DataReaderFactory {

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData, DataStructureBean dsd,
			DataflowBean dataflowBean) {
		if(sourceData instanceof MultiLevelCsvReadableDataLocation){
			return new MultiLevelCsvDataReaderEngine(sourceData, dsd, dataflowBean, null, ((MultiLevelCsvReadableDataLocation)sourceData).getCsvInputConfig(), new FirstFailureExceptionHandler());
		}   		 
		return null;
	}

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData,
			SdmxBeanRetrievalManager retrievalManager) {
		if(sourceData instanceof MultiLevelCsvReadableDataLocation){
			return new MultiLevelCsvDataReaderEngine(sourceData, null, null, retrievalManager, ((MultiLevelCsvReadableDataLocation)sourceData).getCsvInputConfig(), new FirstFailureExceptionHandler());
		}
		return null;
	}
}