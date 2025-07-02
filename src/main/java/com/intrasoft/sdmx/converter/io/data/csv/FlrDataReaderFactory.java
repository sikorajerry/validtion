package com.intrasoft.sdmx.converter.io.data.csv;

import org.estat.struval.engine.impl.FlrReadableDataLocation;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.factory.DataReaderFactory;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.stereotype.Service;

@Service
public class FlrDataReaderFactory implements DataReaderFactory {

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData, DataStructureBean dsd,
			DataflowBean dataflowBean) {
		if(sourceData instanceof FlrReadableDataLocation){
			return new FLRDataReaderEngine(sourceData, dsd, dataflowBean, null, ((FlrReadableDataLocation)sourceData).getFlrInputConfig(), new FirstFailureExceptionHandler());
		}   		 
		return null;
	}

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData,
			SdmxBeanRetrievalManager retrievalManager) {
		if(sourceData instanceof FlrReadableDataLocation){
			return new FLRDataReaderEngine(sourceData, null, null, retrievalManager, ((FlrReadableDataLocation)sourceData).getFlrInputConfig(), new FirstFailureExceptionHandler());
		}
		return null;
	}
}
