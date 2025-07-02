package com.intrasoft.sdmx.converter.io.data.excel;

import org.estat.struval.engine.impl.ExcelReadableDataLocation;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.factory.DataReaderFactory;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.stereotype.Service;

@Service
public class ExcelDataReaderFactory implements DataReaderFactory {

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData, 
			DataStructureBean dsd,
			DataflowBean dataflowBean) {
		
		if(sourceData instanceof ExcelReadableDataLocation){
			return new ExcelDataReaderEngine(sourceData, null, dsd, dataflowBean, ((ExcelReadableDataLocation)sourceData).getExcelInputConfig(), new FirstFailureExceptionHandler());
		}   		 
		return null;
	}

	@Override
	public DataReaderEngine getDataReaderEngine(
			ReadableDataLocation sourceData,
			SdmxBeanRetrievalManager retrievalManager) {
		if(sourceData instanceof ExcelReadableDataLocation){
			return new ExcelDataReaderEngine(sourceData, retrievalManager, null, null, ((ExcelReadableDataLocation)sourceData).getExcelInputConfig(), new FirstFailureExceptionHandler());
		}
		return null;
	}
}
