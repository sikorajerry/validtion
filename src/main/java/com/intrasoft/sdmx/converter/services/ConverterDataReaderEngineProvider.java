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
package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.GesmesInputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.csv.FLRDataReaderEngine;
import com.intrasoft.sdmx.converter.io.data.csv.MultiLevelCsvDataReaderEngine;
import com.intrasoft.sdmx.converter.io.data.csv.SingleLevelCsvDataReaderEngine;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelDataReaderEngine;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.sdmx.structureparser.SdmxBeansSchemaDecorator;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.FLRInputConfig;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.estat.struval.engine.DefaultVersion;
import org.estat.struval.engine.impl.*;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.SDMXCsv20ReaderEngine;
import org.sdmxsource.sdmx.dataparser.engine.reader.SDMXCsvReaderEngine;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.ediparser.manager.EdiParseManager;
import org.sdmxsource.sdmx.ediparser.manager.impl.EdiParseManagerImpl;
import org.sdmxsource.sdmx.ediparser.model.EDIWorkspace;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.util.ObjectUtil;

import java.util.LinkedHashMap;

public class ConverterDataReaderEngineProvider {

	/**
	 * private constructor to prevent instantiation
	 */
	private ConverterDataReaderEngineProvider() {
	}

	/**
	 * builds the appropriate Reader based on the given input type
	 *
	 * @param converterInput
	 * @param converterStructure
	 * @return the DataReaderEngine
	 */
	public static DataReaderEngine getDataReaderEngine(ConverterInput converterInput,
													   ConverterStructure converterStructure,
													   LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine,
													   int order,
													   int maxErrorOccurences,
													   ExceptionHandler exceptionHandler,
													   Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions) {
		ReadableDataLocation dataLocation = converterInput.getInputDataLocation();
		DataStructureBean dataStructure = converterStructure.getDataStructure();
		DataflowBean dataflow = converterStructure.getDataflow();
		SdmxBeanRetrievalManager retrievalManager = converterStructure.getRetrievalManager();
		String defaultVersion = converterStructure.getDefaultVersion();
		InputConfig inputConfig = converterInput.getInputConfig();
		//SDMXCONV-1523 Schema Version could not be set in a Thread Local
		if(converterStructure.getSdmxBeans() instanceof SdmxBeansSchemaDecorator) {
			SdmxBeansSchemaDecorator beansSchemaDecorator = (SdmxBeansSchemaDecorator) converterStructure.getSdmxBeans();
			inputConfig.setStructureSchemaVersion(beansSchemaDecorator.getSdmxSchema());
		}
		Formats inputType = converterInput.getInputFormat();
        DataReaderEngine dataReaderEngine;
		
        switch (inputType) {
        case CSV:
            dataReaderEngine = new SingleLevelCsvDataReaderEngine(  dataLocation,
                                                                    dataStructure,
                                                                    dataflow,
																	retrievalManager,
                                                                    ((CsvInputConfig)inputConfig),
																	exceptionHandler);
            break;
        case MULTI_LEVEL_CSV:
            dataReaderEngine = new MultiLevelCsvDataReaderEngine(   dataLocation,
                                                                    dataStructure,
                                                                    dataflow,
																	retrievalManager,
                                                                    ((CsvInputConfig)inputConfig), exceptionHandler);
            break;
		case SDMX_CSV:
				dataReaderEngine = new SDMXCsvReaderEngine(	dataLocation,
															dataStructure,
															dataflow,
															retrievalManager,
															((SdmxCsvInputConfig) inputConfig),
															exceptionHandler);
			break;
		case SDMX_CSV_2_0:
				dataReaderEngine = new SDMXCsv20ReaderEngine(	dataLocation,
																dataStructure,
																dataflow,
																retrievalManager,
																((SdmxCsvInputConfig) inputConfig),
																exceptionHandler);
			break;
       case FLR:
            dataReaderEngine = new FLRDataReaderEngine(  dataLocation,
                                                         dataStructure,
                                                         dataflow,
                                                         retrievalManager,
                                                         ((FLRInputConfig)inputConfig),
														 exceptionHandler);
            break;
        case COMPACT_SDMX:
        	/*dataReaderEngine = new CompactDataReaderEngine( dataLocation, retrievalManager, dataStructure, dataflow);*/
        	dataReaderEngine = new StruvalStructureSpecificDataReaderEngine(dataLocation, 
																			retrievalManager, 
																			dataStructure, 
																			dataflow,
																			exceptionHandler);
            break;
        case GENERIC_SDMX:
        	dataReaderEngine = new StruvalGenericDataReaderEngine(  dataLocation,
																	retrievalManager,
												                    dataStructure,
												                    dataflow,
																	exceptionHandler);
            break;
        case UTILITY_SDMX:
            dataReaderEngine = new StruvalUtilityDataReaderEngine(	dataLocation,
	                                                                dataflow,
	                                                                retrievalManager,
	                                                                dataStructure,
																	exceptionHandler);
            break;
        case CROSS_SDMX:
        	//This case is used only for output formats internal SDMX-CSV
            dataReaderEngine = new StruvalCrossSectionalDataReaderEngine(dataLocation,
        																retrievalManager,
        																(CrossSectionalDataStructureBean)dataStructure,
        																dataflow,
																		exceptionHandler);
            break;
        case STRUCTURE_SPECIFIC_TS_DATA_2_1:
            dataReaderEngine = new StruvalStructureSpecificDataReaderEngine(dataLocation, retrievalManager, dataStructure, dataflow, exceptionHandler);
            break;
        case GENERIC_DATA_2_1:
        	dataReaderEngine = new StruvalGenericDataReaderEngine(dataLocation,
																  retrievalManager,
																  dataStructure,
																  dataflow,
																  exceptionHandler);
            break;
        case GENERIC_TS_DATA_2_1:
        	dataReaderEngine = new StruvalGenericDataReaderEngine(dataLocation,
																  retrievalManager,
																  dataStructure,
																  dataflow,
																  exceptionHandler);
            break;
        case STRUCTURE_SPECIFIC_DATA_2_1:
            /*dataReaderEngine = new CompactDataReaderEngine(dataLocation, retrievalManager, dataStructure, dataflow);*/
        	dataReaderEngine = new StruvalStructureSpecificDataReaderEngine(dataLocation, 
        																	retrievalManager, 
        																	dataStructure, 
        																	dataflow,
																			exceptionHandler);
            break;
		case STRUCTURE_SPECIFIC_DATA_3_0: {
			if(!ObjectUtil.validObject(dataStructure) && !ObjectUtil.validObject(dataflow)) {
				dataReaderEngine = new StruvalStructureSpecific30DataReaderEngine(dataLocation, retrievalManager,
						dataStructure, dataflow);
			} else {
				dataReaderEngine = new StruvalStructureSpecific30DataReaderEngine(dataLocation, null,
						dataStructure, dataflow);
			}
			break;
		}
        case EXCEL:
            dataReaderEngine = new ExcelDataReaderEngine(dataLocation,
        												 retrievalManager,
                                                         dataStructure,
                                                         dataflow,
                                                         (ExcelInputConfigImpl)inputConfig,
														 exceptionHandler);
            break;
        case GESMES_TS:
        	SdmxBeans sdmxBeans = ((GesmesInputConfig)converterInput.getInputConfig()).getSdmxBeans();
            EdiParseManager ediParseManager = new EdiParseManagerImpl();
            EDIWorkspace ediWorkspace = ediParseManager.parseEDIMessage(dataLocation);
            if (retrievalManager != null) {
           		dataReaderEngine = ediWorkspace.getDataReader(retrievalManager, defaultVersion);
            } else {
            	if(sdmxBeans!=null) {
            		SdmxBeanRetrievalManager retrievalManagerGes = new InMemoryRetrievalManager(sdmxBeans);
            		dataReaderEngine = ediWorkspace.getDataReader(retrievalManagerGes, defaultVersion);
            	}else {
            		dataReaderEngine = ediWorkspace.getDataReader(dataStructure, dataflow);
            	}
            }
            break;
        case MESSAGE_GROUP:
            //should not enter here as the messages will come directly Generic, Compact or utility with multiple Datasets
        default:
            throw new UnsupportedOperationException("reader for "+inputType+" not ready yet");
        }
		
        if (dataReaderEngine instanceof DefaultVersion) {
			((DefaultVersion) dataReaderEngine).setDefaultVersion(defaultVersion);
    	}
		if(dataReaderEngine instanceof DataValidationErrorHolder) {
			((DataValidationErrorHolder) dataReaderEngine).setErrorsByEngine(errorsByEngine);
			((DataValidationErrorHolder) dataReaderEngine).setOrder(order);
			((DataValidationErrorHolder) dataReaderEngine).setMaximumErrorOccurrencesNumber(maxErrorOccurences);
			((DataValidationErrorHolder) dataReaderEngine).setErrorLimit(1);
			((DataValidationErrorHolder) dataReaderEngine).setErrorPositions(errorPositions);
		}

		if (dataReaderEngine instanceof org.estat.sdmxsource.extension.model.ErrorReporter) {
			((org.estat.sdmxsource.extension.model.ErrorReporter) dataReaderEngine).setExceptionHandler(exceptionHandler);
		}
		return dataReaderEngine;
	}
}
