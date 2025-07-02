package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.Resources;
import com.intrasoft.sdmx.converter.config.FlrOutputConfig;
import com.intrasoft.sdmx.converter.config.GesmesOutputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.TsTechnique;
import com.intrasoft.sdmx.converter.io.data.UtilityDataWriterEngine;
import com.intrasoft.sdmx.converter.io.data.csv.FlrDataWriterEngine;
import com.intrasoft.sdmx.converter.io.data.csv.MultilevelCsvDataWriterEngine;
import com.intrasoft.sdmx.converter.io.data.csv.SingleLevelCsvDataWriterEngine;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelDataWriterEngine;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelOutputConfig;
import org.apache.poi.ss.formula.eval.NotImplementedException;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.sdmxsource.sdmx.api.constants.BASE_DATA_FORMAT;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.dataparser.engine.writer.*;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.ediparser.engine.writer.impl.EDIDataWriterEngineImpl;
import org.sdmxsource.sdmx.structureretrieval.manager.SdmxSuperBeanRetrievalManagerImpl;
import org.sdmxsource.util.factory.SdmxSourceWriteableDataLocationFactory;

import java.io.IOException;
import java.io.OutputStream;

public class ConverterDataWriterEngineProvider {


	public static DataWriterEngine getDataWriterEngine(ConverterOutput converterOutput, SdmxBeanRetrievalManager retrievalManager) throws IOException {
		Formats outputType = converterOutput.getOutputFormat();
		OutputConfig outputConfig = converterOutput.getOutputConfig();
		OutputStream outputStream = converterOutput.getOutputStream();

		DataWriterEngine dataWriterEngine = null;
		switch (outputType) {
			case CSV:
				((MultiLevelCsvOutputConfig) outputConfig).setRetrievalManager(new SdmxSuperBeanRetrievalManagerImpl(retrievalManager)); //SDMXRI-905
				dataWriterEngine = new SingleLevelCsvDataWriterEngine(outputStream, (MultiLevelCsvOutputConfig) outputConfig);
				break;
			case SDMX_CSV:
				((SdmxCsvOutputConfig) outputConfig).setRetrievalManager(new SdmxSuperBeanRetrievalManagerImpl(retrievalManager)); //SDMXRI-905
				dataWriterEngine = new SdmxCsvDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxCsvOutputConfig) outputConfig);
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case SDMX_CSV_2_0:
				((SdmxCsvOutputConfig) outputConfig).setRetrievalManager(new SdmxSuperBeanRetrievalManagerImpl(retrievalManager)); //SDMXRI-905
				dataWriterEngine = new SdmxCsv20DataWriterEngine(SDMX_SCHEMA.VERSION_THREE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxCsvOutputConfig) outputConfig);
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_THREE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case iSDMX_CSV:
				((SdmxCsvOutputConfig) outputConfig).setRetrievalManager(new SdmxSuperBeanRetrievalManagerImpl(retrievalManager)); //SDMXRI-905
				dataWriterEngine = new InternalCsvDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxCsvOutputConfig) outputConfig);
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case iSDMX_CSV_2_0:
				((SdmxCsvOutputConfig) outputConfig).setRetrievalManager(new SdmxSuperBeanRetrievalManagerImpl(retrievalManager));
				((SdmxCsvOutputConfig) outputConfig).setInternalSdmxCsvAdjustment(true);
				dataWriterEngine = new InternalSdmxCsv20DataWriterEngine(SDMX_SCHEMA.VERSION_THREE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxCsvOutputConfig) outputConfig);
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_THREE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case MULTI_LEVEL_CSV:
				dataWriterEngine = new MultilevelCsvDataWriterEngine(outputStream, (MultiLevelCsvOutputConfig) outputConfig);
				break;
			case COMPACT_SDMX:
				dataWriterEngine = new CompactDataWriterEngine(SDMX_SCHEMA.VERSION_TWO, BASE_DATA_FORMAT.COMPACT, outputStream);
				((CompactDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((CompactDataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO, BASE_DATA_FORMAT.COMPACT, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case GENERIC_SDMX:
				dataWriterEngine = new GenericDataWriterEngine(SDMX_SCHEMA.VERSION_TWO, outputStream);
				((GenericDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((GenericDataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO, BASE_DATA_FORMAT.GENERIC, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case UTILITY_SDMX:
				dataWriterEngine = new UtilityDataWriterEngine(outputStream);
				((UtilityDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO, BASE_DATA_FORMAT.UTILITY, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case CROSS_SDMX:
				dataWriterEngine = new CrossSectionDataWriterEngine(SDMX_SCHEMA.VERSION_TWO, outputStream);
				((CrossSectionDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((CrossSectionDataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO, BASE_DATA_FORMAT.CROSS_SECTIONAL, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case GENERIC_DATA_2_1:
				dataWriterEngine = new GenericDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, outputStream);
				((GenericDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((GenericDataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.GENERIC, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case GENERIC_TS_DATA_2_1:
				dataWriterEngine = new GenericDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.GENERIC_TS_DATA, outputStream);
				((GenericDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((GenericDataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.GENERIC_TS_DATA, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case STRUCTURE_SPECIFIC_DATA_2_1:
				dataWriterEngine = new CompactDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.STRUCTURE_SPECIFIC_DATA, outputStream);
				((CompactDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((CompactDataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.COMPACT, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case STRUCTURE_SPECIFIC_TS_DATA_2_1:
				dataWriterEngine = new CompactDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.STRUCTURE_SPECIFIC_TS_DATA, outputStream);
				((CompactDataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((CompactDataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.COMPACT, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case STRUCTURE_SPECIFIC_DATA_3_0:
				dataWriterEngine = new StructureSpecificV30DataWriterEngine(SDMX_SCHEMA.VERSION_THREE, BASE_DATA_FORMAT.COMPACT, outputStream);
				((StructureSpecificV30DataWriterEngine) dataWriterEngine).setGeneratedComment("Created with " + Resources.GUI_VERSION);
				((StructureSpecificV30DataWriterEngine) dataWriterEngine).setWriterFactory(new ConverterXMLWriterFactoryImpl());
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_THREE, BASE_DATA_FORMAT.COMPACT, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			case EXCEL:
				dataWriterEngine = new ExcelDataWriterEngine(outputStream, (ExcelOutputConfig) outputConfig);
				break;
			case GESMES_TS:
				boolean singleObservation = false;
				if (TsTechnique.SINGLE_OBSERVATION.equals(((GesmesOutputConfig) outputConfig).getGesmeswritingtechnique())) {
					singleObservation = true;
				}
				dataWriterEngine = new EDIDataWriterEngineImpl(outputStream, new SdmxSourceWriteableDataLocationFactory(), singleObservation);
				break;
			case FLR:
				dataWriterEngine = new FlrDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.CSV, outputStream, (FlrOutputConfig) outputConfig);
				if(dataWriterEngine instanceof SdmxDataWriterEngine) {
					dataWriterEngine = new WritingDataEngineDecorator(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, BASE_DATA_FORMAT.CSV, outputStream, (SdmxDataWriterEngine) dataWriterEngine);
				}
				break;
			default:
				throw new NotImplementedException("the requested writer is not implemented yet ");
		}
		if (outputType.isSdmx21() && ((SdmxOutputConfig)outputConfig).isUseReportingPeriod()) {
			return new ReportingPeriodDataWriterEngine(dataWriterEngine, ((SdmxOutputConfig)outputConfig).getReportingStartYearDate());
		}

		return dataWriterEngine;
	}
}
