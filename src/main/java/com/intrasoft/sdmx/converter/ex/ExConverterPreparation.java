package com.intrasoft.sdmx.converter.ex;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterMetrics;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.ex.io.data.InputParams;
import com.intrasoft.sdmx.converter.ex.ui.OldImplementationUtils;
import com.intrasoft.sdmx.converter.io.data.Formats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.estat.sdmxsource.util.csv.*;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;

/** 
 * Class that prepares the input from the api  so that is can be used with the old version of the converters.
 * 
 * @author Mihaela Munteanu
 *
 */
public class ExConverterPreparation {

	private static Logger logger = LogManager.getLogger(ExConverterPreparation.class);

	public static ConverterMetrics getMetrics() { return metrics; }

	private static ConverterMetrics metrics;
	
    public static void convertWithPreviousImpl( ConverterInput converterInput,
            ConverterOutput converterOutput,
            ConverterStructure converterStructure) throws Exception {
        ReadableDataLocation dt = converterInput.getInputDataLocation();
        InputParams params = buildParams(converterInput, converterOutput, converterStructure, dt);
        try(InputStream inStream = dt.getInputStream()){
            ExConverter.convert(converterInput.getInputFormat(), 
                    converterOutput.getOutputFormat(), 
                    inStream, 
                    converterOutput.getOutputStream(), params);
            metrics = ExConverter.getMetrics();
        }finally {
            if(dt!=null) dt.close();
        }
    }
    
    /** 
     * Builds parameters from New converter parameters to old format (before SDMXCOnverter 6.0.0).
     * 
     * @param converterInput
     * @param converterOutput
     * @param converterStructure
     * @return
     * @throws IOException
     * @throws InvalidFormatException 
     * @throws EncryptedDocumentException 
     * @throws org.estat.sdmxsource.util.excel.InvalidExcelParamsException
	 */
    private static InputParams buildParams(ConverterInput converterInput, ConverterOutput converterOutput, 
    		ConverterStructure converterStructure, ReadableDataLocation dataLocation) 
    				 throws IOException, EncryptedDocumentException,
    						InvalidFormatException, org.estat.sdmxsource.util.excel.InvalidExcelParamsException {
    	InputParams params = new InputParams();
    	params.setFlatFileEncoding("UTF-8");
    	params.setKeyFamilyBean(converterStructure.getDataStructure());
    	params.setStructureRetrievalManager(converterStructure.getRetrievalManager());
    	params.setStructureDefaultVersion(converterStructure.getDefaultVersion());
    	
    	Formats inFormat = converterInput.getInputFormat();
    	
		switch (inFormat.getFamily()) {
		case CSV:
			params = configureConversionParamsForCsvInput(converterInput, converterOutput, converterStructure, params);
			break;
		case SDMX:
			break;
		case GESMES:
			break;
		default:
			throw new RuntimeException("Input format " + inFormat+ " not supported");
		}
		
		Formats outFormat = converterOutput.getOutputFormat();
		switch (outFormat.getFamily()) {
		case CSV:
			params = configureConversionParamsForCsvOutput(converterInput, converterOutput, converterStructure, params);
			break;
		case SDMX:
			params = configureConversionParamsForSdmxOutput(converterInput, converterOutput, converterStructure, params);
			break;
		case GESMES:
			break;
		case UNKNOWN:
			break;
		default:
			throw new RuntimeException("Output format " + outFormat+ " not supported");
		}

		return params;
	}
    
	private static InputParams configureConversionParamsForCsvOutput(ConverterInput converterInput, 
	 		ConverterOutput converterOutput, ConverterStructure converterStructure, InputParams params) {	
		MultiLevelCsvOutputConfig csvOutputConfig = (MultiLevelCsvOutputConfig)converterOutput.getOutputConfig();
	    csvOutputConfig.setDelimiter(csvOutputConfig.getDelimiter());
	    
	    params.setMapCrossXMeasure(csvOutputConfig.isMapCrossXMeasure());
	    
	    if (csvOutputConfig.getOutputHeader() != null) {
	    	params.setHeaderRow(csvOutputConfig.getOutputHeader().getTechnicalValue());
	    }
		params.setLevelNumber(1+"");
	    //SDMXCONV-1095
		params.setCSVOuputEscape(true);
		if (csvOutputConfig.getDateFormat() != null) {
			params.setDateCompatibility(csvOutputConfig.getDateFormat().getDescription());
		} else {
			params.setDateCompatibility(CsvDateFormat.GESMES.getDescription());
		}
		//Transcoding Rules convert to LinkedHashMap
		LinkedHashMap<String, LinkedHashMap<String, String>> transcoding = csvOutputConfig.getTranscoding();
		if(!transcoding.isEmpty()) {
			LinkedHashMap<String, LinkedHashMap<String, String>> newMap = new LinkedHashMap<String, LinkedHashMap<String, String>>();
			for (String key : transcoding.keySet()) {
			    LinkedHashMap<String, String> mapped = (LinkedHashMap<String, String>) transcoding.get(key);
			    newMap.put(key, mapped);
			  }
			params.setStructureSet(newMap);
		}
		
		// Mapping for output
		MultiLevelCsvOutputConfig multilevelConfig = (MultiLevelCsvOutputConfig)converterOutput.getOutputConfig();
		if (multilevelConfig.getColumnMapping() != null && !multilevelConfig.getColumnMapping().isEmpty()) {
			MultiLevelCsvOutColMapping mapping = multilevelConfig.getColumnMapping();
			params.setMappingMap(OldImplementationUtils.getMappingMapOutput(mapping));
		}
		return params;
	 }
    
	private static InputParams configureConversionParamsForSdmxOutput(ConverterInput converterInput, 
	 		ConverterOutput converterOutput, ConverterStructure converterStructure, InputParams params) {
		SdmxOutputConfig sdmxOutputConfig = (SdmxOutputConfig)converterOutput.getOutputConfig();    	
		params.setNamespacePrefix(sdmxOutputConfig.getNamespaceprefix());
		params.setNamespaceUri(sdmxOutputConfig.getNamespaceuri());
		//Reporting Period and REPORTING_YEAR_START_DAY are not of interest as probably only XS will be used with old impl
		 
		return params;
	}
	 
	 private static InputParams configureConversionParamsForCsvInput(ConverterInput converterInput, 
			 ConverterOutput converterOutput, ConverterStructure converterStructure, InputParams params) throws IOException{
		InputParams result = params;
		CsvInputConfig csvInputConfig = ((CsvInputConfig)converterInput.getInputConfig());
				
		if(converterInput.getInputFormat() != Formats.SDMX_CSV || converterInput.getInputFormat() != Formats.SDMX_CSV_2_0) {
			if (converterInput.getInputFormat().isHasHeader()) {
				params.setHeaderBean(((CsvInputConfig)converterInput.getInputConfig()).getHeader());
			} else {
				params.setHeaderBean(new HeaderBeanImpl("IREF123", "ZZ9"));
			}
			params.setDelimiter(params.getDelimiter());
			if (csvInputConfig.getInputColumnHeader()!= null) {
				params.setHeaderRow(csvInputConfig.getInputColumnHeader().getTechnicalValue());
			}
			params.setOrderedFlatInput(csvInputConfig.getInputOrdered());
			if (csvInputConfig.getLevelNumber() != null) {
				params.setLevelNumber(String.valueOf(csvInputConfig.getLevelNumber()));
			} else {
				csvInputConfig.setLevelNumber(1+"");
			}
			params.setCSVInputEscape(csvInputConfig.getIsEscapeCSV());
			if (csvInputConfig.getMapping() != null) {
				params.setMappingMap(OldImplementationUtils.getMappingMap(csvInputConfig.getMapping()));
			} 
			result = params;
		} else {
		   SdmxCsvInputConfig sdmxCsvInputConfig = ((SdmxCsvInputConfig)converterInput.getInputConfig());
		   params.setDelimiter(sdmxCsvInputConfig.getDelimiter());
		   if (sdmxCsvInputConfig.getInputColumnHeader()!= null) {
			   params.setHeaderRow(sdmxCsvInputConfig.getInputColumnHeader().getTechnicalValue());
		   }
			if (converterInput.getInputFormat().isHasHeader()) {
				params.setHeaderBean(sdmxCsvInputConfig.getHeader());
			} else {
				params.setHeaderBean(new HeaderBeanImpl("IREF123", "ZZ9"));
			}

			   result = params;
		}
		return result;
	  }
	 
}
