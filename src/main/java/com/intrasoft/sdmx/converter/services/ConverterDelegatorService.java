package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.*;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.ex.ExConverterPreparation;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelOutputConfig;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.FlrInColumnMapping;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.estat.sdmxsource.util.excel.ExcelInputConfig;
import org.estat.sdmxsource.util.files.TemporaryFilesUtil;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.header.PartyBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.ThreadLocalOutputReporter;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.util.io.SharedApplicationFolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

/**
 * Please don't panic it is just a delegation to old converter 
 * for the cases when the new implementation was not capable of handling the conversion!
 * 
 * 
 * This class appeared as a consequence of SDMXCONV-577, to replace the sdmxsource cross sectional output 
 * with the old one as the sdmxsource did not have support for ordering. The same for Excel and Cross Sectional input
 *    
 * Since SDMXConverter 6.11.0 depending on the input/output the old readers and writers can be used. 
 * If Input is EXCEL or XS or CSV and Output is XS or CSV then use only the old readers and writer. 
 * If Input is not (EXCEL or XS) and Output is not XS then use only the new sdmxsource readers and writers.
 * If Input is (EXCEL or XS) and Output is not XS make two conversions: 
 * 			1. with old readers/writers EXCEL -> CSV and 2. With new readers/writers CSV -> Output.
 * If Input is not (EXCEL nor XS)  and Output is XS make two conversions:
 *   		1. with new readers/writers Input -> CSV and 2. With old readers/writers CSV -> XS.
 * 
 * *New readers/writers mean those respecting sdmxsource interface. 
 * *Old readers/writers are those that were used up to version 6.0.0 in SDMXConverter
 *   
 *  
 * @author Mihaela Munteanu
 *
 */
@Service
public class ConverterDelegatorService {
    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;

	public static ConverterMetrics getMetrics() { return metrics; }

	private static ConverterMetrics metrics;

	public LinkedHashMap<String, FlrInColumnMapping> getFinalFlrMappings() {
		return finalFlrMappings;
	}

	public void setFinalFlrMappings(LinkedHashMap<String, FlrInColumnMapping> finalFlrMappings) {
		this.finalFlrMappings = finalFlrMappings;
	}

	private LinkedHashMap<String, FlrInColumnMapping> finalFlrMappings = null;
    
	/**
     * Performs a conversion from one format to another
     *
     * @param converterInput  the parameters for input:  source format, input data location, input configuration
     * @param converterOutput the parameters for output: destination format, output stream, output configuration
     * @param converterStructure: dataStructure and dataflow
     * @throws Exception
     */
	public void converterDelegate(ConverterInput converterInput,
								  ConverterOutput converterOutput,
								  ConverterStructure converterStructure) throws Exception {
		validateInputParameters(converterInput, converterOutput, converterStructure);

		try (OutputStream intermediateOutputStreamResult = new BufferedOutputStream(converterOutput.getOutputStream())) {
			converterOutput.setOutputStream(intermediateOutputStreamResult);
			//SDMXCONV-816, SDMXCONV-867
			if (Formats.iSDMX_CSV.equals(converterOutput.getOutputFormat()) || Formats.iSDMX_CSV_2_0.equals(converterOutput.getOutputFormat())) {
				ThreadLocalOutputReporter.getWriteAnnotations().set(true);
			} else {
				ThreadLocalOutputReporter.getWriteAnnotations().set(false);
			}
			if (isXSOutput(converterOutput)) {
				if (converterInput.getInputFormat().isSdmx21()) {
					throw new ConverterException("SDMX21 to Cross Sectional not supported");
				}
			}

			if (isCompactInput(converterInput) && isXSOutput(converterOutput)) {
				//FIXME is this a valid case?
				ExConverterPreparation.convertWithPreviousImpl(converterInput, converterOutput, converterStructure);
				metrics = ExConverterPreparation.getMetrics();
			} else if ((isGesmesInput(converterInput) && isXSOutput(converterOutput))) {
				firstNewConvertThenOldConvert(readableDataLocationFactory,
						converterInput, converterOutput, converterStructure);
			} else {
				Converter.convertWithCurrentImpl(converterInput, converterOutput, converterStructure);
				metrics = Converter.getMetrics();
			}
			//SDMXCONV-1047
			if (isFlrOutput(converterOutput)) {
				setFinalFlrMappings(Converter.getFinalFlrMappings());
			}
		}
	}

    /** 
     * Make the conversion in two steps. For example if Input is Generic20 and Output is Cross Sectional. 
     * First a conversion from Generic to Compact is done using the new sdmxsource readers and writers implementation. 
     * Then with the Compact result another conversion is done from Compact to CrossSectional with the old version of converter (non-sdmxsource). 
     * 
     * @param readableDataLocationFactory
     * @param converterInput
     * @param converterOutput
     * @param converterStructure
     * @throws Exception 
     */
	private void firstNewConvertThenOldConvert(ReadableDataLocationFactory readableDataLocationFactory,
			ConverterInput converterInput, ConverterOutput converterOutput, ConverterStructure converterStructure) 
					throws Exception {
		//create parameters for CSV Output
		File tempDir = new File(SharedApplicationFolder.getLocalFileStorageConversion());
		File intermediateResponseFile = null;
		ReadableDataLocation dataLocation = null;
		try {
			intermediateResponseFile = TemporaryFilesUtil.createTempFile("converterResponseFile", ".xml", tempDir);
		} catch (IOException e) {
			throw new ConverterException("IO error while trying to create a temporary file.", e);
		}
		
		try (OutputStream intermediateOutputStreamResult = new BufferedOutputStream(new FileOutputStream(intermediateResponseFile))) {
			OutputStream outputStreamResult = new BufferedOutputStream(converterOutput.getOutputStream());
	    	converterOutput.setOutputStream(outputStreamResult);
			SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
			ConverterOutput step1ConverterOutput = new ConverterOutput(Formats.COMPACT_SDMX, intermediateOutputStreamResult, sdmxOutputConfig);
			
			Converter.convertWithCurrentImpl(converterInput, step1ConverterOutput, converterStructure);
			metrics = Converter.getMetrics();
			intermediateOutputStreamResult.flush();
		
			dataLocation = readableDataLocationFactory.getReadableDataLocation(intermediateResponseFile);
			SdmxInputConfig inputConfig = new SdmxInputConfig();
			ConverterInput step2ConverterInput = new ConverterInput(Formats.COMPACT_SDMX, dataLocation, inputConfig);

			ExConverterPreparation.convertWithPreviousImpl(step2ConverterInput, converterOutput, converterStructure);
			metrics = ExConverterPreparation.getMetrics();
		} finally {
			if(intermediateResponseFile!=null) intermediateResponseFile.delete();
			if(dataLocation!=null) dataLocation.close();
		}
	}

	private boolean isCsvInput(ConverterInput converterInput) {
		return (Formats.CSV.equals(converterInput.getInputFormat()));
	}
	
	private boolean isMultiLevelCsvInput(ConverterInput converterInput) {
		return (Formats.MULTI_LEVEL_CSV.equals(converterInput.getInputFormat()));
	}
		
	private boolean isCsvOutput(ConverterOutput converterOutput) {
		return (Formats.CSV.equals(converterOutput.getOutputFormat()));
	}

	private boolean isFlrOutput(ConverterOutput converterOutput) {
		return (Formats.FLR.equals(converterOutput.getOutputFormat()));
	}
	
	private boolean isMultiLevelCsvOutput(ConverterOutput converterOutput) {
		return (Formats.MULTI_LEVEL_CSV.equals(converterOutput.getOutputFormat()));
	}
	
	private boolean isXSOutput(ConverterOutput converterOutput) {
		return (Formats.CROSS_SDMX.equals(converterOutput.getOutputFormat()));
	}
	
	private boolean isXSInput(ConverterInput converterInput) {
		return (Formats.CROSS_SDMX.equals(converterInput.getInputFormat()));
	}
	
	private boolean isExcelInput(ConverterInput converterInput) {
		return (Formats.EXCEL.equals(converterInput.getInputFormat()));
	}
	
	private boolean isCompactInput(ConverterInput converterInput) {
		return (Formats.COMPACT_SDMX.equals(converterInput.getInputFormat()));
	}

	private boolean isGesmesInput(ConverterInput converterInput) {
		return (Formats.GESMES_TS.equals(converterInput.getInputFormat()));
	}
	
	private boolean isCompactOutput(ConverterOutput converterOutput) {
		return (Formats.COMPACT_SDMX.equals(converterOutput.getOutputFormat()));
	}
	
	private boolean isSdmxCsvInput(ConverterInput converterInput) {
		return (Formats.SDMX_CSV.equals(converterInput.getInputFormat()));
	}
	
	private boolean isSdmxCsvOutput(ConverterOutput converterOutput) {
		return (Formats.SDMX_CSV.equals(converterOutput.getOutputFormat()) || Formats.iSDMX_CSV.equals(converterOutput.getOutputFormat()));
	}
	
	private boolean isNotExcelInputOrXS(ConverterInput converterInput, ConverterOutput converterOutput) {
		return !(isExcelInput(converterInput) ||
				isXSInput(converterInput) ||
				isXSOutput(converterOutput));
	}
	
	/**
	 * Method that validates the input parameters of converter. 
	 * This method can be used directly in COnverter.convert() or 
	 * separately to validate the parameters before conversion.
	 * 
	 * @param converterInput
	 * @param converterOutput
	 * @param converterStructure
	 * @throws InvalidConversionParameters
	 */
    public static void validateInputParameters(	ConverterInput converterInput,
            									ConverterOutput converterOutput,
            									ConverterStructure converterStructure) throws InvalidConversionParameters {
    	
		if(converterInput.getInputDataLocation() == null || converterInput.getInputDataLocation().getInputStream() == null) {
			throw new InvalidConversionParameters("The input file may not be empty!");	
		}
		
		if(converterOutput.getOutputFormat() == null){
			throw new InvalidConversionParameters("The Output format may not be empty!");
		}
		
		if(converterInput.getInputFormat() == null){
			throw new InvalidConversionParameters("The Input format may not be empty!");
		}
		
		if(converterInput.getInputFormat().equals(converterOutput.getOutputFormat())){
			throw new InvalidConversionParameters("The Input Format should not be the same as the Output Format! ");
		}
		
		if(converterStructure.getDataStructure() == null && converterStructure.getDataflow() == null && converterStructure.getRetrievalManager() == null) {
			throw new InvalidConversionParameters("The structure file may not be empty ");
		}
		
		/*if (!(converterStructure.getDataStructure() instanceof CrossSectionalDataStructureBean) && Formats.CROSS_SDMX.equals(converterOutput.getOutputFormat())) {
			throw new InvalidConversionParameters("Data Structure does not support cross sectional"); 
		}*/

        if ((converterOutput.getOutputFormat() == Formats.iSDMX_CSV
				|| converterOutput.getOutputFormat() == Formats.SDMX_CSV) && converterStructure.getDataflow() == null) {
        	throw new InvalidConversionParameters("Please provide a Dataflow when output is SDMX CSV!");
        }
        
        if ((converterStructure.getDataflow() != null && converterStructure.getDataflow().getId() == null) || 
        		(converterStructure.getDataStructure() != null && converterStructure.getDataStructure().getId() == null)) {
        	throw new InvalidConversionParameters("Artefact Id cannot be empty ");
        }
        
        if ((converterStructure.getDataflow() != null && converterStructure.getDataflow().getVersion() == null) || 
        		(converterStructure.getDataStructure() != null && converterStructure.getDataStructure().getVersion() == null)) {
        	throw new InvalidConversionParameters("Agency Id cannot be empty ");
        }
        
        if ((converterStructure.getDataflow() != null && converterStructure.getDataflow().getAgencyId() == null) || 
        		(converterStructure.getDataStructure() != null && converterStructure.getDataStructure().getAgencyId() == null)) {
        	throw new InvalidConversionParameters("Agency Id cannot be empty ");
        }
        
        //validate parameters for conversions that involve non SDMX formats
        if (converterInput.getInputFormat().equals(Formats.CSV)) {
        	validateCsvInputParams(converterInput);
        }
        
        if (converterInput.getInputFormat().equals(Formats.MULTI_LEVEL_CSV)) {
        	validateMultilevelInputParams(converterInput);
        }
        
        if (converterInput.getInputFormat().equals(Formats.SDMX_CSV)) {
        	validateSdmxCsvInputParams(converterInput);
        }
        
        if (converterOutput.getOutputFormat().equals(Formats.CSV)) {
        	validateCsvOutputParams(converterOutput);
        }
        
        if (converterOutput.getOutputFormat().equals(Formats.MULTI_LEVEL_CSV)) {
        	validateMultilevelOutputParams(converterOutput);
        }
        
        if (converterOutput.getOutputFormat().equals(Formats.SDMX_CSV)) {
        	validateSdmxCsvOutputParams(converterOutput);
        } 
        
        if (converterInput.getInputFormat().equals(Formats.EXCEL)) {
        	validateExcelInputParams(converterInput);
        }
        
        if (converterOutput.getOutputFormat().equals(Formats.EXCEL)) {
        	validateExcelOutputParams(converterOutput);
        }
        
        if (converterOutput.getOutputFormat().isSdmx21()) {
        	validateSdmx21OutputParameters(converterOutput);
        }
    }
    
    
    private static void validateCsvInputQuotesAndDelimiters(CsvInputConfig csvConfig) throws InvalidConversionParameters{
    	 if (csvConfig.getIsEscapeCSV().booleanValue() && "\"".equals(csvConfig.getDelimiter())) {
    		 throw new InvalidConversionParameters("Invalid delimiter: double quotes are not accepted as delimiter when input values are escaped!");
    	}    	
    }

	private static void validateCsvOutputQuotesAndDelimiters(MultiLevelCsvOutputConfig csvOutConfig) throws InvalidConversionParameters {
    	//SDMXCONV-1095
		if ((csvOutConfig.isCsvOutputEnableNeverUseQuotes() && csvOutConfig.getEscapeValues() != EscapeCsvValues.ESCAPE_NONE) && "\"".equals(csvOutConfig.getDelimiter())) {
			throw new InvalidConversionParameters("Invalid delimiter: double quotes are not accepted as delimiter when output values are escaped!");
		}
	}

	/**
	 * @param converterOutput
	 */
	private static void validateExcelOutputParams(
			ConverterOutput converterOutput) throws InvalidConversionParameters {
		if (!(converterOutput.getOutputConfig() instanceof ExcelOutputConfig)) {
			throw new InvalidConversionParameters("The OutputConfig object should be of type ExcelInputConfig.");
		}
		
		ExcelOutputConfig excelOutputConfig = (ExcelOutputConfig)converterOutput.getOutputConfig();
		
		if (excelOutputConfig == null) {
			throw new InvalidConversionParameters("The ExcelOutputConfig cannot be empty.");
		}
	}

	/**
	 * @param converterInput
	 */
	private static void validateExcelInputParams(ConverterInput converterInput) throws InvalidConversionParameters {
		if (!(converterInput.getInputConfig() instanceof ExcelInputConfig)) {
			throw new InvalidConversionParameters("The InputConfig object should be of type ExcelInputConfig.");
		}
		
		ExcelInputConfig excelInputConfig = (ExcelInputConfig)converterInput.getInputConfig();
		
		if (excelInputConfig == null) {
			throw new InvalidConversionParameters("The ExcelInputConfig cannot be empty.");
		}
		
		validateHeaderBean(excelInputConfig.getHeader());
	}


	/**
	 * @param converterOutput
	 */
	private static void validateSdmxCsvOutputParams(
			ConverterOutput converterOutput) throws InvalidConversionParameters {
		if (!(converterOutput.getOutputConfig() instanceof SdmxCsvOutputConfig)) {
			throw new InvalidConversionParameters("The OutputConfig object should be of type SdmxCsvOutputConfig.");
		}

		SdmxCsvOutputConfig sdmxCsvOutputConfig = (SdmxCsvOutputConfig)converterOutput.getOutputConfig();
		
		if (sdmxCsvOutputConfig == null) {
			throw new InvalidConversionParameters("The SdmxCsvOutputConfig cannot be empty.");
		}
		
		if (sdmxCsvOutputConfig.getDelimiter() == null ||
				sdmxCsvOutputConfig.getDelimiter().equals("")) {
			throw new InvalidConversionParameters("The delimiter should not be empty !");
		}
		
		if (sdmxCsvOutputConfig.getOutputHeader() == null ||
				sdmxCsvOutputConfig.getOutputHeader().equals("")) {
			throw new InvalidConversionParameters("The CSV Header Row value is not valid. The value should be one of: NO_COLUMN_HEADERS, USE_COLUMN_HEADERS!");
		}
	}

	/**
	 * @param converterOutput
	 */
	private static void validateMultilevelOutputParams( ConverterOutput converterOutput) throws InvalidConversionParameters {
		if (!(converterOutput.getOutputConfig() instanceof MultiLevelCsvOutputConfig)) {
			throw new InvalidConversionParameters("The OutputConfig object should be of type CsvOutputConfig.");
		}
		
		MultiLevelCsvOutputConfig multiLevelCsvOutputConfig = (MultiLevelCsvOutputConfig)converterOutput.getOutputConfig();
		
		if (multiLevelCsvOutputConfig == null) {
			throw new InvalidConversionParameters("The MultiLevelCsvOutputConfig cannot be empty.");
		}
		
		if (multiLevelCsvOutputConfig.getLevels() < 2){
			throw new InvalidConversionParameters("For a MultiLevelCsv file the level must be 2 or higher!");
		}
		
		if (multiLevelCsvOutputConfig.getDelimiter().equals("")) {
			throw new InvalidConversionParameters("The delimiter should not be empty !");
		}
		
		if (multiLevelCsvOutputConfig.getColumnMapping().getDimensionsMappedOnLevel().size() == 0) {
			throw new InvalidConversionParameters("For a MultiLevelCsv file you need to provide a column mapping!");
		}
		
		validateCsvOutputQuotesAndDelimiters(multiLevelCsvOutputConfig);
	}


	/**
	 * @param converterOutput
	 */
	private static void validateCsvOutputParams(ConverterOutput converterOutput) throws InvalidConversionParameters {
		if (!(converterOutput.getOutputConfig() instanceof MultiLevelCsvOutputConfig)) {
			throw new InvalidConversionParameters("The OutputConfig object should be of type MultiLevelCsvOutputConfig.");
		}

		MultiLevelCsvOutputConfig csvOutputConfig = (MultiLevelCsvOutputConfig)converterOutput.getOutputConfig();
		
		if (csvOutputConfig == null) {
			throw new InvalidConversionParameters("The CsvOutputConfig cannot be empty.");
		}
		
		if (csvOutputConfig.getDelimiter() == null ||
				csvOutputConfig.getDelimiter().equals("")) {
			throw new InvalidConversionParameters("The delimiter should not be empty !");
		}
		
		if (csvOutputConfig.getOutputHeader() == null ||
				csvOutputConfig.getOutputHeader().equals("")) {
			throw new InvalidConversionParameters("The CSV Header Row value is not valid. The value should be one of: NO_COLUMN_HEADERS, USE_COLUMN_HEADERS!");
		}
		
		validateCsvOutputQuotesAndDelimiters(csvOutputConfig);
	}


	/**
	 * @param converterInput
	 */
	private static void validateSdmxCsvInputParams(ConverterInput converterInput) throws InvalidConversionParameters {
		if (!(converterInput.getInputConfig() instanceof SdmxCsvInputConfig)) {
			throw new InvalidConversionParameters("The inputConfig object should be of type SdmxCsvInputConfig.");
		}
		
		SdmxCsvInputConfig sdmxCsvInputConfig = (SdmxCsvInputConfig)converterInput.getInputConfig();
		
		if (sdmxCsvInputConfig == null) {
			throw new InvalidConversionParameters("The SdmxCsvInputConfig cannot be empty.");
		}
		
		if (sdmxCsvInputConfig.getDelimiter() == null ||
				sdmxCsvInputConfig.getDelimiter().equals("")) {
			throw new InvalidConversionParameters("The delimiter should not be empty !");
		}
		
		if (sdmxCsvInputConfig.getInputColumnHeader() == null ||
				sdmxCsvInputConfig.getInputColumnHeader().equals("")) {
			throw new InvalidConversionParameters("The CSV Header Row value is not valid. The value should be one of: DISREGARD_COLUMN_HEADERS, USE_COLUMN_HEADERS, NO_COLUMN_HEADERS!");
		}
		
		validateHeaderBean(sdmxCsvInputConfig.getHeader());
	}


	/**
	 * @param converterInput
	 */
	private static void validateMultilevelInputParams(
			ConverterInput converterInput) throws InvalidConversionParameters {
		if (!(converterInput.getInputConfig() instanceof CsvInputConfig)) {
			throw new InvalidConversionParameters("The inputConfig object should be of type CsvInputConfig.");
		}
		
		CsvInputConfig csvInputConfig = (CsvInputConfig)converterInput.getInputConfig();
		
		if (csvInputConfig == null) {
			throw new InvalidConversionParameters("The CsvInputConfig cannot be empty.");
		}
		
		if (csvInputConfig.getLevelNumber() == null || csvInputConfig.getLevelNumber().equals("")){
			throw new InvalidConversionParameters("The number of csv levels cannot be empty.");
		}
		
		if (Integer.parseInt(csvInputConfig.getLevelNumber()) < 2){
			throw new InvalidConversionParameters("For a MultiLevelCsv file the level must be 2 or higher!");
		}
		
		if (csvInputConfig.getDelimiter() == null ||
				csvInputConfig.getDelimiter().equals("")) {
			throw new InvalidConversionParameters("The delimiter should not be empty !");
		}
		
		if (!csvInputConfig.getInputOrdered()) {
			throw new InvalidConversionParameters("Input ordered should be true for CSV/FLR multilevel file!");
		}
		
		if (csvInputConfig.getMapping() == null ||
				csvInputConfig.getMapping().size() == 0) {
			throw new InvalidConversionParameters("For a MultiLevelCsv file you need to provide a column mapping!");
		}
		
		validateHeaderBean(csvInputConfig.getHeader());
		
		validateCsvInputQuotesAndDelimiters(csvInputConfig);
	}


	/**
	 * @param converterInput
	 */
	private static void validateCsvInputParams(ConverterInput converterInput) throws InvalidConversionParameters {
		if (!(converterInput.getInputConfig() instanceof CsvInputConfig)) {
			throw new InvalidConversionParameters("The inputConfig object should be of type CsvInputConfig.");
		}
		
		CsvInputConfig csvInputConfig = (CsvInputConfig)converterInput.getInputConfig();
		
		if (csvInputConfig == null) {
			throw new InvalidConversionParameters("The CsvInputConfig cannot be empty.");
		}
		
		if (csvInputConfig.getLevelNumber() == null || csvInputConfig.getLevelNumber().equals("")){
			throw new InvalidConversionParameters("The number of csv levels cannot be empty.");
		}
		
		if (Integer.parseInt(csvInputConfig.getLevelNumber()) != 1){
			throw new InvalidConversionParameters("The number of csv levels should be 1. MULTI_LEVEL_CSV should be used for more than one level.");
		}
		
		if (csvInputConfig.getDelimiter() == null ||
				csvInputConfig.getDelimiter().equals("")) {
			throw new InvalidConversionParameters("The delimiter should not be empty !");
		}
		
		if (csvInputConfig.getInputColumnHeader() == null ||
				csvInputConfig.getInputColumnHeader().equals("")) {
			throw new InvalidConversionParameters("The CSV Header Row value is not valid. The value should be one of: DISREGARD_COLUMN_HEADERS, USE_COLUMN_HEADERS, NO_COLUMN_HEADERS!");
		}
		
		validateHeaderBean(csvInputConfig.getHeader());
		
		validateCsvInputQuotesAndDelimiters(csvInputConfig);
	}
    
    /**
	 * Method to validate the HeaderBean.
	 * 
	 * @param headerBean
	 * @throws InvalidConversionParameters
	 */
	private static void validateHeaderBean(HeaderBean headerBean) throws InvalidConversionParameters {
    	if (headerBean == null) {
    		throw new InvalidConversionParameters("The HeaderBean should not be null!");
    	}
    	
    	if (headerBean.getSender() == null) {
    		throw new InvalidConversionParameters("Sender is mandatory in the header bean!");
    	}
    	
    	/*if (headerBean.getSender().getContacts() == null || headerBean.getSender().getContacts().size() == 0) {
    		throw new InvalidConversionParameters("Sender contact details are mandatory!");
    	}*/
    	
    	if (headerBean.getSender().getId() == null || "".equals(headerBean.getSender().getId())) {
    		throw new InvalidConversionParameters("Sender ID is mandatory!");
    	}
    	
    	if (headerBean.getReceiver() != null) {
    		for (PartyBean receiverBean : headerBean.getReceiver()) {
	    		if (receiverBean.getId() == null || "".equals(receiverBean.getId())){
	    			throw new InvalidConversionParameters("Receiver ID is mandatory!");
	    		}    
    		}
    	}
    }
	
	private static void validateSdmx21OutputParameters(ConverterOutput converterOutput) throws InvalidConversionParameters {
		if (!(converterOutput.getOutputConfig() instanceof SdmxOutputConfig)) {
			throw new InvalidConversionParameters("The OutputConfig object should be of type SdmxOutputConfig.");
		}
		SdmxOutputConfig sdmxOutputConfig = (SdmxOutputConfig)converterOutput.getOutputConfig();
		
		if (sdmxOutputConfig.isUseReportingPeriod() && sdmxOutputConfig.getReportingStartYearDate() == null) {
			throw new InvalidConversionParameters("Reporting Start Year Day for Reporting Period is expected.");
		}
		
		if (sdmxOutputConfig.isUseReportingPeriod() && sdmxOutputConfig.getReportingStartYearDate() != null) {
			final Pattern reportingStartDatePattern = Pattern.compile("^--(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])(Z|(\\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$");
	        boolean matchesReportingStartDayPattern =  reportingStartDatePattern.matcher(sdmxOutputConfig.getReportingStartYearDate()).matches();
	        if (!matchesReportingStartDayPattern) {
	        	throw new InvalidConversionParameters("Reporting Start Year Day for Reporting Period is expected.");
	        }
		}
		
		
	}
}
