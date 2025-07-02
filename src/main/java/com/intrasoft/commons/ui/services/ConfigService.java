package com.intrasoft.commons.ui.services;

import com.intrasoft.sdmx.converter.CsvDelimiters;
import com.intrasoft.sdmx.converter.Operation;
import com.intrasoft.sdmx.converter.io.data.Formats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.files.TemporaryFilesUtil;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.util.ObjectUtil;
import org.sdmxsource.util.io.SharedApplicationFolder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * configuration service for the swing application. 
 * 
 * @author dragos balan
 */
@Service
public class ConfigService {

	private static Logger logger = LogManager.getLogger(ConfigService.class);
    
    @Value("${file.storage.path.conversion}")
    private String localFileStorageConversion;

	@Value("${file.storage.path.validation}")
	private String localFileStorageStruval;
    
    @Value("${default.input.format}")
    private String defaultInputFormatAsString; 
    private Formats defaultInputFormat; 

    //SDMXCONV-1155
    @Value("${default.webapp.input.timeout}")
    private String defaultWebappInputTimeoutAsString;
    private Integer defaultWebappInputTimeout;

    @Value("${default.output.format}")
    private String defaultOutputFormatAsString;
    private Formats defaultOutputFormat;
    
    @Value("${default.operation.option}")
    private String defaultOperationAsString; 
    private Operation defaultOperation; 
    
    @Value("${registry.url.field.enabled}")
    private String registryUrlFieldEnabledAsString;
    private boolean registryUrlFieldEnabled;
    
    @Value("${default.csv.input.delimiter}")
    private String defaultCsvInputDelimAsString;
    private CsvDelimiters defaultCsvInputDelim;
    
    @Value("${default.csv.input.headerRow}")
    private String defaultCsvInputHeaderRowAsString;
    private CsvInputColumnHeader defaultCsvInputHeaderRow;
    
    @Value("${default.csv.input.dateFormat}")
    private String defaultCsvInputDateFormatAsString;
    private CsvDateFormat defaultCsvInputDateFormat;

    @Value("${default.csvMultilevel.input.levels}")
    private String defaultCsvInputLevelsAsString;
    private Integer defaultMultilevelCsvInputLevels;
    
    @Value("${default.csv.output.delimiter}")
    private String defaultCsvOutputDelimAsString;
    private CsvDelimiters defaultCsvOutputDelim;
    
    @Value("${default.csv.output.headerRow}")
    private String defaultCsvOutputHeaderRowAsString;
    private CsvOutputColumnHeader defaultCsvOutputHeaderRow;

    @Value("${default.csv.subfieldSeparator}")
	private String defaultSubfieldSeparator;
    
    @Value("${default.csv.output.dateFormat}")
    private String defaultCsvOutputDateFromatAsString;
    private CsvDateFormat defaultCsvOutputDateFormat;

    @Value("${default.csvMultilevel.output.levels}")
    private String defaultMultilevelCsvOutputLevelsAsString;
    private Integer defaultMultilevelCsvOutputLevels;
    
    @Value("${validation.output.error.showdetails:false}")
    private String validationOutputErrorDetailsAsString;
    private boolean validationOutputErrorDetails;

    @Value("${version}")
    private String projectVersion;
    
    @Value("${default.webservice.csv.input.headerRow}")
    private String defaultWebServiceCsvInputHeaderRowAsString;
    private Boolean defaultWebServiceCsvInputHeaderRow;
    
    @Value("${default.webservice.csv.input.delimiter}")
    private String defaultWebServiceCsvInputDelimiter;
   
    @Value("${default.webservice.csv.input.quoteCharacter}")
    private String defaultWebServiceCsvInputQuoteCharacter;

    @Value("${registry.proxy.enabled}")
    private String proxyEnabledAsString; 
    private Boolean proxyEnabled; 
    
    @Value("${registry.proxy.host}")
    private String proxyHost; 
    
    @Value("${registry.proxy.port}")
    private String proxyPortAsString;
    private Integer proxyPort;
    
    @Value("${registry.proxy.username}")
    private String proxyUsername;
    
    @Value("${registry.proxy.password}")
    private String proxyPassword;
    
    @Value("${registry.jks.path}")
    private String jksPath;
    
    @Value("${registry.jks.password}")
    private String jksPassword;
    
    @Value("${registry.proxy.excludes}")
    private String exclusions;
    private List<String> proxyExclusions;

	@Value("${registry.username}")
	private String registryUsername;

	@Value("${registry.password}")
	private String registryPassword;
    
    @Value("${validation.header.schema.errors.reported}")
    private String validationHeaderErrorsReportedAsString;
    private boolean validationHeaderErrorsReported;
    
    @Value("${maximum.errors.displayed}")
    private String defaultMaximumErrorsDisplayed;
    private Integer defaultMaximumErrorsDisplayedNumber;
    
    @Value("${minimum.excel.inflateRatio}")
    private String defaultMinimumInflateRatio;
    private Double defaultMinimumInflateRatioNumber;
    //SDMXCONV-760
    @Value("${obsValue.isConfidential}")
    private String obsValueConfidential;
    
    //SDMXCONV-951
    @Value("${default.errorIfEmpty:true}")
    private String validationErrorIfEmptyAsString;
    private boolean validationErrorIfEmpty;

    //SDMXCONV-905
//    private boolean obsValueConfidential;

    @Value("${default.csv.trim.whitespaces}")
    private String trimWhitespacesAsString;
    private boolean trimWhitespaces;

    //SDMXCONV-996
	@Value("${visible.input.formats}")
	private String visibleInputFormatsAsString;
	private List<String> visibleInputFormats;

	@Value("${visible.output.formats}")
	private String visibleOutputFormatsAsString;
	private List<String> visibleOutputFormats;

	//SDMXCONV-1060
    @Value("${default.validation.inlineReportFormat}")
    private String inlineReportFormatAsString;
    private boolean inlineReportFormat;

    //SDMXCONV-1095
    @Value("${default.csv.output.enableNeverUseQuotes}")
    private String defaultCsvOutputEnableNeverUseQuotesAsString;
    private boolean defaultCsvOutputEnableNeverUseQuotes;

    //SDMXCONV-1095
    @Value("${default.iSdmxCsv.adjustment}")
    private String defaultISdmxCsvAdjustmentAsString;
    private boolean defaultISdmxCsvAdjustment;

	//SDMXCONV-871
    @Value("${default.dsd.version}")
    private String defaultDsdVersion;

    @Value("${validation.check.bom.error}")
	private String checkBomErrorAsString;
    private boolean checkBomError;

    @Value("${default.errorIfDataValuesEmpty}")
	private String errorIfDataValuesEmptyAsString;
    private boolean errorIfDataValuesEmpty;

    //SDMXCONV-1187, this is the default timeout the user has to wait for a synchronous response.
    @Value("${default.synchronous.validation.timeout}")
    private String defaultSynchronousValidationTimeoutAsString;
    private Long defaultSynchronousValidationTimeout;

    //SDMXCONV-1187, this is the time in milliseconds after which the Future with the results will be expired
    @Value("${default.validation.expiration.time}")
    private String defaultValidationExpirationTimeAsString;
    private Long defaultValidationExpirationTime;

    //SDMXCONV-1187, this is the time in seconds after which Converter will clean up the expired results
    @Value("${default.validation.cleanup.time}")
    private String defaultValidationCleanupTimeAsString;
    private Long defaultValidationCleanupTime;

    //SDMXCONV-1570
    // Timeout for synchronous data conversion processes. Configured in application properties.
    @Value("${default.synchronous.conversion.timeout}")
    private String defaultSynchronousConversionTimeoutAsString;
    private Long defaultSynchronousConversionTimeout;

    //SDMXCONV-1570
    // Expiration time for completed data conversions. Helps in resource cleanup by defining how long converted data should be retained.
    @Value("${default.conversion.expiration.time}")
    private String defaultConversionExpirationTimeAsString;
    private Long defaultConversionExpirationTime;

    //SDMXCONV-1570
    // Cleanup interval for removing expired data conversions.
    @Value("${default.conversion.cleanup.time}")
    private  String defaultConversionCleanupTimeAsString;
    private  Long defaultConversionCleanupTime;
    @Value("${validation.formula.errors.reported}")
    private String formulaErrorsReportedAsString;
    private boolean formulaErrorsReported;

    @Value("${error.message.path}")
	private String errorMessagePath;
    
	@Value("${validation.error.max.occurrences}")
	private String maximumErrorOccurrences;
	private Integer maximumErrorOccurrencesNumber;

	@Value("${validation.task.queue}")
	private String taskQueue;
	private Integer taskQueueNumber;

    @Value("${conversion.task.queue}")
    private String taskQueueConversion;
    private Integer taskQueueConversionNumber;

    @Value("${default.csv.allowAdditionalColumns}")
    private String allowAdditionalColumnsAsString;
    private Boolean allowAdditionalColumns;

    @PostConstruct
    private void initAndValdateValues(){
        logger.info("initialization and validation of the configured values ...");
        defaultInputFormat = detectFormatFromString(defaultInputFormatAsString);
        //SDMXCONV-1155
        defaultWebappInputTimeout = Integer.valueOf(defaultWebappInputTimeoutAsString);
        defaultOutputFormat = detectFormatFromString(defaultOutputFormatAsString);
        defaultOperation = Operation.valueOf(defaultOperationAsString);
        registryUrlFieldEnabled = Boolean.parseBoolean(registryUrlFieldEnabledAsString);
        defaultCsvInputDelim =  detectDelimiterFromString(defaultCsvInputDelimAsString);
        defaultCsvOutputDelim = detectDelimiterFromString(defaultCsvOutputDelimAsString);
        defaultCsvInputHeaderRow = "".equals(defaultCsvInputHeaderRowAsString) ? 
                                     CsvInputColumnHeader.EMPTY : CsvInputColumnHeader.valueOf(defaultCsvInputHeaderRowAsString);
        defaultCsvOutputHeaderRow = "".equals(defaultCsvOutputHeaderRowAsString) ?
                                     CsvOutputColumnHeader.EMPTY : CsvOutputColumnHeader.valueOf(defaultCsvOutputHeaderRowAsString);
        defaultCsvInputDateFormat = CsvDateFormat.valueOf(defaultCsvInputDateFormatAsString);
        defaultCsvOutputDateFormat = CsvDateFormat.valueOf(defaultCsvOutputDateFromatAsString);
        defaultMultilevelCsvInputLevels = Integer.valueOf(defaultCsvInputLevelsAsString);
        defaultMultilevelCsvOutputLevels = Integer.valueOf(defaultMultilevelCsvOutputLevelsAsString);
        defaultMaximumErrorsDisplayedNumber = Integer.valueOf(defaultMaximumErrorsDisplayed);
        validationOutputErrorDetails = Boolean.valueOf(validationOutputErrorDetailsAsString);
        defaultWebServiceCsvInputHeaderRow = Boolean.valueOf(defaultWebServiceCsvInputHeaderRowAsString);
        proxyPort = Integer.valueOf(proxyPortAsString);
        proxyEnabled = "".equals(proxyEnabledAsString)?false:Boolean.valueOf(proxyEnabledAsString);
        proxyExclusions = listFromString(exclusions);
        validationHeaderErrorsReported = Boolean.parseBoolean(validationHeaderErrorsReportedAsString);
        defaultMinimumInflateRatioNumber = Double.valueOf(defaultMinimumInflateRatio);
        validationErrorIfEmpty = Boolean.parseBoolean(validationErrorIfEmptyAsString);
        trimWhitespaces = Boolean.parseBoolean(trimWhitespacesAsString);
		//obsValueConfidential = Boolean.parseBoolean(obsValueConfidential);
		visibleInputFormats = listFromString(visibleInputFormatsAsString);
		visibleOutputFormats = listFromString(visibleOutputFormatsAsString);
		inlineReportFormat = Boolean.parseBoolean(inlineReportFormatAsString);
		defaultCsvOutputEnableNeverUseQuotes = Boolean.parseBoolean(defaultCsvOutputEnableNeverUseQuotesAsString);
		defaultISdmxCsvAdjustment = Boolean.parseBoolean(defaultISdmxCsvAdjustmentAsString);
		checkBomError = Boolean.parseBoolean(checkBomErrorAsString);
		errorIfDataValuesEmpty = Boolean.parseBoolean(errorIfDataValuesEmptyAsString);
        defaultSynchronousValidationTimeout = Long.valueOf(defaultSynchronousValidationTimeoutAsString);
        defaultValidationExpirationTime = Long.valueOf(defaultValidationExpirationTimeAsString);
        defaultValidationCleanupTime = Long.valueOf(defaultValidationCleanupTimeAsString);
        defaultSynchronousConversionTimeout = Long.valueOf(defaultSynchronousConversionTimeoutAsString);
        defaultConversionExpirationTime = Long.valueOf(defaultConversionExpirationTimeAsString);
        defaultConversionCleanupTime = Long.valueOf(defaultConversionCleanupTimeAsString);
		formulaErrorsReported = Boolean.parseBoolean(formulaErrorsReportedAsString);
		maximumErrorOccurrencesNumber = Integer.valueOf(maximumErrorOccurrences);
		taskQueueNumber = Integer.valueOf(taskQueue);
        taskQueueConversionNumber = Integer.valueOf(taskQueueConversion);
        allowAdditionalColumns = Boolean.parseBoolean(allowAdditionalColumnsAsString);
		logger.info("configuration, reading the properties file done with success! ");
		SharedApplicationFolder.setLocalFileStorageConversion(localFileStorageConversion);
		SharedApplicationFolder.setLocalFileStorageStruval(localFileStorageStruval);
		cleanupTempFoldersIfExist();
    }

	private void cleanupTempFoldersIfExist() {
		try {
			TemporaryFilesUtil.deleteTempDirectory(SharedApplicationFolder.getLocalFileStorageConversion());
			TemporaryFilesUtil.deleteTempDirectory(SharedApplicationFolder.getLocalFileStorageStruval());
			TemporaryFilesUtil.removeAllCacheFolders(0, SharedApplicationFolder.getLocalFileStorageConversion());
			TemporaryFilesUtil.removeAllCacheFolders(0, SharedApplicationFolder.getLocalFileStorageStruval());
		} catch (IOException e) {
			logger.error("Could not cleanup temp folders if exist!", e);
		}
	}

	/**
     * Makes a list of strings providing a strings 
     * which contains comma separated strings
     *
     * @param str String
     * @return List<String> 
     */
    public static List<String> listFromString(String str){
    	List<String> listStrings = new ArrayList<String>();
    	if(ObjectUtil.validString(str)) {
    		//str = str.replaceAll("\\s","");
    		listStrings = Arrays.asList(str.split(", "));
    	}
    	return listStrings;
    }
    
    /**
     * detects the Format from a String
     *
     * @param formatAsString
     * @return
     */
    private Formats detectFormatFromString(String formatAsString){
        Formats result = null;
        if("".equals(formatAsString)){
            result = Formats.EMPTY; 
        }else{
            result = Formats.valueOf(formatAsString); 
        }
        return result; 
    }
    
    /**
     * detects the delimiter from a string
     *
     * @param delimiterAsString
     * @return
     */
    private CsvDelimiters detectDelimiterFromString(String delimiterAsString){
        CsvDelimiters result = null;
        if("".equals(delimiterAsString)){
            result = CsvDelimiters.EMPTY;
        }else{
            result = CsvDelimiters.valueOf(delimiterAsString);
        }
        return  result; 
    }
    
    public Formats getDefaultInputFormat(){
        return defaultInputFormat; 
    }
    
    public Formats getDefaultOutputFormat(){
        return defaultOutputFormat; 
    }
    
    public Operation getDefaultOperation(){
        return defaultOperation; 
    }
    
    public boolean isValidationHeaderErrorsReported(){
        return validationHeaderErrorsReported;
    }
    
    public CsvDelimiters getDefaultCsvInputDelimiter(){
        return defaultCsvInputDelim; 
    }
    
    public CsvInputColumnHeader getDefaultCsvInputHeaderRow(){
        return defaultCsvInputHeaderRow; 
    }
    
    public CsvDateFormat getDefaultCsvInputDateFormat(){
        return defaultCsvInputDateFormat;
    }
    
    public Integer getDefaultMultilevelCsvInputLevels(){
        return defaultMultilevelCsvInputLevels;
    }
    
    public CsvDelimiters getDefaultCsvOutputDelimiter(){
        return defaultCsvOutputDelim; 
    }
    
    public CsvOutputColumnHeader getDefaultCsvOutputHeaderRow(){
        return defaultCsvOutputHeaderRow;
    }
    
    public CsvDateFormat getDefaultCsvOutputDateFormat(){
        return defaultCsvOutputDateFormat; 
    }
    
    public Integer getDefaultMultilevelCsvOutputLevels(){
        return defaultMultilevelCsvOutputLevels;
    }
       
	public boolean isValidationOutputErrorDetails() {
		return validationOutputErrorDetails;
	}

	public String getProjectVersion() {
		return projectVersion;
	}

	public boolean isDefaultWebServiceCsvInputHeaderRow() {
		return defaultWebServiceCsvInputHeaderRow;
	}

	public Boolean getDefaultWebServiceCsvInputHeaderRow() {
		return defaultWebServiceCsvInputHeaderRow;
	}

	public String getDefaultWebServiceCsvInputDelimiter() {
		return defaultWebServiceCsvInputDelimiter;
	}

	public String getDefaultWebServiceCsvInputQuoteCharacter() {
		return defaultWebServiceCsvInputQuoteCharacter;
	}
	
	public boolean isProxyEnabled() {
		return proxyEnabled;
	}
	
	public String getProxyHost() {
		return proxyHost;
	}
	
	public Integer getProxyPort() {
		return proxyPort;
	}
	
	public String getProxyUsername() {
		return proxyUsername;
	}
	
	public String getProxyPassword() {
		return proxyPassword;
	}
	
	public String getJksPath() {
		return jksPath;
	}
	
	public String getJksPassword() {
		return jksPassword;
	}
	
	public Integer getDefaultMaximumErrorsDisplayed(){
	   return this.defaultMaximumErrorsDisplayedNumber;
	}
	
    public List<String> getProxyExclusions() {
		return proxyExclusions;
	}

    public String getRegistryUsername() {
		return registryUsername;
	}

    public String getRegistryPassword() {
		return registryPassword;
	}

    public boolean isRegistryUrlFieldEnabled(){
        return registryUrlFieldEnabled;
    }

    public Double getDefaultMinimumInflateRatioNumber() {
        return defaultMinimumInflateRatioNumber;
    }

    public String getObsValueConfidential() {
        return obsValueConfidential;
    }

    public String getStrObsValueConfidential() {
        return obsValueConfidential;
    }
    
    public boolean isValidationErrorIfEmpty() {
		return validationErrorIfEmpty;
	}

    public boolean isTrimWhitespaces() {
        return trimWhitespaces;
    }


	public List<String> getVisibleInputFormats() {
		return visibleInputFormats;
	}

    public List<String> getVisibleOutputFormats() {
		return visibleOutputFormats;
	}

    public boolean isInlineReportFormat() {
        return inlineReportFormat;
    }

    public void setInlineReportFormat(boolean inlineReportFormat) {
        this.inlineReportFormat = inlineReportFormat;
    }

    public boolean isDefaultCsvOutputEnableNeverUseQuotes() {
        return defaultCsvOutputEnableNeverUseQuotes;
    }

    public boolean isDefaultISdmxCsvAdjustment() {
        return defaultISdmxCsvAdjustment;
    }

    public String getDefaultDsdVersion() {
        return defaultDsdVersion;
    }

    //SDMXCONV-1155
    public Integer getDefaultWebappInputTimeout() {
        return defaultWebappInputTimeout;
    }

	public boolean isCheckBomError() {
		return checkBomError;
	}

	public void setCheckBomError(boolean checkBomError) {
		this.checkBomError = checkBomError;
	}

	public boolean isFormulaErrorsReported() {
		return formulaErrorsReported;
	}

    public String configValuesToString(){
		StringBuilder stringBuilder = new StringBuilder()
				.append("Configuration [")
				.append("inputFormat=").append(defaultInputFormat)
				.append(", outputFormat=").append(defaultOutputFormat)
				.append(", registryUrlEnabled=").append(isRegistryUrlFieldEnabled())
				.append("]");
		return stringBuilder.toString();
	}

	public boolean isErrorIfDataValuesEmpty() {
		return errorIfDataValuesEmpty;
	}

    public Long getDefaultSynchronousValidationTimeout() {
        // we convert the seconds to milliseconds, cause this is the format we use in StruvalWebService
        return defaultSynchronousValidationTimeout;
    }

    public Long getDefaultValidationExpirationTime() {
        // we convert the seconds to milliseconds, cause this is the format we use in StruvalWebService
        return defaultValidationExpirationTime;
    }

    public Long getDefaultValidationCleanupTime() {
        return defaultValidationCleanupTime;
    }

    public Long getDefaultSynchronousConversionTimeout() {
        // we convert the seconds to milliseconds, cause this is the format we use in ConvertWebService
        return defaultSynchronousConversionTimeout;
    }

    public Long getDefaultConversionExpirationTime() {
        // we convert the seconds to milliseconds, cause this is the format we use in ConvertWebService
        return defaultConversionExpirationTime;
    }

    public Long getDefaultConversionCleanupTime() {
        return defaultConversionCleanupTime;
	}

	public String getErrorMessagePath() {
		return errorMessagePath;
	}

	public Integer getMaximumErrorOccurrencesNumber() {
		return maximumErrorOccurrencesNumber;
	}

	public void setMaximumErrorOccurrencesNumber(Integer maximumErrorOccurrencesNumber) {
		this.maximumErrorOccurrencesNumber = maximumErrorOccurrencesNumber;
	}

	public Integer getTaskQueueNumber() {
		return taskQueueNumber;
	}

    public Integer getTaskQueueConversionNumber() {
        return taskQueueConversionNumber;
    }

	public String getDefaultSubfieldSeparator() {
		return defaultSubfieldSeparator;
	}

    public Boolean isAllowAdditionalColumns() {return allowAdditionalColumns;}

	public String getLocalFileStorageConversion() {
		return localFileStorageConversion;
	}

	public String getLocalFileStorageStruval() {
		return localFileStorageStruval;
	}
}
