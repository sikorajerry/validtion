package com.intrasoft.sdmx.converter.services;

import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.sdmxsource.MessageDecoderConverter;
import com.intrasoft.sdmx.converter.util.FormatFamily;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.config.SdmxMLInputConfig;
import org.estat.sdmxsource.engine.decorators.ObservationCounterDecorator;
import org.estat.sdmxsource.sdmx.api.constants.DATA_VERSION;
import org.estat.sdmxsource.sdmx.structureparser.SdmxBeansSchemaDecorator;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.FLRInputConfig;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.estat.struval.ValidationError;
import org.estat.struval.engine.DefaultVersion;
import org.estat.struval.engine.ErrorReporter;
import org.estat.struval.engine.impl.*;
import org.estat.struval.factory.*;
import org.estat.struval.model.ErrorInfo;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ErrorLimitException;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.CsvReadableDataLocation;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.RecordReaderCounter;
import org.sdmxsource.sdmx.dataparser.engine.reader.ThreadLocalOutputReporter;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DeduplicatorDecorator;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.dataparser.manager.DataInformationManager;
import org.sdmxsource.sdmx.dataparser.manager.DataReaderManager;
import org.sdmxsource.sdmx.dataparser.manager.impl.DataValidationManagerImpl;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
import org.sdmxsource.sdmx.structureretrieval.manager.SdmxSuperBeanRetrievalManagerImpl;
import org.sdmxsource.sdmx.util.sdmx.SdmxMessageUtil;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataFormatException;
import org.sdmxsource.sdmx.validation.model.MultipleFailureHandlerEngine;
import org.sdmxsource.util.ObjectUtil;
import org.sdmxsource.util.xml.XmlUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.intrasoft.sdmx.converter.services.InputService.compatibilityCheck;

@Service
public class ValidationService {
	private static Logger logger = LogManager.getLogger(ValidationService.class);

	@Autowired
	private DataInformationManager dataInformationManager;

	@Autowired
	private DataReaderManager dataReaderManager;

	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;

	@Autowired
	private StructureParsingManager structureParsingManager;
	
	@Autowired
    private ConfigService configService;

	private Formats sourceFormat;
	
	private String defaultVersion;

	private static final String DATA_NOT_FOUND = "The data file could not be found";

	// Constant for latest version of Data Structure
	private static final String LATEST = "latest";

	public ValidationServiceReturn validate(File dataFile, 
			                                File structureFile, 
			                                InputConfig inputConfig, 
			                                int errorsLimit) {
		ValidationServiceReturn result;
		try (
				FileInputStream dataInputStream = new FileInputStream(dataFile);
				FileInputStream structureInputStream = new FileInputStream(structureFile)
		) {
			result = validate(dataInputStream, structureInputStream, inputConfig, errorsLimit);
		} catch (IOException e) {
			throw new RuntimeException("The data or the structure file could not be found", e);
		}
		return result;
	}

	/**
	 * Validates the given file against the provided structure
	 * 
	 * @param dataFile - the data file
	 * @param structureFile - the structure file
	 * @return a list of errors (if any)
	 */
	public ValidationServiceReturn validate(InputStream dataFile, 
			                                InputStream structureFile,
			                                InputConfig inputConfig, 
			                                int errorsLimit) {
		ValidationServiceReturn result = new ValidationServiceReturn();
		try {
			ReadableDataLocation dsdLocation = readableDataLocationFactory.getReadableDataLocation(structureFile);
			StructureWorkspace parseStructures = structureParsingManager.parseStructures(dsdLocation);
			//SDMXCONV-792
            SDMX_SCHEMA beansVersion = SdmxMessageUtil.getSchemaVersion(dsdLocation);
			SdmxBeans structureBeans = parseStructures.getStructureBeans(false);
			SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(structureBeans, beansVersion);
			DataStructureBean dsdBean = null;
			if (beansSchemaDecorator.getDataStructures() != null && !beansSchemaDecorator.getDataStructures().isEmpty()) {
				dsdBean = beansSchemaDecorator.getDataStructures().iterator().next();
			}

			DataflowBean dataflowBean = null;
			if (beansSchemaDecorator.getDataflows() != null && !beansSchemaDecorator.getDataflows().isEmpty()) {
				dataflowBean = beansSchemaDecorator.getDataflows().iterator().next();
			}
			result = validate(dataFile, beansSchemaDecorator, inputConfig, dsdBean, dataflowBean, errorsLimit);
		} finally {
			IOUtils.closeQuietly(dataFile);
			IOUtils.closeQuietly(structureFile);
		}
		return result;
	}
	
	/**
	 * Sets in a ThreadLocal variable the version of the structure file.
	 * Which is needed reporting different formats for date.
	 * @see ValidationService#validate(InputStream, SdmxBeans, InputConfig, DataStructureBean, DataflowBean, int)
	 * @param dataFile
	 * @param structureBeans
	 * @param inputConfig
	 * @param dataStructureBean
	 * @param dataflowBean
	 * @param errorsLimit
	 * @param structureVersion
	 * @return
	 */
	public ValidationServiceReturn validate(File dataFile, SdmxBeans structureBeans, InputConfig inputConfig,
			DataStructureBean dataStructureBean, DataflowBean dataflowBean, int errorsLimit, Formats sourceFormat,
			SDMX_SCHEMA structureVersion) {
		ValidationServiceReturn result;
		SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(structureBeans, structureVersion);
		try(FileInputStream dataInputStream = new FileInputStream(dataFile)) {
			this.sourceFormat = sourceFormat;
			result = validate(dataInputStream, beansSchemaDecorator, inputConfig, dataStructureBean, dataflowBean, errorsLimit);
		} catch (IOException e) {
			throw new RuntimeException(DATA_NOT_FOUND, e);
		}
		return result;
	}
	
	/**
	 * validates the given file against the provided structure
	 * 
	 * @param dataFile
	 * @param structureBeans
	 * @return
	 */
	public ValidationServiceReturn validate(File dataFile,
			                                SdmxBeans structureBeans,
			                                InputConfig inputConfig, 
			                                DataStructureBean dataStructureBean, 
			                                DataflowBean dataflowBean, 
			                                int errorsLimit,
			                                Formats sourceFormat,
			                                String defaultVersion) {
		ValidationServiceReturn result;
		try(FileInputStream dataInputStream = new FileInputStream(dataFile)) {
			this.sourceFormat = sourceFormat;
			this.defaultVersion = defaultVersion;
			result = validate(dataInputStream, structureBeans, inputConfig, dataStructureBean, dataflowBean, errorsLimit);
		} catch (IOException e) {
			throw new RuntimeException(DATA_NOT_FOUND, e);
		}
		return result;
	}

	/**
	 * Validates the data in the input stream against the structure (provided as
	 * sdmxBeans).
	 * 
	 * @param dataFile the data input stream
	 * @param structureBeans the sdmxBeans containing the structure
	 * @return a list of errors (if any)
	 */
	public ValidationServiceReturn validate(InputStream dataFile,
			                                SdmxBeans structureBeans,
			                                InputConfig inputConfig, 
			                                DataStructureBean dataStructureBean, 
			                                DataflowBean dataflowBean, 
			                                int errorsLimit) {

		ValidationServiceReturn result = new ValidationServiceReturn();
		List<ValidationError> errors = new ArrayList<>();
		MessageDecoderConverter messageDecoder = new MessageDecoderConverter(configService);
		DataReaderEngine dataReaderEngine = null;
		DataReaderEngine validatingDataReaderEngine = null;
		MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(errorsLimit);
		int numberOfErrors = 0;
		SDMX_SCHEMA sdmxSchema;
		//SDMXCONV-1523
		if (ObjectUtil.validObject(structureBeans) && structureBeans instanceof SdmxBeansSchemaDecorator) {
			sdmxSchema = ((SdmxBeansSchemaDecorator) structureBeans).getSdmxSchema();
			if (ObjectUtil.validObject(inputConfig))
				inputConfig.setStructureSchemaVersion(sdmxSchema);
		}
		StruvalConstraintValidatorFactory constraintValidatorFactory = null;
		DataValidationErrorDeduplicator deduplicatorDuplicateObs = null;
		DataValidationErrorDeduplicator deduplicatorDeep = null;
		DataValidationErrorDeduplicator deduplicatorConstraints = null;
		DataValidationErrorDeduplicator deduplicatorMandatory = null;
		try (BOMInputStream bomInStream = new BOMInputStream(dataFile)) {
			SdmxBeanRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBeans);
			// SDMXCONV-1198
			if (configService.isCheckBomError() && bomInStream.hasBOM()) {
				final String errorString = "Unexpected character encoding: 'UTF-8 BOM'. Please make sure the input file is encoded in 'UTF-8'.";
				throw new SdmxDataFormatException(ExceptionCode.XML_PARSE_EXCEPTION, null, 1, 1, errorString);
			}
			ReadableDataLocation sourceData = readableDataLocationFactory.getReadableDataLocation(bomInStream);
			DATA_VERSION dataInputVersion = DATA_VERSION.NULL;
			final ReadableDataLocation actualSourceData;
			if (inputConfig != null) {
				if (inputConfig instanceof CsvInputConfig) {
					int levelNumber = Integer.parseInt(((CsvInputConfig) inputConfig).getLevelNumber());
					if (levelNumber > 1) {
						actualSourceData = new MultiLevelCsvReadableDataLocation(sourceData, (CsvInputConfig) inputConfig);
					} else {
						actualSourceData = new SingleLevelCsvReadableDataLocation(sourceData, (CsvInputConfig) inputConfig);
					}
					dataInputVersion = DATA_VERSION.NON_SDMX;
				} else if (inputConfig instanceof SdmxCsvInputConfig) {
					actualSourceData = new CsvReadableDataLocation(sourceData, (SdmxCsvInputConfig) inputConfig);
					if (((SdmxCsvInputConfig) inputConfig).isSdmxCsv20()) {
						dataInputVersion = DATA_VERSION.SDMX_30_COMPATIBLE;
					} else {
						dataInputVersion = DATA_VERSION.SDMX_21_COMPATIBLE;
					}
				} else if (inputConfig instanceof FLRInputConfig) {
					FLRInputConfig flrConfig = (FLRInputConfig) inputConfig;
					flrConfig.setBeanRetrieval(retrievalManager);
					actualSourceData = new FlrReadableDataLocation(sourceData, (FLRInputConfig) inputConfig);
					dataInputVersion = DATA_VERSION.NON_SDMX;
				} else if (inputConfig instanceof SdmxMLInputConfig) {
					dataInputVersion = findInputVersionCompatibility(sourceData);
					actualSourceData = sourceData;
				} else {
					actualSourceData = new ExcelReadableDataLocation(sourceData, (ExcelInputConfigImpl) inputConfig);
					dataInputVersion = DATA_VERSION.NON_SDMX;
				}
			} else {
				dataInputVersion = findInputVersionCompatibility(sourceData);
				actualSourceData = sourceData;
			}
			compatibilityCheck(sourceFormat, inputConfig.getStructureSchemaVersion());
			// see the workaround below (in the catch section).getDataType(sourceData)
			if (dataStructureBean == null && dataflowBean == null) {
				dataReaderEngine = dataReaderManager.getDataReaderEngine(actualSourceData, retrievalManager);
			} else {
				dataReaderEngine = dataReaderManager.getDataReaderEngine(actualSourceData, dataStructureBean,
						dataflowBean);
			}
			if (dataReaderEngine instanceof org.estat.sdmxsource.extension.model.ErrorReporter) {
				((org.estat.sdmxsource.extension.model.ErrorReporter) dataReaderEngine).setExceptionHandler(
						exceptionHandler);
			} else {
				logger.warn("dataReaderEngine used for validation does not implement the ErrorReporter interface");
			}
			int order = 0;
			//SDMXCONV-1363 A map that holds all the errors from the validators. Used to deduplicate the errors.
			LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> dataValidationErrors = new LinkedHashMap<>();
			Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
			if (dataReaderEngine instanceof DataValidationErrorHolder) {
				((DataValidationErrorHolder) dataReaderEngine).setMaximumErrorOccurrencesNumber(configService.getMaximumErrorOccurrencesNumber());
				((DataValidationErrorHolder) dataReaderEngine).setErrorsByEngine(dataValidationErrors);
				((DataValidationErrorHolder) dataReaderEngine).setOrder(order);
				((DataValidationErrorHolder) dataReaderEngine).setErrorLimit(errorsLimit);
				errorPositions = ((DataValidationErrorHolder) dataReaderEngine).getErrorPositions();
			}

			//SDMXCONV-749
			if (dataReaderEngine instanceof DefaultVersion) {
				if (sourceFormat != null && defaultVersion != null) {
					String defVersion = setDefaultVersionToStructure(sourceFormat, defaultVersion);
					((DefaultVersion) dataReaderEngine).setDefaultVersion(defVersion);
				} else {
					((DefaultVersion) dataReaderEngine).setDefaultVersion(null);
				}
			}
			dataReaderEngine = new ObservationCounterDecorator(dataReaderEngine, inputConfig, sourceFormat, dataflowBean);
			// Report an error of bad formed XML document at start, because of the limit of the errors is not reported at all
			exceptionHandler.wellFormedXml(actualSourceData.getInputStream(), XmlUtil.isXML(actualSourceData));
			//SDMXCONV-1338
			if (sourceFormat != null && sourceFormat.getFamily() == FormatFamily.SDMX || inputConfig == null) {
				validatingDataReaderEngine = new ValidateHeaderDataReaderEngine(dataInformationManager,
						dataReaderEngine, sourceData, exceptionHandler, configService.isValidationHeaderErrorsReported());
			} else {
				validatingDataReaderEngine = new ValidateCSVDataReaderEngine(dataInformationManager, dataReaderEngine,
						sourceData, exceptionHandler);
			}
			DataValidationManagerImpl dataValidationManager = new DataValidationManagerImpl();
			dataValidationManager.setSuperBeanRetrievalManager(new SdmxSuperBeanRetrievalManagerImpl(retrievalManager));

			//Add Struval Deep Validation Factory
			{
				// SDMXCONV-693 Set Boolean value to determine if we will report Time Period
				// if SDMX_SCHEMA.VERSION_TWO and not Csv report time_period = true
				boolean isTwoPointZero = false;
				if (ObjectUtil.validObject(this.sourceFormat) && this.sourceFormat.getFamily() == FormatFamily.SDMX && (SdmxMessageUtil.getSchemaVersion(
						sourceData) == SDMX_SCHEMA.VERSION_TWO)) {
					isTwoPointZero = true;
				}
				String isConfidential = configService.getObsValueConfidential();
				StruvalDeepValidatorFactory struvalDeep = new StruvalDeepValidatorFactory();
				deduplicatorDeep = new DataValidationErrorDeduplicator(struvalDeep, isTwoPointZero, isConfidential, dataInputVersion,
						dataValidationErrors, configService.getMaximumErrorOccurrencesNumber(), order, errorsLimit, errorPositions,
						inputConfig.getStructureSchemaVersion());
				dataValidationManager.addValidatorFactory(deduplicatorDeep);
			}
			//Add StruvalMandatoryAttributesFactory that checks if mandatory attributes have values
			{
				deduplicatorMandatory = new DataValidationErrorDeduplicator(new StruvalMandatoryAttributesFactory(),
						dataValidationErrors, configService.getMaximumErrorOccurrencesNumber(), order, errorsLimit,
						errorPositions, inputConfig.getStructureSchemaVersion());
				dataValidationManager.addValidatorFactory(deduplicatorMandatory);
			}
			//Add StruvalDuplicateObsValidationFactory that checks for duplicate observations
			{
				deduplicatorDuplicateObs = new DataValidationErrorDeduplicator(new StruvalDuplicateObsValidationFactory(), dataValidationErrors,
						configService.getMaximumErrorOccurrencesNumber(), order, errorsLimit, errorPositions,
						inputConfig.getStructureSchemaVersion());
				dataValidationManager.addValidatorFactory(deduplicatorDuplicateObs);
			}
			//Add StruvalConstraintValidatorFactory that checks if constraints are followed
			{
				constraintValidatorFactory = new StruvalConstraintValidatorFactory();
				constraintValidatorFactory.setBeanRetrievalManager(retrievalManager);
				deduplicatorConstraints = new DataValidationErrorDeduplicator(constraintValidatorFactory,
						retrievalManager, dataValidationErrors, configService.getMaximumErrorOccurrencesNumber(), order,
						errorsLimit, errorPositions, inputConfig.getStructureSchemaVersion());
				dataValidationManager.addValidatorFactory(deduplicatorConstraints);
			}
			try {
				dataValidationManager.validateData(validatingDataReaderEngine, exceptionHandler);
			} catch (ErrorLimitException e) {
				result.setHasMoreErrors(true);
				logger.info("maximum number of errors reached {}", errorsLimit);
			} finally {
				if (dataReaderEngine != null)
					dataReaderEngine.close();
				//throw only the deduplicated errors
				numberOfErrors = DeduplicatorDecorator.handleExceptions(dataValidationErrors, exceptionHandler);
				result.setNumberOfErrorsFound(numberOfErrors);
			}
			if (dataReaderEngine instanceof ObservationCounterDecorator) {
				result.setObsCount(((ObservationCounterDecorator) dataReaderEngine).getObservationCounter());
			}
		} catch (RuntimeException | IOException rex) {
			// workaround for a design flaw in SdmxSource api 
			// (which tries to read the header of the input file)
			// in case the input file has syntax errors, 
			// this getter below throws a Runtime Exception
			// (having XmlStreamException as the cause
			if (rex.getCause() instanceof SdmxException) {
				SdmxException ex = (SdmxException) rex;
				ErrorInfo errorInfo = ErrorReporter.fromException(ex); //SDMXCONV-1148
				errors.add(new ValidationError(errorInfo, configService.getMaximumErrorOccurrencesNumber()));
				numberOfErrors = errors.size();
			} else { // one of the cases is: rex.getCause() instanceof XMLStreamException
				if (!(rex instanceof ErrorLimitException)) {
					ErrorInfo errorInfo = ErrorReporter.fromException(rex); //SDMXCONV-1148
					errors.add(new ValidationError(errorInfo, configService.getMaximumErrorOccurrencesNumber()));
					numberOfErrors = errors.size();
				}
			}
		} finally {
			ThreadLocalOutputReporter.unset();
			IOUtils.closeQuietly(dataFile);
			if (dataReaderEngine != null)
				dataReaderEngine.close();
			if (validatingDataReaderEngine != null)
				validatingDataReaderEngine.close();
			errors.addAll(buildErrorReport(exceptionHandler.getExceptions()));
			messageDecoder.clear();
			result.setNumberOfErrorsFound(numberOfErrors);
			if(deduplicatorConstraints!=null)
				deduplicatorConstraints.destroy();
			if(deduplicatorDeep!=null)
				deduplicatorDeep.destroy();
			if(deduplicatorMandatory!=null)
				deduplicatorMandatory.destroy();
			if(deduplicatorDuplicateObs!=null)
				deduplicatorDuplicateObs.destroy();
		}
		result.setErrors(errors);
		return result;
	}

	private DATA_VERSION findInputVersionCompatibility(ReadableDataLocation sourceData) {
		DATA_VERSION dataInputVersion = DATA_VERSION.NULL;
		SDMX_SCHEMA inputVersion = SdmxMessageUtil.getSchemaVersion(sourceData);
		if(SDMX_SCHEMA.VERSION_THREE == inputVersion) {
			dataInputVersion = DATA_VERSION.SDMX_30_COMPATIBLE;
		} else if(SDMX_SCHEMA.VERSION_TWO_POINT_ONE == inputVersion) {
			dataInputVersion = DATA_VERSION.SDMX_21_COMPATIBLE;
		}else if(SDMX_SCHEMA.VERSION_TWO == inputVersion || SDMX_SCHEMA.VERSION_ONE == inputVersion) {
			dataInputVersion = DATA_VERSION.SDMX_20_COMPATIBLE;
		}else if(SDMX_SCHEMA.EDI == inputVersion) {
			dataInputVersion = DATA_VERSION.EDI;
		}else if(SDMX_SCHEMA.XLSX == inputVersion) {
			dataInputVersion = DATA_VERSION.NON_SDMX;
		}
		return dataInputVersion;
	}

	/**
	 * builds the error list from the list of throwables
	 * 
	 * @param exceptionsList - list of throwables
	 * @return a list of ValidationError objects
	 */
	private List<ValidationError> buildErrorReport(List<Throwable> exceptionsList) {
		List<ValidationError> result = new ArrayList<>();
		for (Throwable exception : exceptionsList) {
			if (!(exception instanceof ErrorLimitException)) {
				ErrorInfo errorInfo = ErrorReporter.fromException((Exception)exception); //SDMXCONV-1148
				result.add(new ValidationError(errorInfo, configService.getMaximumErrorOccurrencesNumber()));
			}
		}
		return result;
	}
	
	/**
	 * This method is used to set Converter Structure with default version and
	 * with the retrieval manager and null for specific dataStructure/flow
	 * If we are here it means we have multiple dsds  in the datastructure
	 * and the readers must be set with the retrieval manager only
	 * see SDMXCONV-749
	 * @param sourceFormat
	 * @param defaultVersion
	 * @return ConverterStructure
	 */
	private String setDefaultVersionToStructure(Formats sourceFormat, String defaultVersion) {
		String version = null;
		switch (sourceFormat) {
	        //sourceFormat.isSdmx21()
		 	case STRUCTURE_SPECIFIC_DATA_2_1: 
		 	case STRUCTURE_SPECIFIC_TS_DATA_2_1: 
		 	case GENERIC_DATA_2_1: 
		 	case GENERIC_TS_DATA_2_1: 
	    		if(defaultVersion==null || "".equals(defaultVersion)) {
	    			version = "1.0";
	    		}else {
		    		if(LATEST.equals(defaultVersion)) {
		    			version = null;
		    		}else {
		    			version = defaultVersion;
		    		}
	    		}
	    	break;
			case SDMX_CSV:
		 	case GESMES_TS:
		 	case COMPACT_SDMX:
		 	case UTILITY_SDMX:
		 	case CROSS_SDMX:
		 	case GENERIC_SDMX:
	    		if(defaultVersion!=null && !"".equals(defaultVersion)) {
	    			version = defaultVersion;
	    		}
	    		if(LATEST.equals(defaultVersion)) {
	    			version = null;
	    		}
	    	break;
		}
		return version;
	}
}
