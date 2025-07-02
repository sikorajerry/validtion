package com.intrasoft.sdmx.converter.io.data.csv;

import com.intrasoft.sdmx.converter.io.data.TranscodingEngine;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.UnescapedQuoteHandling;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.extension.datavalidation.Pair;
import org.estat.sdmxsource.extension.model.ErrorReporter;
import org.estat.sdmxsource.sdmx.api.constants.DATASET_LEVEL;
import org.estat.sdmxsource.util.TimeFormatBuilder;
import org.estat.sdmxsource.util.csv.*;
import org.estat.struval.builder.impl.ObservationBuilder;
import org.sdmxsource.sdmx.api.constants.*;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.exception.SdmxSyntaxException;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.beans.registry.ProvisionAgreementBean;
import org.sdmxsource.sdmx.api.model.data.*;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.InternalAnnotationEngine;
import org.sdmxsource.sdmx.dataparser.engine.reader.RecordReaderCounter;
import org.sdmxsource.sdmx.dataparser.engine.reader.ThreadLocalOutputReporter;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.AnnotationBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.ComplexNodeValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyableImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetHeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.sdmx.util.beans.ConceptRefUtil;
import org.sdmxsource.sdmx.util.beans.reference.MaintainableRefBeanImpl;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.sdmx.validation.exceptions.TypeOfException;
import org.sdmxsource.sdmx.validation.model.MultipleFailureHandlerEngine;
import org.sdmxsource.util.ObjectUtil;
import org.sdmxsource.util.io.ByteCountHolder;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.poi.util.IOUtils.closeQuietly;
import static org.sdmxsource.sdmx.dataparser.engine.reader.AnnotationType.*;
import static org.sdmxsource.util.ObjectUtil.findDuplicateBySetAdd;

public class SingleLevelCsvDataReaderEngine implements DataValidationErrorHolder, DataReaderEngine, ErrorReporter, RecordReaderCounter, InternalAnnotationEngine {

	private static Logger logger = LogManager.getLogger(SingleLevelCsvDataReaderEngine.class);
	private final boolean useXSMeasures;
	private final boolean isTimeSeries;
	private final Map<String, ComponentBean> mapOfComponentBeans = new HashMap<>();
	private final Map<String, String> xsMeasureIdToMeasureCode = new HashMap<>();
	private final CsvInputConfig csvInputConfig;
	private CountingInputStream countingStream;
	private ReadableDataLocation dataLocation;
	private final CsvParserSettings settings;
	private final String dimensionAtObservation;
	private DataStructureBean dsd;
	private DataflowBean dataflow;
	private final Deque<Closeable> closables = new LinkedList<>();
	private final Deque<Keyable> listOfKeyable = new LinkedList<>();
	private final Deque<Observation> listOfObservation = new LinkedList<>();
	/**
	 * The observation builder.
	 */
	private final ObservationBuilder observationBuilder = new ObservationBuilder();
	/**
	 * The time format builder
	 */
	private final TimeFormatBuilder timeFormatBuilder = new TimeFormatBuilder();
	// SDMXCONV-1185
	private final Map<String, CodeDataInfo> columnsPresenceMapping = new HashMap<>();
	private final List<KeyValue> missingDimensions = new ArrayList<>();
	private final List<KeyValue> missingAttributes = new ArrayList<>();
	private final List<KeyValue> missingObsAttributes = new ArrayList<>();
	// SDMXCONV-1194
	private final boolean errorIfDataValuesEmpty;
	/**
	 * The exception handler. When validation is performed this will be of type
	 * MultipleFailureHandler
	 */
	private ExceptionHandler exceptionHandler;
	private final TranscodingEngine transcoding;
	private List<KeyValue> datasetAttributes;
	private List<KeyValue> previousReadDataSetAttributes;
	private Keyable currentKeyable;
	private Observation currentObservation;
	private CsvParser parser;
	private boolean movedToNextDataset;
	private String[] headerColumns;
	private DatasetHeaderBean currentDatasetHeaderBean;
	private int rowNumber = 0;

	/**
	 * count the observation that are ignored
	 */
	private int ignoredObsCount;
	/**
	 * count the observations that are processed
	 */
	private volatile int obsCount;
	/**
	 * count the series that are processed
	 */
	private volatile int seriesCount;
	/**
	 * count the dataset that are processed
	 */
	private volatile int datasetCount;

	private boolean isCurrentRowNull = false;

	private LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();

	private Integer maximumErrorOccurrencesNumber;

	private int order;

	private int errorLimit;

	private Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();

	List<AnnotationBeanImpl> obsAnnotations = new ArrayList<>();

	private int firstRowOfDataLength;
	Map<String, AttributeBean> attributesObservationLevelWithPositions;
	Map<String, AttributeBean> attributesSeriesLevelWithPositions;
	@Override
	public List<AnnotationBeanImpl> getObsAnnotations() {
		return obsAnnotations;
	}
	@Override
	public void setObsAnnotations(List<AnnotationBeanImpl> obsAnnotations) {
		this.obsAnnotations = obsAnnotations;
	}

	@Override
	public Object2ObjectLinkedOpenHashMap<String, ErrorPosition> getErrorPositions() {
		return errorPositions;
	}

	@Override
	public void setErrorPositions(Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions) {
		this.errorPositions = errorPositions;
	}

	/**
	 * Constructs a SingleLevelCsvDataReaderEngine with the specified parameters.
	 * This constructor initializes the data reading engine for CSV files based on SDMX (Statistical Data and Metadata eXchange) standards.
	 *
	 * @param dataLocation the input stream of the CSV file to be read.
	 * @param dsd the data structure definition used for interpreting the CSV data.
	 * @param dataflowBean contains details about the data flow, used for data interpretation.
	 * @param beanRetrieval manages retrieval of SDMX beans which are essential for data interpretation.
	 * @param csvInputConfig configuration settings for CSV input.
	 * @param exceptionHandler handles exceptions that occur during the reading process.
	 * @throws IllegalArgumentException if necessary parameters for initialization are missing or if the structure retrieval fails.
	 */
	public SingleLevelCsvDataReaderEngine(ReadableDataLocation dataLocation,
										  DataStructureBean dsd,
										  DataflowBean dataflowBean,
										  SdmxBeanRetrievalManager beanRetrieval,
										  CsvInputConfig csvInputConfig,
										  ExceptionHandler exceptionHandler) {

		this.dataLocation = dataLocation;
		// Validate essential parameters for data interpretation
		if (beanRetrieval == null && dsd == null && dataflowBean == null) {
			throw new IllegalArgumentException(
					"SingleLevelCsvDataReaderEngine expects either a SdmxBeanRetrievalManager or a DataStructureBean to be able to interpret the structures");
		}
		this.dsd = dsd;
		this.dataflow = dataflowBean;
		// Retrieve the DataStructureBean based on the presence of external references
		if(beanRetrieval != null) {
			if (dataflowBean != null && (dataflowBean.isExternalReference() == null || !dataflowBean.isExternalReference().isTrue())) {
				this.dsd = beanRetrieval.getMaintainableBean(DataStructureBean.class, dataflowBean.getDataStructureRef());
			} else {
				Set<DataStructureBean> maintainableBeans = beanRetrieval.getMaintainableBeans(DataStructureBean.class, new MaintainableRefBeanImpl(), false, true);
				if (!maintainableBeans.isEmpty()) {
					this.dsd = maintainableBeans.iterator().next();
				} else {
					throw new IllegalArgumentException(
							"SingleLevelCsvDataReaderEngine expects DataStructureBean to be able to interpret the structures");
				}
			}
		}
		// Check if the dimension list from the DSD is retrievable
		if (this.dsd != null && this.dsd.getDimensionList() == null) {
			throw new IllegalArgumentException(
					"Structure retrieval was not possible, could not retrieve dimension list from this dsd");
		}
		this.csvInputConfig = csvInputConfig;
		// Initialize component mapping
		for (ComponentBean component : this.dsd.getComponents()) {
			Map.Entry<String, ComponentBean> pair = getComponentId(component);
			this.mapOfComponentBeans.put(pair.getKey(), pair.getValue());
		}
		this.transcoding = new TranscodingEngine(csvInputConfig.getTranscoding());
		this.dimensionAtObservation = getDimensionAtObservation();
		this.isTimeSeries = this.dsd.getTimeDimension() != null
				&& dimensionAtObservation.equalsIgnoreCase(this.dsd.getTimeDimension().getId());
		this.exceptionHandler = exceptionHandler;
		// Configure CSV parser settings
		this.settings = new CsvParserSettings();
		settings.setIgnoreLeadingWhitespaces(csvInputConfig.isTrimSpaces());
		settings.setIgnoreTrailingWhitespaces(csvInputConfig.isTrimSpaces());
		settings.setInputBufferSize(10000000);
		settings.setKeepQuotes(false);
		settings.setLineSeparatorDetectionEnabled(true);
		settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_DELIMITER);
		CsvFormat format = new CsvFormat();
		format.setDelimiter(csvInputConfig.getDelimiter());
		settings.setFormat(format);
		settings.setEmptyValue("");
		settings.setNullValue(null);
		// Adjust maximum columns based on configuration
		if (ObjectUtil.validObject(csvInputConfig.getNumberOfColumns()) && csvInputConfig.getNumberOfColumns() > 499) {
			settings.setMaxColumns(csvInputConfig.getNumberOfColumns().intValue() + 1);
		}
		populateXsMeasureIdToMeasureCode();
		this.useXSMeasures = csvInputConfig.isMapCrossXMeasure() && this.xsMeasureIdToMeasureCode != null && !this.xsMeasureIdToMeasureCode.isEmpty();
		this.errorIfDataValuesEmpty = csvInputConfig.isErrorIfDataValuesEmpty();
		settings.getFormat().setComment('\0');
	}

	/**
	 * For SDMX VERSION TWO the component id must be the Concept ref except from Primary measure and time period.
	 * @param component
	 * @return Map.Entry
	 */
	private Map.Entry<String, ComponentBean> getComponentId(ComponentBean component) {
		String id = null;
		ComponentBean componentBean = component;
		if(csvInputConfig.getStructureSchemaVersion() == SDMX_SCHEMA.VERSION_TWO) {
			if(component != this.dsd.getTimeDimension() && component != this.dsd.getPrimaryMeasure()) {
				id = ConceptRefUtil.getConceptId(component.getConceptRef());
			} else if(component == this.dsd.getTimeDimension()) {
				id = getDataStructure().getTimeDimension().getId();
				componentBean = getDataStructure().getTimeDimension();
			} else if(component == this.dsd.getPrimaryMeasure()) {
				id = getDataStructure().getPrimaryMeasure().getId();
				componentBean =  getDataStructure().getPrimaryMeasure();
			}
		} else {
			id = component.getId();
		}
		return Pair.of(id, componentBean);
	}

	@Override
	public HeaderBean getHeader() {
		return csvInputConfig.getHeader();
	}

	@Override
	public DataReaderEngine createCopy() {
		return null;
	}

	@Override
	public ProvisionAgreementBean getProvisionAgreement() {
		return null;
	}

	@Override
	public DataflowBean getDataFlow() {
		return this.dataflow;
	}

	@Override
	public DataStructureBean getDataStructure() {
		return this.dsd;
	}

	@Override
	public int getDatasetPosition() {
		return 0;
	}

	@Override
	public int getKeyablePosition() {
		return 0;
	}

	@Override
	public int getObsPosition() {
		return 0;
	}

	@Override
	public DatasetHeaderBean getCurrentDatasetHeaderBean() {
		return this.currentDatasetHeaderBean;
	}

	@Override
	public List<KeyValue> getDatasetAttributes() {
		return this.datasetAttributes;
	}

	@Override
	public Keyable getCurrentKey() {
		return this.currentKeyable;
	}

	@Override
	public Observation getCurrentObservation() {
		return this.currentObservation;
	}

	/**
	 * Method to check if the attributes in Dataset are correct
	 */
	private void checkDatasetValues(String[] currentRow) {
		// SDMXCONV-915
		if (currentRow != null) {
			getDataSetAttributeFromLineOfCsv(currentRow);
		}
		if (!previousReadDataSetAttributes.equals(getDatasetAttributes())) {
			String notPresent = elementToReport(previousReadDataSetAttributes, getDatasetAttributes());
			previousReadDataSetAttributes = getDatasetAttributes();

			String errorMessage = "Row " + rowNumber + " was found with a different value for dataset level attribute "
					+ notPresent + ". Value should be unique across all observations.";
			final DataValidationError dataValidationError = new DataValidationError
					.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
					.errorPosition(new ErrorPosition(rowNumber))
					.typeOfException(TypeOfException.SdmxSyntaxException)
					.args(errorMessage, new ErrorPosition(rowNumber))
					.order(++this.order)
					.build();
			addError(ValidationEngineType.READER, dataValidationError);
		}
	}

	/**
	 * <strong>Find Attributes to report</strong>
	 * <p>
	 * Check the list of attributes of the header with the list of attributes on the
	 * current line. If this is different we have to report which attributes are the
	 * wrong ones.
	 * </p>
	 *
	 * @param attributes        Previous row attributes
	 * @param attributesNextRow Current row attributes
	 * @return list of attributes that differ as a String
	 */
	private String elementToReport(List<KeyValue> attributes, List<KeyValue> attributesNextRow) {
		List<KeyValue> notPresentKv = attributes.stream().filter(element -> !attributesNextRow.contains(element))
				.collect(Collectors.toList());
		return notPresentKv.stream().map(KeyValue::getConcept).collect(Collectors.joining(","));
	}

	// SDMXCONV-873
	private boolean isTooBig(String column) {
		// SDMXCONV-1072
		return column != null && column.length() > 200 && column.contains("\"");
	}

	/*
	 * remove from the observation queue the first obs and set it as the current
	 * observation read the next line, split line, if this line keayble is not the
	 * same as current then store it in keyable queue and store obs in observation
	 * queue else if the keyable is the same, set obs as current obs
	 */
	@Override
	public boolean moveNextObservation() {
		boolean result = false;
		if (!listOfObservation.isEmpty()) {
			currentObservation = listOfObservation.remove();
			result = true;
		} else {
			String[] newRow = readNextLineOfCsvFile();
			checkDatasetValues(newRow);
			// reach the end of file
			if (newRow != null) {
				if (isCurrentRowSizeNotValid(newRow)) {
					String errorMessage = "Row " + rowNumber + " of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
							.errorPosition(new ErrorPosition(rowNumber))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(errorMessage, new ErrorPosition(rowNumber))
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
				}
				Keyable newKeyable = getKeyableFromLineOfCsv(newRow);
				if (newKeyable != null && newKeyable.equals(currentKeyable)) {
					result = setNewCurrentObservation(newRow, newKeyable);
				} else {
					setNewCurrentKeyable(newRow, newKeyable);
					result = false;
				}
			}
		}
		return result;
	}

	/**
	 * <h3>Method that adds the current observation to the list of
	 * observations.</h3>
	 *
	 * @param newRow  array of strings with the csv row of data values.
	 * @param keyable the current keyable
	 * @return boolean true when an observation was found
	 */
	private boolean setNewCurrentObservation(String[] newRow, Keyable keyable) {
		boolean resultExists;
		listOfObservation.addAll(getObservationFromLineOfCsv(newRow, keyable));
		if (listOfObservation.isEmpty()) {
			resultExists = false;
		} else {
			currentObservation = listOfObservation.remove();
			resultExists = true;
		}
		return resultExists;
	}

	/**
	 * <h3>Method that adds the current keyable to the list of keyables and the new
	 * observations of this new keyable.</h3>
	 *
	 * @param newRow  array of strings with the csv row of data values.
	 * @param keyable the current keyable
	 */
	private void setNewCurrentKeyable(String[] newRow, Keyable keyable) {
		getGroupFromLineOfCsv(newRow);
		listOfKeyable.add(keyable);
		listOfObservation.addAll(getObservationFromLineOfCsv(newRow, keyable));
	}

	/*
	 * If there is a pending keyable we remove it from the queue and set it as
	 * current If there isn't a pending keyable we read the next line from csv and
	 * again store the keyable
	 */
	@Override
	public boolean moveNextKeyable() {
		boolean result = true;

		if (isCurrentRowNull)
			return false;

		if (!listOfKeyable.isEmpty()) {
			currentKeyable = listOfKeyable.remove();
		} else {
			String[] newRow = readNextLineOfCsvFile();
			checkDatasetValues(newRow);
			if (newRow != null && isCurrentRowSizeNotValid(newRow)) {
				String errorMessage = "Row " + rowNumber + " of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used";
				final DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
						.errorPosition(new ErrorPosition(rowNumber))
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(errorMessage, new ErrorPosition(rowNumber))
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
			Keyable newKeyable = null;
			while (newRow != null && (newKeyable == null || currentKeyable.equals(newKeyable))) {
				getGroupFromLineOfCsv(newRow);
				newKeyable = getKeyableFromLineOfCsv(newRow);
				listOfObservation.addAll(getObservationFromLineOfCsv(newRow, newKeyable));
				newRow = readNextLineOfCsvFile();
				checkDatasetValues(newRow);
				if (newRow != null && isCurrentRowSizeNotValid(newRow)) {
					String errorMessage = "Row " + rowNumber + " of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
							.errorPosition(new ErrorPosition(rowNumber))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(errorMessage, new ErrorPosition(rowNumber))
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
				}
			}
			if (newRow == null) {
				result = false;
			}
			currentKeyable = newKeyable;
		}
		return result;
	}

	@Override
	public boolean moveNextDataset() {
		if (!this.movedToNextDataset) {
			this.movedToNextDataset = true;
			// set the current DatasetHeaderBean
			DatasetStructureReferenceBean datasetStructureReferenceBean = new DatasetStructureReferenceBeanImpl(null,
					dsd.asReference(), null, null, dimensionAtObservation);
			// SDMXCONV-880
			if (getHeader() != null && getHeader().getAction() != null && getHeader().getAction().getAction() != null) {
				this.currentDatasetHeaderBean = new DatasetHeaderBeanImpl(dsd.getId(),
						DATASET_ACTION.getAction(getHeader().getAction().getAction()), datasetStructureReferenceBean);
			} else {
				this.currentDatasetHeaderBean = new DatasetHeaderBeanImpl(dsd.getId(), DATASET_ACTION.INFORMATION, datasetStructureReferenceBean);
			}
			String[] firstRowOfData;
			if (csvInputConfig.getInputColumnHeader().equals(CsvInputColumnHeader.USE_HEADER)) {
				// if the CSV has a header, then this is the first line being read
				headerColumns = readNextLineOfCsvFile();
				// if no mapping is provided the we build one from the column header
				if (csvInputConfig.getMapping() == null || csvInputConfig.getMapping().isEmpty()) {
					csvInputConfig.setMapping(buildMappingFromColumnHeader(headerColumns));
				} else {
					// SDMXCONV-1135
					if (csvInputConfig.isValidateHeaderRowAgainstMapping()) {
						checkHeaderRowAgainstMapping();
					}
				}
				// the next row is the data row
				firstRowOfData = readNextLineOfCsvFile();
			} else {
				if (csvInputConfig.getInputColumnHeader().equals(CsvInputColumnHeader.DISREGARD_HEADER)) {
					// ignore the first line containing the header columns and read the next line to
					// load the first record
					headerColumns = readNextLineOfCsvFile();
					if (csvInputConfig.getMapping() == null || csvInputConfig.getMapping().isEmpty()) {
						//logger.info("...so we build the mapping from the dsd components");
						csvInputConfig.setMapping(buildDefaultMapping(useXSMeasures));
					} else {
						logger.debug("the csv reader already has a mapping, we don't build one from dsd components");
					}
				}
				firstRowOfData = readNextLineOfCsvFile();
				//logger.info("the csv input does not have a header");
				if (csvInputConfig.getMapping() == null || csvInputConfig.getMapping().isEmpty()) {
					//logger.info("...so we build the mapping from the dsd components");
					csvInputConfig.setMapping(buildDefaultMapping(useXSMeasures));
				} else {
					logger.debug("the csv reader already has a mapping, we don't build one from dsd components");
				}
			}
			final Set<String> columnsSet = csvInputConfig.getMapping().keySet();
			final String[] mappingColumns = Arrays.copyOf(columnsSet.toArray(), columnsSet.size(), String[].class);
			if (firstRowOfData == null) {
				// SDMXCONV-1185
				if (ObjectUtil.validArray(mappingColumns)) {
					checkMandatoryColumns(mappingColumns);
				}
				return false;
			} else {
				this.datasetCount = this.datasetCount + 1;
				//length of the first Row of Data
				this.firstRowOfDataLength = firstRowOfData.length;
				int numOfColumnsFromMap = countNotEmptyMappingCols(csvInputConfig.getMapping()); // SDMXCONV-955
				// if we have a wrong delimiter the first row is seen as one column or the size is less then the mapping
				if (this.firstRowOfDataLength == 1) {
					String errorMessage = "Row " + rowNumber + " of file was found with number of values different from the number of columns; " +
							"check the field separator, the number of fields in the row and if a non-authorized character was used";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
							.errorPosition(new ErrorPosition(rowNumber))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(errorMessage, new ErrorPosition(rowNumber))
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
					return false; // SDMXCONV-1111
				}
				// SDMXCONV-1185
				if (ObjectUtil.validArray(mappingColumns)) {
					checkMandatoryColumns(mappingColumns);
				}
				/* See also SDMXCONV-1528
				 File with mapping (simple csv): will raise an error if they are more columns in the file than what is declared in the mapping.
				 In other words if a column is not used in the mapping.
				 This covers also the case of no mapping and no header where we build a default mapping */
				if(numOfColumnsFromMap > 0 && firstRowOfDataLength > numOfColumnsFromMap
						&& ObjectUtil.validObject(csvInputConfig.isAllowAdditionalColumns()) && !csvInputConfig.isAllowAdditionalColumns()) {
					String errorMessage = "Row " + rowNumber + " of file was found with number of values different from the number of columns; " +
							"check the field separator, the number of fields in the row and if a non-authorized character was used";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
							.errorPosition(new ErrorPosition(rowNumber))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(errorMessage, new ErrorPosition(rowNumber))
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
					return false;
				}
				//SDMXCONV-1528 error anyway (“CSV does not have expected number of fields”), regardless of parameter allowAdditionalColumns
				if(numOfColumnsFromMap > 0 && firstRowOfDataLength < numOfColumnsFromMap) {
					String errorMessage = "Row " + rowNumber + " of file was found with number of values different from the number of columns; " +
							"check the field separator, the number of fields in the row and if a non-authorized character was used";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
							.errorPosition(new ErrorPosition(rowNumber))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(errorMessage, new ErrorPosition(rowNumber))
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
					return false;
				}
				// File with header, no mapping: will raise an error if the header contains different number of columns from data. From definition of CSV.
				if (isCurrentRowSizeNotValid(firstRowOfData)) {
					String errorMessage = "Row " + rowNumber + " of file was found with number of values different from the number of columns; " +
							"check the field separator, the number of fields in the row and if a non-authorized character was used";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
							.errorPosition(new ErrorPosition(rowNumber))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(errorMessage, new ErrorPosition(rowNumber))
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
					return false; // SDMXCONV-1111
				}

				this.attributesSeriesLevelWithPositions = getAttachmentAttributes(CodeDataInfo.Type.ATTRIBUTE);
				this.attributesObservationLevelWithPositions = getAttachmentAttributes(CodeDataInfo.Type.OBS_ATTRIBUTE);
				// gets the attributes for this data set from the complete list of CsvValues
				// mapped to Concepts
				getDataSetAttributeFromLineOfCsv(firstRowOfData);
				previousReadDataSetAttributes = getDatasetAttributes();
				getGroupFromLineOfCsv(firstRowOfData);
				// store the keyable
				Keyable currentRowKeyable = getKeyableFromLineOfCsv(firstRowOfData);
				listOfKeyable.add(currentRowKeyable);
				// store the observation
				List<Observation> currentRowObervation = getObservationFromLineOfCsv(firstRowOfData, currentRowKeyable);
				listOfObservation.addAll(currentRowObervation);
				return true;
			}
		}
		return false;
	}

	private void checkAttributesPerLevel(List<AttributeBean> attributes, String[] headerColumns, CodeDataInfo.Type type) {
		for (AttributeBean attribute : attributes) {
			if (ObjectUtil.validObject(attribute) && attribute.isMandatory()
					&& !ObjectUtil.contains(headerColumns, attribute.getId())) {
				// SDMXCONV-1185
				// If an attribute is missing from headerRow update the columnsPresenceMapping
				// accordingly
				columnsPresenceMapping.put(attribute.getId(), new CodeDataInfo(null, false, type));
				// Throw the error only on validation
				if (exceptionHandler instanceof MultipleFailureHandlerEngine) {
					String errorMessage = "A mandatory attribute is missing from the header row.";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.MISSING_CONCEPT_FROM_HEADER, errorMessage)
							.errorPosition(new ErrorPosition(1))
							.typeOfException(TypeOfException.SdmxDataFormatException)
							.args("attribute", attribute.getId())
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
				}
			} else {
				columnsPresenceMapping.put(attribute.getId(), new CodeDataInfo(null, true, type));
			}
		}
	}

	/**
	 * <u>Method that checks if a mandatory concept is missing from the header row.</u>
	 * <p>Check for mandatory attributes, dimensions and primary measure,
	 * used only if the csv file is empty and contains only the header row.
	 * If a mandatory element is missing an error is added in the exception handler.
	 * </p>
	 * <p>Check for duplicate column/concepts inside header row.</p>
	 * <small>(Be aware this method should be called only once and not in an
	 * iteration, for performance reasons.)</small>
	 *
	 * @param headerColumns The array of strings that contains the header row columns.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1163">SDMXCONV-1163</a>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1355">SDMXCONV-1355</a>
	 */
	private void checkMandatoryColumns(String[] headerColumns) {
		if (ObjectUtil.validObject(dsd)) {
			checkAttributesPerLevel(this.dsd.getDatasetAttributes(), headerColumns, CodeDataInfo.Type.ATTRIBUTE);
			checkAttributesPerLevel(this.dsd.getGroupAttributes(), headerColumns, CodeDataInfo.Type.ATTRIBUTE);
			checkAttributesPerLevel(this.dsd.getSeriesAttributes(dimensionAtObservation), headerColumns, CodeDataInfo.Type.ATTRIBUTE);
			checkAttributesPerLevel(this.dsd.getObservationAttributes(dimensionAtObservation), headerColumns, CodeDataInfo.Type.OBS_ATTRIBUTE);
			List<DimensionBean> dimensions = new ArrayList<>();
			for (DimensionBean dimension : dsd.getDimensions()) {
				if (!dimension.isMeasureDimension() || !csvInputConfig.isMapMeasure()) {
					dimensions.add(dimension);
				}
			}
			for (DimensionBean dimension : dimensions) {
				if (ObjectUtil.validObject(dimension)
						&& !(ObjectUtil.contains(headerColumns, dimension.getId())
						|| ObjectUtil.contains(headerColumns, ConceptRefUtil.getConceptId(dimension.getConceptRef())))) {
					// If a dimension is missing from headerRow update the columnsPresenceMapping
					// accordingly
					columnsPresenceMapping.put(dimension.getId(),
							new CodeDataInfo(null, false, CodeDataInfo.Type.DIMENSION));
					// Throw the error only on validation
					if (exceptionHandler instanceof MultipleFailureHandlerEngine) {
						String errorMessage = "A dimension is missing from the header row.";
						final DataValidationError dataValidationError = new DataValidationError
								.DataValidationErrorBuilder(ExceptionCode.MISSING_CONCEPT_FROM_HEADER, errorMessage)
								.errorPosition(new ErrorPosition(1))
								.typeOfException(TypeOfException.SdmxDataFormatException)
								.args("dimension", dimension.getId())
								.order(++this.order)
								.build();
						addError(ValidationEngineType.READER, dataValidationError);
					}
				} else {
					columnsPresenceMapping.put(dimension.getId(),
							new CodeDataInfo(null, true, CodeDataInfo.Type.DIMENSION));

				}
			}

			// Add the missing KeyValues to the Keyable
			for (Map.Entry<String, CodeDataInfo> entry : columnsPresenceMapping.entrySet()) {
				if (!entry.getValue().isErrorDisplayable()) {
					if (entry.getValue().getType() == CodeDataInfo.Type.DIMENSION) {
						missingDimensions.add(new KeyValueErrorDataImpl(entry.getKey(), new CodeDataInfo(null,
								entry.getValue().isErrorDisplayable(), CodeDataInfo.Type.DIMENSION)));
					} else if (entry.getValue().getType() == CodeDataInfo.Type.ATTRIBUTE) {
						missingAttributes.add(new KeyValueErrorDataImpl(entry.getKey(), new CodeDataInfo(null,
								entry.getValue().isErrorDisplayable(), CodeDataInfo.Type.ATTRIBUTE)));
					} else if (entry.getValue().getType() == CodeDataInfo.Type.OBS_ATTRIBUTE) {
						missingObsAttributes.add(new KeyValueErrorDataImpl(entry.getKey(), new CodeDataInfo(null,
								entry.getValue().isErrorDisplayable(), CodeDataInfo.Type.OBS_ATTRIBUTE)));
					}
				}
			}
		}
		//SDMXCONV-1355
		// Throw the error only on validation
		if (exceptionHandler instanceof MultipleFailureHandlerEngine && ObjectUtil.validArray(this.headerColumns)) {
			Set<String> duplicates = findDuplicateBySetAdd(Arrays.asList(this.headerColumns));
			if (ObjectUtil.validCollection(duplicates)) {
				String errorMessage = "Error found in the header row.";
				final DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.DUPLICATE_CONCEPT_HEADERROW, errorMessage)
						.errorPosition(new ErrorPosition(1))
						.typeOfException(TypeOfException.SdmxDataFormatException)
						.args(String.join(", ", duplicates))
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		}
	}

	/**
	 * SDMXCONV-1135 Method to check that columns in headerRow are in the exact same
	 * order with the one in mapping Applicable only to SingleLevelCsv
	 *
	 * @return
	 */
	private void checkHeaderRowAgainstMapping() {
		// Index to be used for order in columns of headerRow
		int i;
		int colPos = 1;
		for (Map.Entry<String, CsvInColumnMapping> entry : csvInputConfig.getMapping().entrySet()) {
			i = entry.getValue().getColumns().get(0).getIndex()-1;
			if (!entry.getKey().equals(headerColumns[i])) {
				for (int j = 0; j < i; j++) {
					colPos += headerColumns[j].length();
				}
				colPos += i; // We include the delimiters
				final DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.CONCEPT_NAME_IS_INCORRECT, "")
						.errorPosition(new ErrorPosition(1, colPos))
						.typeOfException(TypeOfException.SdmxDataFormatException)
						.args(entry.getKey(), headerColumns[i])
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
				return;
			}
		}
	}

	/**
	 * Method that counts the components that exist in the mapping and have a valid value.
	 * If there is a header Row and it is used for reading the columns then this should be zero.
	 *
	 * @param mapping
	 * @return
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-955">SDMXCONV-955</a>
	 */
	private int countNotEmptyMappingCols(Map<String, CsvInColumnMapping> mapping) {
		int numOfCols = 0;
		if(this.csvInputConfig.getInputColumnHeader() != CsvInputColumnHeader.USE_HEADER) {
			for (CsvInColumnMapping map : mapping.values()) {
				// Count only the columns that have a mapping
				// fixed value or Column number
				if (!map.isFixed() && ObjectUtil.validCollection(map.getColumns())) {
					for (CsvColumn ignored : map.getColumns()) {
						++numOfCols;
					}
				}
			}
		}
		return numOfCols;
	}

	/**
	 * reads the next line in the csv file
	 *
	 * @return the values found on the line
	 */
	private String[] readNextLineOfCsvFile() {
		String[] row = moveNextLine();
		if (!FlatEncodingCheckUtil.isCheckLineEncodingCorrect(row)) {
			String errorMsg = "The encoding of the file is not correct and contains invalid characters in line (" + rowNumber + " ). Please correct the file and try again!";
			final DataValidationError dataValidationError = new DataValidationError
					.DataValidationErrorBuilder(ExceptionCode.DATA_INVALID_CHARACTER, errorMsg)
					.errorPosition(new ErrorPosition(rowNumber))
					.typeOfException(TypeOfException.SdmxDataFormatException)
					.args(errorMsg)
					.order(++this.order)
					.build();
			addError(ValidationEngineType.READER, dataValidationError);
		}

		if (!ObjectUtil.validArray(row))
			isCurrentRowNull = true;
		return row;
	}

	/**
	 *	<b>Move the parser to the next row. </b>
	 *	<p>If the row is not null, but has all the columns with null values we search until we find next valid row.</p>
	 *	@see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1511">SDMXCONV-1511</a>
	 *  @return String[] Current row parsed
	 */
	private String[] moveNextLine() {
		String[] row = null;
		try {
			do {
				row = parser.parseNext();
				if (rowNumber == 0 && row == null) {
					exceptionHandler.handleException(new SdmxSyntaxException(ExceptionCode.WORKBOOK_READER_ERROR,
							"The input file is empty, please check it and try to upload one more time"));
				}
				++rowNumber;
				//SDMXCONV-761 Set the row that is currently being read, SDMXCONV-1014
				errorPositions.put(DATASET_LEVEL.NONE.name(), new ErrorPosition(rowNumber));
			} while (checkDataRowForAllNullValues(row));
		} catch (TextParsingException e) {
			if (e.getMessage().startsWith("Length of parsed input")) {
				rowNumber = Math.toIntExact(e.getLineIndex());
				String errorMessage = "Length of parsed input exceeds the maximum number of characters defined in your parser settings (4096). The parsed line (" + rowNumber + ") is too long. Please check the delimiter!";
				final DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition(rowNumber))
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(errorMessage)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
				row = new String[]{e.getParsedContent()};
			} else {
				throw e;
			}
		}
		ByteCountHolder.setByteCount(this.countingStream.getByteCount());
		return row;
	}

	/**
	 * <strong>Check the row if contains only nulls as values</strong>
	 * <p>
	 * Check if the operation is validation, if the the parameter
	 * errorIfDataRowEmpty is set to true, if the row is not null but has only null
	 * values.
	 * </p>
	 *
	 * @param row String[] with all the values parsed from univocity
	 * @return boolean if the row has all Nulls.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1194">SDMXCONV-1194</a>
	 */
	private boolean checkDataRowForAllNullValues(String[] row) {
		// Check if operation is Validation
		if (this.exceptionHandler != null
				&& this.exceptionHandler instanceof MultipleFailureHandlerEngine
					&& this.errorIfDataValuesEmpty) {
			// Check if the row of data exists but every single element is null
			if (ObjectUtil.validArray(row) && ObjectUtil.isAllNulls(row)) {
				String errorMsg = "Row of data was found with no values present";
				final DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMsg)
						.errorPosition(new ErrorPosition(rowNumber))
						.typeOfException(TypeOfException.SdmxDataFormatException)
						.args(errorMsg)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
				return true;
			}
		}
		return false;
	}

	/**
	 * Builds a mapping from the provided column header. If columnHeader is null
	 * then return empty Map.
	 *
	 * @param columnHeader
	 * @return
	 */
	private Map<String, CsvInColumnMapping> buildMappingFromColumnHeader(String[] columnHeader) {
		Map<String, CsvInColumnMapping> result = new LinkedHashMap<>();
		if(!ObjectUtil.validMap(csvInputConfig.getComplexComponentMapping())) {
			csvInputConfig.setComplexComponentMapping(new HashMap<>());
		}
		if (ObjectUtil.validArray(columnHeader)) {
			// if we have a wrong delimiter the header is seen as one component
			// SDMXCONV-1134
			// Variable to store the current index of column character, read in headerRow
			int charIndex = 1;
			for (int i = 1; i <= columnHeader.length; i++) {
				if (this.mapOfComponentBeans.containsKey(columnHeader[i-1])) {
					result.put(columnHeader[i-1], new CsvInColumnMapping(new Integer[]{i}, false, 1, null, columnHeader[i-1]));
				} else if (getDataStructure().hasComplexComponents()) {
					/* In case we have a complex component this could be analysed to COMP1, COMP2 in the header row */
					String componentWithoutDigit = columnHeader[i-1].replaceAll("\\d$", "");
					/* Strip the header column from the trailing digit and search to find it inside datastructure and in the list of components */
					if(getDataStructure().isComponentComplex(componentWithoutDigit) && this.mapOfComponentBeans.containsKey(componentWithoutDigit)) {
						/* If exists put it with the trailing digit inside mapping of complex */
						csvInputConfig.getComplexComponentMapping().put(columnHeader[i-1], componentWithoutDigit);
						result.put(columnHeader[i-1], new CsvInColumnMapping(new Integer[]{i}, false, 1, null, columnHeader[i-1]));
					}
				} else {
					if (ObjectUtil.validString(columnHeader[i-1])) {
						// SDMXCONV-1134, in case of strict mode throw the error, otherwise ignore it
						if (ObjectUtil.validObject(csvInputConfig.isAllowAdditionalColumns()) && !csvInputConfig.isAllowAdditionalColumns()) {
							String errorMsg = "";
							final DataValidationError dataValidationError = new DataValidationError
									.DataValidationErrorBuilder(ExceptionCode.DATASET_UNDEFINED_DIMENSION, errorMsg)
									.errorPosition(new ErrorPosition(1, charIndex))
									.typeOfException(TypeOfException.SdmxDataFormatException)
									.args(columnHeader[i-1])
									.order(++this.order)
									.build();
							addError(ValidationEngineType.READER, dataValidationError);
						}
						result.put(columnHeader[i-1], new CsvInColumnMapping(new Integer[]{i}, false, 1, null, columnHeader[i-1]));
					} else {
						// SDMXCONV-1137
						String errorMsg = "Empty string as header column at position " + charIndex;
						final DataValidationError dataValidationError = new DataValidationError
								.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMsg)
								.errorPosition(new ErrorPosition(1, charIndex))
								.typeOfException(TypeOfException.SdmxSyntaxException)
								.args(errorMsg, new ErrorPosition(1, charIndex))
								.order(++this.order)
								.build();
						addError(ValidationEngineType.READER, dataValidationError);
					}
				}
				int length = 0; // SDMXCONV-1177 Avoid a nullPointer exception if the header column is empty
				if (ObjectUtil.validObject(columnHeader, columnHeader[i-1])) {
					length = columnHeader[i-1].length();
				}
				charIndex += length + 1; // The +1 is for the delimiter
			}
		}
		return result;
	}

	/**
	 * creates the default mapping from the components of the dsd
	 *
	 * @return
	 */
	private Map<String, CsvInColumnMapping> buildDefaultMapping(boolean useXSMeasures) {
		Map<String, CsvInColumnMapping> result = new LinkedHashMap<>();
		List<ComponentBean> componentBeans = getCsvComponents();
		List<ComponentBean> itemsTobeRemoved = new ArrayList<>();
		// for cases when we have a conversion to CROSS_SDMX
		if (dsd instanceof CrossSectionalDataStructureBean) {
			// csv input contains the cross structure measures so we remove the dimension component
			if (useXSMeasures) {
				for (ComponentBean bean : componentBeans) {
					if (bean instanceof DimensionBean && ((DimensionBean) bean).isMeasureDimension()) {
						itemsTobeRemoved.add(bean);
					}
				}
			} else {
				// this means we have the measure dimension as columns and we remove the cross structure measures
				for (ComponentBean bean : componentBeans) {
					if (bean instanceof CrossSectionalMeasureBean) {
						itemsTobeRemoved.add(bean);
					}
				}
			}
		}
		if (!itemsTobeRemoved.isEmpty()) {
			componentBeans.removeAll(itemsTobeRemoved);
		}
		for (int i = 1; i <= componentBeans.size(); i++) {
			result.put(componentBeans.get(i-1).getId(), new CsvInColumnMapping(new Integer[]{i}, false, 1, null, componentBeans.get(i-1).getId()));
		}
		return result;
	}

	/*
	 * method that returns the Keyable from the current line of the CSV
	 */
	private Keyable getKeyableFromLineOfCsv(String[] row) {
		List<KeyValue> dimensions = new ArrayList<>();
		List<KeyValue> attributes;
		String obsTime = null;
		LinkedHashMap<String, List<String>> complexComponentsValues = new LinkedHashMap<>();
		for (Map.Entry<String, CsvInColumnMapping> entryConcept : csvInputConfig.getMapping().entrySet()) {
			ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
			if(!ObjectUtil.validObject(componentBean)) {
				/* In case we have a complex component this could be COMP1, COMP2 in the mapping or the header row */
				String componentWithoutDigit = entryConcept.getKey().replaceAll("\\d$", "");
				componentBean = this.mapOfComponentBeans.get(componentWithoutDigit);
			}
			if (componentBean instanceof DimensionBean) {
				if (componentBean.getStructureType() == SDMX_STRUCTURE_TYPE.TIME_DIMENSION) {
					obsTime = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
				} else {
					if (!componentBean.getId().equalsIgnoreCase(dimensionAtObservation)) {
						if (((DimensionBean) componentBean).isMeasureDimension()) {
							if (useXSMeasures) {
								throw new IllegalArgumentException(
										"A Measure Dimension was found in the input file, please uncheck 'Map CrossX Measures' or set 'mapCrossXMeasure' to false!");
							} else {
								String value = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
								CodeDataInfo codeDataInfo = createCodeDataInfo(value, columnsPresenceMapping.get(componentBean.getId()), CodeDataInfo.Type.DIMENSION);
								dimensions.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
							}
						} else {
							String value = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
							CodeDataInfo codeDataInfo = createCodeDataInfo(value,
									columnsPresenceMapping.get(componentBean.getId()), CodeDataInfo.Type.DIMENSION);
							dimensions.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
						}

					}
				}
			} else {
				AttributeBean attributeBean;
				if(ObjectUtil.validObject(componentBean)) {
					attributeBean = this.attributesSeriesLevelWithPositions.get(componentBean.getId());
					if(ObjectUtil.validObject(attributeBean)) {
						complexComponentsValues = valueFromColumnMapping(entryConcept.getKey(), entryConcept.getValue(), complexComponentsValues, row);
					}
				}
			}
		}
		attributes = createAttributesCodeInfo(complexComponentsValues);
		Keyable keyable;
		// SDMXCONV-1185
		dimensions.addAll(missingDimensions);
		attributes.addAll(missingAttributes);
		// With XS Measure support, we might have a TimeDimension but the Keyable will
		// need to have a cross-sectional object
		if (isTimeSeries) {
			// Time Series means that the dimension at observation is the Time Dimension and
			// there is a Time Dimension
			if (obsTime != null) {
				TIME_FORMAT timeFormat = timeFormatBuilder.build(obsTime);
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, timeFormat);
			} else {
				// What do we do here throw an error ?
				// Because we have time series but the Time Dimension is missing
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, (TIME_FORMAT) null);
			}
		} else {
			if (obsTime != null) {
				TIME_FORMAT timeFormat = timeFormatBuilder.build(obsTime);
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, timeFormat, dimensionAtObservation, obsTime);
			} else {
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, null, dimensionAtObservation, "");
			}
		}
		if(ObjectUtil.validObject(keyable))
			this.seriesCount = this.seriesCount + 1;
		return keyable;
	}

	private void getGroupFromLineOfCsv(String[] row) {
		// TODO for each group have a current key
		List<GroupBean> listOfGroupBeans = dsd.getGroups();
		for (GroupBean bean : listOfGroupBeans) {
			List<KeyValue> dimensions = new ArrayList<>();
			List<KeyValue> attributes;
			LinkedHashMap<String, List<String>> complexComponentsValues = new LinkedHashMap<>();
			for (Map.Entry<String, CsvInColumnMapping> entryConcept : csvInputConfig.getMapping().entrySet()) {
				ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
				if(!ObjectUtil.validObject(componentBean)) {
					/* In case we have a complex component this could be COMP1, COMP2 in the mapping or the header row */
					String componentWithoutDigit = entryConcept.getKey().replaceAll("\\d$", "");
					componentBean = this.mapOfComponentBeans.get(componentWithoutDigit);
				}
				if (componentBean instanceof DimensionBean && bean.getDimensionRefs().contains(entryConcept.getKey())) {
					String value = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
					final CodeDataInfo codeDataInfo = createCodeDataInfo(value,
																		 columnsPresenceMapping.get(componentBean.getId()),
																		 CodeDataInfo.Type.DIMENSION);
					dimensions.add(new KeyValueErrorDataImpl(entryConcept.getKey(), codeDataInfo));
				} else {
					if (componentBean instanceof AttributeBean
							&& ((AttributeBean) componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.GROUP
								&& bean.getId().equals(((AttributeBean) componentBean).getAttachmentGroup())) {
						complexComponentsValues = valueFromColumnMapping(entryConcept.getKey(), entryConcept.getValue(), complexComponentsValues, row);
					}
				}
			}
			attributes = createAttributesCodeInfo(complexComponentsValues);
			// SDMXCONV-1185
			dimensions.addAll(missingDimensions);
			if (ObjectUtil.validObject(attributes) && !attributes.isEmpty()) {
				attributes.addAll(missingAttributes);
				Keyable keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, bean.getId());
				listOfKeyable.add(keyable);
			}
		}
	}

	/**
	 * <strong>Retrieves attributes and position integer.</strong>
	 * <p>We check from datastructure the list of attributes for the relative attachment Level.</p>
	 * <p>This has to be done with DataStructureBean#getObservationAttributes(String crossSectionalConcept)
	 * to include also the attributes attached to all dimensions (including the TIME_PERIOD),
	 * which is functionally equivalent to attaching them to the observation level.</p>
	 *
	 * @param attributeType The type of attribute to retrieve, either observation or series.
	 * @return A map of attribute positions to AttributeBean objects.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1449">SDMXCONV-1449</a>
	 */
	private Map<String, AttributeBean> getAttachmentAttributes(CodeDataInfo.Type attributeType) {
		List<AttributeBean> attributes = null;
		if (CodeDataInfo.Type.OBS_ATTRIBUTE == attributeType) {
			attributes = this.getDataStructure().getObservationAttributes(dimensionAtObservation);
		} else if (CodeDataInfo.Type.ATTRIBUTE == attributeType) {
			attributes = this.getDataStructure().getSeriesAttributes(dimensionAtObservation);
		}
		Map<String, AttributeBean> attributesPerPosition = new LinkedHashMap<>();
		for(Map.Entry componentMap : this.mapOfComponentBeans.entrySet()) {
			 if(attributes.contains(componentMap.getValue())) {
				 attributesPerPosition.put((String) componentMap.getKey(), (AttributeBean) componentMap.getValue());
			 }
		}
		return attributesPerPosition;
	}

	/**
	 * <strong>Create the List of keyValues for attributes from complex components Map.</strong>
	 * <p>If the value is only one the attribute is not a complex one. In case there are multiple values we make this attribute complex.</p>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1351">SDMXCONV-1351</a>
	 * @param complexComponentsValues LinkedHashMap<String, List<String>>
	 * @return List<KeyValue> attributes
	 */
	private List<KeyValue> createAttributesCodeInfo(LinkedHashMap<String, List<String>> complexComponentsValues) {
		List<KeyValue> attributes = new ArrayList<>();
		if(ObjectUtil.validMap(complexComponentsValues)) {
			for (Map.Entry<String, List<String>> complexComponent : complexComponentsValues.entrySet()) {
				if (ObjectUtil.validCollection(complexComponent.getValue())) {
					if (dsd.isComponentComplex(complexComponent.getKey())) {
						final CodeDataInfo codeDataInfo = createCodeDataInfo(null, columnsPresenceMapping.get(complexComponent.getKey()), CodeDataInfo.Type.ATTRIBUTE);
						for (String attributeValue : complexComponent.getValue()) {
							ComplexNodeValue complexNodeValue = new ComplexNodeValueImpl(attributeValue, null);
							codeDataInfo.getComplexNodeValues().add(complexNodeValue);
						}
						attributes.add(new KeyValueErrorDataImpl(complexComponent.getKey(), codeDataInfo));
					} else {
						String value = complexComponent.getValue().get(0);
						if (ObjectUtil.validString(value)) {
							final CodeDataInfo codeDataInfo = createCodeDataInfo(value, columnsPresenceMapping.get(complexComponent.getKey()), CodeDataInfo.Type.ATTRIBUTE);
							attributes.add(new KeyValueErrorDataImpl(complexComponent.getKey(), codeDataInfo));
						}
					}
				}
			}
		}
		return attributes;
	}

	/**
	 * Method that returns the Data Set Attributes
	 */
	private void getDataSetAttributeFromLineOfCsv(String[] row) {
		List<KeyValue> attributes;
		LinkedHashMap<String, List<String>> complexComponentsValues = new LinkedHashMap<>();
		for (Map.Entry<String, CsvInColumnMapping> entryConcept : csvInputConfig.getMapping().entrySet()) {
			ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
			if(!ObjectUtil.validObject(componentBean)) {
				/* In case we have a complex component this could be COMP1, COMP2 in the mapping or the header row */
				String componentWithoutDigit = entryConcept.getKey().replaceAll("\\d$", "");
				componentBean = this.mapOfComponentBeans.get(componentWithoutDigit);
			}
			if (componentBean instanceof AttributeBean
					&& ((AttributeBean) componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.DATA_SET) {
				complexComponentsValues = valueFromColumnMapping(entryConcept.getKey(), entryConcept.getValue(), complexComponentsValues, row);
			}
		}
		attributes = createAttributesCodeInfo(complexComponentsValues);
		this.datasetAttributes = attributes;
	}

	/*
	 * Method that returns the Observation from the current line of the CSV
	 */
	private List<Observation> getObservationFromLineOfCsv(String[] row, Keyable keyable) {
		String obsValue = null;
		String obsTime = null;
		KeyValue crossSectionalValue = null;
		List<KeyValue> xsMeasures = new ArrayList<>();
		List<KeyValue> measures;
		List<KeyValue> attributes;
		LinkedHashMap<String, List<String>> complexComponentsValues = new LinkedHashMap<>();
		LinkedHashMap<String, List<String>> complexMeasuresValues = new LinkedHashMap<>();
		for (Map.Entry<String, CsvInColumnMapping> entryConcept : csvInputConfig.getMapping().entrySet()) {
			ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
			if(!ObjectUtil.validObject(componentBean)) {
				/* In case we have a complex component this could be COMP1, COMP2 in the mapping or the header row */
				String componentWithoutDigit = entryConcept.getKey().replaceAll("\\d$", "");
				componentBean = this.mapOfComponentBeans.get(componentWithoutDigit);
			}
			if (componentBean != null) {
				switch (componentBean.getStructureType()) {
					case TIME_DIMENSION:
						obsTime = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
						break;
					case PRIMARY_MEASURE:
						if (!useXSMeasures) {
							obsValue = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
							// SDMXCONV-822
							if ("".equals(obsValue))
								obsValue = null;
						} // TODO throw an error
						break;
					case DATA_ATTRIBUTE:
						AttributeBean attributeBean = this.attributesObservationLevelWithPositions.get(componentBean.getId());
						if(ObjectUtil.validObject(attributeBean)) {
							complexComponentsValues = valueFromColumnMapping(entryConcept.getKey(), entryConcept.getValue(), complexComponentsValues, row);
						}
						break;
					case CROSS_SECTIONAL_MEASURE:
						if (useXSMeasures) {
							String value = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
							final CodeDataInfo codeDataInfo = createCodeDataInfo(value, columnsPresenceMapping.get(componentBean.getId()), CodeDataInfo.Type.DIMENSION);
							xsMeasures.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
						}
						break;
					case DIMENSION:
					case MEASURE_DIMENSION:
						if (!isTimeSeries && dimensionAtObservation.equalsIgnoreCase(componentBean.getId())) {
							String value = valueFromCsvInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
							final CodeDataInfo codeDataInfo = createCodeDataInfo(value, columnsPresenceMapping.get(componentBean.getId()), CodeDataInfo.Type.DIMENSION);
							crossSectionalValue = new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo);
						}
						break;
					case MEASURE:
							complexMeasuresValues = valueFromColumnMapping(entryConcept.getKey(), entryConcept.getValue(), complexMeasuresValues, row);
						break;
				}
			}
		}
		attributes = createAttributesCodeInfo(complexComponentsValues);
		measures = createMeasures(complexMeasuresValues);
		final String normalizedObsTime;
		if (obsTime != null) {
			normalizedObsTime = obsTime;
		} else {
			normalizedObsTime = "";
		}
		// SDMXCONV-1185
		if (ObjectUtil.validObject(attributes)) {
			attributes.addAll(missingObsAttributes);
		}
		obsAnnotations.clear();
		// SDMXCONV-1399
		errorPositions.put(DATASET_LEVEL.OBS.name(), new ErrorPosition(this.rowNumber));
		// SDMXCONV-1399
		setObservationAnnotation(ANN_INTERNAL_POSITION, "row:" + this.rowNumber);
		// SDMXCONV-816, SDMXCONV-867
		if (ThreadLocalOutputReporter.getWriteAnnotations().get()) {
			setObservationAnnotation(ANN_INPUT_FORMAT, "CSV");
			setObservationAnnotation(ANN_OBS_COORDINATES, "row:" + this.rowNumber);
		}
		if (useXSMeasures) {
			return buildObservationXsMeasures(keyable, attributes, normalizedObsTime, xsMeasures);
		} else {
			this.obsCount = this.obsCount + 1;
			Observation observation = buildObservationNoXsMeasures(row, keyable, attributes, measures, obsValue, normalizedObsTime, crossSectionalValue);
			return Arrays.asList(observation);
		}
	}

	private void obsValueAsMeasure(String obsValue, List<KeyValue> measures) {
		if(ObjectUtil.validString(obsValue)) {
			if(measures == null) {
				measures = new ArrayList<>();
			}
			KeyValue primary = new KeyValueImpl(obsValue, PrimaryMeasureBean.FIXED_ID);
			measures.add(primary);
		}
	}

	/**
	 * <strong>Create the List of keyValues for measures from complex components Map.</strong>
	 * <p>If the value is only one the measure is not a complex one. In case there are multiple values we make this measure complex.</p>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1351">SDMXCONV-1351</a>
	 * @param complexMeasuresValues LinkedHashMap<String, List<String>>
	 * @return List<KeyValue> measures
	 */
	private List<KeyValue> createMeasures(LinkedHashMap<String, List<String>> complexMeasuresValues) {
		List<KeyValue> measures = new ArrayList<>();
		if(ObjectUtil.validMap(complexMeasuresValues)) {
			for (Map.Entry<String, List<String>> complexComponent : complexMeasuresValues.entrySet()) {
				if (ObjectUtil.validCollection(complexComponent.getValue())) {
					if (dsd.isComponentComplex(complexComponent.getKey())) {
						KeyValue measureValue = new KeyValueImpl((String) null, complexComponent.getKey());
						for (String value : complexComponent.getValue()) {
							ComplexNodeValue complexNodeValue = new ComplexNodeValueImpl(value, null);
							measureValue.getComplexNodeValues().add(complexNodeValue);
						}
						measures.add(measureValue);
					} else {
						String value = complexComponent.getValue().get(0);
						if (ObjectUtil.validString(value)) {
							KeyValue obsValue = new KeyValueImpl(value, complexComponent.getKey());
							measures.add(obsValue);
						}
					}
				}
			}
		}
		return measures;
	}

	/**
	 * To be used only when XS Measures are used.
	 *
	 * @param keyable
	 * @param attributes
	 * @param obsTime
	 * @param xsMeasures
	 * @return
	 */
	private List<Observation> buildObservationXsMeasures(Keyable keyable, List<KeyValue> attributes, String obsTime,
														 List<KeyValue> xsMeasures) {
		List<Observation> xsObservations = new ArrayList<>();
		AnnotationBeanImpl[] annotationsArr = obsAnnotations.toArray(new AnnotationBeanImpl[obsAnnotations.size()]);
		for (KeyValue xsMeasure : xsMeasures) {
			String xsMeasureCode = xsMeasureIdToMeasureCode.get(xsMeasure.getConcept());
			final CodeDataInfo codeDataInfo = createCodeDataInfo(xsMeasureCode,
					columnsPresenceMapping.get(dimensionAtObservation), CodeDataInfo.Type.DIMENSION);
			KeyValue crossSectionalValue = new KeyValueErrorDataImpl(dimensionAtObservation, codeDataInfo);
			String obsValue = xsMeasure.getCode();
			Observation observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, crossSectionalValue, annotationsArr); // SDMXCONV-1041
			this.obsCount = this.obsCount + 1;
			xsObservations.add(observation);
		}
		return xsObservations;
	}

	/**
	 * To be used only when XS measures are not used.
	 * <p>The case of SDMX 3.0 is included.</p>
	 *
	 * @param row                 Row of strings, read from inputFile.
	 * @param keyable             The keyable made from the input row
	 * @param attributes          List of attributes KeyValues
	 * @param measures            List of measures KeyValues
	 * @param obsValue            The observation Value
	 * @param obsTime             Time Value
	 * @param crossSectionalValue Cross Value
	 * @return Observation
	 */
	private Observation buildObservationNoXsMeasures(String[] row,
													 Keyable keyable,
													 List<KeyValue> attributes,
													 List<KeyValue> measures,
													 String obsValue,
													 String obsTime,
													 KeyValue crossSectionalValue) {
		Observation observation;
		AnnotationBeanImpl[] annotationsArr = obsAnnotations.toArray(new AnnotationBeanImpl[obsAnnotations.size()]);
		if (getDataStructure().hasComplexComponents()) {
			obsValueAsMeasure(obsValue, measures);
			observation = this.observationBuilder.Build(keyable, attributes, measures, obsTime, obsValue, crossSectionalValue, annotationsArr);
		} else if(!getDataStructure().hasComplexComponents() && csvInputConfig.getStructureSchemaVersion() == SDMX_SCHEMA.VERSION_THREE) {
			obsValueAsMeasure(obsValue, measures);
			observation = this.observationBuilder.Build(keyable, attributes, measures, obsTime, obsValue, crossSectionalValue, annotationsArr);
		} else {
			if (isTimeSeries) {
				observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, annotationsArr); // SDMXCONV-1041
			} else {
				if (crossSectionalValue == null) {
					StringBuilder value = new StringBuilder();
					if (csvInputConfig.getMapping().get(dimensionAtObservation) != null) {
						for (CsvColumn column : csvInputConfig.getMapping().get(dimensionAtObservation).getColumns()) {
							value.append(row[column.getIndex()-1]);
						}
					}
					CodeDataInfo codeDataInfo = createCodeDataInfo(value.toString(), columnsPresenceMapping.get(dimensionAtObservation), CodeDataInfo.Type.DIMENSION);
					crossSectionalValue = new KeyValueErrorDataImpl(dimensionAtObservation, codeDataInfo);
				}
				observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, crossSectionalValue, annotationsArr); // SDMXCONV-1041
			}
		}
		return observation;
	}

	/**
	 * <strong>Method that checks if the columnsPresenceMapping.get(component) is null </strong>
	 * <p>(not present in the DSD) and it then it creates and returns a new CodeDataInfo Object.</p>
	 *
	 * @param code
	 * @param codeDataInfo
	 * @param type
	 * @return
	 */
	private CodeDataInfo createCodeDataInfo(String code, CodeDataInfo codeDataInfo, CodeDataInfo.Type type) {
		CodeDataInfo result;
		if (codeDataInfo != null) {
			result = new CodeDataInfo(code, codeDataInfo.isErrorDisplayable(), type);
		} else {
			result = new CodeDataInfo(code, true, type);
		}
		return result;
	}

	/**
	 * <strong>Find the values or value for a complex component.</strong>
	 * <p>Returns a map with a key the name of the concept and for value the list of values for this concept. When many this concept is complex.</p>
	 *
	 * @param concept                 The name of the column currently read. This is from the mapping it could be complex with number at the end.
	 * @param csvInColumnMapping      It shows us where to find the value for the given concept
	 * @param complexComponentsValues The Map we store all the values
	 * @param row                     The String array of all the row currently being read.
	 * @return LinkedHashMap
	 */
	private LinkedHashMap<String, List<String>> valueFromColumnMapping(String concept, CsvInColumnMapping csvInColumnMapping, LinkedHashMap<String, List<String>> complexComponentsValues, String[] row) {
		if(this.mapOfComponentBeans.containsKey(concept) && !getDataStructure().isComponentComplex(concept)) {
			String value = valueFromCsvInColumnMapping(concept, csvInColumnMapping, row);
			complexComponentsValues.put(concept, Collections.singletonList(value));
		//it is a complex attribute but in the map it appears with the id only without digit
		} else if(getDataStructure().hasComplexComponents() && getDataStructure().isComponentComplex(concept) && this.mapOfComponentBeans.containsKey(concept)) {
				String conceptValue = row[csvInColumnMapping.getColumns().get(0).getIndex()-1];
				List<String> values = new ArrayList<>();
				if (ObjectUtil.validString(conceptValue, csvInputConfig.getSubFieldSeparationChar())
						&& conceptValue.contains(csvInputConfig.getSubFieldSeparationChar())) {
					for (String val : conceptValue.split(csvInputConfig.getSubFieldSeparationChar())) {
						if (transcoding.hasTranscodingRules(getConcept(concept))) {
							values.add(transcoding.getValueFromTranscoding(getConcept(concept), val));
						} else {
							if (ObjectUtil.validString(val))
								values.add(val);
						}
					}
				} else {
					if (transcoding.hasTranscodingRules(getConcept(concept))) {
						values.add(transcoding.getValueFromTranscoding(getConcept(concept), conceptValue));
					} else {
						if (ObjectUtil.validString(conceptValue))
							values.add(conceptValue);
					}
				}
				complexComponentsValues.put(concept, values);
		} else {
			/* In case we have a complex component this could be COMP1, COMP2 in the mapping or the header row */
			String componentWithoutDigit = concept.replaceAll("\\d$", "");
			/* Strip the column from the trailing digit and search to find it inside datastructure and in the list of components */
			if(getDataStructure().hasComplexComponents()
					&& getDataStructure().isComponentComplex(componentWithoutDigit)
						&& this.mapOfComponentBeans.containsKey(componentWithoutDigit)) {
				List complexValues = complexComponentsValues.get(componentWithoutDigit);
				if(!ObjectUtil.validCollection(complexValues)) {
					complexValues = new ArrayList();
				}
				String value = valueFromCsvInColumnMapping(concept, csvInColumnMapping, row);
				if(ObjectUtil.validString(value)) {
					complexValues.add(value);
				}
				complexComponentsValues.put(componentWithoutDigit, complexValues);
			}
		}
		return complexComponentsValues;
	}

	/**
	 * Find value for simple (not complex) concepts.
	 * @param concept
	 * @param csvInColumnMapping
	 * @param row
	 * @return
	 */
	private String valueFromCsvInColumnMapping(String concept, CsvInColumnMapping csvInColumnMapping, String[] row) {
		String value;
		if (csvInColumnMapping.isFixed()) {
			String conceptValue = csvInColumnMapping.getFixedValue();
			if (transcoding.hasTranscodingRules(getConcept(concept))) {
				value = transcoding.getValueFromTranscoding(getConcept(concept), conceptValue);
			} else {
				value = csvInColumnMapping.getFixedValue();
			}
		} else {
			value = "";
			if (csvInColumnMapping.getColumns().size() == 1
					&& csvInColumnMapping.getColumns().get(0).getIndex() <= row.length) {
				String conceptValue = row[csvInColumnMapping.getColumns().get(0).getIndex()-1];
				if (transcoding.hasTranscodingRules(getConcept(concept))) {
					value = transcoding.getValueFromTranscoding(getConcept(concept), conceptValue);
				} else {
					if (ObjectUtil.validObject(conceptValue))
						value = conceptValue;
				}
			} else {
				StringBuilder valueBuilder = new StringBuilder(value);
				for (CsvColumn csvColumn : csvInColumnMapping.getColumns()) {
					if (csvColumn.getIndex() <= row.length) {
						String conceptValue = row[csvColumn.getIndex()-1];
						if (conceptValue != null) {
							valueBuilder.append(conceptValue);
						}
					} else {
						logger.warn("a csv column found in mapping {} has a higher index than the number of columns in the csv {}", csvColumn, row.length);
					}
				}
				value = valueBuilder.toString();
			}
		}
		// SDMXCONV-1045, now this is being handled implicitly from univocity parser
		return value;
	}

	private String getConcept(String concept) {
		if(this.getDataStructure().hasComplexComponents() && ObjectUtil.validMap(csvInputConfig.getComplexComponentMapping())) {
			String complexConceptWithoutDigit = csvInputConfig.getComplexComponentMapping().get(concept);
			if(ObjectUtil.validString(complexConceptWithoutDigit)) {
				return complexConceptWithoutDigit;
			}
		}
		return concept;
	}

	@Override
	public void reset() {
		closeStreams();
		openStreams();
		initialize();
	}

	@Override
	public void copyToOutputStream(OutputStream outputStream) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		closeStreams();
		closeQuietly(countingStream);
	}

	private void initialize() {
		this.datasetAttributes = new ArrayList<>();
		this.currentKeyable = null;
		this.currentObservation = null;
	}

	private void openStreams() {
		parser = new CsvParser(this.settings);
		InputStream inputStream = dataLocation.getInputStream();
		countingStream = new CountingInputStream(inputStream);
		this.closables.add(inputStream);
		this.closables.add(countingStream);
		parser.beginParsing(countingStream, "UTF-8");
	}

	private void closeStreams() {
		if (parser != null) {
			parser.stopParsing();
		}
		while (!closables.isEmpty()) {
			try {
				closables.pop().close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private String getDimensionAtObservation() {
		// when XS measures are used then we use the Measure Dimension
		List<DimensionBean> listOfDimensionBeans = this.dsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
		if (useXSMeasures && !listOfDimensionBeans.isEmpty()) {
			return listOfDimensionBeans.get(0).getId();
		}
		if (this.dsd.getTimeDimension() != null) {
			return this.dsd.getTimeDimension().getId();
		} else {
			if (!listOfDimensionBeans.isEmpty()) {
				return listOfDimensionBeans.get(0).getId();
			} else {
				// SDMXCONV-945
				return DIMENSION_AT_OBSERVATION.ALL.getVal();
			}
		}
	}

	private void populateXsMeasureIdToMeasureCode() {
		if (dsd instanceof CrossSectionalDataStructureBean) {
			CrossSectionalDataStructureBean crossDsd = (CrossSectionalDataStructureBean) dsd;
			for (CrossSectionalMeasureBean measure : crossDsd.getCrossSectionalMeasures()) {
				this.xsMeasureIdToMeasureCode.put(measure.getId(), measure.getCode());
			}
		}
	}

	private List<ComponentBean> getCsvComponents() {
		List<ComponentBean> returnList = new ArrayList<>(dsd.getDimensionList().getDimensions());
		returnList.add(dsd.getPrimaryMeasure());
		returnList.addAll(dsd.getObservationAttributes());
		returnList.addAll(dsd.getDatasetAttributes());
		returnList.addAll(dsd.getSeriesAttributes(dimensionAtObservation));
		returnList.addAll(dsd.getGroupAttributes());
		if (dsd instanceof CrossSectionalDataStructureBean) {
			returnList.addAll(((CrossSectionalDataStructureBean) dsd).getCrossSectionalMeasures());
		}
		//TODO: SDMXCONV-1608 add measures in case of sdmx 3.0
		return returnList;
	}

	private boolean isCurrentRowSizeNotValid(String[] currentRow) {
		// SDMXCONV-873
		if (currentRow != null && currentRow.length > 0) {
			for (int i = 0; i < currentRow.length; i++) {
				if (isTooBig(currentRow[i])) {
					currentRow[i] = "confidential (possible unclosed double quotes, or too big column on input file)";
					String errorMsg = "Row " + rowNumber + " was found with possible unclosed double quote.";
					final DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMsg)
							.errorPosition(new ErrorPosition(rowNumber))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(errorMsg, new ErrorPosition(rowNumber))
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
				}
			}
		}
		if (currentRow != null) {
			//SDMXCONV-1510 if we have a header check the num of columns with the header and not the previous one.
			if(this.csvInputConfig.getInputColumnHeader()==CsvInputColumnHeader.USE_HEADER) {
				return (headerColumns.length) != currentRow.length;
			}
			//We check with the number of columns of the previous row.
			return (this.firstRowOfDataLength != currentRow.length);
		} else {
			return true;
		}
	}

	@Override
	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	@Override
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	@Override
	public int getIngoredObsCount() {
		return this.ignoredObsCount;
	}

	@Override
	public int getObsCount() {
		return this.obsCount;
	}
	@Override
	public int getSeriesCount() {
		return this.seriesCount;
	}
	@Override
	public int getDatasetCount() {
		return this.datasetCount;
	}
	@Override
	public LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> getErrorsByEngine() {
		return this.errorsByEngine;
	}

	@Override
	public void setErrorsByEngine(LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine) {
		this.errorsByEngine = errorsByEngine;
	}

	@Override
	public Integer getMaximumErrorOccurrencesNumber() {
		return this.maximumErrorOccurrencesNumber;
	}

	@Override
	public void setMaximumErrorOccurrencesNumber(Integer maximumErrorOccurrencesNumber) {
		this.maximumErrorOccurrencesNumber = maximumErrorOccurrencesNumber;
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	@Override
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public int getErrorLimit() {
		return this.errorLimit;
	}

	@Override
	public void setErrorLimit(int errorLimit) {
		this.errorLimit = errorLimit;
	}
}
