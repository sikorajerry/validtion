/**
 * Copyright (c) 2015 European Commission.
 *
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl5
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.io.data.csv;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.engine.reader.ConceptInfo;
import org.estat.sdmxsource.engine.reader.LevelConceptMap;
import org.estat.sdmxsource.extension.model.ErrorReporter;
import org.estat.sdmxsource.sdmx.api.constants.DATASET_LEVEL;
import org.estat.sdmxsource.util.TimeFormatBuilder;
import org.estat.sdmxsource.util.csv.CsvColumn;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.FlatEncodingCheckUtil;
import org.estat.struval.builder.impl.ObservationBuilder;
import org.sdmxsource.sdmx.api.constants.*;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.ResolutionSettings;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptSchemeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.*;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.dataparser.engine.reader.utils.CsvReaderUtilities;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.AnnotationBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyableImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.ObservationImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetHeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.base.AnnotationMutableBeanImpl;
import org.sdmxsource.sdmx.util.beans.ConceptRefUtil;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataFormatException;
import org.sdmxsource.sdmx.validation.exceptions.TypeOfException;
import org.sdmxsource.util.ObjectUtil;
import org.sdmxsource.util.io.ByteCountHolder;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * DataReaderEngine for CSV Multilevel.
 * A mapping is expected for the CSV Multilevel.
 *
 * This reader does not support mappings on CrossSectional Measures.
 * e.g. for EDU_CONCEPTS: STUDENTS, TEACHERS, DIRECTORS etc.
 * If the DSD is a Cross Sectional then the supported form is that
 * 			 measures that belong to a series appear in one record.
 *
 *  e.g.
 *  1;GR;F
 *  2;DEATHS;
 *  3;2005;23871;P;
 *  3;2006;25432;P;
 *  2;EMIGT;
 *  3;2005;6331;P
 *  3;2006;5931;P
 *  2;IMMIT;
 *  3;2005;10581;P
 *  3;2006;11937;P
 *  2;LBIRTHST;
 *  3;2005;28340;P
 *  3;2006;25935;P
 *
 *  The second case(the one seen below) is not supported. Use MultiLevelCrossCsvDataReaderEngine for this:
 *  1;GR;F
 *  2;2005;23871;6331;10581;28340;P - where first value after time(2005) is for DEATHS,second for EMIGT, third for IMMIT and last for LBIRTHST
 *  2;2006;25432;5931;11937;25935;P
 *
 * @author Mihaela Munteanu
 *
 * @since 22.05.2017
 */
public class MultiLevelCsvDataReaderEngine extends AbstractDataReaderEngine
		implements ErrorReporter, RecordReaderCounter, DataValidationErrorHolder {

	/**
	 * Default serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = LogManager.getLogger(MultiLevelCsvDataReaderEngine.class);

	private ExceptionHandler exceptionHandler;
	private final SdmxBeanRetrievalManager beanRetrieval;
	//private DimensionBean dimensionAtObservation;
	private String dimensionAtObservation;
	/** The time format builder. */
	private final TimeFormatBuilder timeFormatBuilder = new TimeFormatBuilder();

	private CsvInputConfig csvInputConfig;

	private static final String DEFAULT_HEADER_ID = "ID1";
	private static final String DEFAULT_SENDER_ID = "ZZ9";

	private final boolean useXSMeasures;
	private final boolean useMeasures;
	private boolean isTimeSeries;
	private final Deque<Closeable> closables = new LinkedList<>();
	private Deque<Keyable> listOfKeyable = new LinkedList<>();
	private Deque<Observation> listOfObservation = new LinkedList<>();
	/* variables used to navigate through the elements */
	private Integer currentLevel;
	private Keyable previousKeyable;
	private List<KeyValue> datasetAttributes;
	protected List<KeyValue> previousDatasetAttributes;
	private String[] csvRow;
	private boolean movedToNextDataset;
	private CsvParser parser;
	/** the values for each component (dimension, observation, attribute)
	 *  that will be populated from the CSV file at each step */
	private Map<String, String> currentCsvValues = new Object2ObjectLinkedOpenHashMap<>();
	private Map<String, String> xsMeasureIdToMeasureCode = new Object2ObjectLinkedOpenHashMap<>();
	private Map<String, String> measureIdToMeasureCode = new Object2ObjectLinkedOpenHashMap<>();
	private Map<String, ComponentBean> mapOfComponentBeans = new Object2ObjectLinkedOpenHashMap<>();
	private final Map<String, ComponentBean> mapOfConcepts = new Object2ObjectLinkedOpenHashMap<>();
	private Map<String, ConceptBean> mapOfConceptBeans = new Object2ObjectLinkedOpenHashMap<>();
	/* final variables from parameters*/
	private final ReadableDataLocation dataLocation;
	private final CsvParserSettings settings;
	private final Integer numberOfLevels;
	/** A map that is transformed so that the key is the Level */
	private final Map<Integer, LevelConceptMap> conceptMappingByLevel;
	/* A map that holds for each level the number of columns */
	private final Map<Integer, Integer> columnsNumberByLevel = new Object2ObjectLinkedOpenHashMap<>();

	private int rowNumber = 0;

	private AnnotationBeanImpl inputFormatAnn = null;
	private AnnotationBeanImpl obsCoordinatesAnn = null;
	private List<AnnotationBeanImpl> annotations = new ObjectArrayList<>();

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

	private LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();

	private Integer maximumErrorOccurrencesNumber;

	private int order;
	
	/**
	 * The observation builder.
	 */
	private final ObservationBuilder observationBuilder = new ObservationBuilder();

	private int errorLimit;

	private Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
	private Observation previousObs;

	private CountingInputStream countingStream;

	@Override
	public Object2ObjectLinkedOpenHashMap<String, ErrorPosition> getErrorPositions() {
		return errorPositions;
	}

	@Override
	public void setErrorPositions(Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions) {
		this.errorPositions = errorPositions;
	}
	private final Map<String, KeyValueImpl> datasetKeyValuePool = new HashMap<>();

	private final Map<String, KeyValueImpl> keyAbleKeyValuePool = new HashMap<>();

	public MultiLevelCsvDataReaderEngine(	ReadableDataLocation dataLocation,
											 DataStructureBean dsd,
											 DataflowBean dataflow,
											 SdmxBeanRetrievalManager beanRetrieval,
											 CsvInputConfig csvInputConfig,
											 ExceptionHandler exceptionHandler){

		super(dataLocation, beanRetrieval, dsd, dataflow);
		this.beanRetrieval = beanRetrieval;
		this.dataLocation = dataLocation;
		if (beanRetrieval == null && dsd == null) {
			throw new IllegalArgumentException(
					"DataReaderEngine expects either a SdmxBeanRetrievalManager or a DataStructureBean to be able to interpret the structures");
		} else {
			this.currentDsd = dsd;
		}
		this.currentDataflow = dataflow;
		if (csvInputConfig.getHeader() != null) {
			this.headerBean = csvInputConfig.getHeader();
		} else {
			this.headerBean = new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID);
		}

		this.settings = new CsvParserSettings();
		settings.setLineSeparatorDetectionEnabled(true);
		CsvFormat format = new CsvFormat();
		format.setDelimiter(csvInputConfig.getDelimiter());
		settings.setFormat(format);
		// SDMXCONV-1359
		settings.getFormat().setComment('\0');
		this.exceptionHandler = exceptionHandler;
		this.conceptMappingByLevel = buildConceptMap(Integer.parseInt(csvInputConfig.getLevelNumber()),csvInputConfig.getMapping());
		this.numberOfLevels = Integer.parseInt(csvInputConfig.getLevelNumber());
		this.csvInputConfig = csvInputConfig;
		this.useXSMeasures = csvInputConfig.isMapCrossXMeasure();
		this.useMeasures = csvInputConfig.isMapMeasure();
		this.dimensionAtObservation = getDimensionAtObservation();
	}

	public MultiLevelCsvDataReaderEngine(	ReadableDataLocation dataLocation,
											 DataStructureBean defaultDsd,
											 DataflowBean defaultDf,
											 SdmxBeanRetrievalManager beanRetrieval,
											 CsvInputConfig csvInputConfig) {
		this(dataLocation, defaultDsd, defaultDf, beanRetrieval, csvInputConfig, new FirstFailureExceptionHandler());
	}

	@Override
	public DataReaderEngine createCopy() {
		return new MultiLevelCsvDataReaderEngine(dataLocation, currentDsd, currentDataflow, beanRetrieval, csvInputConfig, exceptionHandler);
	}

	@Override
	public List<KeyValue> getDatasetAttributes() {
		return this.datasetAttributes;
	}

	/**
	 * Method to erase Concept Values for the concepts on current level or lower.
	 * A higher number means a lower lever.
	 * e.g.  1;P
	 *       2;N100;A;;
	 *       3;2003;44.3;A;
	 *
	 * Level 3 is lower than 2 and 1.
	 */
	private void emptyConceptValuesForLowerThanCurrentLevel() {
		for (int lvl = currentLevel; lvl <= currentLevel; lvl++) {
			LevelConceptMap conceptMapForLevel = conceptMappingByLevel.get(lvl);
			conceptMapForLevel.getMapping().keySet().forEach((key) -> {
				//go through all concepts for this level and empty the values in currentCSvValue
				currentCsvValues.put(key, null);
			});
		}
	}

	//returns the first line of the CSV, called again it moves to the 2nd line and so on
	private String[] getNextLineOfCsvFile(){
		++rowNumber;
		//SDMXCONV-761 Set the row that is currently being read
		errorPositions.put(DATASET_LEVEL.NONE.name(), new ErrorPosition(this.rowNumber));
		//SDMXCONV-816, SDMXCONV-867
		if(ThreadLocalOutputReporter.getWriteAnnotations().get()) {
			AnnotationMutableBeanImpl annotation = new AnnotationMutableBeanImpl();
			annotation.setTitle(errorPositions.get(DATASET_LEVEL.NONE.name()).toString());
			annotation.setType(AnnotationType.ANN_OBS_COORDINATES.name());
			obsCoordinatesAnn =  new AnnotationBeanImpl(annotation,null);
			//clear the list before adding the annotations for every annotation
			annotations.clear();
			annotations.add(obsCoordinatesAnn);
			annotations.add(inputFormatAnn);
		}
		String[] nextLine = parser.parseNext();
		if(!FlatEncodingCheckUtil.isCheckLineEncodingCorrect(nextLine)) {
			String errorMsg = "The encoding of the file is not correct and contains invalid characters in line (" + rowNumber + " ). Please correct the file and try again!";
			throw new SdmxDataFormatException(ExceptionCode.DATA_INVALID_CHARACTER, errorMsg, "", new ErrorPosition(), errorMsg);
		}
		ByteCountHolder.setByteCount(this.countingStream.getByteCount());
		return nextLine;
	}

	/**
	 * Method that returns the Keyable for Series from the current values of the CSV
	 */
	private Keyable getKeyableFromCurrentCsvValues(){
		List<KeyValue> dimensions = new ObjectArrayList<>();
		List<KeyValue> attributes = new ObjectArrayList<>();
		String obsTime = null;
		boolean isNotAllDimensionsEmpty = false;
		for (Map.Entry<String, String> conceptCode : currentCsvValues.entrySet()) {
			ComponentBean componentBean = this.mapOfComponentBeans.get(conceptCode.getKey());
			if(!ObjectUtil.validObject(componentBean)) {
				componentBean = this.mapOfConcepts.get(conceptCode.getKey());
			}
			String componentId = conceptCode.getKey();
			if(ObjectUtil.validObject(componentBean))
				componentId = getComponentId(componentBean);
			if (componentBean instanceof DimensionBean) {
				if (componentBean.getStructureType()==SDMX_STRUCTURE_TYPE.TIME_DIMENSION) {
					obsTime = conceptCode.getValue();
				}else{
					if (!componentBean.equals(dimensionAtObservation)) {
						if (componentBean.getStructureType() == SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION) {
							if (useXSMeasures) {
								throw new IllegalArgumentException("A Measure Dimension was found in the input file, please uncheck 'Map CrossX Measures' or set 'mapCrossXMeasure' to false!");
							} else {
								dimensions.add(getOrCreateKeyValue(conceptCode.getValue(), componentId));
							}
						} else {
							dimensions.add(getOrCreateKeyValue(conceptCode.getValue(), componentId));
						}
					}
				}
			}else{
				if(componentBean instanceof AttributeBean){
					if (((AttributeBean)componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.DIMENSION_GROUP) {
						if (conceptCode.getValue() != null && conceptCode.getValue() != "") {
							attributes.add(getOrCreateKeyValue(conceptCode.getValue(), componentId));
						}
					}
				}
			}
			if(ObjectUtil.validString(conceptCode.getValue())) {
				isNotAllDimensionsEmpty = true;
			}
		}
		final Keyable keyable;
		final DataflowBean dataflow = getDataFlow();
		final DataStructureBean dsd = getDataStructure();
		// With XS Measure support, we might have a TimeDimension but the Keyable will need to have a cross-sectional object
		if (isTimeSeries) {
			// Time Series means that the dimension at observation is the Time Dimension and there is a Time Dimension
			if (obsTime != null) {
				TIME_FORMAT timeFormat = timeFormatBuilder.build(obsTime);
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, timeFormat);
			} else {
				// What do we do here throw an error ?
				// Because we have time series but the Time Dimension is missing
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, (TIME_FORMAT)null);
			}
		} else {
			if (obsTime != null) {
				TIME_FORMAT timeFormat = timeFormatBuilder.build(obsTime);
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, timeFormat, dimensionAtObservation, obsTime);
			} else {
				keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, null, dimensionAtObservation, "");
			}
		}

		if(ObjectUtil.validObject(keyable) && isNotAllDimensionsEmpty) {
			this.seriesCount = this.seriesCount + 1;
		}
		return keyable;
	}

	/**
	 * Method that returns the Keyable for Group from the current values of the CSV
	 */
	private Keyable getGroupFromCurrentCsvValues() {
		List<GroupBean> listOfGroupBeans = getDataStructure().getGroups();
		for (GroupBean bean: listOfGroupBeans) {
			List<KeyValue> dimensions = new ObjectArrayList<>();
			List<KeyValue> attributes = new ObjectArrayList<>();
			for (Map.Entry<String, String> conceptCode : currentCsvValues.entrySet()) {
				ComponentBean componentBean = this.mapOfComponentBeans.get(conceptCode.getKey());
				if(!ObjectUtil.validObject(componentBean)) {
					componentBean = this.mapOfConcepts.get(conceptCode.getKey());
				}
				String componentId = conceptCode.getKey();
				if(ObjectUtil.validObject(componentBean))
					componentId = getComponentId(componentBean);
				if (componentBean instanceof DimensionBean && bean.getDimensionRefs().contains(componentId)) {
					dimensions.add(new KeyValueImpl(conceptCode.getValue(), componentId));
				} else {
					if (componentBean instanceof AttributeBean) {
						if (((AttributeBean)componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.GROUP
								&& bean.getId().equals(((AttributeBean)componentBean).getAttachmentGroup())) {
							if (conceptCode.getValue() != null && conceptCode.getValue() != "") {
								attributes.add(new KeyValueImpl(conceptCode.getValue(), componentId));
							}
						}
					}
				}
			}
			if (ObjectUtil.validObject(attributes) && !attributes.isEmpty()) {
				Keyable keyable = new KeyableImpl(getDataFlow(), getDataStructure(), dimensions, attributes, bean.getId());
				return keyable;
			}
		}
		return null;
	}
	/*
	 * method that returns the Observation from the current line of the CSV
	 */
	private List<Observation> getObservationFromCurrentCsvValues(Keyable keyable){
		List<KeyValue> attributes = new ObjectArrayList<>();
		List<KeyValue> xsMeasures = new ObjectArrayList<>();
		List<KeyValue> measures = new ObjectArrayList<>();
		String obsValue = null;
		String obsTime = null;
		KeyValue crossSectionalValue = null;
		for (Map.Entry<String, String> entryConcept : currentCsvValues.entrySet()) {
			ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
			if(!ObjectUtil.validObject(componentBean)) {
				componentBean = this.mapOfConcepts.get(entryConcept.getKey());
			}
			String componentId = entryConcept.getKey();
			if(ObjectUtil.validObject(componentBean))
				componentId = getComponentId(componentBean);
			if (componentBean != null) {
				switch(componentBean.getStructureType()) {
					case TIME_DIMENSION:
						obsTime = entryConcept.getValue();
						break;
					case PRIMARY_MEASURE:
						if (!useXSMeasures) {
							obsValue = entryConcept.getValue();
						}
						else {
							// TODO throw an error
						}
						break;
					case DATA_ATTRIBUTE:
						// TODO this is not correct we need to check using the dimension at observation
						if (((AttributeBean)componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.OBSERVATION) {
							String value = entryConcept.getValue();
							if (value != null && !value.equals("")) {
								attributes.add(new KeyValueImpl(value, componentId));
							}
						}
						break;
					case CROSS_SECTIONAL_MEASURE:
						if (useXSMeasures) {
							String value = entryConcept.getValue();
							xsMeasures.add(new KeyValueImpl(value, componentId));
						}
						break;
					case DIMENSION:
					case MEASURE_DIMENSION:
						if (!isTimeSeries && dimensionAtObservation.equals(componentId)) {
							String value = entryConcept.getValue();
							crossSectionalValue = new KeyValueImpl(value, dimensionAtObservation);
						}

						break;
				}
			} else {
				logger.debug("Unknown column "+entryConcept.getKey()+" mapping found either in the column mapping file or in the csv header");
			}
			//SDMXCONV-1161
			ConceptBean conceptBean = this.mapOfConceptBeans.get(entryConcept.getKey());
			if (conceptBean != null) {
				String value = entryConcept.getValue();
				measures.add(new KeyValueImpl(value, conceptBean.getId()));
			}
		}

		final String normalizedObsTime;
		if (obsTime != null){
			normalizedObsTime = obsTime;
		}
		else {
			normalizedObsTime = "";
		}

		if (useXSMeasures) {
			if(checkIfObservationIsValid(keyable,attributes,null,normalizedObsTime,null,xsMeasures)) {
				this.obsCount = this.obsCount + 1;
				return builldObservationXsMeasures(keyable, attributes, normalizedObsTime, xsMeasures);
			}
		} else if (useMeasures) {
			if(checkIfObservationIsValid(keyable,attributes,null,normalizedObsTime,null,measures)) {
				this.obsCount = this.obsCount + 1;
				return builldObservationMeasures(keyable, attributes, normalizedObsTime, measures);
			}
		} else {
			if(checkIfObservationIsValid(keyable,attributes,obsValue,normalizedObsTime,crossSectionalValue,null)) {
				Observation observation = buildObservationNoXsMeasures(keyable, attributes, obsValue, normalizedObsTime,
						crossSectionalValue);
				this.obsCount = this.obsCount + 1;
				return Arrays.asList(observation);
			}
		}
		return null;
	}

	public boolean checkIfObservationIsValid(Keyable keyable, List<KeyValue> attributes,String obsValue, String normalizedObsTime, KeyValue crossSectionalValue,List<KeyValue> measures) {
		if (ObjectUtil.validObject(crossSectionalValue)) {
			return true;
		}
		if(ObjectUtil.validObject(obsValue)){
			return true;
		}
		if (ObjectUtil.validString(normalizedObsTime)) {
			return true;
		}
		if (ObjectUtil.validCollection(attributes)) {
			return true;
		}
		if(ObjectUtil.validCollection(measures)) {
			return true;
		}
		if(ObjectUtil.validObject(keyable) && isKeyableNotNull(keyable)){
			return true;
		}
		return false;
	}

	private boolean isKeyableNotNull(Keyable keyable) {
		if(!ObjectUtil.validObject(keyable))
			return false;
		if(ObjectUtil.isAllNulls(keyable.getKey()))
			return false;

		boolean isEmpty = true;
		for(KeyValue kv : keyable.getKey()) {
			if(ObjectUtil.validObject(kv) && ObjectUtil.validObject(kv.getCode()))
				isEmpty = false;
		}
		return !isEmpty;
	}

	/**
	 * To be used only when XS Measures are used.
	 * @param keyable
	 * @param attributes
	 * @param obsTime
	 * @param xsMeasures
	 * @return
	 */
	private List<Observation> builldObservationXsMeasures(Keyable keyable, List<KeyValue> attributes, String obsTime, List<KeyValue> xsMeasures) {
		List<Observation> xsObservations = new ArrayList<>();
		AnnotationBeanImpl[] annotationsArr = annotations.toArray(new AnnotationBeanImpl[annotations.size()]);
		for(KeyValue xsMeasure : xsMeasures) {
			String xsMeasureCode = xsMeasureIdToMeasureCode.get(xsMeasure.getConcept());
			KeyValue crossSectionalValue = new KeyValueImpl(xsMeasureCode, dimensionAtObservation);
			String obsValue = xsMeasure.getCode();
			Observation observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, crossSectionalValue, annotationsArr); //SDMXCONV-1041
			xsObservations.add(observation);
		}
		return xsObservations;
	}

	/**
	 * To be used only when explicit Measures are used.
	 *
	 * @param keyable
	 * @param attributes
	 * @param obsTime
	 * @param measures
	 * @return List<Observation>
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-796">SDMXCONV-796</a>
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1161">SDMXCONV-1161</a>
	 */
	private List<Observation> builldObservationMeasures(Keyable keyable, List<KeyValue> attributes, String obsTime, List<KeyValue> measures) {
		List<Observation> observations = new ArrayList<>();
		AnnotationBeanImpl[] annotationsArr = annotations.toArray(new AnnotationBeanImpl[annotations.size()]);
		for (KeyValue measure : measures) {
			String measureCode = measureIdToMeasureCode.get(measure.getConcept());
			KeyValue crossSectionalValue = new KeyValueImpl(measureCode, dimensionAtObservation);
			String obsValue = measure.getCode();
			Observation observation;
			observation = new ObservationImpl(keyable, obsTime, obsValue, attributes, crossSectionalValue, annotationsArr);
			observations.add(observation);
		}
		return observations;
	}

	private void populateXsMeasureIdToMeasureCode() {
		this.xsMeasureIdToMeasureCode.clear();
		if (currentDsd instanceof CrossSectionalDataStructureBean) {
			CrossSectionalDataStructureBean crossDsd = (CrossSectionalDataStructureBean)currentDsd;
			for(CrossSectionalMeasureBean measure : crossDsd.getCrossSectionalMeasures()) {
				this.xsMeasureIdToMeasureCode.put(measure.getId(), measure.getCode());
			}
		}
	}

	private void populateMeasureIdToMeasureCode() {
		this.measureIdToMeasureCode.clear();
		List<DimensionBean> dim = currentDsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
		if(!dim.isEmpty() && beanRetrieval!=null) {
			CrossReferenceBean ref = dim.get(0).getRepresentation().getRepresentation();
			MaintainableBean maintainable = beanRetrieval.getMaintainableBean(ref, false, false);
			SdmxBeans beans = beanRetrieval.getSdmxBeans(maintainable.asReference(), ResolutionSettings.RESOLVE_CROSS_REFERENCES.DO_NOT_RESOLVE);
			Set<ConceptSchemeBean> concepts = beans.getConceptSchemes(ref);
			for(ConceptSchemeBean concept : concepts) {
				for(ConceptBean item : concept.getItems()) {
					this.measureIdToMeasureCode.put(item.getId(), item.getId());
				}
			}
		}
	}

	/**
	 * To be used only when XS measures are not used
	 * @param keyable
	 * @param attributes
	 * @param obsValue
	 * @param obsTime
	 * @param crossSectionalValue
	 * @return
	 */
	private Observation buildObservationNoXsMeasures(Keyable keyable, List<KeyValue> attributes,
													 String obsValue, String obsTime, KeyValue crossSectionalValue) {
		Observation observation;
		AnnotationBeanImpl[] annotationsArr = annotations.toArray(new AnnotationBeanImpl[annotations.size()]);
		if (isTimeSeries) {
			observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, annotationsArr); //SDMXCONV-1041
		} else {
			if (crossSectionalValue == null) {
				String value = currentCsvValues.get(dimensionAtObservation);
				crossSectionalValue = new KeyValueImpl(value, dimensionAtObservation);
			}
			observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, crossSectionalValue, annotationsArr); //SDMXCONV-1046 & SDMXCONV-1050
		}
		return observation;
	}

	/**
	 * Method that returns the Observation for Group from the current values of the CSV
	 */
	private void getCurrentDatasetAttributesFromCurrentCsvValues() {
		List<KeyValue> attributes = new ArrayList<>();
		for (Map.Entry<String, String> conceptCode : currentCsvValues.entrySet()) {
			ComponentBean componentBean = getDataStructure().getComponent(conceptCode.getKey());
			if (componentBean instanceof AttributeBean) {
				if (((AttributeBean) componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.DATA_SET) {
					if (conceptCode.getValue() != null && !conceptCode.getValue().isEmpty()) {
						attributes.add(getOrCreateKeyValue(conceptCode.getValue(), componentBean.getId()));
					}
				}
			}
		}
		this.datasetAttributes = attributes;
		checkDataSetValues(); // When new dataset is read check dataset values if the same
	}

	private KeyValueImpl getOrCreateKeyValue(String value, String componentId) {
		String key = value + ":" + componentId;
		return datasetKeyValuePool.computeIfAbsent(key, k -> new KeyValueImpl(value, componentId));
	}

	/**
	 * Method that is called for every row to check dataSet attributes.
	 * If are different throw an error.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1424">SDMXCONV-1424</a>
	 */
	protected void checkDataSetValues() {
		if (ObjectUtil.validCollection(this.datasetAttributes) && ObjectUtil.validCollection(this.previousDatasetAttributes)) {
			if (!CsvReaderUtilities.compareKeyValues(this.datasetAttributes, previousDatasetAttributes, false)) {
				String errorMessage = "Row " + rowNumber + " was found with different dataset attribute values.";
				final DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition(rowNumber))
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(errorMessage)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		}
		previousDatasetAttributes = this.datasetAttributes;
	}

	@Override
	public void reset() {
		//SDMXCONV-816, SDMXCONV-867
		if(ThreadLocalOutputReporter.getWriteAnnotations().get()) {
			AnnotationMutableBeanImpl annotation = new AnnotationMutableBeanImpl();
			annotation.setTitle("MULTI_LEVEL_CSV");
			annotation.setType(AnnotationType.ANN_INPUT_FORMAT.name());
			inputFormatAnn =  new AnnotationBeanImpl(annotation,null);
		}
		super.reset();
		closeStreams();
		openStreams();
		initialize();
	}

	@Override
	public void close() {
		closeStreams();
		if (dataLocation != null) {
			dataLocation.close();
		}
	}

	private void initialize() {
		this.previousDatasetAttributes = new ArrayList<>();
	}

	private void openStreams() {
		parser = new CsvParser(this.settings);
		InputStream inputStream = dataLocation.getInputStream();
		this.countingStream = new CountingInputStream(inputStream);
		this.closables.add(inputStream);
		this.closables.add(this.countingStream);
		parser.beginParsing(this.countingStream, "UTF-8");
	}

	private void closeStreams() {
		if (parser != null) {
			parser.stopParsing();
		}
		while(!closables.isEmpty()) {
			try {
				closables.pop().close();
			} catch (IOException e) {
				logger.error("Exception IOException ", e);
			}
		}
	}

	private String getDimensionAtObservation(){
		final DataStructureBean dsd = getDataStructure();
		// when XS measures are used then we use the Measure Dimension
		List<DimensionBean> listOfDimensionBeans =  dsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
		if ((useXSMeasures||useMeasures) && !listOfDimensionBeans.isEmpty()) {
			return listOfDimensionBeans.get(0).getId();
		}
		if (dsd.getTimeDimension() != null) {
			return dsd.getTimeDimension().getId();
		} else {
			if (!listOfDimensionBeans.isEmpty()){
				return listOfDimensionBeans.get(0).getId();
			} else {
				//SDMXCONV-945
				return DIMENSION_AT_OBSERVATION.ALL.getVal();
				//List<DimensionBean> dimensions = dsd.getDimensionList().getDimensions();
				//return dimensions.get(dimensions.size()-1);
			}
		}
	}

	/**
	 * Method to populate the map with concept values from csv file.
	 *
	 * Fills in Components for the current level
	 * When the level is found populate currentCsvValues for the concepts
	 */
	private void populateCurrentConceptValues() {
		LevelConceptMap conceptMapForLevel = conceptMappingByLevel.get(currentLevel);
			for (Map.Entry<String, ConceptInfo> entryConcept : conceptMapForLevel.getMapping().entrySet()) {
				//go through all concepts for this level and fill the values in currentCSvValue
				ConceptInfo conceptInfo = entryConcept.getValue();
				String conceptName = entryConcept.getKey();
				if (conceptInfo.isFixed()) {
					currentCsvValues.put(conceptName, conceptInfo.getFixedValue());
				} else {
					if (conceptInfo.getColumn() != null && conceptInfo.getColumn() < csvRow.length && conceptInfo.getColumn() != -1) {
						String conceptValue = csvRow[conceptInfo.getColumn()];
						String transcodedValue = "";
						if (csvInputConfig.getTranscoding() != null
								&& csvInputConfig.getTranscoding().containsKey(conceptName)
								&& csvInputConfig.getTranscoding().get(conceptName).containsKey(conceptValue)) {
							transcodedValue = csvInputConfig.getTranscoding().get(conceptName).get(conceptValue);
						} else {
							transcodedValue = conceptValue;
						}
						currentCsvValues.put(conceptName, transcodedValue);
					} else {
						currentCsvValues.put(conceptName, null);
					}
				}
			}
	}

	/**
	 * Builds a Map of maps with the level as key.
	 * The outside mapping is organized for each level has only the concepts that are mapped on the respective level.
	 * A map for each level which further contains a mapping for each concept name(CsvMapping).
	 * (of dimensions, attributes and observations)
	 * the  inside mapping (CsvMapping) contains the fixed value or the column and the value taken from the csv file.
	 *
	 * e.g. Returned result:
	 * level 1 contains  FREQ with fixed=false, column=2
	 *                   JD_TYPE with fixed=true, value="P"
	 * level 2 contains  COLLECTIONS with fixed=false, column=2
	 *                   COUNTRY with fixed=false, column=3
	 * level 3 contains  OBS_VALUE with fixed=false, column=2
	 * 				    OBS_NOTE with fixed=false, column=3
	 * 					OBS_STATUS with fixed=false, column=4
	 *
	 * @param mapping
	 * @return Map<Integer, CsvMapping>
	 */
	private Map<Integer, LevelConceptMap> buildConceptMap(int nrOfLevels, Map<String, CsvInColumnMapping> mapping) {
		Map<Integer, LevelConceptMap> conceptMappingByLevels = new LinkedHashMap<>();

		for (int lvl = 1; lvl <= nrOfLevels; lvl++) {
			LevelConceptMap csvMappingByConcept = new LevelConceptMap();
			for (Map.Entry<String, CsvInColumnMapping> entry : mapping.entrySet()){
				// position zero holds column numbers - for multilevel just one column,
				// position 1 = fixed (true/false),
				// position 2 = level,
				// position 3 = fixedValue if fixed = true
				CsvInColumnMapping csvInColumnMapping = entry.getValue();
				Integer conceptLevel = csvInColumnMapping.getLevel();
				Integer conceptColumn = null;
				if (csvInColumnMapping.getColumns() != null && csvInColumnMapping.getColumns().size() > 0) {
					conceptColumn = csvInColumnMapping.getColumns().get(0).getIndex();//for multilevel only one column is accepted
				}
				Integer thisLevel = (conceptLevel != null && conceptLevel > 0) ? conceptLevel : 0;
				if (thisLevel == lvl) {
					String conceptName = entry.getKey();
					//replace TIME with TIME_PERIOD as expected in the sdmxsource impl
					if ("TIME".equals(entry.getKey())){
						conceptName = "TIME_PERIOD";
					}
					csvMappingByConcept.put(conceptName, new ConceptInfo(   conceptColumn,
							csvInColumnMapping.isFixed(),
							csvInColumnMapping.getFixedValue()));
				}
			}
			conceptMappingByLevels.put(lvl, csvMappingByConcept);
		}
		return conceptMappingByLevels;
	}

	/** Method to parse the String level value as an integer and validate it. */
	private Integer getLevelNumberFromCurrentRow() {
		try {
			Integer lvl;
			String stringLevel;
			if (csvRow == null) {//at this point csvRow is not expected to be null
				throw new IllegalArgumentException("An empty row or end of file was expected but none found. "
						+ "Check if the format of the csv input file is correct. "
						+ "Check if the number of levels inside the file equals the given number of CSv Levels as parameter: " + numberOfLevels );
			}
			stringLevel = csvRow[0];
			lvl = Integer.valueOf(stringLevel);
			if (lvl <= 0)
				throw new IllegalArgumentException("First column of each row should be "
						+ "an integer greater than zero representing the level. "
						+ "Value " + stringLevel + " does not satisfy condition.");
			if (lvl > numberOfLevels)
				throw new IllegalArgumentException("First column of each row represents the level "
						+ "and cannot be greater than the number of levels" + numberOfLevels + ".  "
						+ "Value " + stringLevel + " does not satisfy condition.");

			return lvl;
		} catch (IllegalFormatException | NumberFormatException illegalFormatException) {
			throw new IllegalArgumentException("First column of each row should be an integer "
					+ "representing the level. Value " + csvRow[0] + " does not satisfy condition.");
		}
	}

	@Override
	protected Observation lazyLoadObservation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Keyable lazyLoadKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean moveNextDatasetInternal() {
		if (!this.movedToNextDataset) {
			this.currentLevel = 0;
			while (currentLevel <= numberOfLevels && !movedToNextDataset) {
				this.csvRow = getNextLineOfCsvFile();
				this.currentLevel = getLevelNumberFromCurrentRow();
				columnsNumberByLevel.put(currentLevel, csvRow.length);
				populateCurrentConceptValues();
				if (currentLevel == numberOfLevels) {
					//a complete info exists by getting to the lowest level
					//it is set here to prevent reading of another row after a lowest level row was found
					this.movedToNextDataset = true;
					if(ObjectUtil.validArray(this.csvRow) && this.csvRow.length > 1) {
						this.datasetCount = this.datasetCount + 1;
					}
				}
				LevelConceptMap conceptMapping = conceptMappingByLevel.get(currentLevel);
				Integer numOfColumnsByLvl = columnsNumberByLevel.get(currentLevel);
				// File with mapping (multi-level csv): will raise an error if they are more columns in a line of the file than what is declared
				// for that level in the mapping. SDMXCONV-1528
				if(ObjectUtil.validObject(csvInputConfig.isAllowAdditionalColumns()) && !csvInputConfig.isAllowAdditionalColumns() && ObjectUtil.validObject(numOfColumnsByLvl, conceptMapping)) {
					if(ObjectUtil.validObject(numOfColumnsByLvl.intValue(), conceptMapping.getMapping())
							&& numOfColumnsByLvl.intValue()-1 > conceptMapping.getMapping().size()) {
						String errorMessage = "Row " + rowNumber + " of level " + currentLevel + " of file was found with number of values different from the number of columns; " +
								"check the field separator, the number of fields in the row and if a non-authorized character was used";
						final DataValidationError dataValidationError = new DataValidationError
								.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMessage)
								.errorPosition(new ErrorPosition(rowNumber))
								.typeOfException(TypeOfException.SdmxSyntaxException)
								.args(errorMessage, new ErrorPosition(rowNumber))
								.order(++this.order)
								.build();
						addError(ValidationEngineType.READER, dataValidationError);
						return false; // SDMXCONV-1528
					}
				}
			}
			//set the current DatasetHeaderBean
			setDataSetHeaderBean();
			//sets the attributes for this data set from the complete list of CsvValues mapped to Concepts
			getCurrentDatasetAttributesFromCurrentCsvValues();

			//build Keyable for group
			addGroupKeyable();
			//build regular keyable, put in the list
			Keyable keyable = getKeyableFromCurrentCsvValues();
			this.listOfKeyable.add(keyable);
			//build observation(s)
			List<Observation> obs = getObservationFromCurrentCsvValues(keyable);
			if(ObjectUtil.validObject(obs)) {
				this.listOfObservation.addAll(obs);
			}
			return true;
		}
		return false;
	}

	/** Builds and sets dataset header to be used for dataset */
	private void setDataSetHeaderBean() {
		this.dimensionAtObservation= getDimensionAtObservation();
		this.isTimeSeries= currentDsd!=null && currentDsd.getTimeDimension()!=null && dimensionAtObservation.equals(currentDsd.getTimeDimension().getId());
		populateXsMeasureIdToMeasureCode();
		populateMeasureIdToMeasureCode();
		DatasetStructureReferenceBean datasetStructureReferenceBean = new DatasetStructureReferenceBeanImpl(null, getDataStructure().asReference(), null, null, dimensionAtObservation, this.useMeasures);
		//SDMXCONV-880
		if(getHeader()!=null && getHeader().getAction()!=null && getHeader().getAction().getAction()!=null){
			this.datasetHeaderBean = new DatasetHeaderBeanImpl(getDataStructure().getId(), DATASET_ACTION.getAction(getHeader().getAction().getAction()), datasetStructureReferenceBean);
		} else {
			this.datasetHeaderBean = new DatasetHeaderBeanImpl(getDataStructure().getId(), DATASET_ACTION.INFORMATION, datasetStructureReferenceBean);
		}
		// SDMXCONV-1161
		// If there are concepts under a measure dimensions,
		// then measure dimension is represented by all the concept scheme
		List<DimensionBean> dim = this.currentDsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
		if (useMeasures) {
			for (ComponentBean component : this.currentDsd.getComponents()) {
				if (component.getStructureType() != SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION) {
					this.mapOfConcepts.put(ConceptRefUtil.getConceptId(component.getConceptRef()), component);
					this.mapOfComponentBeans.put(getComponentId(component), component);
				}
			}
			if (!dim.isEmpty()) {
				CrossReferenceBean ref = dim.get(0).getRepresentation().getRepresentation();
				if(this.beanRetrieval!=null) {
					MaintainableBean maintainable = this.beanRetrieval.getMaintainableBean(ref, false, false);
					SdmxBeans beans = this.beanRetrieval.getSdmxBeans(maintainable.asReference(),
							ResolutionSettings.RESOLVE_CROSS_REFERENCES.DO_NOT_RESOLVE);
					Set<ConceptSchemeBean> concepts = beans.getConceptSchemes(ref);
					for (ConceptSchemeBean concept : concepts) {
						for (ConceptBean item : concept.getItems()) {
							this.mapOfConceptBeans.put(item.getId(), item);
						}
					}
				}
			}
		} else {
			for (ComponentBean component : getDataStructure().getComponents()) {
				// In SDMX v2.1 the component id identifies the component
				mapOfComponentBeans.put(getComponentId(component), component);
				this.mapOfConcepts.put(ConceptRefUtil.getConceptId(component.getConceptRef()), component);
			}
		}
	}

	private String getComponentId(ComponentBean component) {
		String id = null;
		if(csvInputConfig.getStructureSchemaVersion() == SDMX_SCHEMA.VERSION_TWO) {
			if(component != getDataStructure().getTimeDimension() && component != getDataStructure().getPrimaryMeasure()) {
				id = ConceptRefUtil.getConceptId(component.getConceptRef());
			} else if(component == getDataStructure().getTimeDimension()) {
				id = getDataStructure().getTimeDimension().getId();
			} else if(component == getDataStructure().getPrimaryMeasure()) {
				id = getDataStructure().getPrimaryMeasure().getId();
			}
		} else {
			id = component.getId();
		}
		return id;
	}

	/*
	 * If there is a pending keyable we remove it from the queue and set it as current
	 * If there isn't a pending keyable we read the next lines from csv
	 * until a different keyable is found and again store the keyable
	 */
	@Override
	protected boolean moveNextKeyableInternal() {
		if(!listOfKeyable.isEmpty()){
			currentKey = listOfKeyable.remove();
			if (currentKey == null) {
				logger.debug("current key is null");
			}
			previousKeyable = currentKey;
		} else {
			Keyable newKeyable = null;
			while (newKeyable == null || isEqual(newKeyable, previousKeyable)) {
				this.csvRow = getNextLineOfCsvFile();
				if (this.csvRow == null) {
					return false;
				}
				this.currentLevel = getLevelNumberFromCurrentRow();
				if (!checkifColumnNumberIsValidForLevel(currentLevel, csvRow)) {
					String errorMsg = "Level "+currentLevel+" was found with different number of columns.";
					exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.WORKBOOK_READER_ERROR,
							errorMsg,
							"",
							this.errorPositions.get(DATASET_LEVEL.NONE.name()),
							errorMsg));
				}
				emptyConceptValuesForLowerThanCurrentLevel();
				populateCurrentConceptValues();
				getCurrentDatasetAttributesFromCurrentCsvValues();
				addGroupKeyable();
				newKeyable = getKeyableFromCurrentCsvValues();
			}

			//set the currentKeyable - main purpose of this interface method
			// TODO why cant we use newKeyable here ?
			currentKey = getKeyableFromCurrentCsvValues();
			if (currentLevel == numberOfLevels) {
				//empty list of observations as the new keyable is not related to the old observations
				listOfObservation = new LinkedList<>();
				List<Observation> obs = getObservationFromCurrentCsvValues(currentKey);
				if(ObjectUtil.validObject(obs)) {
					this.listOfObservation.addAll(obs);
				}
			}
		}
		previousKeyable = currentKey;//currentKey will become null with the first moveNextKeyable call
		if (currentKey == null) {
			return false;
		}
		return true;
	}
	private boolean isEqual(Keyable newKeyable, Keyable previousKeyable) {
		return previousKeyable.equals(newKeyable) && (isTimeSeries || getDataStructure().getTimeDimension() == null || previousKeyable.getObsTime().equals(newKeyable.getObsTime()));
	}

	/*
	 * remove from the observation queue the first obs and set it as the current observation
	 * read the next line,  if exists empty the currentConceptValues for respective levels and repopulate
	 * if the currentLevel is the lowest level (i.e. equal to number of levels) a Keyable(if Keyable is different than the current)
	 * and an Observation can be built
	 */
	@Override
	protected boolean moveNextObservationInternal() {
		if(ObjectUtil.validObject(listOfObservation) && !listOfObservation.isEmpty()){
			currentObs = listOfObservation.remove();
			return true;
		} else {
			this.csvRow = getNextLineOfCsvFile();
			if (this.csvRow == null) {
				return false;
			}
			this.currentLevel = getLevelNumberFromCurrentRow();
			if (!checkifColumnNumberIsValidForLevel(currentLevel, csvRow)) {
				String errorMsg = "Level "+currentLevel+" was found with different number of columns.";
				exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.WORKBOOK_READER_ERROR,
						errorMsg,
						"",
						this.errorPositions.get(DATASET_LEVEL.NONE.name()),
						errorMsg));
			}
			emptyConceptValuesForLowerThanCurrentLevel();
			populateCurrentConceptValues();
			getCurrentDatasetAttributesFromCurrentCsvValues();
			Keyable newKeyable = getKeyableFromCurrentCsvValues();
			if (currentLevel == numberOfLevels) {
				if (!isEqual(newKeyable, currentKey)) {
					//addGroupKeyable();
					listOfKeyable.add(newKeyable);
					listOfObservation = new LinkedList<>();//reset listOfObservations as Keyable has changed
					List<Observation> obs = getObservationFromCurrentCsvValues(newKeyable);
					if(ObjectUtil.validObject(obs)) {
						//this new Observation(s) is related to a new Keyable hence cannot be set as current
						this.listOfObservation.addAll(obs);
					}
					return false;
				}
				List<Observation> obs = getObservationFromCurrentCsvValues(currentKey);
				if(ObjectUtil.validObject(obs)) {
					this.listOfObservation.addAll(obs);
				}
				Observation newObservation = listOfObservation.remove();
				if (currentObs != null && newObservation.equals(currentObs)) {
					return false;
				}
				currentObs = newObservation;
				return true;
			} else {
				errorPositions.put(DATASET_LEVEL.OBS.name(), new ErrorPosition(this.rowNumber));
				return moveNextObservationInternal(); //should move to a next keyable as the current one does not have observations anymore
			}
		}
	}

	private void addGroupKeyable() {
		Keyable groupKeyable = getGroupFromCurrentCsvValues();
		if (groupKeyable != null) {
			listOfKeyable.add(groupKeyable);
		}
	}

	private boolean checkifColumnNumberIsValidForLevel(Integer level, String[] currentRow){
		boolean result = true;
		int lenghtOfLevel = columnsNumberByLevel.get(level);
		if (currentRow.length != lenghtOfLevel) {
			result = false;
		}
		return result;
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
	public ExceptionHandler getExceptionHandler() {
		return this.exceptionHandler;
	}

	@Override
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
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
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public int getOrder() {
		return this.order;
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
