package com.intrasoft.sdmx.converter.io.data.csv;

import com.intrasoft.sdmx.converter.io.data.TranscodingEngine;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthParser;
import com.univocity.parsers.fixed.FixedWidthParserSettings;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.extension.model.ErrorReporter;
import org.estat.sdmxsource.sdmx.api.constants.DATASET_LEVEL;
import org.estat.sdmxsource.util.TimeFormatBuilder;
import org.estat.sdmxsource.util.csv.*;
import org.estat.struval.builder.impl.ObservationBuilder;
import org.sdmxsource.sdmx.api.constants.*;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.ResolutionSettings.RESOLVE_CROSS_REFERENCES;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptSchemeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.beans.registry.ProvisionAgreementBean;
import org.sdmxsource.sdmx.api.model.data.*;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.*;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.AnnotationBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.ComplexNodeValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyableImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.ObservationImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetHeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.base.AnnotationMutableBeanImpl;
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

import static org.sdmxsource.sdmx.dataparser.engine.reader.AnnotationType.*;


public class FLRDataReaderEngine implements DataValidationErrorHolder, DataReaderEngine, ErrorReporter, RecordReaderCounter, InternalAnnotationEngine {

    private static final Logger logger = LogManager.getLogger(FLRDataReaderEngine.class);
    private final DataStructureBean dsd;
    private final DataflowBean dataflow;
    private final Deque<Closeable> closables = new LinkedList<>();
    private final Deque<Keyable> listOfKeyable = new LinkedList<>();
    private final Deque<Observation> listOfObservation = new LinkedList<>();
    private final CountingInputStream inputStream;
    private final FixedWidthParserSettings settings;
    /**
     * the time format builder
     */
    private final TimeFormatBuilder timeFormatBuilder = new TimeFormatBuilder();
    //SDMXCONV-1185
    private final Map<String, CodeDataInfo> columnsPresenceMapping = new HashMap<>();
    private final List<KeyValue> missingDimensions = new ArrayList<>();
    private final List<KeyValue> missingAttributes = new ArrayList<>();
    private final List<KeyValue> missingObsAttributes = new ArrayList<>();
    private final List<KeyValue> missingMeasures = new ArrayList<>();
    List<KeyValue> previousReadDataSetAttributes;
    /**
     * the exception handler
     */
    private ExceptionHandler exceptionHandler;
    private SdmxBeanRetrievalManager beanRetrieval;
    private int order;
    private int errorLimit;
    private final Map<String, ComponentBean> mapOfComponentBeans = new HashMap<>();
    private final Map<String, ComponentBean> mapOfConcepts = new HashMap<>();
    private final Map<String, String> xsMeasureIdToMeasureCode = new HashMap<>();
    private final Map<String, ConceptBean> mapOfConceptBeans = new HashMap<>();
    private final Map<String, String> measureIdToMeasureCode = new HashMap<>();
    private final FLRInputConfig flrInputConfig;
    private List<KeyValue> datasetAttributes;
    private Keyable currentKeyable;
    private Observation currentObservation;
    private FixedWidthParser parser;
    private boolean movedToNextDataset;
    private String[] firstRowOfData;
    private String[] headerColumns;
    private final String dimensionAtObservation;
    private DatasetHeaderBean currentDatasetHeaderBean;
    private final boolean useXSMeasures;
    private final boolean useMeasures;
    private final boolean isTimeSeries;
    private final List<FLRConceptToColumnPositionMapping> listOFConceptsToPositionMapping;
    private int rowNumber = 0;
    private boolean isCurrentRowNull = false;
    private AnnotationBeanImpl inputFormatAnn = null;
    private AnnotationBeanImpl obsCoordinatesAnn = null;
    List<AnnotationBeanImpl> obsAnnotations = new ArrayList<>();

    private int firstRowOfDataLength;

    @Override
    public List<AnnotationBeanImpl> getObsAnnotations() {
        return obsAnnotations;
    }
    @Override
    public void setObsAnnotations(List<AnnotationBeanImpl> obsAnnotations) {
        this.obsAnnotations = obsAnnotations;
    }
    private final TranscodingEngine transcoding;

    private final ObservationBuilder observationBuilder = new ObservationBuilder();
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
    private Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();

    /**
     * data location based constructor
     *
     * @param dataLocation
     * @param dsd
     * @param dataflowBean
     * @param beanRetrieval
     * @param flrInputConfig
     * @param exceptionHandler
     */
    public FLRDataReaderEngine(ReadableDataLocation dataLocation, DataStructureBean dsd, DataflowBean dataflowBean, SdmxBeanRetrievalManager beanRetrieval, FLRInputConfig flrInputConfig, ExceptionHandler exceptionHandler) {
        this(dataLocation.getInputStream(), dsd, dataflowBean, beanRetrieval, flrInputConfig, exceptionHandler);
    }

    /**
     * input stream based constructor
     *
     * @param inputStream
     * @param dsd
     * @param dataflowBean
     * @param beanRetrieval
     * @param flrInputConfig
     * @param exceptionHandler
     */
    public FLRDataReaderEngine(InputStream inputStream, DataStructureBean dsd, DataflowBean dataflowBean, SdmxBeanRetrievalManager beanRetrieval, FLRInputConfig flrInputConfig, ExceptionHandler exceptionHandler) {

        this.beanRetrieval = beanRetrieval;
        this.inputStream = new CountingInputStream(inputStream);
        if (beanRetrieval == null && dsd == null) {
            throw new IllegalArgumentException("FLRDataReaderEngine expects either a SdmxBeanRetrievalManager or a DataStructureBean to be able to interpret the structures");
        } else if (dsd != null) {
            this.dsd = dsd;
        } else if (dataflowBean != null && (dataflowBean.isExternalReference() == null || !dataflowBean.isExternalReference().isTrue())) {
            this.dsd = beanRetrieval.getMaintainableBean(DataStructureBean.class, dataflowBean.getDataStructureRef());
        } else {
            Set<DataStructureBean> maintainableBeans = beanRetrieval.getMaintainableBeans(DataStructureBean.class, new MaintainableRefBeanImpl(), false, true);
            if (!maintainableBeans.isEmpty()) {
                this.dsd = maintainableBeans.iterator().next();
            } else {
                throw new IllegalArgumentException("FLRDataReaderEngine expects DataStructureBean to be able to interpret the structures");
            }
        }

        if (flrInputConfig.getBeanRetrieval() != null) {
            this.beanRetrieval = flrInputConfig.getBeanRetrieval();
        }
        this.flrInputConfig = flrInputConfig;
        this.useXSMeasures = flrInputConfig.isMapCrossXMeasure();
        this.useMeasures = flrInputConfig.isMapMeasure();
        this.transcoding = new TranscodingEngine(flrInputConfig.getTranscoding());
        List<DimensionBean> dim = this.dsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);

        // SDMXCONV-796
        // If there are concepts under a measure dimensions,
        // then measure dimension is represented by all the concept scheme
        if (useMeasures) {
            for (ComponentBean component : this.dsd.getComponents()) {
                if (component.getStructureType() != SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION) {
                    this.mapOfComponentBeans.put(getComponentId(component), component);
                    this.mapOfConcepts.put(ConceptRefUtil.getConceptId(component.getConceptRef()), component);
                }
            }
            if (!dim.isEmpty()) {
                CrossReferenceBean ref = dim.get(0).getRepresentation().getRepresentation();
                if (this.beanRetrieval != null) {
                    MaintainableBean maintainable = this.beanRetrieval.getMaintainableBean(ref, false, false);
                    SdmxBeans beans = this.beanRetrieval.getSdmxBeans(maintainable.asReference(), RESOLVE_CROSS_REFERENCES.DO_NOT_RESOLVE);
                    Set<ConceptSchemeBean> concepts = beans.getConceptSchemes(ref);
                    for (ConceptSchemeBean concept : concepts) {
                        for (ConceptBean item : concept.getItems()) {
                            this.mapOfConceptBeans.put(item.getId(), item);
                        }
                    }
                }
            }
        } else {
            for (ComponentBean component : this.dsd.getComponents()) {
                this.mapOfComponentBeans.put(getComponentId(component), component);
                this.mapOfConcepts.put(ConceptRefUtil.getConceptId(component.getConceptRef()), component);
            }
        }

        this.dimensionAtObservation = getDimensionAtObservation();
        this.isTimeSeries = this.dsd.getTimeDimension() != null && dimensionAtObservation.equalsIgnoreCase(this.dsd.getTimeDimension().getId());

        this.exceptionHandler = exceptionHandler;
        this.dataflow = dataflowBean;
        this.listOFConceptsToPositionMapping = returnPositionOfConceptsFromMapping(flrInputConfig.getMapping());
        FixedWidthFields fields = getFixedWidthFields(flrInputConfig);
        this.settings = new FixedWidthParserSettings(fields);
        settings.getFormat().setPadding(flrInputConfig.getPadding().charAt(0));
        settings.getFormat().setLineSeparator("\n");
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setRecordEndsOnNewline(true);
        // If a row has more characters than what is defined, skip them until the end of the line.
        settings.setSkipTrailingCharsUntilNewline(true);
        // SDMXCONV-1359
        settings.getFormat().setComment('\0');
        populateXsMeasureIdToMeasureCode();
        populateMeasureIdToMeasureCode();
    }

    public SdmxBeanRetrievalManager getBeanRetrieval() {
        return beanRetrieval;
    }

    @Override
    public Object2ObjectLinkedOpenHashMap<String, ErrorPosition> getErrorPositions() {
        return errorPositions;
    }

    @Override
    public void setErrorPositions(Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions) {
        this.errorPositions = errorPositions;
    }

    private FixedWidthFields getFixedWidthFields(FLRInputConfig flrInputConfig) {
        FixedWidthFields fields = new FixedWidthFields();
        for (FLRConceptToColumnPositionMapping columnPositionMapping : listOFConceptsToPositionMapping) {
            if (columnPositionMapping.getFixedWidth().getEnd() != columnPositionMapping.getFixedWidth().getStart()) {
                //if start position is different from end position, for example name="FREQ" value="1-3", the length is (3-1+1)
                fields.addField(columnPositionMapping.getConcept(), columnPositionMapping.getFixedWidth().getEnd() - columnPositionMapping.getFixedWidth().getStart() + 1, flrInputConfig.getPadding().charAt(0));
            } else {
                //if start position is the same as end position the length is one for example name="JD_CATEGORY" value="5-5", the length is (5-5+1)
                fields.addField(columnPositionMapping.getConcept(), 1, flrInputConfig.getPadding().charAt(0));
            }
        }
        return fields;
    }

    private String getComponentId(ComponentBean component) {
        String id = null;
        if(flrInputConfig.getStructureSchemaVersion() == SDMX_SCHEMA.VERSION_TWO) {
            if(component != this.dsd.getTimeDimension() && component != this.dsd.getPrimaryMeasure()) {
                id = ConceptRefUtil.getConceptId(component.getConceptRef());
            } else if(component == this.dsd.getTimeDimension()) {
                id = getDataStructure().getTimeDimension().getId();
            } else if(component == this.dsd.getPrimaryMeasure()) {
                id = getDataStructure().getPrimaryMeasure().getId();
            }
        } else {
            id = component.getId();
        }
        return id;
    }

    /**
     * Method to check if the attributes in Dataset are correct
     */
    private void checkDatasetValues(String[] currentRow) {
        //SDMXCONV-915
        if (currentRow != null) {
            getDataSetAttributeFromLineOfFlr(currentRow);
        }
        if (!previousReadDataSetAttributes.equals(getDatasetAttributes())) {
            String notPresent = elementToReport(previousReadDataSetAttributes, getDatasetAttributes());
            previousReadDataSetAttributes = getDatasetAttributes();
            String errorMsg = "Row " + rowNumber + " was found with a different value for dataset level attribute " + notPresent + ". Value should be unique across all observations.";
            final DataValidationError dataValidationError = new DataValidationError.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMsg).errorDisplayable(true).errorPosition(new ErrorPosition(rowNumber)).typeOfException(TypeOfException.SdmxDataFormatException).args(errorMsg).order(++this.order).build();
            addError(ValidationEngineType.READER, dataValidationError);

        }
    }

    /**
     * <strong>Find Attributes to report</strong>
     * <p>Check the list of attributes of the header with the list of attributes on the current line.
     * If this is different we have to report which attributes are the wrong ones.</p>
     *
     * @param attributes        Previous row attributes
     * @param attributesNextRow Current row attributes
     * @return list of attributes that differ as a String
     */
    private String elementToReport(List<KeyValue> attributes, List<KeyValue> attributesNextRow) {
        List<KeyValue> notPresentKv = attributes.stream().filter(element -> !attributesNextRow.contains(element)).collect(Collectors.toList());
        String notPresent = notPresentKv.stream().map(attr -> attr.getConcept()).collect(Collectors.joining(","));
        return notPresent;
    }
    private CodeDataInfo createCodeDataInfo(List<String> values, String componentId, CodeDataInfo.Type type,Map.Entry<String, FlrInColumnMapping> entryConcept) {
        CodeDataInfo codeDataInfo = null;
        if(ObjectUtil.validCollection(values)) {
            if(dsd.isComponentComplex(entryConcept.getKey())) {
                codeDataInfo = createCodeDataInfo(null, columnsPresenceMapping.get(componentId), type);
                for(String value : values) {
                    if(ObjectUtil.validString(value)) {
                        ComplexNodeValue complexNodeValue = new ComplexNodeValueImpl(value, null);
                        codeDataInfo.getComplexNodeValues().add(complexNodeValue);
                    }
                }
            } else {
                String value = values.get(0);
                if(ObjectUtil.validString(value)) {
                    codeDataInfo = createCodeDataInfo(value, columnsPresenceMapping.get(componentId), type);
                }
            }
        }
        return codeDataInfo;
    }

    @Override
    public HeaderBean getHeader() {
        return flrInputConfig.getHeader();
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

    /*
     * remove from the observation queue the first obs and set it as the current observation
     * read the next line, split line, if this line keayble is not the same as current then store it in keyable queue and store obs in observation queue
     * else if the keyable is the same, set obs as current obs
     */
    @Override
    public boolean moveNextObservation() {
        boolean result = false;
        if (!listOfObservation.isEmpty()) {
            currentObservation = listOfObservation.remove();
            result = true;
        } else {
            String[] newRow = readNextLineOfFlrFile();
            checkDatasetValues(newRow);
            //reach the end of file
            if (newRow != null) {
                Keyable newKeyable = getKeyableFromLineOfFlr(newRow);
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
     * <h3>Method that adds the current keyable to the list of keyables and the new
     * observations of this new keyable.</h3>
     *
     * @param newRow  array of strings with the csv row of data values.
     * @param keyable the current keyable
     */
    private void setNewCurrentKeyable(String[] newRow, Keyable keyable) {
        //getGroupFromLineOfFlr(newRow);
        listOfKeyable.add(keyable);
        listOfObservation.addAll(getObservationFromLineOfFlr(newRow, keyable));
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
        listOfObservation.addAll(getObservationFromLineOfFlr(newRow, keyable));
        if (listOfObservation.isEmpty()) {
            resultExists = false;
        } else {
            currentObservation = listOfObservation.remove();
            resultExists = true;
        }
        return resultExists;
    }

    /*
     * If there is a pending keyable we remove it from the queue and set it as current
     * If there isn't a pending keyable we read the next line from flr and again store the keyable
     */
    @Override
    public boolean moveNextKeyable() {
        boolean result = true;

        if (isCurrentRowNull) return false;

        if (!listOfKeyable.isEmpty()) {
            currentKeyable = listOfKeyable.remove();
        } else {
            String[] newRow = readNextLineOfFlrFile();
            checkDatasetValues(newRow);
            Keyable newKeyable = null;
            while (newRow != null && (newKeyable == null || currentKeyable.equals(newKeyable))) {
                getGroupFromLineOfFlr(newRow);
                newKeyable = getKeyableFromLineOfFlr(newRow);
                listOfObservation.addAll(getObservationFromLineOfFlr(newRow, newKeyable));
                newRow = readNextLineOfFlrFile();
                checkDatasetValues(newRow);
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
            DatasetStructureReferenceBean datasetStructureReferenceBean = new DatasetStructureReferenceBeanImpl(null, dsd.asReference(), null, null, dimensionAtObservation, this.useMeasures);
            //SDMXCONV-880
            if (getHeader() != null && getHeader().getAction() != null && getHeader().getAction().getAction() != null) {
                this.currentDatasetHeaderBean = new DatasetHeaderBeanImpl(dsd.getId(), DATASET_ACTION.getAction(getHeader().getAction().getAction()), datasetStructureReferenceBean);
            } else {
                this.currentDatasetHeaderBean = new DatasetHeaderBeanImpl(dsd.getId(), DATASET_ACTION.INFORMATION, datasetStructureReferenceBean);
            }
            if (flrInputConfig.getInputColumnHeader().equals(CsvInputColumnHeader.USE_HEADER)) {
                //if the FLR has a header, then this is the first line being read and we ignore it
                headerColumns = readNextLineOfFlrFile();
                //the next row is the data row
                firstRowOfData = readNextLineOfFlrFile();
            } else {
                if (flrInputConfig.getInputColumnHeader().equals(CsvInputColumnHeader.DISREGARD_HEADER)) {
                    //ignore the first line containing the header columns and read the next line to load the first record
                    headerColumns = readNextLineOfFlrFile();
                }
                //for DISREGARD_HEADER and NO_HEADER this is the first row of data
                firstRowOfData = readNextLineOfFlrFile();
            }

            //SDMXCONV-1185
            final Set<String> columnsSet = flrInputConfig.getMapping().keySet();
            final String[] mappingColumns = Arrays.copyOf(columnsSet.toArray(), columnsSet.size(), String[].class);
            if (ObjectUtil.validArray(mappingColumns)) {
                checkMandatoryColumns(mappingColumns);
            }

            if (firstRowOfData == null) {
                return false;
            } else {
                this.datasetCount = this.datasetCount + 1;
                //gets the attributes for this data set from the complete list of CsvValues mapped to Concepts
                getDataSetAttributeFromLineOfFlr(firstRowOfData);
                previousReadDataSetAttributes = getDatasetAttributes();
                getGroupFromLineOfFlr(firstRowOfData);
                // store the keyable
                Keyable currentRowKeyable = getKeyableFromLineOfFlr(firstRowOfData);
                if (firstRowOfData != null && !ObjectUtil.isAllNulls(firstRowOfData) ) {
                    listOfKeyable.add(currentRowKeyable);
                }
                List<Observation> currentRowObervation = getObservationFromLineOfFlr(firstRowOfData, currentRowKeyable);
                if(ObjectUtil.validObject(currentRowObervation)) {
                    listOfObservation.addAll(currentRowObervation);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * <h3>Method that checks if a mandatory concept is missing from the mapping.</h3>
     * If a mandatory element is missing an error is added in the exception handler.
     * <small>(Be aware this method should be called only once and not in an iteration, for performance reasons.)</small>
     *
     * @param mappingColumns The array of strings that contains the header row columns.
     * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1163">SDMXCONV-1163</a>
     */
    private void checkMandatoryColumns(String[] mappingColumns) {
        if (ObjectUtil.validObject(dsd)) {
            List<AttributeBean> attributes = dsd.getAttributes();
            for (AttributeBean attribute : attributes) {
                CodeDataInfo.Type type = (attribute.getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.OBSERVATION ? CodeDataInfo.Type.OBS_ATTRIBUTE : CodeDataInfo.Type.ATTRIBUTE);
                if (ObjectUtil.validObject(attribute) && attribute.isMandatory() && !ObjectUtil.contains(mappingColumns, attribute.getId())) {
                    //SDMXCONV-1185
                    // If an attribute is missing from headerRow update the columnsPresenceMapping accordingly
                    columnsPresenceMapping.put(attribute.getId(), new CodeDataInfo(null, false, type));
                    // Throw the error only on validation
                    if (exceptionHandler instanceof MultipleFailureHandlerEngine) {
                        String errorMsg = "A mandatory attribute is missing from the header row.";
                        final DataValidationError dataValidationError = new DataValidationError.DataValidationErrorBuilder(ExceptionCode.MISSING_CONCEPT_FROM_HEADER, errorMsg).errorDisplayable(true).errorPosition(new ErrorPosition(1)).typeOfException(TypeOfException.SdmxDataFormatException).args("attribute", attribute.getId()).order(++this.order).build();
                        addError(ValidationEngineType.READER, dataValidationError);
                    }
                } else {
                    columnsPresenceMapping.put(attribute.getId(), new CodeDataInfo(null, true, type));
                }
            }
            List<DimensionBean> dimensions = new ArrayList<>();
            for (DimensionBean dimension : dsd.getDimensions()) {
                if (!dimension.isMeasureDimension() || !flrInputConfig.isMapMeasure()) {
                    dimensions.add(dimension);
                }
            }
            for (DimensionBean dimension : dimensions) {
                if (ObjectUtil.validObject(dimension) && !(ObjectUtil.contains(mappingColumns, dimension.getId()) || ObjectUtil.contains(mappingColumns, ConceptRefUtil.getConceptId(dimension.getConceptRef())))) {
                    // If a dimension is missing from headerRow update the columnsPresenceMapping accordingly
                    columnsPresenceMapping.put(dimension.getId(), new CodeDataInfo(null, false, CodeDataInfo.Type.DIMENSION));
                    // Throw the error only on validation
                    if (exceptionHandler instanceof MultipleFailureHandlerEngine) {
                        String errorMsg = "A dimension is missing from the header row.";
                        final DataValidationError dataValidationError = new DataValidationError.DataValidationErrorBuilder(ExceptionCode.MISSING_CONCEPT_FROM_HEADER, errorMsg).errorDisplayable(true).errorPosition(new ErrorPosition(1)).typeOfException(TypeOfException.SdmxDataFormatException).args("dimension", dimension.getId()).order(++this.order).build();
                        addError(ValidationEngineType.READER, dataValidationError);
                    }
                } else {
                    columnsPresenceMapping.put(dimension.getId(), new CodeDataInfo(null, true, CodeDataInfo.Type.DIMENSION));

                }
            }

            // Add the missing KeyValues to the Keyable
            for (Map.Entry<String, CodeDataInfo> entry : columnsPresenceMapping.entrySet()) {
                if (!entry.getValue().isErrorDisplayable()) {
                    if (entry.getValue().getType() == CodeDataInfo.Type.DIMENSION) {
                        missingDimensions.add(new KeyValueErrorDataImpl(entry.getKey(), new CodeDataInfo(null, entry.getValue().isErrorDisplayable(), CodeDataInfo.Type.DIMENSION)));
                    } else if (entry.getValue().getType() == CodeDataInfo.Type.ATTRIBUTE) {
                        missingAttributes.add(new KeyValueErrorDataImpl(entry.getKey(), new CodeDataInfo(null, entry.getValue().isErrorDisplayable(), CodeDataInfo.Type.ATTRIBUTE)));
                    } else if (entry.getValue().getType() == CodeDataInfo.Type.OBS_ATTRIBUTE) {
                        missingObsAttributes.add(new KeyValueErrorDataImpl(entry.getKey(), new CodeDataInfo(null, entry.getValue().isErrorDisplayable(), CodeDataInfo.Type.OBS_ATTRIBUTE)));
                    } else if (entry.getValue().getType() == CodeDataInfo.Type.MEASURE) {
                        missingMeasures.add(new KeyValueErrorDataImpl(entry.getKey(), new CodeDataInfo(null, entry.getValue().isErrorDisplayable(), CodeDataInfo.Type.MEASURE)));
                    }
                }
            }
        }
    }

    /**
     * reads the next line in the flr file
     *
     * @return the values found on the line
     */
    private String[] readNextLineOfFlrFile() {
        ++rowNumber;
        //SDMXCONV-761 Set the row that is currently being read
        errorPositions.put(DATASET_LEVEL.NONE.name(), new ErrorPosition(this.rowNumber));
        //SDMXCONV-816, SDMXCONV-867
        if (ThreadLocalOutputReporter.getWriteAnnotations().get()) {
            AnnotationMutableBeanImpl annotation = new AnnotationMutableBeanImpl();
            annotation.setTitle("row:" + this.rowNumber);
            annotation.setType(AnnotationType.ANN_OBS_COORDINATES.name());
            obsCoordinatesAnn = new AnnotationBeanImpl(annotation, null);
            //clear the list before adding the annotations for every annotation
            obsAnnotations.clear();
            obsAnnotations.add(obsCoordinatesAnn);
            obsAnnotations.add(inputFormatAnn);
        }
        String[] nextLine = parser.parseNext();
        if (!ObjectUtil.validArray(nextLine)) isCurrentRowNull = true;
        if (!FlatEncodingCheckUtil.isCheckLineEncodingCorrect(nextLine)) {
            String errorMsg = "The encoding of the file is not correct and contains invalid characters in line (" + rowNumber + " ). Please correct the file and try again!";
            final DataValidationError dataValidationError = new DataValidationError.DataValidationErrorBuilder(ExceptionCode.DATA_INVALID_CHARACTER, errorMsg).errorDisplayable(true).errorPosition(new ErrorPosition(rowNumber)).typeOfException(TypeOfException.SdmxDataFormatException).args(errorMsg).order(++this.order).build();
            addError(ValidationEngineType.READER, dataValidationError);
        }
        ByteCountHolder.setByteCount(inputStream.getByteCount());
        return nextLine;
    }

    /**
     * Method that checks if the columnsPresenceMapping.get(component) is null (not present in the DSD) and it then
     * it creates and returns a new CodeDataInfo Object.
     *
     * @param code
     * @param codeDataInfo
     * @param type
     * @return
     */
    private CodeDataInfo createCodeDataInfo(String code, CodeDataInfo codeDataInfo, CodeDataInfo.Type type) {
        CodeDataInfo result = null;
        if (codeDataInfo != null) {
            result = new CodeDataInfo(code, codeDataInfo.isErrorDisplayable(), type);
        } else {
            result = new CodeDataInfo(code, true, type);
        }
        return result;
    }


    /*
     * method that returns the Keyable from the current line of the FLR
     */
    private Keyable getKeyableFromLineOfFlr(String[] row) {
        List<KeyValue> dimensions = new ArrayList<>();
        List<KeyValue> attributes = new ArrayList<>();
        List<String> obsTimes = null;
        for (Map.Entry<String, FlrInColumnMapping> entryConcept : flrInputConfig.getMapping().entrySet()) {
            ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
            if(!ObjectUtil.validObject(componentBean)) {
                componentBean = this.mapOfConcepts.get(entryConcept.getKey());
            }
            String componentId = entryConcept.getKey();
            if(ObjectUtil.validObject(componentBean))
                componentId = getComponentId(componentBean);
            if (componentBean instanceof DimensionBean) {
                if (componentBean.getStructureType() == SDMX_STRUCTURE_TYPE.TIME_DIMENSION) {
                    obsTimes = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                } else {
                    if (!componentBean.getId().equalsIgnoreCase(dimensionAtObservation)) {
                        if (((DimensionBean) componentBean).isMeasureDimension()) {
                            if (useXSMeasures) {
                                throw new IllegalArgumentException("A Measure Dimension was found in the input file, please uncheck 'Map CrossX Measures' or set 'mapCrossXMeasure' to false!");
                            } else {
                                List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                                CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentBean.getId(), CodeDataInfo.Type.DIMENSION, entryConcept);
                                dimensions.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
                            }
                        } else {
                            List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                            CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentBean.getId(), CodeDataInfo.Type.DIMENSION, entryConcept);
                            dimensions.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
                        }

                    }
                }
            } else {
                if (componentBean instanceof AttributeBean) {
                    if (((AttributeBean) componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.DIMENSION_GROUP) {
                        List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                        if (values != null) {
                            CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentBean.getId(), CodeDataInfo.Type.ATTRIBUTE,entryConcept);
                            if(ObjectUtil.validObject(codeDataInfo)) {
                                attributes.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
                            }
                        }
                    }
                }
            }
        }

        //SDMXCONV-1185
        dimensions.addAll(missingDimensions);
        attributes.addAll(missingAttributes);
        Keyable keyable;

        // With XS Measure support, we might have a TimeDimension but the Keyable will need to have a cross-sectional object
        if (isTimeSeries) {
            // Time Series means that the dimension at observation is the Time Dimension and there is a Time Dimension
            if (obsTimes != null) {
                TIME_FORMAT timeFormat = timeFormatBuilder.build(obsTimes.get(0));
                keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, timeFormat);
            } else {
                // What do we do here throw an error ?
                // Because we have time series but the Time Dimension is missing
                keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, (TIME_FORMAT) null);
            }
        } else {
            if (obsTimes != null) {
                TIME_FORMAT timeFormat = timeFormatBuilder.build(obsTimes.get(0));
                keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, timeFormat, dimensionAtObservation, obsTimes.get(0));
            } else {
                keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, null, dimensionAtObservation, "");
            }
        }
        if(ObjectUtil.validObject(keyable)) {
            this.seriesCount = this.seriesCount + 1;
        }
        return keyable;
    }

    private void getGroupFromLineOfFlr(String[] row) {
        List<GroupBean> listOfGroupBeans = dsd.getGroups();
        if (ObjectUtil.validObject(listOfGroupBeans)) {
            errorPositions.put(DATASET_LEVEL.GROUP.name(), new ErrorPosition(this.rowNumber));
        }
        for (GroupBean bean : listOfGroupBeans) {
            List<KeyValue> dimensions = new ArrayList<>();
            List<KeyValue> attributes = new ArrayList<>();
            for (Map.Entry<String, FlrInColumnMapping> entryConcept : flrInputConfig.getMapping().entrySet()) {
                ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
                if(!ObjectUtil.validObject(componentBean)) {
                    componentBean = this.mapOfConcepts.get(entryConcept.getKey());
                }
                String componentId = entryConcept.getKey();
                if(ObjectUtil.validObject(componentBean))
                    componentId = getComponentId(componentBean);
                if (componentBean instanceof DimensionBean && bean.getDimensionRefs().contains(componentId)) {
                    List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                    CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentId, CodeDataInfo.Type.DIMENSION, entryConcept);
                    dimensions.add(new KeyValueErrorDataImpl(componentId, codeDataInfo));
                } else {
                    if (componentBean instanceof AttributeBean) {
                        if (((AttributeBean) componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.GROUP
                                && bean.getId().equals(((AttributeBean) componentBean).getAttachmentGroup())) {
                            List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                            if (ObjectUtil.validCollection(values)) {
                                CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentId, CodeDataInfo.Type.ATTRIBUTE, entryConcept);
                                if (ObjectUtil.validObject(codeDataInfo)) {
                                    attributes.add(new KeyValueErrorDataImpl(componentId, codeDataInfo));
                                }
                            }
                        }
                    }
                }
            }
            //SDMXCONV-1185
            dimensions.addAll(missingDimensions);
            if (ObjectUtil.validObject(attributes) && !attributes.isEmpty()) {
                attributes.addAll(missingAttributes);
                Keyable keyable = new KeyableImpl(dataflow, dsd, dimensions, attributes, bean.getId());
                listOfKeyable.add(keyable);
            }
        }
    }

    /**
     * Method that returns the Data Set Attributes
     */
    private void getDataSetAttributeFromLineOfFlr(String[] row) {
        List<KeyValue> attributes = new ArrayList<>();
        for (Map.Entry<String, FlrInColumnMapping> entryConcept : flrInputConfig.getMapping().entrySet()) {
            ComponentBean componentBean = getDataStructure().getComponent(entryConcept.getKey());
            if (componentBean instanceof AttributeBean) {
                if (((AttributeBean) componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.DATA_SET) {
                    List<String> values = valueFromFlrInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
                    CodeDataInfo codeDataInfo = createCodeDataInfo(values, entryConcept.getKey(), CodeDataInfo.Type.ATTRIBUTE,entryConcept);
                    if(ObjectUtil.validObject(codeDataInfo)) {
                        attributes.add(new KeyValueErrorDataImpl(entryConcept.getKey(), codeDataInfo));
                    }
                }
            }
        }
        this.datasetAttributes = attributes;
    }


    /*
     * method that returns the Observation from the current line of the FLR
     */
    private List<Observation> getObservationFromLineOfFlr(String[] row, Keyable keyable) {
        List<KeyValue> attributes = new ArrayList<>();
        List<String> obsValues = null;
        List<String> obsTimes = null;
        List<KeyValue> xsMeasures = new ArrayList<>();
        List<KeyValue> dimensions = new ArrayList<>();
        List<KeyValue> measures = new ArrayList<>();
        KeyValue crossSectionalValue = null;
        for (Map.Entry<String, FlrInColumnMapping> entryConcept : flrInputConfig.getMapping().entrySet()) {
            if (entryConcept != null && entryConcept.getKey() != null && (this.mapOfComponentBeans.get(entryConcept.getKey()) != null || this.mapOfConcepts.get(entryConcept.getKey()) != null)) {
                ComponentBean componentBean = this.mapOfComponentBeans.get(entryConcept.getKey());
                if(!ObjectUtil.validObject(componentBean)) {
                    componentBean = this.mapOfConcepts.get(entryConcept.getKey());
                }
                String componentId = entryConcept.getKey();
                if(ObjectUtil.validObject(componentBean))
                    componentId = getComponentId(componentBean);
                switch (componentBean.getStructureType()) {
                    case TIME_DIMENSION:
                        obsTimes = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                        break;
                    case PRIMARY_MEASURE:
                        if (!useXSMeasures) {
                            obsValues = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                        } else {
                            logger.error("No Cross Sectional Value found!");
                        }
                        break;
                    case DATA_ATTRIBUTE:
                        if (((AttributeBean) componentBean).getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.OBSERVATION) {
                            List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                            if (ObjectUtil.validCollection(values)) {
                                CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentBean.getId(), CodeDataInfo.Type.OBS_ATTRIBUTE,entryConcept);
                                if(ObjectUtil.validObject(codeDataInfo)) {
                                    attributes.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
                                }
                            }
                        }
                        break;
                    case CROSS_SECTIONAL_MEASURE:
                        if (useXSMeasures) {
                            List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                            CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentBean.getId(), CodeDataInfo.Type.DIMENSION, entryConcept);
                            xsMeasures.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
                        }
                        break;
                    case DIMENSION:
                    case MEASURE_DIMENSION:
                        if (!isTimeSeries && dimensionAtObservation.equalsIgnoreCase(componentBean.getId())) {
                            List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                            CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentBean.getId(), CodeDataInfo.Type.DIMENSION, entryConcept);
                            crossSectionalValue = new KeyValueErrorDataImpl(dimensionAtObservation, codeDataInfo);
                        }
                        break;
                    case MEASURE:
                        List<String> values = valueFromFlrInColumnMapping(componentId, entryConcept.getValue(), row);
                        CodeDataInfo codeDataInfo = createCodeDataInfo(values, componentBean.getId(), CodeDataInfo.Type.MEASURE, entryConcept);
                        measures.add(new KeyValueErrorDataImpl(componentBean.getId(), codeDataInfo));
                        break;
                }
            } else {
                logger.debug("Unknown column " + entryConcept.getKey() + " mapping found either in the column mapping file or in the flr header");
            }
            //SDMXCONV-796
            ConceptBean conceptBean = this.mapOfConceptBeans.get(entryConcept.getKey());
            if (conceptBean != null) {
                List<String> values = valueFromFlrInColumnMapping(entryConcept.getKey(), entryConcept.getValue(), row);
                CodeDataInfo codeDataInfo = createCodeDataInfo(values, conceptBean.getId(), CodeDataInfo.Type.DIMENSION, entryConcept);
                dimensions.add(new KeyValueErrorDataImpl(conceptBean.getId(), codeDataInfo));
            }
        }

        final String normalizedObsTime;
        //this should not be complex
        if (obsTimes != null && obsTimes.get(0) != null) {
            normalizedObsTime = obsTimes.get(0);
        } else {
            normalizedObsTime = "";
        }

        //SDMXCONV-1185
        if (ObjectUtil.validObject(attributes)) {
            attributes.addAll(missingObsAttributes);
        }

        obsAnnotations.clear();
        errorPositions.put(DATASET_LEVEL.OBS.name(), new ErrorPosition(this.rowNumber));
        setObservationAnnotation(ANN_INTERNAL_POSITION, "row:" + this.rowNumber);
        // SDMXCONV-816, SDMXCONV-867
        if (ThreadLocalOutputReporter.getWriteAnnotations().get()) {
            setObservationAnnotation(ANN_INPUT_FORMAT, "FLR");
            setObservationAnnotation(ANN_OBS_COORDINATES, "row:" + this.rowNumber);
        }
        //SDMXCONV-1321 - We check if row is null
        if (useXSMeasures) {
            if(ObjectUtil.validArray(row) && !ObjectUtil.isAllNulls(row)) {
                return builldObservationXsMeasures(keyable, attributes, normalizedObsTime, xsMeasures);
            }
        }
        if (useMeasures) {
            if(ObjectUtil.validArray(row) && !ObjectUtil.isAllNulls(row)) {
                return builldObservationExplicitMeasures(keyable, attributes, normalizedObsTime, dimensions);
            }
        } else {
            if(ObjectUtil.validArray(row) && !ObjectUtil.isAllNulls(row)) {
                this.obsCount = this.obsCount + 1;
                Observation observation = buildObservationNoXsMeasures(row, keyable, attributes, measures, obsValues,
                        normalizedObsTime, crossSectionalValue);
                return Collections.singletonList(observation);
            }
        }
        return null;
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
    private List<Observation> builldObservationXsMeasures(Keyable keyable, List<KeyValue> attributes, String obsTime, List<KeyValue> xsMeasures) {
        List<Observation> xsObservations = new ArrayList<>();
        AnnotationBeanImpl[] annotationsArr = obsAnnotations.toArray(new AnnotationBeanImpl[obsAnnotations.size()]);
        for (KeyValue xsMeasure : xsMeasures) {
            String xsMeasureCode = xsMeasureIdToMeasureCode.get(xsMeasure.getConcept());
            CodeDataInfo codeDataInfo = createCodeDataInfo(xsMeasureCode, columnsPresenceMapping.get(dimensionAtObservation), CodeDataInfo.Type.DIMENSION);
            KeyValue crossSectionalValue = new KeyValueErrorDataImpl(dimensionAtObservation, codeDataInfo);
            String obsValue = xsMeasure.getCode();
            Observation observation;
            observation = new ObservationImpl(keyable, obsTime, obsValue, attributes, crossSectionalValue, annotationsArr);

            this.obsCount = this.obsCount + 1;
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
     * @return
     * @see "https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-796"
     */
    private List<Observation> builldObservationExplicitMeasures(Keyable keyable, List<KeyValue> attributes, String obsTime, List<KeyValue> measures) {
        List<Observation> observations = new ArrayList<>();
        AnnotationBeanImpl[] annotationsArr = observations.toArray(new AnnotationBeanImpl[observations.size()]);
        for (KeyValue measure : measures) {
            String measureCode = measureIdToMeasureCode.get(measure.getConcept());
            final CodeDataInfo codeDataInfo = createCodeDataInfo(measureCode, columnsPresenceMapping.get(dimensionAtObservation), CodeDataInfo.Type.DIMENSION);
            KeyValue crossSectionalValue = new KeyValueErrorDataImpl(dimensionAtObservation, codeDataInfo);
            String obsValue = measure.getCode();
            Observation observation = new ObservationImpl(keyable, obsTime, obsValue, attributes, crossSectionalValue, annotationsArr);
            this.obsCount = this.obsCount + 1;
            observations.add(observation);
        }

        return observations;
    }


    /**
     * Builds an Observation object based on the provided parameters.
     * This method handles different versions of SDMX schema and different types of data structures.
     *
     * @param row The row of data to be processed.
     * @param keyable The keyable object used for building the observation.
     * @param attributes The list of attributes for the observation.
     * @param measures The list of measures for the observation (SDMX 3.0)
     * @param obsValues The observation value.
     * @param obsTime The observation time.
     * @param crossSectionalValue The cross-sectional value for the observation.
     * @return The built Observation object.
     */
    private Observation buildObservationNoXsMeasures(String[] row,
                                                     Keyable keyable,
                                                     List<KeyValue> attributes,
                                                     List<KeyValue> measures,
                                                     List<String> obsValues,
                                                     String obsTime,
                                                     KeyValue crossSectionalValue) {
        // Convert annotations list to array for further processing
        AnnotationBeanImpl[] annotationsArr = obsAnnotations.toArray(new AnnotationBeanImpl[obsAnnotations.size()]);
        Observation observation;
        String obsValue = null;
        if(ObjectUtil.validCollection(obsValues)) {
            obsValue = obsValues.get(0);
        }
        // Check if the data structure has complex components and is compatible with SDMX schema version 3
        if (getDataStructure().hasComplexComponents() && getDataStructure().isCompatible(SDMX_SCHEMA.VERSION_THREE)) {
            obsValueAsMeasure(obsValue, measures);
            observation = this.observationBuilder.Build(keyable, attributes, measures, obsTime, obsValue, crossSectionalValue, annotationsArr);
        }
        // Check if the data structure does not have complex components and the structure schema version is 3
        else if(!getDataStructure().hasComplexComponents() && flrInputConfig.getStructureSchemaVersion() == SDMX_SCHEMA.VERSION_THREE) {
            obsValueAsMeasure(obsValue, measures);
            observation = this.observationBuilder.Build(keyable, attributes, measures, obsTime, obsValue, crossSectionalValue, annotationsArr);
        }
        else {
            // If it's a time series data
            if (isTimeSeries) {
                observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, annotationsArr); // SDMXCONV-1041
            }
            else {
                // If cross-sectional value is not provided, create it from the dimension at observation
                if (crossSectionalValue == null) {
                    String value = "";
                    List<Integer> listOfIndexes = returnListOfIndexesForConcept(dimensionAtObservation);
                    for (Integer index : listOfIndexes) {
                        value += row[index];
                    }
                    CodeDataInfo codeDataInfo = createCodeDataInfo(value, columnsPresenceMapping.get(dimensionAtObservation), CodeDataInfo.Type.DIMENSION);
                    crossSectionalValue = new KeyValueErrorDataImpl(dimensionAtObservation, codeDataInfo);
                }
                observation = this.observationBuilder.Build(keyable, attributes, obsTime, obsValue, crossSectionalValue, annotationsArr); // SDMXCONV-1041
            }
        }
        return observation;
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

    private List<String> valueFromFlrInColumnMapping(String concept, FlrInColumnMapping flrInColumnMapping, String[] row) {
        List<String> values = new ArrayList<>();
        String value = null;
        //the index for each column of the concept
        List<Integer> indexList = returnListOfIndexesForConcept(concept);
        if (flrInColumnMapping.isFixed()) {
            String conceptValue = flrInColumnMapping.getFixedValue();
            if (transcoding.hasTranscodingRules(concept)) {
                value = transcoding.getValueFromTranscoding(concept, conceptValue);
            } else {
                value = flrInColumnMapping.getFixedValue();
            }
            if(ObjectUtil.validString(value)) {
                values.add(value);
            }
        } else {
            try {
                //List of one value means there is only one value for that concept for example FREQ value (e.g MON) is row[0]
                if (indexList.size() == 1) {
                    String conceptValue = row[indexList.get(0)];
                    if (transcoding.hasTranscodingRules(concept)) {
                        value = transcoding.getValueFromTranscoding(concept, conceptValue);
                    } else {
                        value = conceptValue;
                    }
                    if(ObjectUtil.validString(value)) {
                        List<String> subFieldValues = subFieldSeparatorValues(value, concept);
                        if(ObjectUtil.validCollection(subFieldValues)) {
                            values.addAll(subFieldValues);
                        } else {
                            if (transcoding.hasTranscodingRules(concept)) {
                                values.add(transcoding.getValueFromTranscoding(concept, value));
                            } else {
                                if (ObjectUtil.validString(value))
                                    values.add(value);
                            }
                        }
                    }
                } else { //After SDMXCONV-1515 this means the component is Complex/Array
                    for (Integer index : indexList) {
                        if (index < row.length) {
                            String conceptValue = row[index];
                            if (conceptValue != null) {
                                value = conceptValue;
                                if (transcoding.hasTranscodingRules(concept)) {
                                    values.add(transcoding.getValueFromTranscoding(concept, value));
                                } else {
                                    if (ObjectUtil.validString(value))
                                        values.add(value);
                                }
                            }
                        } else {
                            logger.error("a flr column for concept {} found in mapping {} has a higher index than the number of columns in the flr {}", concept, index, row.length);
                        }
                    }
                }
            } catch (ArrayIndexOutOfBoundsException ex) {
                //SDMXCONV-962 SDMXCONV-979
                String errorMsg = " an flr column for concept " + concept + " found in mapping position [" + flrInputConfig.getMapping().get(concept).getPositions().get(0).getStart() + "-" + flrInputConfig.getMapping().get(concept).getPositions().get(0).getEnd() + "] has a higher index than the number of columns in the flr.";
                final DataValidationError dataValidationError = new DataValidationError.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMsg).errorDisplayable(true).errorPosition(new ErrorPosition(rowNumber)).typeOfException(TypeOfException.SdmxDataFormatException).args(errorMsg).order(++this.order).build();
                addError(ValidationEngineType.READER, dataValidationError);
                //This error means that the mapping position is out of range
            }
        }
        return values;
    }

    private List<String> subFieldSeparatorValues(String conceptValue, String concept) {
        List<String> values = new ArrayList<>();
        if(getDataStructure().hasComplexComponents()
                && getDataStructure().isComponentComplex(concept)
                    && ObjectUtil.validString(conceptValue, flrInputConfig.getSubFieldSeparationChar())
                        && conceptValue.contains(flrInputConfig.getSubFieldSeparationChar())) {
            for (String val : conceptValue.split(flrInputConfig.getSubFieldSeparationChar())) {
                if (transcoding.hasTranscodingRules(concept)) {
                    values.add(transcoding.getValueFromTranscoding(concept, val));
                } else {
                    if (ObjectUtil.validString(val))
                        values.add(val);
                }
            }
        }
        return values;
    }

    @Override
    public void reset() {
        //SDMXCONV-816, SDMXCONV-867
        if (ThreadLocalOutputReporter.getWriteAnnotations().get()) {
            AnnotationMutableBeanImpl annotation = new AnnotationMutableBeanImpl();
            annotation.setTitle("FLR");
            annotation.setType(AnnotationType.ANN_INPUT_FORMAT.name());
            inputFormatAnn = new AnnotationBeanImpl(annotation, null);
        }
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
        IOUtils.closeQuietly(inputStream);
    }

    private void initialize() {
        this.datasetAttributes = new ArrayList<>();
        this.currentKeyable = null;
        this.currentObservation = null;
    }

    private void openStreams() {
        parser = new FixedWidthParser(this.settings);
        this.closables.add(inputStream);
        parser.beginParsing(inputStream, "UTF-8");
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
        List<DimensionBean> listOfDimensionBeans = dsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
        if ((useXSMeasures || useMeasures) && !listOfDimensionBeans.isEmpty()) {
            return listOfDimensionBeans.get(0).getId();
        }
        if (dsd.getTimeDimension() != null) {
            return dsd.getTimeDimension().getId();
        } else {
            if (!listOfDimensionBeans.isEmpty()) {
                return listOfDimensionBeans.get(0).getId();
            } else {
                //SDMXCONV-945
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

    private void populateMeasureIdToMeasureCode() {
        List<DimensionBean> dim = dsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
        if (!dim.isEmpty() && beanRetrieval != null) {
            CrossReferenceBean ref = dim.get(0).getRepresentation().getRepresentation();
            MaintainableBean maintainable = beanRetrieval.getMaintainableBean(ref, false, false);
            SdmxBeans beans = beanRetrieval.getSdmxBeans(maintainable.asReference(), RESOLVE_CROSS_REFERENCES.DO_NOT_RESOLVE);
            Set<ConceptSchemeBean> concepts = beans.getConceptSchemes(ref);
            for (ConceptSchemeBean concept : concepts) {
                for (ConceptBean item : concept.getItems()) {
                    this.measureIdToMeasureCode.put(item.getId(), item.getId());
                }
            }
        }
    }
    /**
         <p>The FLRConceptToColumnPositionMapping is an object containing the position of the concepts from the mapping
         for example FREQ = "1-3+4-5" is FixedWidth(1,3) position (column) 0 and FixedWidth(4,5) position (column) 1
         meaning the value for FREQ will be a concatenation between the value in column 0 and column 1
         we also use it to better parse the FLR.
         </p>
     */
    private List<FLRConceptToColumnPositionMapping> returnPositionOfConceptsFromMapping(Map<String, FlrInColumnMapping> mapping) {
        List<FLRConceptToColumnPositionMapping> result = new ArrayList<>();
        int position = 0;
        Map<FixedWidth, String> map = new TreeMap<>();
        if (ObjectUtil.validMap(mapping)) {
            for (FlrInColumnMapping flrInColumnMapping : mapping.values()) {
                if (ObjectUtil.validObject(flrInColumnMapping.getPositions()) && !flrInColumnMapping.getPositions().isEmpty())
                    for (FixedWidth fixedWidth : flrInColumnMapping.getPositions()) {
                        ComponentBean component = this.mapOfComponentBeans.get(fixedWidth.getConceptName());
                        if(!ObjectUtil.validObject(component)) {
                            component = this.mapOfConcepts.get(fixedWidth.getConceptName());
                        }
                        String id = fixedWidth.getConceptName();
                        if(ObjectUtil.validObject(component))
                            id = getComponentId(component);
                        map.put(fixedWidth, id);
                    }
            }
        }
        for (FixedWidth fixedWidth : map.keySet()) {
            FLRConceptToColumnPositionMapping columnPositionMapping = new FLRConceptToColumnPositionMapping();
            ComponentBean component = this.mapOfComponentBeans.get(fixedWidth.getConceptName());
            if(!ObjectUtil.validObject(component)) {
                component = this.mapOfConcepts.get(fixedWidth.getConceptName());
            }
            String id = fixedWidth.getConceptName();
            if(ObjectUtil.validObject(component))
                id = getComponentId(component);
            columnPositionMapping.setConcept(id);
            columnPositionMapping.setFixedWidth(fixedWidth);
            columnPositionMapping.setIndexPosition(position);
            ++position;
            result.add(columnPositionMapping);
        }
        return result;
    }

    /**
         <p>The list of indexes for each Concept
         for example FREQ = "1-3+4-5" is FixedWidth(1,3) position (column) 0 and
         FixedWidth(4,5) position (column) 1
         the List<Integer> result of the method will be [0,1]
         we will need this list to get the positions of the FREQ values (that we will
         concatenate to get the final value) from the row[]
        </p>
     */
    private List<Integer> returnListOfIndexesForConcept(String concept) {
        List<Integer> result = new ArrayList<>();
        for (FLRConceptToColumnPositionMapping columnPositionMapping : listOFConceptsToPositionMapping) {
            if (concept.equals(columnPositionMapping.getConcept()) ) {
                result.add(columnPositionMapping.getIndexPosition());
            }
        }
        return result;
    }



    //Added for struval
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

