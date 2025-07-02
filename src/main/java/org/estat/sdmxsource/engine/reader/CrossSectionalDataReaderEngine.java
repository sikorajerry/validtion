/**
 * Copyright (c) 2015 European Commission.
 * <p>
 * Licensed under the EUPL, Version 1.1 or â€“ as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl5
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package org.estat.sdmxsource.engine.reader;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.estat.sdmxsource.sdmx.api.constants.DATASET_LEVEL;
import org.estat.sdmxsource.util.TimeFormatBuilder;
import org.estat.struval.builder.SdmxDataSetUrnParser;
import org.estat.struval.builder.impl.ObservationBuilder;
import org.estat.struval.builder.impl.SdmxCompactDataUrnParser;
import org.estat.struval.engine.NodeTracker;
import org.estat.struval.engine.PositionTracker;
import org.estat.struval.engine.impl.IgnoreErrorsExceptionHandler;
import org.estat.struval.engine.impl.NodeChildTracker;
import org.estat.struval.engine.impl.NodeTrackerComposite;
import org.estat.struval.engine.impl.SdmxHeaderReaderEngine;
import org.sdmxsource.sdmx.api.constants.DATA_TYPE;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.constants.TIME_FORMAT;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.*;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.beans.registry.ProvisionAgreementBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.AbstractDataReaderEngine;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyableImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetHeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.util.ObjectUtil;

import javax.xml.stream.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * Data reader of data in Cross Sectional format. The Reader is expected to be
 * used by calling first moveNextDataSet(), then moveNextKeyable() ,
 * getNextKey(), then for each Keyable moveNextObservation(),
 * getNextObservation()
 *
 * e.g. DataReaderEngine reader = new
 * CrossSectionalDataReaderEngine(datalocation, dsd, dataflow);
 * reader.moveNextDataSet(); while (reader.moveNextKeyable()) { Keyable key =
 * getNextKey(); //use key ... while (reader.moveNextObservation()) {
 * Observation obs = reader.getNextObservation(); //use observation ... } }
 *
 * @author Mihaela Munteanu
 *
 */
public class CrossSectionalDataReaderEngine extends AbstractDataReaderEngine implements DataValidationErrorHolder, PositionTracker {

	/**
	 * default serial
	 */
	private static final long serialVersionUID = 1L;

	private final SdmxDataSetUrnParser sdmxCompactDataUrnParser = new SdmxCompactDataUrnParser();
	private final HashMap<String, String> conceptToComponentId = new HashMap<String, String>();
	/** The observation builder. */
	private final ObservationBuilder observationBuilder = new ObservationBuilder();
	/** The time format builder. */
	private final TimeFormatBuilder timeFormatBuilder = new TimeFormatBuilder();
	/** The Readable source to be parsed */
	protected ReadableDataLocation dataLocation;
	/** The given Dataflow */
	protected DataflowBean dataflow;
	/** Flag to see if the Observation is built when Keyable is processsed */
	boolean observationSetFromKeyable = false;
	/** Input Stream used for getting the current Items */
	private InputStream inputStream;
	/** Stream Reader used for getting the current Items */
	private XMLStreamReader parser;
	/**
	 * Map with DataSet Attributes e.g. ("TAB_NUM","RQFI05V1"), ("REV_NUM", "1")
	 */
	private final Map<String, String> dataSetAttributes = new HashMap<String, String>();
	/** Map with DataSet concepts and values if there are any */
	private final Map<String, String> dataSetConcepts = new HashMap<String, String>();
	/** Map with Group Attributes if there are any */
	private final Map<String, String> groupAttributes = new HashMap<String, String>();
	/**
	 * Map with Group concepts and values e.g. ("FREQ","A"), ("COUNTRY","DK")
	 */
	private final Map<String, String> groupConcepts = new HashMap<String, String>();
	/**
	 * Map with Section Attributes e.g. (UNIT_MULT,"0"), (DECI,"0"),
	 * (UNIT,"PERS")
	 */
	private final Map<String, String> sectionAttributes = new HashMap<String, String>();
	/**
	 * Map with Section concepts and values e.g. ("FREQ","A"), ("COUNTRY","DK")
	 */
	private final Map<String, String> sectionConcepts = new HashMap<String, String>();
	/** Map with Observation Attributes e.g. ("OBS_STATUS","A") */
	private final Map<String, String> observationAttributes = new HashMap<String, String>();
	/**
	 * Map with Observation concepts and values e.g. ("SEX", "F"), ("CAS", "003"
	 * )
	 */
	private final Map<String, String> observationConcepts = new HashMap<String, String>();
	/**
	 * List with available dimesions from the dsd. If an attribute from item tag
	 * is not in this list it is considered an attribute of the artefact.
	 */
	private final List<String> availableDimensions = new ArrayList<String>();
	/** The measure dimensions concepts available from DSD */
	private final List<String> availableCrossSectionalMeasures = new ArrayList<String>();
	/** The value of observation as it is read from parser */
	private String observationValue;
	/**
	 * If there are Cross Sectional measures then observation tag looks like
	 * this: <PJANT value="34444" SEX="F"/>
	 */
	private String crossSectionalObservation;
	/** the current observation time */
	private String obsTime;
	private LEVEL currentLevel = LEVEL.NONE;
	private ExceptionHandler exceptionHandler;
	private NodeTracker nodeTextTracker;
	private Set<String> normalizedSeriesAttributes;
	private Set<String> normalizedDatasetAttributes;
	private Set<String> normalizedObsAttributes;
	private Set<String> normalizedGroupAttributes;
	private String crossSectionalComponentId;
	private String primaryMeasureConceptId;
	private final Deque<Keyable> groupQueue = new LinkedList<Keyable>();
	/** The XS dataset components. */
	private Set<String> datasetComponents;

	/** The XS section components. */
	private Set<String> sectionComponents;

	/** The XS group components. */
	private Set<String> groupComponents;

	/** The XS observation components. */
	private Set<String> obsComponents;

	/** Observation built before moveNextObservation is called */
	private Observation previousObs;

	private Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
	private LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();
	private Integer maximumErrorOccurrencesNumber;
	private int order;
	private int errorLimit;

	/**
	 * Creates a reader engine based on the data location, and the data
	 * structure to use to interpret the data
	 *
	 * @param dataLocation
	 *            the location of the data
	 * @param dataStructureBean
	 *            the dsd to use to interpret the data
	 */
	public CrossSectionalDataReaderEngine(ReadableDataLocation dataLocation,
										  CrossSectionalDataStructureBean dataStructureBean, DataflowBean dataflowBean) {
		this(dataLocation, null, dataStructureBean, dataflowBean);
	}

	/**
	 * Creates a reader engine based on the data location, the location of
	 * available data structures that can be used to retrieve dsds, and the
	 * default dsd to use
	 *
	 * @param dataLocation
	 *            the location of the data
	 * @param beanRetrieval
	 *            giving the ability to retrieve dsds for the datasets this
	 *            reader engine is reading. This can be null if there is only
	 *            one relevent dsd - in which case the default dsd should be
	 *            provided
	 * @param dataStructureBean
	 *            the dsd to use if the beanRetrieval is null, or if the bean
	 *            retrieval does not return the dsd for the given dataset
	 */
	public CrossSectionalDataReaderEngine(ReadableDataLocation dataLocation,
										  SdmxBeanRetrievalManager beanRetrieval, CrossSectionalDataStructureBean dataStructureBean,
										  DataflowBean dataflowBean) {
		super(dataLocation, beanRetrieval, dataStructureBean, dataflowBean);
		this.dataLocation = dataLocation;
		this.beanRetrieval = beanRetrieval;
		this.defaultDsd = dataStructureBean;
		this.dataflow = dataflowBean;

		resetInternal(dataLocation);

		if (beanRetrieval == null && defaultDsd == null) {
			throw new IllegalArgumentException(
					"AbstractDataReaderEngine expects either a SdmxBeanRetrievalManager or a DataStructureBean to be able to interpret the structures");
		}
	}

	@Override
	public Object2ObjectLinkedOpenHashMap<String, ErrorPosition> getErrorPositions() {
		return errorPositions;
	}

	@Override
	public void setErrorPositions(Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions) {
		this.errorPositions = errorPositions;
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
		return order;
	}

	@Override
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public int getErrorLimit() {
		return errorLimit;
	}

	@Override
	public void setErrorLimit(int errorLimit) {
		this.errorLimit = errorLimit;
	}

	/**
	 * Determine current dsd.
	 */
	private void determineCurrentDsd() {
		this.currentDsd = null;
		this.currentDataflow = null;
		this.datasetHeaderBean = new DatasetHeaderBeanImpl(this.parser, this.headerBean);
		DatasetStructureReferenceBean datasetStructureReference = datasetHeaderBean.getDataStructureReference();
		if (datasetStructureReference == null && this.headerBean != null
				&& this.headerBean.getStructures().size() == 1) {
			datasetStructureReference = this.headerBean.getStructures().get(0);
		}

		if (datasetStructureReference != null) {
			parseDataStructureRef(datasetStructureReference);

		} else if (this.defaultDsd != null) {
			this.setCurrentDsd((CrossSectionalDataStructureBean) defaultDsd);
		} else if (this.sdmxCompactDataUrnParser != null) {
			// Last hope try to get the DSD from the URN.
			String namespaceUri = this.parser.getNamespaceURI();
			StructureReferenceBean strRef = this.sdmxCompactDataUrnParser.buildStructureReference(namespaceUri);
			if (strRef != null) {
				datasetStructureReference = new DatasetStructureReferenceBeanImpl(strRef);
				this.datasetHeaderBean = this.datasetHeaderBean.modifyDataStructureReference(datasetStructureReference);
				parseDataStructureRef(datasetStructureReference);
			}
		}

		if (this.currentDsd == null) {
			throw new SdmxSemmanticException(
					"Can not read dataset, the data set does no reference any data structures, and there was no default data structure definition provided");
		}
	}

	/**
	 * Parses the data structure ref.
	 *
	 * @param datasetStructureReference
	 *            the dataset structure reference
	 */
	private void parseDataStructureRef(DatasetStructureReferenceBean datasetStructureReference) {
		switch (datasetStructureReference.getStructureReference().getMaintainableStructureType()) {
			case DSD:
				if (this.defaultDsd != null && datasetStructureReference.getStructureReference().isMatch(this.defaultDsd)) {
					this.setCurrentDsd((CrossSectionalDataStructureBean) defaultDsd);
				} else if (this.beanRetrieval != null) {
					this.setCurrentDsd(datasetStructureReference.getStructureReference());
				}

				break;
			case DATAFLOW:
				if (this.beanRetrieval != null) {
					this.currentDataflow = this.beanRetrieval.getMaintainableBean(DataflowBean.class,
							datasetStructureReference.getStructureReference());
					if (currentDataflow == null) {
						throw new SdmxNoResultsException("Could not read dataset, the data set references dataflow '"
								+ datasetStructureReference.getStructureReference().getMaintainableUrn()
								+ "' which could not be resolved");
					}
				} else {
					this.setCurrentDsd((CrossSectionalDataStructureBean) defaultDsd);
					this.currentDataflow = this.dataflow;
				}

				break;
			default:
				throw new SdmxNotImplementedException("Can not read dataset for structure of type: "
						+ datasetStructureReference.getStructureReference());
		}

		if (this.currentDsd == null) {
			throw new SdmxNoResultsException("Could not read dataset, the data set references data structure (DSD) '"
					+ datasetStructureReference.getStructureReference().toString() + "' which could not be resolved");
		}
	}

	protected void setCurrentDsd(MaintainableRefBean dsdRef) {
		DataStructureBean dsd = this.beanRetrieval.getMaintainableBean(DataStructureBean.class, dsdRef);
		if (dsd == null) {
			throw new SdmxNoResultsException("Could not read dataset, the data set references data structure (DSD) '"
					+ dsdRef.toString() + "' which could not be resolved");
		}

		if (dsd instanceof CrossSectionalDataStructureBean) {
			this.setCurrentDsd((CrossSectionalDataStructureBean) dsd);
		} else {
			throw new SdmxSemmanticException("Provided DSD not SDMX v2.0 Cross Sectional compatible");
		}
	}

	private void setCurrentDsd(CrossSectionalDataStructureBean dsd) {
		if (dsd == null) {
			throw new NullPointerException("dsd");
		}

		this.currentDsd = dsd;
		this.conceptToComponentId.clear();
		for (ComponentBean component : currentDsd.getComponents()) {
			this.conceptToComponentId.put(component.getConceptRef().getFullId(), component.getId());
		}

		availableDimensions.clear();
		List<DimensionBean> dimensionBeans = dsd.getDimensions(SDMX_STRUCTURE_TYPE.DIMENSION);
		for (DimensionBean dimensionBean : dimensionBeans) {
			availableDimensions.add(dimensionBean.getId());
		}

		List<CrossSectionalMeasureBean> crossSectionalMeasuresDimensionBeans = dsd.getCrossSectionalMeasures();
		for (CrossSectionalMeasureBean measureBean : crossSectionalMeasuresDimensionBeans) {
			availableCrossSectionalMeasures.add(measureBean.getId());
		}

		datasetComponents = new HashSet<String>();
		for (ComponentBean component : ((CrossSectionalDataStructureBean) this.currentDsd).getCrossSectionalAttachDataSet(true)) {
			datasetComponents.add(component.getId());
		}

		sectionComponents = new HashSet<String>();
		for (ComponentBean component : ((CrossSectionalDataStructureBean) this.currentDsd).getCrossSectionalAttachSection(true)) {
			sectionComponents.add(component.getId());
		}
		groupComponents = new HashSet<String>();
		for (ComponentBean component : ((CrossSectionalDataStructureBean) this.currentDsd).getCrossSectionalAttachGroup(true)) {
			groupComponents.add(component.getId());
		}
		obsComponents = new HashSet<String>();
		for (ComponentBean component : ((CrossSectionalDataStructureBean) this.currentDsd).getCrossSectionalAttachObservation()) {
			obsComponents.add(component.getId());
		}


		primaryMeasureConceptId = dsd.getPrimaryMeasure().getConceptRef().getFullId();
	}

	/**
	 * @param dataLocation
	 * @throws FactoryConfigurationError
	 */
	private void resetInternal(ReadableDataLocation dataLocation) throws FactoryConfigurationError {
		resetDataSet();
		XMLInputFactory factory = XMLInputFactory.newInstance();
		try {
			inputStream = dataLocation.getInputStream();
			parser = factory.createXMLStreamReader(inputStream, "UTF-8");

			nodeTextTracker = new NodeTrackerComposite(new NodeChildTracker(parser, DATA_TYPE.CROSS_SECTIONAL_2_0));
			if (exceptionHandler == null) {
				exceptionHandler = new IgnoreErrorsExceptionHandler();
			}

			this.headerBean = SdmxHeaderReaderEngine.processHeader(parser, nodeTextTracker, defaultDsd,
					exceptionHandler);
		} catch (XMLStreamException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean moveNextDatasetInternal() {
		do {
			if (this.currentLevel == LEVEL.DATASET) {
				processDataSet();
				this.currentLevel = LEVEL.NONE;
				return true;
			}
		} while (next(LEVEL.DATASET));

		return false;
	}

	/**
	 * Resets all fields loaded from the previous <code>Keyable</code>.
	 * Navigates the xml until an Observation is found and true is returned.
	 * Otherwise return false.
	 */
	@Override
	public final boolean moveNextKeyableInternal() {

		resetObservationFields();
		if (!groupQueue.isEmpty()) {
			this.currentKey = groupQueue.pop();
			return true;
		}

		do {
			switch (currentLevel) {
				case SECTION:
					sectionConcepts.clear();
					sectionAttributes.clear();
					processComponents(sectionConcepts, sectionAttributes, LEVEL.SECTION);
					break;
				case GROUP:
					groupConcepts.clear();
					groupAttributes.clear();
					processComponents(groupConcepts, groupAttributes, LEVEL.GROUP);
					break;
				case KEY:
					final String nodeName = parser.getLocalName();
					proccessObservation(nodeName);

					buildCurrentKey();
					buildCurrentObservation();
					// the first observation is built
					observationSetFromKeyable = true;
					currentLevel = LEVEL.NONE;
					return true;
				default:
					// default do nothing.
					break;

			}

		} while (next(LEVEL.KEY));

		return false;
	}

	/**
	 * Process data set.
	 */
	private void processDataSet() {
		resetDataSet();
		errorPositions.put(DATASET_LEVEL.DATASET.toString(), trackPosition(parser.getLocation()));
		this.determineCurrentDsd();
		processComponents(dataSetConcepts, dataSetAttributes, LEVEL.DATASET);

		List<DimensionBean> measureDims = currentDsd.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
		if (!measureDims.isEmpty()) {
			final DimensionBean measureDim = measureDims.get(0);
			crossSectionalComponentId = measureDim.getId();
		} else {
			crossSectionalComponentId = currentDsd.getDimensions(SDMX_STRUCTURE_TYPE.DIMENSION).iterator().next().getId();
		}

		final DatasetStructureReferenceBean currentReference = this.datasetHeaderBean.getDataStructureReference();
		DatasetStructureReferenceBean newDataStructureReference = null;

		if (currentReference != null) {
			newDataStructureReference = new DatasetStructureReferenceBeanImpl(
					currentReference.getId(), currentReference.getStructureReference(),
					currentReference.getServiceURL(), currentReference.getStructureURL(), crossSectionalComponentId);
		} else {
			newDataStructureReference = new DatasetStructureReferenceBeanImpl(
					currentDsd.getId(), currentDsd.asReference(),
					null, null, crossSectionalComponentId);
		}
		this.datasetHeaderBean = this.datasetHeaderBean.modifyDataStructureReference(newDataStructureReference);

		normalizedSeriesAttributes = new HashSet<String>();
		for (AttributeBean attribute : currentDsd.getSeriesAttributes(crossSectionalComponentId)) {
			normalizedSeriesAttributes.add(attribute.getId());
		}
		normalizedDatasetAttributes = new HashSet<String>();
		for (AttributeBean attribute : currentDsd.getDatasetAttributes()) {
			normalizedDatasetAttributes.add(attribute.getId());
		}

		normalizedObsAttributes = new HashSet<String>();
		for (AttributeBean attribute : currentDsd.getObservationAttributes(crossSectionalComponentId)) {
			normalizedObsAttributes.add(attribute.getId());
		}

		normalizedGroupAttributes = new HashSet<String>();
		for (AttributeBean attribute : currentDsd.getGroupAttributes()) {
			normalizedGroupAttributes.add(attribute.getId());
		}
	}

	/**
	 * Reset data set.
	 */
	private void resetDataSet() {
		groupQueue.clear();
		dataSetConcepts.clear();
		dataSetAttributes.clear();
		resetSeriesFields();
		resetObservationFields();
	}

	@Override
	public boolean moveNextObservationInternal() {
		if (observationSetFromKeyable) {
			observationSetFromKeyable = false;
			return true;
		}
		if (crossSectionalObservation == null) {
			return false; // the Observation has OBS_VALUE as tag which means
			// that there is a keyable for each oservation
		}

		do {
			switch (currentLevel) {
				case OBS:
					final String nodeName = parser.getLocalName();
					proccessObservation(nodeName);

					buildCurrentObservation();
					currentLevel = LEVEL.NONE;
					return true;
				default:
					break;
			}
		} while (next(LEVEL.OBS));

		// TODO Check why this is done here and why we return false
		// afterwards
		buildCurrentObservation();
		return false;
	}

	/**
	 * @param nodeName
	 */
	private void proccessObservation(String nodeName) {
		int observationAttributesNumber = parser.getAttributeCount();
		for (int i = 0; i < observationAttributesNumber; i++) {
			String attributeLocalName = parser.getAttributeLocalName(i);
			String attribName = getComponentId(attributeLocalName);
			String attribValue = parser.getAttributeValue(i);
			errorPositions.put(getDatasetLevelFor(attribName).name(), trackPosition(parser.getLocation()));
			if (!attribName.equals("value")) {
				if (availableDimensions.contains(attribName)) {
					observationConcepts.put(attribName, attribValue);
				} else if (DimensionBean.TIME_DIMENSION_FIXED_ID.equals(attribName)) {
					this.obsTime = attribValue;
				} else { // TODO check for observation level attributes
					observationAttributes.put(attribName, attribValue);
				}
			} else {// the value should be added as obs value
				observationValue = attribValue;
			}
		}

		if (!nodeName.equals(primaryMeasureConceptId)) {
			observationConcepts.put(crossSectionalComponentId, nodeName);
			crossSectionalObservation = nodeName;
		}
	}

	/**
	 * Checks if is measure node.
	 *
	 * @param nodeName
	 *            the node name
	 * @return true, if is measure node
	 */
	private boolean isMeasureNode(String nodeName) {
		return nodeName.equals(primaryMeasureConceptId) || availableCrossSectionalMeasures.contains(nodeName);
	}

	/**
	 * @param setConcepts
	 * @param attributeMap
	 * @param level
	 */
	private void processComponents(final Map<String, String> setConcepts, final Map<String, String> attributeMap, LEVEL level) {
		int dataSetAttributesNumber = parser.getAttributeCount();
		for (int i = 0; i < dataSetAttributesNumber; i++) {
			String attributeLocalName = parser.getAttributeLocalName(i);
			String attribName = getComponentId(attributeLocalName);
			String attribValue = parser.getAttributeValue(i);
			errorPositions.put(getDatasetLevelFor(attribName).name(), trackPosition(parser.getLocation()));
			if (availableDimensions.contains(attribName)) {
				setConcepts.put(attribName, attribValue);
			} else if (DimensionBean.TIME_DIMENSION_FIXED_ID.equals(attribName)) {
				this.obsTime = attribValue;
			} else {
				attributeMap.put(attribName, attribValue);
			}
		}
	}

	/**
	 * Gets the component id.
	 *
	 * @param componentId
	 *            the component id
	 * @return the component id
	 */
	private String getComponentId(String componentId) {
		String actualComponentId = this.conceptToComponentId.get(componentId);
		if (actualComponentId != null) {
			return actualComponentId;
		}

		return componentId;
	}

	/**
	 * Reset series fields.
	 */
	private void resetSeriesFields() {
		this.currentKey = null;
		this.obsTime = null;
		this.sectionAttributes.clear();
		this.sectionConcepts.clear();
		this.groupAttributes.clear();
		this.groupConcepts.clear();
	}

	/**
	 * Reset observation fields.
	 */
	private void resetObservationFields() {
		observationValue = null;
		crossSectionalObservation = null;
		observationConcepts.clear();
		observationAttributes.clear();
	}

	/**
	 * When the parser gets to a point where a Keyable object can be built this
	 * method is called.
	 *
	 */
	private void buildCurrentKey() {
		Set<KeyValue> conceptKeys = new HashSet<KeyValue>();
		for (String key : dataSetConcepts.keySet()) {
			KeyValue keyValue = new KeyValueImpl(dataSetConcepts.get(key), key);
			conceptKeys.add(keyValue);
		}

		for (String key : groupConcepts.keySet()) {
			KeyValue keyValue = new KeyValueImpl(groupConcepts.get(key), key);
			conceptKeys.add(keyValue);
		}

		for (String key : sectionConcepts.keySet()) {
			KeyValue keyValue = new KeyValueImpl(sectionConcepts.get(key), key);
			conceptKeys.add(keyValue);
		}

		for (String key : observationConcepts.keySet()) {
			if (!key.equals(crossSectionalComponentId)) {
				KeyValue keyValue = new KeyValueImpl(observationConcepts.get(key), key);
				conceptKeys.add(keyValue);
			}
		}

		List<KeyValue> attributeKeys = buildXsAttributes();
		List<KeyValue> normalizedAttributeKeys = new ArrayList<KeyValue>();
		HashMap<String, List<KeyValue>> normalizedGroupAttributeMap = new HashMap<String, List<KeyValue>>();

		for (KeyValue keyValue : attributeKeys) {
			if (normalizedSeriesAttributes.contains(keyValue.getConcept())) {
				normalizedAttributeKeys.add(keyValue);
			} else if (normalizedGroupAttributes.contains(keyValue.getConcept())) {
				AttributeBean groupAttribute = this.currentDsd.getGroupAttribute(keyValue.getConcept());
				if (groupAttribute != null) {
					final String groupId = groupAttribute.getAttachmentGroup();

					List<KeyValue> groupKeys = normalizedGroupAttributeMap.get(groupId);
					if (groupKeys == null) {
						groupKeys = new ArrayList<KeyValue>();
						normalizedGroupAttributeMap.put(groupId, groupKeys);
					}
					groupKeys.add(keyValue);
				}
			}
		}

		// TODO handle normalizedGroupAttributeMap
		for (Map.Entry<String, List<KeyValue>> entry : normalizedGroupAttributeMap.entrySet()) {
			// check if we have any attributes to write.
			if (entry.getValue().isEmpty()) {
				continue;
			}

			GroupBean group = this.currentDsd.getGroup(entry.getKey());
			Set<String> groupDimensions = new HashSet<String>(group.getDimensionRefs());
			List<KeyValue> groupDimensionValues = new ArrayList<KeyValue>();
			for (KeyValue dimensionValue : conceptKeys) {
				if (groupDimensions.contains(dimensionValue.getConcept())) {
					groupDimensionValues.add(dimensionValue);
				}
			}

			this.groupQueue.push(new KeyableImpl(currentDataflow, currentDsd, groupDimensionValues, entry.getValue(),
					entry.getKey()));
		}

		if (this.currentDsd.getTimeDimension() != null && ObjectUtil.validString(this.obsTime)) {
			TIME_FORMAT timeFormat = this.timeFormatBuilder.build(this.obsTime);

			this.currentKey = new KeyableImpl(currentDataflow, currentDsd, new ArrayList<KeyValue>(conceptKeys), normalizedAttributeKeys,
					timeFormat, this.crossSectionalComponentId, obsTime);
		} else {
			this.currentKey = new KeyableImpl(currentDataflow, currentDsd, new ArrayList<KeyValue>(conceptKeys), normalizedAttributeKeys, null,
					this.crossSectionalComponentId, "");
		}
	}

	/**
	 * Builds the xs attributes.
	 *
	 * @return the list
	 */
	private List<KeyValue> buildXsAttributes() {
		List<KeyValue> attributeKeys = new ArrayList<KeyValue>();
		for (String key : dataSetAttributes.keySet()) {
			KeyValue keyValue = new KeyValueImpl(dataSetAttributes.get(key), key);
			attributeKeys.add(keyValue);
		}

		for (String key : groupAttributes.keySet()) {
			KeyValue keyValue = new KeyValueImpl(groupAttributes.get(key), key);
			attributeKeys.add(keyValue);
		}

		for (String key : sectionAttributes.keySet()) {
			KeyValue keyValue = new KeyValueImpl(sectionAttributes.get(key), key);
			attributeKeys.add(keyValue);
		}
		for (String key : observationAttributes.keySet()) {
			KeyValue keyValue = new KeyValueImpl(observationAttributes.get(key), key);
			attributeKeys.add(keyValue);
		}

		return attributeKeys;
	}

	/**
	 * Builds the current observation.
	 */
	private void buildCurrentObservation() {
		List<KeyValue> attributeKeys = buildXsAttributes();

		List<KeyValue> normalizedAttributeKeys = new ArrayList<KeyValue>();
		for (KeyValue keyValue : attributeKeys) {
			if (normalizedObsAttributes.contains(keyValue.getConcept())) {
				normalizedAttributeKeys.add(keyValue);
			}
		}

		KeyValue crossSectionValue = null;
		if (crossSectionalObservation != null) {
			crossSectionValue = new KeyValueImpl(crossSectionalObservation, this.crossSectionalComponentId);
		} else {
			final String crossValue = observationConcepts.get(this.crossSectionalComponentId);
			crossSectionValue = new KeyValueImpl(crossValue, this.crossSectionalComponentId);

		}
		previousObs = this.observationBuilder.Build(currentKey, normalizedAttributeKeys, this.currentKey.getObsTime(), observationValue, crossSectionValue);
	}

	@Override
	public void close() {
		closeStreams();
		if (dataLocation != null) {
			dataLocation.close();
			dataLocation = null;
		}
	}

	private void closeStreams() {
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (Exception e) {
				throw new RuntimeException("Error trying to close parser InputStream : " + e);
			}
		}

		if (parser != null) {
			try {
				parser.close();
			} catch (XMLStreamException e) {
				throw new RuntimeException("Error trying to close runAheadParser : " + e);
			}
		}
	}

	@Override
	public HeaderBean getHeader() {
		return headerBean;
	}

	@Override
	public DataflowBean getDataFlow() {
		return dataflow;
	}

	@Override
	public DataStructureBean getDataStructure() {
		return currentDsd;
	}

	@Override
	public int getKeyablePosition() {
		throw new SdmxException("Unsupported Method - getKeyablePosition");
	}

	@Override
	public int getObsPosition() {
		throw new SdmxException("Unsupported Method - getObsPosition");
	}

	@Override
	public DatasetHeaderBean getCurrentDatasetHeaderBean() {
		return this.datasetHeaderBean;
	}

	@Override
	public void reset() {
		resetInternal(this.dataLocation);
	}

	@Override
	public void copyToOutputStream(OutputStream outputStream) {
		throw new SdmxException("Unsupported Method - copyToOutputStream");
	}

	@Override
	public DataReaderEngine createCopy() {
		throw new SdmxException("Unsupported Method - createCopy");
	}

	@Override
	public List<KeyValue> getDatasetAttributes() {
		List<KeyValue> attributes = new ArrayList<KeyValue>();
		for (final Map.Entry<String, String> pair : this.dataSetAttributes.entrySet()) {
			if (normalizedDatasetAttributes.contains(pair.getKey())) {
				attributes.add(new KeyValueImpl(pair.getValue(), pair.getKey()));
			}
		}
		return attributes;
	}

	@Override
	public ProvisionAgreementBean getProvisionAgreement() {
		return null;
	}

	private boolean next(LEVEL requestedLevel) {

		String nodeName;
		currentLevel = LEVEL.NONE;
		try {
			while (parser.hasNext()) {
				int event = parser.next();
				if (event == XMLStreamConstants.START_ELEMENT) {
					nodeName = parser.getLocalName();
					if (nodeName.equals("DataSet")) {
						errorPositions.put(DATASET_LEVEL.DATASET.name(), trackPosition(parser.getLocation()));
						this.currentLevel = LEVEL.DATASET;
						return requestedLevel == LEVEL.DATASET;
					} else if (nodeName.equals("Group")) {
						errorPositions.put(DATASET_LEVEL.GROUP.name(), trackPosition(parser.getLocation()));
						this.currentLevel = LEVEL.GROUP;
						switch (requestedLevel) {
							case KEY:
								return true;
							case OBS:
								return false;
							default:
								// do nothing
								break;
						}
					} else if (nodeName.equals("Section")) {
						errorPositions.put(DATASET_LEVEL.XS_SECTION.name(), trackPosition(parser.getLocation()));
						this.currentLevel = LEVEL.SECTION;
						switch (requestedLevel) {
							case KEY:
								return true;
							case OBS:
								return false;
							default:
								// do nothing
								break;
						}

					} else if (isMeasureNode(nodeName)) {
						if (requestedLevel == LEVEL.KEY) {
							errorPositions.put(DATASET_LEVEL.SERIES.name(), trackPosition(parser.getLocation()));
							this.currentLevel = LEVEL.KEY;
							return true;
						} else if (requestedLevel == LEVEL.OBS) {
							errorPositions.put(DATASET_LEVEL.OBS.name(), trackPosition(parser.getLocation()));
							this.currentLevel = LEVEL.OBS;
							return true;
						}
					}
				} else if (event == XMLStreamConstants.END_ELEMENT) {
					nodeName = parser.getLocalName();

					this.currentLevel = LEVEL.NONE;
					if (nodeName.equals("Section") || nodeName.equals("Group")) {
						if (requestedLevel == LEVEL.OBS) {
							return false;
						}
					} else if (nodeName.equals("DataSet")) {
						if (requestedLevel != LEVEL.DATASET) {
							return false;
						}
					} else if (nodeName.equals("CrossSectionalData")) {
						return false;
					}
				}
			}
		} catch (XMLStreamException e) {
			throw new SdmxException(e, "Error while trying to get Next Keyable from data source");
		}

		return true;

	}

	@Override
	protected Observation lazyLoadObservation() {
		if (previousObs != null) {
			return previousObs;
		}
		return null;
	}

	@Override
	protected Keyable lazyLoadKey() {
		// TODO Auto-generated method stub
		return null;
	}

	private DATASET_LEVEL getDatasetLevelFor(String componentId) {
		if (this.obsComponents.contains(componentId)) {
			return DATASET_LEVEL.OBS;
		}

		if (this.sectionComponents.contains(componentId)) {
			return DATASET_LEVEL.XS_SECTION;
		}

		if (this.groupComponents.contains(componentId)) {
			return DATASET_LEVEL.GROUP;
		}

		if (this.datasetComponents.contains(componentId)) {
			return DATASET_LEVEL.DATASET;
		}

		return DATASET_LEVEL.NONE;
	}

	private enum LEVEL {
		NONE,
		DATASET,
		KEY,
		OBS,
		SECTION,
		GROUP
	}


}
