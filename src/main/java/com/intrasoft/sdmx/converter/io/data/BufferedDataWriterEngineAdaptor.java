/**
 * 
 */
package com.intrasoft.sdmx.converter.io.data;

import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;
import com.intrasoft.sdmx.converter.model.ndata.ObservationData;
import com.intrasoft.sdmx.converter.structures.DataStructureScanner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.formula.eval.NotImplementedException;
import org.sdmxsource.sdmx.api.constants.DIMENSION_AT_OBSERVATION;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.constants.TIME_FORMAT;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.registry.ProvisionAgreementBean;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.util.date.DateUtil;
import org.sdmxsource.util.ObjectUtil;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.sdmxsource.sdmx.dataparser.engine.writer.WritingDataEngineDecorator.removeArrayElement;

/**
 * Abstract implementation of DataWriterEngine created for the following reasons: 
 * 
 *  1. improve the existing writer API by exposing a better set of methods which follow the pattern: 
 *  open element -> write element with attributes -> close element. This new API is better suited for all formats (xml, csv, excel)
 *   
 * 	2. Delay the writeXXXX methods until all data is available (observations, attributes)
 *
 * 	3. Provide a buffer for all attributes and keys (for dataset, series, groups and observations) and transforms the
 * 	writeAttribute calls into writeDatasetAttributes, writeSeriesAttributes, writeGroupAttributes, writeObservationAttributes
 * 
 *  4. Easier export of old Writer code from the old since this class is an adapter between the old API and the new one
 *  (so it's memory efficient and provides the very useful methods of the old API)
 *  
 *  5. ensure the DataWriterEngine methods are called in the correct sequence
 *  (so that clients of the DataWriterEngine do not use its methods in a wrong sequence)
 * 
 * Disadvantages of using this class: since it uses a lot of "switch" statements it may not be as fast as other readers
 * 
 * @author dragos balan
 */
public abstract class BufferedDataWriterEngineAdaptor implements DataWriterEngine, BufferedDataWriterEngine {

	private static Logger logger = LogManager.getLogger(BufferedDataWriterEngineAdaptor.class);
	
	/**
     * Boolean that indicates 
     * that we have to treat measure concepts
     * instead of measures
     */
    protected boolean mapMeasures;

    protected boolean isHeaderWritten;

    protected DataStructureBean dsd; //SDMXCONV-1184
	
	public boolean isMapMeasures() {
		return mapMeasures;
	}

	public void setMapMeasures(boolean mapMeasures) {
		this.mapMeasures = mapMeasures;
	}
	
	/**
     * Boolean that indicates 
     * that we have to treat CrossX concepts
     * instead of measures
     */
    protected boolean mapCrossXMeasures;
	
	public boolean isMapCrossXMeasures() {
		return mapCrossXMeasures;
	}

	public void setMapCrossXMeasures(boolean mapCrossXMeasures) {
		this.mapCrossXMeasures = mapCrossXMeasures;
	}

	/**
	 * enumeration for the possible positions/states in the writer
	 * @author dragos balan
	 */
	public enum Position {
		WRITER_CREATED, HEADER, DATASET, GROUP, SERIES, OBSERVATION, WRITER_CLOSED
	}

    /**
     * holds the header for later use
     */
	private HeaderBean headerBuffer;
	
	/**
	 * buffer for dataset attributes, series attributes, group attributes, observations attributes
	 */
	private Attrs attributesBuffer;
	
	/**
	 * buffer for series and group keys
	 */
	private Keys keysBuffer;

    /**
     * buffer for observation data
     */
	private ObservationData observationBuffer;

	/**
	 * the current position inside the writer.
	 * This is used to track the previous position so that this class knows open-write-close combination to call
	 */
	private Position currentPosition = Position.WRITER_CREATED;

	protected String dimensionAtObservation;
	
	/**
	 * the data structure scanner
	 */
	protected DataStructureScanner structureScanner = null;

	/**
	 *
	 */
	public BufferedDataWriterEngineAdaptor() {}

	protected Position getCurrentPosition(){
		return currentPosition; 
	}
	
	protected void handleIncorrectPosition(Position ...expectedPositions){
		throw new IllegalStateException("Illegal position: expected one of : "+Arrays.asList(expectedPositions)+" but found "+currentPosition);
	}
	
	private void initKeyValuesBuffer(){
		keysBuffer = new Keys(); 
	}
	
	private void initAttributeValuesBuffer(){
		attributesBuffer = new Attrs(); 
	}

    @Override
	public void writeHeader(HeaderBean header) {
    	headerBuffer = header;
		if(currentPosition.equals(Position.WRITER_CREATED)){
			openWriter(); 
		}else{
			handleIncorrectPosition(Position.WRITER_CREATED);
		}
		currentPosition = Position.HEADER;
		if(ObjectUtil.validObject(dsd)) {
			this.structureScanner = new DataStructureScanner(dsd);
			writeHeader();
		}
	}

	/**
	 * <h1>Method that writes the header at start</h1>
	 * If the dsd is passed as a parameter inside configuration we write the header at start.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1184">SDMXCONV-1184</a>
	 */
	private void writeHeader(){
		if(!isHeaderWritten) {
			openHeader();
			doWriteHeader(headerBuffer);
			closeHeader();
			isHeaderWritten = true;
		}
	}

	@Override
	public void startDataset(	DataflowBean dataflow, 
								DataStructureBean dataStructureBean, 
								DatasetHeaderBean header,
								AnnotationBean... annotations) {
		annotations = removeArrayElement(annotations);
		this.structureScanner = new DataStructureScanner(dataStructureBean);
		switch(currentPosition){
		case WRITER_CREATED: 
			openWriter(); 
			break; 
		case HEADER:
			writeHeader();
			break; 
		default: 
			handleIncorrectPosition(Position.WRITER_CREATED, Position.HEADER);
		}
		
		// The Dimension At Observation is retrieved from the DataSetHeaderBean. This is mandatory for 2.1 formats. For SDMX  
		if (header != null) {
			if (header.isTimeSeries()) {
				dimensionAtObservation = dataStructureBean.getTimeDimension().getId();
			}
			else if (header.getDataStructureReference() != null) {
				if (!DIMENSION_AT_OBSERVATION.ALL.getVal().equals(header.getDataStructureReference().getDimensionAtObservation())) {
					dimensionAtObservation = header.getDataStructureReference().getDimensionAtObservation();
				}
			}
		}
	
		// This algorithm probably exists also elsewhere
		// fail safe or when AllDimensions are set
		if (dimensionAtObservation == null  || "AllDimensions".equalsIgnoreCase(dimensionAtObservation)) {
			if (dataStructureBean.getTimeDimension() != null) {
				dimensionAtObservation = dataStructureBean.getTimeDimension().getId();
			} else {
				 List<DimensionBean> measureDimensions = dataStructureBean.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
				 List<DimensionBean> normalDimensions = dataStructureBean.getDimensions(SDMX_STRUCTURE_TYPE.DIMENSION);
				 if (measureDimensions.isEmpty()) {
					 if (dataStructureBean instanceof CrossSectionalDataStructureBean) {
						 CrossSectionalDataStructureBean crossDsd = (CrossSectionalDataStructureBean)dataStructureBean;
						 List<ComponentBean> crossSectionalAttachObservation = crossDsd.getCrossSectionalAttachObservation(SDMX_STRUCTURE_TYPE.DIMENSION);
						 if (crossSectionalAttachObservation.isEmpty()) {
							 // impossible scenario
							 dimensionAtObservation = normalDimensions.get(normalDimensions.size() -1).getId();
						 } else {
							 dimensionAtObservation = crossSectionalAttachObservation.get(crossSectionalAttachObservation.size() -1).getId();
						 }
					 } else {
						 // get the last
						 dimensionAtObservation = normalDimensions.get(normalDimensions.size() -1).getId();
					 }
				 } else {
					 dimensionAtObservation = measureDimensions.get(measureDimensions.size() -1).getId();
				 }
			}
		}
		currentPosition = Position.DATASET;
		openDataset(header, annotations);
		initAttributeValuesBuffer();
	}
	
	@Override
	public void startDataset(ProvisionAgreementBean provision, 
							DataflowBean dataflow, 
							DataStructureBean dataStructureBean, 
							DatasetHeaderBean header, 
							AnnotationBean...annotations){
		annotations = removeArrayElement(annotations);
		throw new NotImplementedException("method not yet implemented. Please use the other implementation!");
	}

	@Override
	public void startGroup(	String groupId, 
							AnnotationBean... annotations) {
		annotations = removeArrayElement(annotations);
		switch(currentPosition){
		//SDMXCONV-876
		case OBSERVATION:
			currentPosition = Position.GROUP;
			startGroup(groupId, annotations);
		case DATASET:
			doWriteDatasetAttributes(attributesBuffer);
			break;
		case GROUP: 
			doWriteGroupKeysAndAttributes(keysBuffer, attributesBuffer);
			closeGroup(); 
			break; 
		default: 
			handleIncorrectPosition(Position.DATASET, Position.GROUP);
		}
		
		currentPosition = Position.GROUP;
		openGroup(groupId, annotations);
		initKeyValuesBuffer(); 
		initAttributeValuesBuffer();
	}

	@Override
	public void startSeries(AnnotationBean... annotations) {
		annotations = removeArrayElement(annotations);
		switch(currentPosition){
		case DATASET:
			doWriteDatasetAttributes(attributesBuffer);
			break;
		case GROUP: 
			doWriteGroupKeysAndAttributes(keysBuffer, attributesBuffer);
			closeGroup(); 
			break; 
		case SERIES:
			doWriteSeriesKeysAndAttributes(keysBuffer, attributesBuffer);
			closeSeries();
			break; 
		case OBSERVATION:
			doWriteObservation(observationBuffer, attributesBuffer);
			closeObservation();
			closeSeries(); 
			break; 
		default: 
			handleIncorrectPosition(Position.DATASET, Position.GROUP, Position.SERIES, Position.OBSERVATION);
		}
		
		currentPosition = Position.SERIES;
		openSeries(annotations); 
		initKeyValuesBuffer(); 
		initAttributeValuesBuffer();
	}
	
	@Override
	public void writeGroupKeyValue(String id, String value) {
		if(currentPosition.equals(Position.GROUP)){
			keysBuffer.add(id, value); 
		}else{
			handleIncorrectPosition(Position.GROUP);
		}
	}

	@Override
	public void writeSeriesKeyValue(String id, String value) {
		//logger.debug("Write series key with id:{}, value:{} for {}", id, value, currentPosition);
		if(currentPosition.equals(Position.SERIES)){
			keysBuffer.add(id, value); 
		}else{
			handleIncorrectPosition(Position.SERIES);
		}
	}

	@Override
	public void writeAttributeValue(String id, String value) {
        //logger.debug("writing attributes {}={} for {}", id, value, currentPosition);
		if(		currentPosition.equals(Position.DATASET) 
				|| currentPosition.equals(Position.GROUP) 
				|| currentPosition.equals(Position.SERIES) 
				|| currentPosition.equals(Position.OBSERVATION)){
			attributesBuffer.add(id, value);
		}else{
			handleIncorrectPosition(Position.DATASET, Position.GROUP, Position.SERIES, Position.OBSERVATION);
		}
	}

	@Override
	public void writeObservation(	String obsConceptValue, 
									String obsValue, 
									AnnotationBean... annotations) {
		annotations = removeArrayElement(annotations);
		// Here we need to get the dimension at observation. TIME_DIMENSION might not exist!
		writeObservation(  dimensionAtObservation,
                            obsConceptValue,
                            obsValue,
                            annotations);
	}

	@Override
	public void writeObservation(	String observationConceptId, 
									String obsConceptValue, 
									String obsValue,
									AnnotationBean... annotations) {
		annotations = removeArrayElement(annotations);
		//logger.debug("Write observation with observationConceptId:{}, {}, {} in {}",observationConceptId, obsConceptValue, obsValue, currentPosition);
		switch(currentPosition){
		case SERIES: 
			doWriteSeriesKeysAndAttributes(keysBuffer, attributesBuffer);
			break; 
		case OBSERVATION:
			doWriteObservation(observationBuffer, attributesBuffer);
			
			/* If explicit measures are present we want
			 * to store the values of many observation
			 * and write one observation when series is closed.
			 * Observation always is writen when closed is called
			 * so we don't close the observation until next series.
			 * Those changes are made primarily for csv single writer. 
			 */
			if(!isMapMeasures() && !isMapCrossXMeasures()) {
				closeObservation();
			}
			break; 
		default: 
			handleIncorrectPosition(Position.SERIES, Position.OBSERVATION);
		}
		
		currentPosition = Position.OBSERVATION;
		openObservation(annotations);
		observationBuffer = new ObservationData(observationConceptId, obsConceptValue, obsValue);
		
		/* If explicit measures are present we want
		 * to store the values of many observation
		 * and write one observation when series is closed.
		 * We don't want to initialize the buffers until next series.
		 */
		if(!isMapMeasures() && !isMapCrossXMeasures()) {
			initAttributeValuesBuffer();
			initKeyValuesBuffer();
		}
	}

	@Override
	public void writeObservation(	Date obsTime, 
									String obsValue, 
									TIME_FORMAT sdmxTimeFormat,
									AnnotationBean... annotations) {
		annotations = removeArrayElement(annotations);
		// TIME_DIMENSION_FIXED_ID is a constant
		writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID,
                DateUtil.formatDate(obsTime, sdmxTimeFormat),
				obsValue,
                annotations);
	}

	
	
	@Override
	public void writeObservation(String obsConceptValue, AnnotationBean... annotations) {
		annotations = removeArrayElement(annotations);
		// Here we need to get the dimension at observation. TIME_DIMENSION might not exist!
		writeObservation(  dimensionAtObservation,
                            obsConceptValue,
                            null,
                            annotations);
		
	}

	@Override
	public void close(FooterMessage... footer) {
		switch (currentPosition) {
			case DATASET:
				if (ObjectUtil.validObject(attributesBuffer)) {
					doWriteDatasetAttributes(attributesBuffer);
					closeDataset();
					closeWriter();
				}
				break;
			case GROUP:
				doWriteGroupKeysAndAttributes(keysBuffer, attributesBuffer);
				closeGroup();
				closeDataset();
				closeWriter();
				break;
			case SERIES:
				doWriteSeriesKeysAndAttributes(keysBuffer, attributesBuffer);
				closeSeries();
				closeDataset();
				closeWriter();
				break;
			case OBSERVATION:
				doWriteObservation(observationBuffer, attributesBuffer);
				closeObservation();
				closeSeries();
				closeDataset();
				closeWriter();
				break;
			default:
				closeDataset();
				closeWriter();
		}
		currentPosition = Position.WRITER_CLOSED;
	}

	public DataStructureScanner getStructureScanner(){
        if(structureScanner == null){
            logger.warn("getStructureScanner will return null. This is because you are calling getStructureScanner outside of the methods specified in the contract.");
            logger.warn("Please note that getStructureScanner follows the same contract as getDataStructure");
            logger.warn("Please consult the documentation for BufferedDataWriterEngine.getDataStructure() for details");
        }
        return structureScanner;
	}

	public DataStructureBean getDataStructure(){
	    DataStructureBean result = null;
	    if(structureScanner == null){
	        logger.warn("getDataStructure will return null. This is because you are calling getDataStructure outside of the methods specified in the contract.");
	        logger.warn("Please consult the documentation for BufferedDataWriterEngine.getDataStructure()");
        }else{
	        result = structureScanner.getDataStructure();
        }
	    return result;
    }

	//SDMXRI-1166
	public void close(){
		close(new FooterMessage[]{});
	}
}
