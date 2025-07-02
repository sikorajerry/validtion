package com.intrasoft.sdmx.converter.io.data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalMeasureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.MeasureBean;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.util.ObjectUtil;

import com.intrasoft.sdmx.converter.ComponentValuesBuffer;
import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;
import com.intrasoft.sdmx.converter.model.ndata.ObservationData;
import com.intrasoft.sdmx.converter.util.GroupsCache;

/**
 * This a buffer holding all values for dsd components
 * (group keys, group attributes, series keys, series attributes, obs values, obs attributes).
 *
 * Created by dragos balan
 */
public abstract class ComponentBufferWriterEngine extends BufferedDataWriterEngineAdaptor {

	private static Logger logger = LogManager.getLogger(ComponentBufferWriterEngine.class);

    /**
     * the cache for group keys and values
     */
    private GroupsCache groupsCache = new GroupsCache();

    /**
     * the group id that is currently open
     */
    private String currentGroupId;

    /**
     * a buffer with values for all keys and attributes for all dsd components
     * (dataset attributes, group keys, group attributes, series keys, series attributes, obs values, obs attributes).
     */
    private ComponentValuesBuffer componentValuesBuffer = null;

    //v.3.0
    private List<ComponentValuesBuffer> componentValuesBuffers = new ArrayList<ComponentValuesBuffer>();

    public ComponentValuesBuffer getComponentValuesBuffer() {
		return componentValuesBuffer;
	}

	public void setComponentValuesBuffer(ComponentValuesBuffer componentValuesBuffer) {
		this.componentValuesBuffer = componentValuesBuffer;
	}

	//v.3.0
	public List<ComponentValuesBuffer> getComponentValuesBuffers() {
		return componentValuesBuffers;
	}

	//v.3.0
	public void setComponentValuesBuffers(List<ComponentValuesBuffer> componentValuesBuffers) {
		this.componentValuesBuffers = componentValuesBuffers;
	}

	private boolean groupSeries = false;

	protected LinkedHashMap<String, String> complexAttributesMap = new LinkedHashMap<String, String>();


	/**
     * the name of the primary measure
     */
    private String primaryMeasureName = null;

    //v.3.0
    private List<String> primaryMeasureNames = new ArrayList<String>();

    /**
     * map holding the transcoding data
     */
    private TranscodingEngine transcoding;
    
    /**
     * a buffer with no transcoding
     */
    public ComponentBufferWriterEngine(){
        this(null);
    }

    /**
     * a buffer with transcodings
     * @param transcoding
     */
	public ComponentBufferWriterEngine(LinkedHashMap<String, LinkedHashMap<String, String>> transcoding) {
	    this.transcoding = new TranscodingEngine(transcoding);
	}

	/**
     * open dataset notification
     * @param header
     * @param annotations
     */
    public void openDataset(DatasetHeaderBean header,
                            AnnotationBean...annotations){
        List<String> dsdComponentsForCrossXMeasures = getStructureScanner().getComponents(isMapCrossXMeasures());
        List<String> dsdComponents = getCrossXMeasuresComponents(dsdComponentsForCrossXMeasures);
        if(!getDataStructure().hasComplexComponents()
				&& ObjectUtil.validObject(getDataStructure().getPrimaryMeasure())
					&& ObjectUtil.validString(getDataStructure().getPrimaryMeasure().getId())) {
			primaryMeasureName = getDataStructure().getPrimaryMeasure().getId();
			logger.debug("opening dataset for primary measure {}", primaryMeasureName);
			componentValuesBuffer = new ComponentValuesBuffer(primaryMeasureName, dsdComponents);
		} else {
			// this hack is when dsd is SDMX 3.0
			// primaryMeasureName = PrimaryMeasureBean.FIXED_ID;
			// GPA: there is no primaryMeasure for SDMX 3.0 only measures
			primaryMeasureName = null;
			
			for(MeasureBean measureBean : getDataStructure().getMeasures()) {
				boolean isComplex = false;
				if(measureBean.getRepresentationMaxOccurs().isUnbounded() || measureBean.getRepresentationMaxOccurs().getOccurrences()>1)
					isComplex = true;
				
				if(isComplex)
					dsdComponentsForCrossXMeasures.remove(measureBean.getId());
			}
			
			for(MeasureBean measureBean : getDataStructure().getMeasures()) {
				if(dsdComponentsForCrossXMeasures.contains(measureBean.getId())) {
					componentValuesBuffer = new ComponentValuesBuffer(measureBean.getId(), dsdComponentsForCrossXMeasures);
					componentValuesBuffers.add(componentValuesBuffer);
					primaryMeasureNames.add(measureBean.getId());
					break;
				}
			}
			
			if(componentValuesBuffers.isEmpty()) {
				componentValuesBuffer = new ComponentValuesBuffer("ONLY_COMPLEX_MEASURES", dsdComponentsForCrossXMeasures);
				componentValuesBuffers.add(componentValuesBuffer);
				primaryMeasureNames.add("ONLY_COMPLEX_MEASURES");
			}
			componentValuesBuffer = null;
		}
    }

	private List<String> getCrossXMeasuresComponents(List<String> dsdComponents) {
		List<String> components = new ArrayList<>();
		for(String component : dsdComponents) {
			components.add(findComponentFromId(component));
		}
		return components;
	}

	public void doWriteDatasetAttributes(Attrs datasetAttributes) {
    	logger.debug("writing data set attributes, {}", datasetAttributes);
    	addAttrsToComponentBuffer(datasetAttributes);
    }

    public void closeDataset(){
        if(ObjectUtil.validObject(getStructureScanner(), componentValuesBuffer)){
            componentValuesBuffer.emptyValues(getStructureScanner().getDatasetLevelAttributes());

        } else if(ObjectUtil.validObject(getStructureScanner()) && ObjectUtil.validCollection(componentValuesBuffers)) {
        	for(ComponentValuesBuffer coValuesBuffer : componentValuesBuffers)
        		coValuesBuffer.emptyValues(getStructureScanner().getDatasetLevelAttributes());
        }
        //Empty attributes of dataset when/if start new read them again
        if(ObjectUtil.validMap(complexAttributesMap)) {
        	for(String datasetAttribute : getStructureScanner().getDatasetLevelAttributes()) {
        		if(complexAttributesMap.containsKey(datasetAttribute)) {
					complexAttributesMap.put(datasetAttribute, "");
				}
			}
		}
    }

    public void openSeries(AnnotationBean... annotations){}

    public void doWriteSeriesKeysAndAttributes(Keys seriesKeys, Attrs seriesAttributes) {
    	groupSeries = false;
        if (groupsCache.hasGroupForKey(seriesKeys)) {
        	groupSeries = true;
            Pair<String, Attrs> groupInfo = groupsCache.getGroupForSeries(seriesKeys);
            addAttrsToComponentBuffer(groupInfo.getRight());
        }
        addKeysToComponentBuffer(seriesKeys);
        addAttrsToComponentBuffer(seriesAttributes);
    }

    public void closeSeries(){
    	if(ObjectUtil.validObject(componentValuesBuffer)) {
	        componentValuesBuffer.emptyValues(getStructureScanner().getSeriesLevelAttributes());
	        componentValuesBuffer.emptyValues(getStructureScanner().getDimensions(false));
    	}
    	if(ObjectUtil.validCollection(componentValuesBuffers)) {
    		for(ComponentValuesBuffer coValuesBuffer : componentValuesBuffers) {
    			coValuesBuffer.emptyValues(getStructureScanner().getSeriesLevelAttributes());
    			coValuesBuffer.emptyValues(getStructureScanner().getGroupLevelAttributes());
    			coValuesBuffer.emptyValues(getStructureScanner().getDimensions(false));
    		}
    	}
		//Empty attributes of series/group when/if start new read them again
		if(ObjectUtil.validMap(complexAttributesMap)) {
			for(String seriesAttribute : getStructureScanner().getSeriesLevelAttributes()) {
				if(complexAttributesMap.containsKey(seriesAttribute)) {
					complexAttributesMap.put(seriesAttribute, "");
				}
			}
			for(String groupAttribute : getStructureScanner().getGroupLevelAttributes()) {
				if(complexAttributesMap.containsKey(groupAttribute)) {
					complexAttributesMap.put(groupAttribute, "");
				}
			}
		}
    }

    public void openGroup(String groupId, AnnotationBean... annotations){
        currentGroupId = groupId;
    }

    public void doWriteGroupKeysAndAttributes(Keys groupKeys, Attrs groupAttributes) {
        groupsCache.addGroup(currentGroupId, groupKeys, groupAttributes);
    }

    public void closeGroup(){
        currentGroupId = null;
    }

    public void openObservation(AnnotationBean... annotations){}

    public void doWriteObservation(ObservationData observation, Attrs obsAttrs){
    	
    	//Check if explicit measures are enabled 
        //and if we are trying to write the Measure dimension
        if(isMapMeasures()) {
        	addExplicitMeasuresToComponentBuffer(observation.getObsValue(), observation.getObsConceptValue());
        } else if(isMapCrossXMeasures()) {
        	addCrossMeasuresToComponentBuffer(observation.getObsValue(), observation.getObsConceptValue());
    	} else {
    		if(ObjectUtil.validObject(componentValuesBuffer)) {
	        	componentValuesBuffer.addValueFor(observation.getObsValue(), primaryMeasureName);
	        	componentValuesBuffer.addValueFor(observation.getObsConceptValue(), observation.getObsConceptId());
    		}
        }
        addAttrsToComponentBuffer(obsAttrs);
    }

    public void closeObservation(){
    	if(ObjectUtil.validObject(componentValuesBuffer)) {
	        doWriteComponentsValues(componentValuesBuffer);
	        componentValuesBuffer.emptyValues(getStructureScanner().getObservationLevelAttributes());
    	}
    	if(ObjectUtil.validCollection(componentValuesBuffers)) {
    		for(ComponentValuesBuffer coValuesBuffer : componentValuesBuffers) {
    			doWriteComponentsValues(coValuesBuffer);
    			coValuesBuffer.emptyValues(getStructureScanner().getObservationLevelAttributes());
    		}
    	}
		//Empty attributes of series/group when/if start new read them again
		if(ObjectUtil.validMap(complexAttributesMap)) {
			for(String obsAttribute : getStructureScanner().getObservationLevelAttributes()) {
				if(complexAttributesMap.containsKey(obsAttribute)) {
					complexAttributesMap.put(obsAttribute, "");
				}
			}
			for(String measure : getStructureScanner().getMeasures()) {
				if(complexAttributesMap.containsKey(measure)) {
					complexAttributesMap.put(measure, "");
				}
			}
		}
    }

    private void addAttrsToComponentBuffer(Attrs attributes){
        for(String attrName: attributes.getAttributeNames()){
        	String value = attributes.getAttributeValue(attrName);
        	if(ObjectUtil.validObject(componentValuesBuffer)) {
	        	if(transcoding.hasTranscodingRules(attrName)) {
	        		componentValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(attrName, value), attrName);
	        	} else {
	        		componentValuesBuffer.addValueFor(value, attrName);
	        	}
        	}
        	if(ObjectUtil.validCollection(componentValuesBuffers)) {
        		for(ComponentValuesBuffer coValuesBuffer : componentValuesBuffers) {
        			if(transcoding.hasTranscodingRules(attrName)) {
        				coValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(attrName, value), attrName);
    	        	} else {
    	        		coValuesBuffer.addValueFor(value, attrName);
    	        	}
        		}
        	}
        }
    }

    private void addKeysToComponentBuffer(Keys tsKey){
        for(String seriesKey: tsKey.getKeyNames()){
        	String value = tsKey.getKeyValue(seriesKey);
        	if(ObjectUtil.validObject(componentValuesBuffer)) {
				if(transcoding.hasTranscodingRules(seriesKey)) {
	        		componentValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(seriesKey, value), seriesKey);
	        	} else {
	        		componentValuesBuffer.addValueFor(value, seriesKey);
	        	}
        	}
        	if(ObjectUtil.validCollection(componentValuesBuffers)) {
        		for(ComponentValuesBuffer coValuesBuffer : componentValuesBuffers) {
        			if(transcoding.hasTranscodingRules(seriesKey)) {
        				coValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(seriesKey, value), seriesKey);
    	        	} else {
    	        		coValuesBuffer.addValueFor(value, seriesKey);
    	        	}
        		}
        	}
        }
    }

    protected abstract void doWriteComponentsValues(ComponentValuesBuffer componentValues);
    
    private void addExplicitMeasuresToComponentBuffer(String value, String key) {
    	if(ObjectUtil.validObject(componentValuesBuffer)) {
			if(transcoding.hasTranscodingRules(key)) {
	    		componentValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(key, value), key);
	    	} else {
	    		componentValuesBuffer.addValueFor(value, key);
	    	}
    	}
    	if(ObjectUtil.validCollection(componentValuesBuffers)) {
    		for(ComponentValuesBuffer coValuesBuffer : componentValuesBuffers) {
    			if(transcoding.hasTranscodingRules(key)) {
    				coValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(key, value), key);
    	    	} else {
    	    		coValuesBuffer.addValueFor(value, key);
    	    	}
    		}
    	}
    };
    
    private void addCrossMeasuresToComponentBuffer(String value, String key) {
    	if(ObjectUtil.validObject(componentValuesBuffer)) {
			if(transcoding.hasTranscodingRules(key)) {
	    		componentValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(key, value), key);
	    	} else {
	    		componentValuesBuffer.addValueFor(value, key);
	    	}
    	}
    	if(ObjectUtil.validCollection(componentValuesBuffers)) {
    		for(ComponentValuesBuffer coValuesBuffer : componentValuesBuffers) {
    			if(transcoding.hasTranscodingRules(key)) {
    				coValuesBuffer.addValueFor(transcoding.getValueFromTranscoding(key, value), key);
    	    	} else {
    	    		coValuesBuffer.addValueFor(value, key);
    	    	}
    		}
    	}
    }

    protected LinkedHashMap<String, String> getBufferedValues(){
    	if(ObjectUtil.validObject(componentValuesBuffer)) {
    		return componentValuesBuffer.toMap();
    	} else {
    		// GPA: to be implemented for EXCEL. For now get the first only
    		return componentValuesBuffers.get(0).toMap();
    	}
    }

	public TranscodingEngine getTranscoding() {
		return transcoding;
	}

	/**
	 * In case of crossectional measures, we need to map the measures id with the code,
	 * because the code is used by the reader.
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-950">SDMXCONV-950</a>
	 *
	 * @param dimension component's Id
	 * @return
	 */
	public String findComponentFromId(String dimension) {
		String dimensionCode = null;
		CrossSectionalDataStructureBean dsd = null;
		if(getStructureScanner().isCrossSectionalDataStructure()){
			dsd = (CrossSectionalDataStructureBean) getDataStructure();
		}
		if(dsd!=null){
			CrossSectionalMeasureBean crossXMeasure = dsd.getCrossSectionalMeasure(dimension);
			if(crossXMeasure!=null && dimension.equalsIgnoreCase(crossXMeasure.getId())) {
				dimensionCode = crossXMeasure.getCode();
			}
		}
		return dimensionCode;
	}
	
	public boolean isGroupSeries() {
		return groupSeries;
	}
}
