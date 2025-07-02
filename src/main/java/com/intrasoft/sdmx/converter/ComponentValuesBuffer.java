package com.intrasoft.sdmx.converter;

import java.util.*;

/**
 * A buffer for all dsd components and their current values
 *
 * Created by dragos balan
 */
public class ComponentValuesBuffer {

    /**
     * map holding the name of the dimension and the value of that dimension
     */
    private LinkedHashMap<String, String> componentValues;

    /**
     * the name of the primary measure
     */
    private final String primaryMeasureName;

    /**
     *
     * @param primaryMeasureName
     * @param dimensionNames
     */
    public ComponentValuesBuffer(String primaryMeasureName,
                                List<String> dimensionNames) {
        this.primaryMeasureName = primaryMeasureName;
        this.componentValues = new LinkedHashMap<>(dimensionNames.size());
        for (String dimension : dimensionNames) {
            componentValues.put(dimension, "");
        }
    }

    public ComponentValuesBuffer(ComponentValuesBuffer other){
        this.primaryMeasureName = other.primaryMeasureName;
        this.componentValues = new LinkedHashMap<>(other.getComponents().size());
        for (String component: other.getComponents()) {
            this.componentValues.put(component, other.getValueFor(component));
        }
    }

    public void addValueFor(String value, String dimOrAttrName){
        if(componentValues.keySet().contains(dimOrAttrName)){
            componentValues.put(dimOrAttrName, value);
        }else if(dimOrAttrName.equalsIgnoreCase("allDimensions")) {
        	
        }else{
        	//SDMXCONV-801
        	//need to map the explisit measures
        	componentValues.put(dimOrAttrName, value);
            //throw new IllegalArgumentException("Cannot add a value for a non-declared dimension "+dimOrAttrName);
        }
    }

    public void emptyValueList(){
        for(String dim: componentValues.keySet()){
            componentValues.put(dim, "");
        }
    }
    
    public void emptyValues(List<String> attributeNames){
    	for(String attr: attributeNames){
    	    componentValues.put(attr, "");
        }
    }
    
    public String getValueFor(String dimOrAttrName){
        return componentValues.get(dimOrAttrName);
    }

    public String getObsValue(){
        return componentValues.get(primaryMeasureName);
    }

    public List<String> getComponents(){
        List<String> componentList = new ArrayList<>();
        for(Map.Entry<String, String> entry: componentValues.entrySet()) {
            componentList.add(entry.getKey());
        }
        return componentList;
    }

    public LinkedHashMap<String, String> toMap(){
        return new LinkedHashMap<>(componentValues);
    }

    public String toString(){
        StringBuilder result = new StringBuilder("ComponentValuesBuffer[");
        for(String dimension: componentValues.keySet()){
            result.append("[dim=").append(dimension)
                    .append(", val=").append(componentValues.get(dimension))
                    .append("]");
        }
        result.append("]");
        return result.toString();
    }
}
