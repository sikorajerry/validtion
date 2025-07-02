package com.intrasoft.sdmx.converter.services;

import org.estat.sdmxsource.util.csv.CsvOutColumnMapping;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.MeasureBean;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class MappingService {

    /**
     *  a map having:
     *  levels as keys
     *  a map of colIndex and dsd components as values
     */
    private Map<Integer, TreeMap<Integer, String>> dimensionsMappedOnLevel = new HashMap<Integer, TreeMap<Integer, String>>();

    public Map<Integer, TreeMap<Integer, String>> populateDimOnLevel(List<CsvOutColumnMapping> listOfMappings) {
        if(ObjectUtil.validCollection(listOfMappings)) {
            this.dimensionsMappedOnLevel.clear();
        }
        Map<Integer, Integer> lastColIndexForEachLevel = new HashMap<>();
        for (int i=0; i < listOfMappings.size(); i++) {
            CsvOutColumnMapping mapping = listOfMappings.get(i);
            Integer currentLevel = mapping.getLevel();
            if(!lastColIndexForEachLevel.containsKey(currentLevel)){
                lastColIndexForEachLevel.put(currentLevel, Integer.valueOf(0));
            }
            Integer temp = lastColIndexForEachLevel.get(currentLevel);
            lastColIndexForEachLevel.put(currentLevel, Integer.valueOf(temp.intValue()+1));
            this.dimensionsMappedOnLevel = addMapping( currentLevel,
                    lastColIndexForEachLevel.get(currentLevel),
                    listOfMappings.get(i).getComponent());
        }
        return this.dimensionsMappedOnLevel;
    }

    private Map<Integer, TreeMap<Integer, String>> addMapping(Integer level, Integer columnIndex, String componentId){
        if(!dimensionsMappedOnLevel.containsKey(level)){
            dimensionsMappedOnLevel.put(level, new TreeMap<Integer, String>());
        }
        dimensionsMappedOnLevel.get(level).put(columnIndex, componentId);
        return dimensionsMappedOnLevel;
    }

    @Deprecated
    public List<CsvOutColumnMapping> toList(Map<Integer, TreeMap<Integer, String>> dimensionsMappedOnLevel) {
        this.dimensionsMappedOnLevel = dimensionsMappedOnLevel;
        List<CsvOutColumnMapping> result = new ArrayList<>();
        for(Integer level: dimensionsMappedOnLevel.keySet()){
            Map<Integer, String> colIndexWithComponentsMap = dimensionsMappedOnLevel.get(level);
            for(Integer colIndex: colIndexWithComponentsMap.keySet()){
                CsvOutColumnMapping mapping = new CsvOutColumnMapping();
                mapping.setComponent(colIndexWithComponentsMap.get(colIndex));
                mapping.setLevel(level);
                mapping.setIndex(colIndex);
                result.add(mapping);
            }
        }
        return result;
    }

    public List<CsvOutColumnMapping> toComplex(DataStructureBean structureBean, List<String> components) {
        List<CsvOutColumnMapping> result = new ArrayList<>();
        for(Integer level: dimensionsMappedOnLevel.keySet()){
            Map<Integer, String> colIndexWithComponentsMap = dimensionsMappedOnLevel.get(level);
            for(Integer colIndex: colIndexWithComponentsMap.keySet()){
                CsvOutColumnMapping mapping = new CsvOutColumnMapping();
                mapping.setComponent(colIndexWithComponentsMap.get(colIndex));
                mapping.setLevel(level);
                mapping.setIndex(colIndex);
                result.add(mapping);
            }
        }
        //add occurrences
        for (String originalDim : components) {
            //get all components from mappings
            for (CsvOutColumnMapping dim :result) {
                //if the component exists in the mapping or starts with a dimension og the structure
                if (dim.getComponent().equals(originalDim)
                        || (dim.getComponent().startsWith(originalDim) && (org.apache.commons.lang3.StringUtils.isNumeric(dim.getComponent().substring(originalDim.length()))))) {
                    ComponentBean componentBean = structureBean.getComponent(originalDim);
                    if (componentBean instanceof AttributeBean || componentBean instanceof MeasureBean) {
                        dim.setMinOcurrences(componentBean.getRepresentationMinOccurs());
                        dim.setMaxOccurences(componentBean.getRepresentationMaxOccurs().isUnbounded() ? Integer.MAX_VALUE : componentBean.getRepresentationMaxOccurs().getOccurrences());
                    }
                }
            }
        }
        return result;
    }

    public Map<Integer, TreeMap<Integer, String>> getDimensionsMappedOnLevel() {
        return dimensionsMappedOnLevel;
    }

    public void setDimensionsMappedOnLevel(Map<Integer, TreeMap<Integer, String>> dimensionsMappedOnLevel) {
        this.dimensionsMappedOnLevel = dimensionsMappedOnLevel;
    }
}
