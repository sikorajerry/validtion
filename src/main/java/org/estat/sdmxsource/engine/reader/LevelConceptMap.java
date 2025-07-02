package org.estat.sdmxsource.engine.reader;

import java.util.LinkedHashMap;
import java.util.Map;

/** Intermediate class to support a map containing 
 *  concepts and their position or value: 
 *  fixed value if fixed==true or the column position */ 
public class LevelConceptMap {

	private Map<String, ConceptInfo> conceptMappingByLevel;

	public LevelConceptMap() {		
		conceptMappingByLevel = new LinkedHashMap<>();
	}

	public ConceptInfo put(String conceptName, ConceptInfo conceptMap) {
		return conceptMappingByLevel.put(conceptName, conceptMap);
	}

	public Map<String, ConceptInfo>getMapping() {
		return conceptMappingByLevel;
	}
}
