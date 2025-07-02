package org.estat.sdmxsource.engine.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;

public class CsvTestObject {
	
	String dataStructureName = null;
	
	List<String> dataAttributes = new ArrayList<String>();
	
	List<String> keyables = new ArrayList<String>();
	
	Map<String, List<String>> observations = new HashMap<String, List<String>>();

	public List<String> getDataAttributes() {
		return dataAttributes;
	}

	public List<String> getKeyables() {
		return keyables;
	}

	public Map<String, List<String>> getObservationsByKeyable() {
		return observations;
	}
	
	public boolean addDataAttribute(String keyValue) {
		return dataAttributes.add(keyValue);
	}
	
	public boolean addKeyable(String keyable) {
		return keyables.add(keyable);
	}
	
	public boolean addObservation(String keyable, String observation) {
		List<String> observationsForKeyable = observations.get(keyable);
		if (observationsForKeyable == null) {
			observationsForKeyable = new ArrayList<String>();
		}
		
		observationsForKeyable.add(observation);
		observations.put(keyable, observationsForKeyable);
		
		return true;
	}
	
	public List<String> getObservation(String keyable) {
		return observations.get(keyable);
	}

	public String getDataStructureName() {
		return dataStructureName;
	}

	public void setDataStructureName(String dataStructureName) {
		this.dataStructureName = dataStructureName;
	}

}
