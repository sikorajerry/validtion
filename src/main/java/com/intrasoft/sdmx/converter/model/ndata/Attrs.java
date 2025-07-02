package com.intrasoft.sdmx.converter.model.ndata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Attrs {
	
	private LinkedHashMap<String, String> attributesWithValues = new LinkedHashMap<>();
	
	public void add(String attrName, String attrValue){
		attributesWithValues.put(attrName, attrValue); 
	}
	
	public List<String> getAttributeNames(){
		List<String> attributesList = new ArrayList<>();
		for(Map.Entry<String, String> entry: attributesWithValues.entrySet()) {
			attributesList.add(entry.getKey());
		}
		return attributesList;
	}
	
	public String getAttributeValue(String attrName){
		return attributesWithValues.get(attrName); 
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((attributesWithValues == null) ? 0 : attributesWithValues.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Attrs other = (Attrs) obj;
		if (attributesWithValues == null) {
			if (other.attributesWithValues != null)
				return false;
		} else if (!attributesWithValues.equals(other.attributesWithValues))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Attrs [attributesWithValues=" + attributesWithValues + "]";
	}
	
	
	
	
}
