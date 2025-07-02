package com.intrasoft.sdmx.converter.model.ndata;

import java.util.*;

public class Keys {
	
	private Map<String, String> keysWithValues = new LinkedHashMap<>();
	
	public void add(String keyName, String keyValue){
		keysWithValues.put(keyName, keyValue); 
	}
	
	private Set<Map.Entry<String, String>> values(){
		return keysWithValues.entrySet(); 
	}
	
	public List<String> getKeyNames(){
		List<String> keyList = new ArrayList<>();
		for(Map.Entry<String, String> entry: keysWithValues.entrySet()) {
			keyList.add(entry.getKey());
		}
		return keyList;
	}
	
	public String getKeyValue(String attrName){
		return keysWithValues.get(attrName); 
	}
	
	public boolean includes(Keys otherKeys){
		return keysWithValues.entrySet().containsAll(otherKeys.values()); 
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keysWithValues == null) ? 0 : keysWithValues.hashCode());
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
		Keys other = (Keys) obj;
		if (keysWithValues == null) {
			if (other.keysWithValues != null)
				return false;
		} else if (!keysWithValues.equals(other.keysWithValues))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Keys [keysWithValues=" + keysWithValues + "]";
	}
	
	
}
