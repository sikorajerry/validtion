package com.intrasoft.sdmx.converter.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.intrasoft.sdmx.converter.model.data.GroupKey;
import com.intrasoft.sdmx.converter.model.data.TimeseriesKey;

public class GroupKeysCache {
	
	private List<GroupKey> listOfGroups = new ArrayList<GroupKey>();
			
	public boolean hasGroupForKey(TimeseriesKey seriesKey){
	    if (getGroupForSeries(seriesKey) !=null) {
	    	return true;
	    } else {
	    	return false;
	    }	
	}
	
	public Map<String,String> getAttributesForKey(TimeseriesKey seriesKey) {
		GroupKey key = getGroupForSeries(seriesKey);
		if(key!=null){
			return key.getAttributeValues();
		} else {
			return new HashMap<String, String>();
		}
	}
	
	public void addGroupKey(GroupKey groupKey) {
		listOfGroups.add(groupKey);
	}
	
	private GroupKey getGroupForSeries(TimeseriesKey seriesKey){
		GroupKey result = null;
		Map<String,String> keysFromSeries = seriesKey.getKeyValues();
	    for (GroupKey group: listOfGroups) {
	    	Map<String,String> keysFromGroups = group.getKeyValues();
	    	if (keysFromSeries.entrySet().containsAll(keysFromGroups.entrySet())) {
	    		result = group;
	    	}
	    }
	    return result;
	}
}
