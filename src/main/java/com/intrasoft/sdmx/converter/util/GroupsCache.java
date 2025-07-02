package com.intrasoft.sdmx.converter.util;

import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.Map;

public class GroupsCache {
	
	private Map<Keys, Attrs> keysAndAttributes = new LinkedHashMap<>();
	private Map<Keys, String> keysAndNames = new LinkedHashMap<>();

	public void addGroup(String groupId, Keys groupKey, Attrs groupAttrs) {
		keysAndNames.put(groupKey, groupId);
		keysAndAttributes.put(groupKey, groupAttrs);
	}

    /**
     * returns true if this cache contains a group having as keys a subset of the seriesKeys
     * @param seriesKeys
     * @return
     */
    public boolean hasGroupForKey(Keys seriesKeys){
	    boolean result = false;
        for (Keys groupKeys: keysAndAttributes.keySet()) {
            if (seriesKeys.includes(groupKeys)) {
                result = true;
                break;
            }
        }
        return result;
    }

	/**
	 * returns the name and attributes of the first group that has the set of keys a subset of the given seriesKeys
	 *
	 * @param seriesKeys
	 * @return	a tuple formed from the group id and the group attributes
	 */
	public Pair<String, Attrs> getGroupForSeries(Keys seriesKeys){
		Pair<String, Attrs> result = null;
	    for (Keys groupKeys: keysAndAttributes.keySet()) {
	    	if (seriesKeys.includes(groupKeys)) {
	    		result = Pair.of(keysAndNames.get(groupKeys), keysAndAttributes.get(groupKeys));
	    		break;
	    	}
	    }
	    return result;
	}
}
