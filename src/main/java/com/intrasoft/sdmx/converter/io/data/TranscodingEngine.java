package com.intrasoft.sdmx.converter.io.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.util.ObjectUtil;

import java.util.LinkedHashMap;

/**
 * This class implements the transcoding processes used in flat readers/writers.
 */
public class TranscodingEngine implements TranscodingEngineInterface{

	private static Logger logger = LogManager.getLogger(TranscodingEngine.class);

	private LinkedHashMap<String, LinkedHashMap<String, String>> transcoding;

	public TranscodingEngine(LinkedHashMap<String, LinkedHashMap<String, String>> transcoding) {
		this.transcoding = transcoding;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasTranscodingRules(final String key) {
		boolean hasrule = false;
		if (transcoding == null || transcoding.isEmpty()) {
			// there are no transcoding rules
			return hasrule;
		} else {
			if (transcoding.containsKey(key)) {
				hasrule = true;
			}
			return hasrule;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getValueFromTranscoding(final String key, final String value) {
		String returnedValue = value;
		final LinkedHashMap<String, LinkedHashMap<String, String>> structureSetMap = transcoding;
		LinkedHashMap<String, String> rule = null;
		if(ObjectUtil.validMap(structureSetMap))
			rule = structureSetMap.get(key);
		// check if the value belongs in any rule
		if(ObjectUtil.validMap(rule) && rule.containsKey(value)) {
			final Object[] setKeys = rule.keySet().toArray();
			for (int i = 0; i < setKeys.length; i++) {
				if (setKeys[i].equals(value)) {
					returnedValue = rule.get(setKeys[i]);
				}
			}
		} else if(!ObjectUtil.validString(value) && rule!=null && rule.containsKey("")) { //default value
			final Object[] setKeys = rule.keySet().toArray();
			for (int i = 0; i < setKeys.length; i++) {
				if (setKeys[i].equals("")) {
					returnedValue = rule.get(setKeys[i]);
				}
			}
		} else {
			returnedValue = value;
		}
		return returnedValue;
	}
}
