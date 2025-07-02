package com.intrasoft.sdmx.converter.util;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;

import com.intrasoft.sdmx.converter.model.data.GroupKey;
import com.intrasoft.sdmx.converter.model.data.TimeseriesKey;
import com.intrasoft.sdmx.converter.util.GroupKeysCache;

public class TestGroupKeysCache {

	private GroupKeysCache classUnderTest;
	
	@Test
	public void groupContainsTimeSeries() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<>();
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Key2", "Value2");
		keysFromGroup.put("Key3", "Value3");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromGroup2 = new HashMap<>();
		keysFromGroup2.put("Key3", "test1");
		keysFromGroup2.put("Key1", "test2");
		keysFromGroup2.put("Key2", "test3");
		keysFromGroup2.put("Key4", "test4");
		GroupKey groupKey2 = new GroupKey();
		groupKey2.setKeyValues(keysFromGroup2);
		classUnderTest.addGroupKey(groupKey2);
		
		Map<String,String> keysFromTimeSeries = new HashMap<>();
		keysFromTimeSeries.put("Key1", "Value1");
		keysFromTimeSeries.put("Key2", "Value2");
		keysFromTimeSeries.put("Key3", "Value3");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertTrue(classUnderTest.hasGroupForKey(timeseriesKey));		
	}
	
	@Test
	public void timeSeriesHasWrongKey() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<>();
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Key2", "Value2");
		keysFromGroup.put("Key3", "Value3");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromGroup2 = new HashMap<>();
		keysFromGroup2.put("Key3", "test1");
		keysFromGroup2.put("Key1", "test2");
		keysFromGroup2.put("Key2", "test3");
		keysFromGroup2.put("Key4", "test4");
		GroupKey groupKey2 = new GroupKey();
		groupKey2.setKeyValues(keysFromGroup2);
		classUnderTest.addGroupKey(groupKey2);
		
		Map<String,String> keysFromTimeSeries = new HashMap<>();
		keysFromTimeSeries.put("Key1", "Value1");
		keysFromTimeSeries.put("Wrong", "Value2");
		keysFromTimeSeries.put("Key3", "Value3");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertFalse(classUnderTest.hasGroupForKey(timeseriesKey));		
	}
	
	@Test
	public void groupHasWrongKey() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<>();
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Wrong", "Value2");
		keysFromGroup.put("Key3", "Value3");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromGroup2 = new HashMap<>();
		keysFromGroup2.put("Key3", "test1");
		keysFromGroup2.put("Key1", "test2");
		keysFromGroup2.put("Key2", "test3");
		keysFromGroup2.put("Key4", "test4");
		GroupKey groupKey2 = new GroupKey();
		groupKey2.setKeyValues(keysFromGroup2);
		classUnderTest.addGroupKey(groupKey2);
		
		Map<String,String> keysFromTimeSeries = new HashMap<>();
		keysFromTimeSeries.put("Key1", "Value1");
		keysFromTimeSeries.put("Key2", "Value2");
		keysFromTimeSeries.put("Key3", "Value3");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertFalse(classUnderTest.hasGroupForKey(timeseriesKey));		
	}
	
	@Test
	public void timeSeriesHasWrongKeyValue() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<>();
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Key2", "Value2");
		keysFromGroup.put("Key3", "Value3");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromTimeSeries = new HashMap<>();
		keysFromTimeSeries.put("Key1", "Value1");
		keysFromTimeSeries.put("Key4", "Value2");
		keysFromTimeSeries.put("Key3", "Wrong");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertFalse(classUnderTest.hasGroupForKey(timeseriesKey));		
	}
	
	@Test
	public void testTimeseriesAsTreeMapIsInGroup() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<String, String>();
		keysFromGroup.put("Key3", "Value3");
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Key2", "Value2");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromGroup2 = new HashMap<String,String>();
		keysFromGroup2.put("Key3", "test1");
		keysFromGroup2.put("Key1", "test2");
		keysFromGroup2.put("Key2", "test3");
		keysFromGroup2.put("Key4", "test4");
		GroupKey groupKey2 = new GroupKey();
		groupKey2.setKeyValues(keysFromGroup2);
		classUnderTest.addGroupKey(groupKey2);
		
		Map<String,String> keysFromTimeSeries = new TreeMap<String,String>();
		keysFromTimeSeries.put("Key1", "Value1");		
		keysFromTimeSeries.put("Key3", "Value3");
		keysFromTimeSeries.put("Key2", "Value2");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertTrue(classUnderTest.hasGroupForKey(timeseriesKey));		
	}
	
	@Test
	public void testTimeseriesAsTreeMapIsNotInGroup() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<>();
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Key2", "Value2");
		keysFromGroup.put("Key3", "Value3");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromGroup2 = new HashMap<>();
		keysFromGroup2.put("Key3", "test1");
		keysFromGroup2.put("Key1", "test2");
		keysFromGroup2.put("Key2", "test3");
		keysFromGroup2.put("Key4", "test4");
		GroupKey groupKey2 = new GroupKey();
		groupKey2.setKeyValues(keysFromGroup2);
		classUnderTest.addGroupKey(groupKey2);
		
		Map<String,String> keysFromTimeSeries = new TreeMap<>();
		keysFromTimeSeries.put("Key1", "Value1");
		keysFromTimeSeries.put("Key4", "Value2");
		keysFromTimeSeries.put("Key3", "Wrong");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertFalse(classUnderTest.hasGroupForKey(timeseriesKey));		
	}
	
	@Test
	public void testTimeseriesThsatBelongsToGroup() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<>();
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Key2", "Value2");
		keysFromGroup.put("Key3", "Value3");
		Map<String,String> attributesFromGroup = new HashMap<>();
		attributesFromGroup.put("Attribute1", "AttrValue1");
		attributesFromGroup.put("Attribute2", "AttrValue2");
		attributesFromGroup.put("Attribute3", "AttrValue3");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		groupKey.setAttributeValues(attributesFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromGroup2 = new HashMap<>();
		keysFromGroup2.put("Key3", "test1");
		keysFromGroup2.put("Key1", "test2");
		keysFromGroup2.put("Key2", "test3");
		keysFromGroup2.put("Key4", "test4");
		GroupKey groupKey2 = new GroupKey();
		groupKey2.setKeyValues(keysFromGroup2);
		classUnderTest.addGroupKey(groupKey2);
		
		Map<String,String> keysFromTimeSeries = new HashMap<>();
		keysFromTimeSeries.put("Key1", "Value1");
		keysFromTimeSeries.put("Key2", "Value2");
		keysFromTimeSeries.put("Key3", "Value3");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertTrue(attributesFromGroup.equals(classUnderTest.getAttributesForKey(timeseriesKey)));	
		Assert.assertEquals(attributesFromGroup, classUnderTest.getAttributesForKey(timeseriesKey));
	}
	
	@Test
	public void testTimeseriesWitchDoesntBelongToGroup() {
		classUnderTest = new GroupKeysCache();
		
		Map<String,String> keysFromGroup = new HashMap<>();
		keysFromGroup.put("Key1", "Value1");
		keysFromGroup.put("Key2", "Value2");
		keysFromGroup.put("Key3", "Value3");
		Map<String,String> attributesFromGroup = new HashMap<>();
		attributesFromGroup.put("Attribute1", "AttrValue1");
		attributesFromGroup.put("Attribute2", "AttrValue2");
		attributesFromGroup.put("Attribute3", "AttrValue3");
		GroupKey groupKey = new GroupKey();
		groupKey.setKeyValues(keysFromGroup);
		groupKey.setAttributeValues(attributesFromGroup);
		classUnderTest.addGroupKey(groupKey);
		
		Map<String,String> keysFromGroup2 = new HashMap<>();
		keysFromGroup2.put("Key3", "test1");
		keysFromGroup2.put("Key1", "test2");
		keysFromGroup2.put("Key2", "test3");
		keysFromGroup2.put("Key4", "test4");
		GroupKey groupKey2 = new GroupKey();
		groupKey2.setKeyValues(keysFromGroup2);
		classUnderTest.addGroupKey(groupKey2);
		
		Map<String,String> keysFromTimeSeries = new HashMap<>();
		keysFromTimeSeries.put("Key1", "Value1");
		keysFromTimeSeries.put("Wrong", "Value2");
		keysFromTimeSeries.put("Key3", "Value3");
		keysFromTimeSeries.put("Key4", "Value4");
		TimeseriesKey timeseriesKey = new TimeseriesKey();
		timeseriesKey.setKeyValues(keysFromTimeSeries);
		
		Assert.assertFalse(attributesFromGroup.equals(classUnderTest.getAttributesForKey(timeseriesKey)));		
	}
	
}
