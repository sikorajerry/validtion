package com.intrasoft.sdmx.converter.util;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;
import com.intrasoft.sdmx.converter.util.GroupsCache;

import static org.junit.Assert.*;

public class TestGroupsCache {

	private GroupsCache classUnderTest = new GroupsCache();
	
	@Test
	public void timeseriesKeysIncludesGroupKeys() {
		classUnderTest = new GroupsCache();
		
		Keys groupKeys1 = new Keys();
		groupKeys1.add("Key1", "Value1");
		groupKeys1.add("Key2", "Value2");
		groupKeys1.add("Key3", "Value3");
		classUnderTest.addGroup("group1", groupKeys1, new Attrs());
		
		Keys groupKeys2 = new Keys();
		groupKeys2.add("Key3", "test1");
		groupKeys2.add("Key1", "test2");
		groupKeys2.add("Key2", "test3");
		groupKeys2.add("DOES NOT MATCH", "test4");
		classUnderTest.addGroup("group2", groupKeys2, new Attrs());
		
		Keys seriesKeys = new Keys();
		seriesKeys.add("Key1", "Value1");
		seriesKeys.add("Key2", "Value2");
		seriesKeys.add("Key3", "Value3");
		seriesKeys.add("Key4", "Value4");
		
		assertTrue(classUnderTest.hasGroupForKey(seriesKeys));
		assertEquals("group1", classUnderTest.getGroupForSeries(seriesKeys).getLeft());
	}
	
	@Test
	public void oneSeriesKeyDoesNotMatchTheGroupKeys() {
		classUnderTest = new GroupsCache();
		
		Keys groupKeys1 = new Keys();
		groupKeys1.add("Key1", "Value1");
		groupKeys1.add("Key2", "Value2");
		groupKeys1.add("Key3", "Value3");
		classUnderTest.addGroup("group1", groupKeys1, new Attrs());
		
		Keys groupKeys2 = new Keys();
		groupKeys2.add("Key3", "test1");
		groupKeys2.add("Key1", "test2");
		groupKeys2.add("Key2", "test3");
		groupKeys2.add("Key4", "test4");
		classUnderTest.addGroup("group2", groupKeys2, new Attrs());
		
		Keys keysFromTimeSeries = new Keys();
		keysFromTimeSeries.add("Key1", "Value1");
		keysFromTimeSeries.add("NotMatching", "Value2");
		keysFromTimeSeries.add("Key3", "Value3");
		keysFromTimeSeries.add("Key4", "Value4");
		
		assertFalse(classUnderTest.hasGroupForKey(keysFromTimeSeries));
	}
	
	@Test
	public void oneGroupKeysDoesNotMatchTheSeriesKey() {
		classUnderTest = new GroupsCache();
		
		Keys groupKeys1 = new Keys();
		groupKeys1.add("Key1", "Value1");
		groupKeys1.add("NotMatching", "Value2");
		groupKeys1.add("Key3", "Value3");
		classUnderTest.addGroup("group1", groupKeys1, new Attrs());
		
		Keys groupKeys2 = new Keys();
		groupKeys2.add("Key3", "test1");
		groupKeys2.add("Key1", "test2");
		groupKeys2.add("Key2", "test3");
		groupKeys2.add("Key4", "test4");
		classUnderTest.addGroup("group2", groupKeys2, new Attrs());
		
		Keys keysFromTimeSeries = new Keys();
		keysFromTimeSeries.add("Key1", "Value1");
		keysFromTimeSeries.add("Key2", "Value2");
		keysFromTimeSeries.add("Key3", "Value3");
		keysFromTimeSeries.add("Key4", "Value4");
		
		assertFalse(classUnderTest.hasGroupForKey(keysFromTimeSeries));
	}
	
	@Test
	public void oneSeriesValueDoesNotMatchTheGroupValues() {
		classUnderTest = new GroupsCache();
		
		Keys groupKeys1 = new Keys();
		groupKeys1.add("Key1", "Value1");
		groupKeys1.add("Key2", "Value2");
		groupKeys1.add("Key3", "Value3");
		classUnderTest.addGroup("group1", groupKeys1, new Attrs());
		
		Keys keysFromTimeSeries = new Keys();
		keysFromTimeSeries.add("Key1", "NotMatching");
		keysFromTimeSeries.add("Key2", "Value2");
		keysFromTimeSeries.add("Key3", "Value3");
		keysFromTimeSeries.add("Key4", "Value4");
		
		assertFalse(classUnderTest.hasGroupForKey(keysFromTimeSeries));
	}
	
	@Test
	public void oneGroupValueDoesNotMatchTheGroupValues() {
		classUnderTest = new GroupsCache();
		
		Keys groupKeys1 = new Keys();
		groupKeys1.add("Key1", "Value1");
		groupKeys1.add("NotMatching", "Value2");
		groupKeys1.add("Key3", "Value3");
		classUnderTest.addGroup("group1", groupKeys1, new Attrs());
		
		Keys keysFromTimeSeries = new Keys();
		keysFromTimeSeries.add("Key1", "Value1");
		keysFromTimeSeries.add("Key2", "Value2");
		keysFromTimeSeries.add("Key3", "Value3");
		keysFromTimeSeries.add("Key4", "Value4");
		
		assertFalse(classUnderTest.hasGroupForKey(keysFromTimeSeries));
	}
	
	
	@Test
	public void groupCacheReturnsTheMatchingAttributes() {
		classUnderTest = new GroupsCache();
		
		Keys groupKeys1 = new Keys();
		groupKeys1.add("Key1", "Value1");
		groupKeys1.add("Key2", "Value2");
		groupKeys1.add("Key3", "Value3");
		
		Attrs attributesFromGroup = new Attrs();
		attributesFromGroup.add("Attribute1", "AttrValue1");
		attributesFromGroup.add("Attribute2", "AttrValue2");
		attributesFromGroup.add("Attribute3", "AttrValue3");
		
		classUnderTest.addGroup("group1", groupKeys1, attributesFromGroup);
		
		Keys groupKeys2 = new Keys();
		groupKeys2.add("Key3", "test1");
		groupKeys2.add("Key1", "test2");
		groupKeys2.add("Key2", "test3");
		groupKeys2.add("Key4", "test4");
		classUnderTest.addGroup("group2", groupKeys2, new Attrs());
		
		Keys seriesKeys = new Keys();
		seriesKeys.add("Key1", "Value1");
		seriesKeys.add("Key2", "Value2");
		seriesKeys.add("Key3", "Value3");
		seriesKeys.add("Key4", "Value4");

		Pair<String, Attrs> matchingGroup = classUnderTest.getGroupForSeries(seriesKeys);
		assertEquals(attributesFromGroup, matchingGroup.getRight());
		assertEquals("group1", matchingGroup.getLeft());
	}
	
	@Test
	public void groupCacheReturnsNullAttributesWhenSeriesKeysDoNotMatch() {
		classUnderTest = new GroupsCache();
		
		Keys groupKeys1 = new Keys();
		groupKeys1.add("Key1", "Value1");
		groupKeys1.add("NotMatched", "Value2");
		groupKeys1.add("Key3", "Value3");
		
		Attrs attributesFromGroup = new Attrs();
		attributesFromGroup.add("Attribute1", "AttrValue1");
		attributesFromGroup.add("Attribute2", "AttrValue2");
		attributesFromGroup.add("Attribute3", "AttrValue3");
		
		classUnderTest.addGroup("group1", groupKeys1, attributesFromGroup);
		
		Keys groupKeys2 = new Keys();
		groupKeys2.add("Key3", "test1");
		groupKeys2.add("Key1", "test2");
		groupKeys2.add("Key2", "test3");
		groupKeys2.add("Key4", "test4");
		classUnderTest.addGroup("group2", groupKeys2, new Attrs());
		
		Keys keysFromTimeSeries = new Keys();
		keysFromTimeSeries.add("Key1", "Value1");
		keysFromTimeSeries.add("Key2", "Value2");
		keysFromTimeSeries.add("Key3", "Value3");
		keysFromTimeSeries.add("Key4", "Value4");
		
		assertNull(classUnderTest.getGroupForSeries(keysFromTimeSeries));
	}
}
