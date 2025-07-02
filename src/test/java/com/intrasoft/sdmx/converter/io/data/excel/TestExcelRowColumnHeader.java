package com.intrasoft.sdmx.converter.io.data.excel;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;

import com.intrasoft.sdmx.converter.io.data.excel.ExcelRowColumnHeader;

import org.junit.Assert;

public class TestExcelRowColumnHeader {
	
	private ExcelRowColumnHeader classUnderTest;
	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
	
	@Test
	public void testContainsValues() {
		classUnderTest = new ExcelRowColumnHeader(Arrays.asList("DIM1", "DIM2")); 
		classUnderTest.addValues(Arrays.asList("a", "b")); 
		classUnderTest.addValues(Arrays.asList("b", "c"));
		classUnderTest.addValues(Arrays.asList("c", "d"));
		classUnderTest.addValues(Arrays.asList("d", "e"));
		
		Assert.assertEquals(true, classUnderTest.containsValues(Arrays.asList("a", "b")));
		Assert.assertEquals(false, classUnderTest.containsValues(Arrays.asList("x", "y")));
	}

	@Test
	public void testAddValues() {
		classUnderTest = new ExcelRowColumnHeader(Arrays.asList("DIM1", "DIM2")); 
		Assert.assertEquals(0, classUnderTest.addValues(Arrays.asList("a", "b"))); 
		Assert.assertEquals(1, classUnderTest.addValues(Arrays.asList("b", "c")));
		Assert.assertEquals(2, classUnderTest.addValues(Arrays.asList("c", "d")));
		Assert.assertEquals(3, classUnderTest.addValues(Arrays.asList("d", "e")));
	}
	
	@Test
	public void testGetIndexForValues() {
		classUnderTest = new ExcelRowColumnHeader(Arrays.asList("DIM1", "DIM2")); 
		classUnderTest.addValues(Arrays.asList("a", "b")); 
		classUnderTest.addValues(Arrays.asList("b", "c"));
		classUnderTest.addValues(Arrays.asList("c", "d"));
		classUnderTest.addValues(Arrays.asList("d", "e"));
		
		Assert.assertEquals(1, classUnderTest.getIndexForValues(Arrays.asList("b", "c")));
		Assert.assertEquals(3, classUnderTest.getIndexForValues(Arrays.asList("d", "e")));
	}
	
	@Test
	public void testGetValuesForDimension(){
		classUnderTest = new ExcelRowColumnHeader(Arrays.asList("DIM1", "DIM2")); 
		classUnderTest.addValues(Arrays.asList("a", "b")); 
		classUnderTest.addValues(Arrays.asList("b", "c"));
		classUnderTest.addValues(Arrays.asList("c", "d"));
		classUnderTest.addValues(Arrays.asList("d", "e"));
		
		List<String> valuesForDim1 = classUnderTest.getValuesForDimension("DIM1"); 
		Assert.assertNotNull(valuesForDim1);
		Assert.assertEquals(4, valuesForDim1.size());
		Assert.assertEquals("a", valuesForDim1.get(0));
		Assert.assertEquals("b", valuesForDim1.get(1));
		Assert.assertEquals("c", valuesForDim1.get(2));
		Assert.assertEquals("d", valuesForDim1.get(3));
		
		List<String> valuesForDim2 = classUnderTest.getValuesForDimension("DIM2"); 
		Assert.assertNotNull(valuesForDim2);
		Assert.assertEquals(4, valuesForDim2.size());
		Assert.assertEquals("b", valuesForDim2.get(0));
		Assert.assertEquals("c", valuesForDim2.get(1));
		Assert.assertEquals("d", valuesForDim2.get(2));
		Assert.assertEquals("e", valuesForDim2.get(3));
	}

}
