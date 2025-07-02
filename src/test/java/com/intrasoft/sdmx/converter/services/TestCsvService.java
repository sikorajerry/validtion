package com.intrasoft.sdmx.converter.services;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvColumn;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvOutColumnMapping;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidColumnMappingException;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestCsvService {
	
	@Autowired
	private CsvService csvService; 
	
	@Rule
    public ExpectedException expectedException = ExpectedException.none();

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
	public void testCheckCorrectMappingFile() throws Exception{
		List<String> dimensionsFromDsd = 
		Arrays.asList("TABLE_IDENTIFIER", "FREQ", "REF_AREA", "REF_SECTOR", "EDU_TYPE", "ISC11P_LEVEL", "ISC11P_CAT", "ISC11P_SUB", "GRADE", "FIELD", 
				"INTENSITY", "COUNTRY_ORIGIN", "COUNTRY_CITIZENSHIP", "SEX", "AGE", "STAT_UNIT", "UNIT_MEASURE", "TIME_PERIOD", "OBS_VALUE", 
				"OBS_STATUS", "COMMENT_OBS", "TIME_PER_COLLECT", "ORIGIN_CRITERION", "REF_YEAR_AGES", "REF_PER_START", "REF_PER_END", "COMPILING_ORG", 
				"DECIMALS", "UNIT_MULT");
		
		File mappingFile = new File("./test_files/UOE_NON_FINANCE/mapping3Levels.xml"); 
		csvService.checkColumnMappingFile(mappingFile, dimensionsFromDsd, 3);
	}
	
	@Test
	public void testCheckInexistentMappingFile() throws Exception{
	    expectedException.expect(InvalidColumnMappingException.class);
	    expectedException.expectMessage("Error reading the column mapping file from disk");
	    	    
		List<String> dimensionsFromDsd = Arrays.asList("TABLE_IDENTIFIER", "FREQ");
		
		File mappingFile = new File("this_file_does_not_exist.txt"); 
		csvService.checkColumnMappingFile(mappingFile, dimensionsFromDsd, 3);
	}
	
	@Test
	public void testCheckNonXmlMappingFile() throws Exception{
		expectedException.expect(InvalidColumnMappingException.class);
		expectedException.expectMessage("Error parsing the column mapping file from disk");
	    
	    List<String> dimensionsFromDsd = Arrays.asList("TABLE_IDENTIFIER", "FREQ");
		
		File mappingFile = new File("./test_files/UOE_NON_FINANCE/csv.csv"); 
		csvService.checkColumnMappingFile(mappingFile, dimensionsFromDsd, 3);
	}
	
	@Test
	public void testCheckMappingFileWithDifferentNumberOfDimensions() throws Exception{
	    expectedException.expect(InvalidColumnMappingException.class);
        expectedException.expectMessage("Invalid mapping file:REF_AREA does not belong to the selected structure");        
	    
        List<String> dimensionsFromDsd = Arrays.asList("TABLE_IDENTIFIER", "FREQ");		
		
	    File mappingFile = new File("./test_files/UOE_NON_FINANCE/mapping3Levels.xml"); 
		csvService.checkColumnMappingFile(mappingFile, dimensionsFromDsd, 3);
	}
		
	@Test
	public void testCheckMappingFileWithDifferentDimensions() throws Exception{
	    expectedException.expect(InvalidColumnMappingException.class);
        expectedException.expectMessage("Invalid mapping file:UNIT_MULT does not belong to the selected structure");
	    List<String> dimensionsFromDsd = 
		Arrays.asList(
				"TABLE_IDENTIFIER", "FREQ", "REF_AREA", "REF_SECTOR", "EDU_TYPE", "ISC11P_LEVEL", "ISC11P_CAT", "ISC11P_SUB", "GRADE", "FIELD", 
				"INTENSITY", "COUNTRY_ORIGIN", "COUNTRY_CITIZENSHIP", "SEX", "AGE", "STAT_UNIT", "UNIT_MEASURE", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", 
				"COMMENT_OBS", "TIME_PER_COLLECT", "ORIGIN_CRITERION", "REF_YEAR_AGES", "REF_PER_START", "REF_PER_END", "COMPILING_ORG", "DECIMALS", 
				"This dimension will not match");
		
		File mappingFile = new File("./test_files/UOE_NON_FINANCE/mapping3Levels.xml"); 
		csvService.checkColumnMappingFile(mappingFile, dimensionsFromDsd, 3);
	}
	
	@Test
    public void testCheckMappingFileWithNonIntegerLevel() throws Exception{
        expectedException.expect(InvalidColumnMappingException.class);
        expectedException.expectMessage("Error reading the levels in the column mapping file: One of the levels is not a number");
        List<String> dimensionsFromDsd = 
        Arrays.asList(
                "TABLE_IDENTIFIER", "FREQ", "REF_AREA", "REF_SECTOR", "EDU_TYPE", "ISC11P_LEVEL", "ISC11P_CAT", "ISC11P_SUB", "GRADE", "FIELD", 
                "INTENSITY", "COUNTRY_ORIGIN", "COUNTRY_CITIZENSHIP", "SEX", "AGE", "STAT_UNIT", "UNIT_MEASURE", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", 
                "COMMENT_OBS", "TIME_PER_COLLECT", "ORIGIN_CRITERION", "REF_YEAR_AGES", "REF_PER_START", "REF_PER_END", "COMPILING_ORG", "DECIMALS", 
                "UNIT_MULT");
        
        File mappingFile = new File("./test_files/UOE_NON_FINANCE/mappingWithNonIntegerLevels.xml"); 
        csvService.checkColumnMappingFile(mappingFile, dimensionsFromDsd, 3);
    }
	
	@Test
    public void testCheckMappingFileWithLevelsHigherThanDeclared() throws Exception{
        expectedException.expect(InvalidColumnMappingException.class);
        expectedException.expectMessage("The mapping for COMMENT_OBS has a level of 3 higher than expected (2)");
        List<String> dimensionsFromDsd = 
        Arrays.asList(
                "TABLE_IDENTIFIER", "FREQ", "REF_AREA", "REF_SECTOR", "EDU_TYPE", "ISC11P_LEVEL", "ISC11P_CAT", "ISC11P_SUB", "GRADE", "FIELD", 
                "INTENSITY", "COUNTRY_ORIGIN", "COUNTRY_CITIZENSHIP", "SEX", "AGE", "STAT_UNIT", "UNIT_MEASURE", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", 
                "COMMENT_OBS", "TIME_PER_COLLECT", "ORIGIN_CRITERION", "REF_YEAR_AGES", "REF_PER_START", "REF_PER_END", "COMPILING_ORG", "DECIMALS", 
                "UNIT_MULT");
        
        File mappingFile = new File("./test_files/UOE_NON_FINANCE/mapping3Levels.xml"); 
        csvService.checkColumnMappingFile(mappingFile, dimensionsFromDsd, 2);
    }

	@Test
	public void testReadColumnHeaders(){
		List<CsvColumn> columnHeaders = csvService.readColumnHeaders(new File("./test_files/UOE_NON_FINANCE/csv.csv"), ";", true);
		assertNotNull(columnHeaders); 
		assertEquals(29, columnHeaders.size());
		
		CsvColumn toBeTested = columnHeaders.get(0);
		assertNotNull(toBeTested);
		assertEquals("TABLE_IDENTIFIER", toBeTested.getConcept());
		assertEquals(Integer.valueOf(1), toBeTested.getIndex());
		
		toBeTested = columnHeaders.get(1);
		assertEquals("FREQ", toBeTested.getConcept());
		assertEquals(Integer.valueOf(2), toBeTested.getIndex());
		
		toBeTested = columnHeaders.get(28);
		assertEquals("UNIT_MULT", toBeTested.getConcept());
		assertEquals(Integer.valueOf(29), toBeTested.getIndex());
	}
	
	@Test
    public void testReadColumnHeadersWithGeneratedHeaders(){
        List<CsvColumn> columnHeaders = csvService.readColumnHeaders(new File("./test_files/UOE_NON_FINANCE/csv.csv"), ";", false);
        assertNotNull(columnHeaders); 
        assertEquals(29, columnHeaders.size());
        
        CsvColumn toBeTested = columnHeaders.get(0);
        assertNotNull(toBeTested);
        assertEquals("Column 1", toBeTested.getHeader());
        assertEquals(Integer.valueOf(1), toBeTested.getIndex());
        
        toBeTested = columnHeaders.get(1);
        assertEquals("Column 2", toBeTested.getHeader());
        assertEquals(Integer.valueOf(2), toBeTested.getIndex());
        
        toBeTested = columnHeaders.get(28);
        assertEquals("Column 29", toBeTested.getHeader());
        assertEquals(Integer.valueOf(29), toBeTested.getIndex());
    }
	
	@Test
	public void testGetUsedColumnsIndexesInMapping(){
	    Map<String, CsvInColumnMapping> mappings = new LinkedHashMap<String, CsvInColumnMapping>(); 
	    
	    CsvInColumnMapping firstMapping = new CsvInColumnMapping();
        firstMapping.setColumns(Arrays.asList(new CsvColumn[]{new CsvColumn(0)}));
        firstMapping.setFixed(false); 
        firstMapping.setLevel(Integer.valueOf(3)); 
        firstMapping.setFixedValue(""); 
        mappings.put("DIM 1", firstMapping);
        
        CsvInColumnMapping secondMapping = new CsvInColumnMapping();
        secondMapping.setColumns(Arrays.asList(new CsvColumn[]{
                                     new CsvColumn(1), 
                                     new CsvColumn(6), 
                                     new CsvColumn(2)}));
        secondMapping.setFixed(false); 
        secondMapping.setLevel(Integer.valueOf(2)); 
        secondMapping.setFixedValue("caca"); 
        mappings.put("ATTR 2", secondMapping);
        
        List<Integer> columnsUsed = csvService.getIndexesOfColumnsInMapping(mappings); 
        assertNotNull(columnsUsed); 
        assertEquals(4, columnsUsed.size());
        assertEquals(Integer.valueOf(0), columnsUsed.get(0)); 
        assertEquals(Integer.valueOf(1), columnsUsed.get(1)); 
        assertEquals(Integer.valueOf(6), columnsUsed.get(2)); 
        assertEquals(Integer.valueOf(2), columnsUsed.get(3)); 
	}
	
	@Test
    public void testGetUsedHeadersInMapping(){
        Map<String, CsvInColumnMapping> mappings = new LinkedHashMap<String, CsvInColumnMapping>(); 
        
        CsvInColumnMapping firstMapping = new CsvInColumnMapping();
        firstMapping.setColumns(Arrays.asList(new CsvColumn[]{new CsvColumn(1)}));
        firstMapping.setFixed(false); 
        firstMapping.setLevel(Integer.valueOf(3)); 
        firstMapping.setFixedValue(""); 
        mappings.put("DIM 1", firstMapping);
        
        CsvInColumnMapping secondMapping = new CsvInColumnMapping();
        secondMapping.setColumns(Arrays.asList(new CsvColumn[]{
                                     new CsvColumn(2),
                                     new CsvColumn(7),
                                     new CsvColumn(3)}));
        secondMapping.setFixed(false); 
        secondMapping.setLevel(Integer.valueOf(2)); 
        secondMapping.setFixedValue("caca"); 
        mappings.put("ATTR 2", secondMapping);
        
        List<String> columnsUsed = csvService.getHeadersOfColumnsInMapping(mappings); 
        assertNotNull(columnsUsed); 
        assertEquals(4, columnsUsed.size());
        assertEquals("Column 1", columnsUsed.get(0)); 
        assertEquals("Column 2", columnsUsed.get(1)); 
        assertEquals("Column 7", columnsUsed.get(2)); 
        assertEquals("Column 3", columnsUsed.get(3)); 
    }
	
	@Test
    public void testGetUsedColumnsInMapping(){
        Map<String, CsvInColumnMapping> mappings = new LinkedHashMap<String, CsvInColumnMapping>(); 
        
        CsvInColumnMapping firstMapping = new CsvInColumnMapping();
        firstMapping.setColumns(Arrays.asList(new CsvColumn[]{new CsvColumn(1, "Column 1", "DIM 1")}));
        firstMapping.setFixed(false); 
        firstMapping.setLevel(Integer.valueOf(3)); 
        firstMapping.setFixedValue(""); 
        mappings.put("DIM 1", firstMapping);
        
        CsvInColumnMapping secondMapping = new CsvInColumnMapping();
        secondMapping.setColumns(Arrays.asList(new CsvColumn[]{
                                     new CsvColumn(2, "Column 2", "ATTR 2"),
                                     new CsvColumn(7),
                                     new CsvColumn(3)}));
        secondMapping.setFixed(false); 
        secondMapping.setLevel(Integer.valueOf(2)); 
        secondMapping.setFixedValue("caca"); 
        mappings.put("ATTR 2", secondMapping);
        
        List<CsvColumn> columnsUsed = csvService.getColumnsInMapping(mappings); 
        assertNotNull(columnsUsed); 
        assertEquals(4, columnsUsed.size());
        assertNotNull(columnsUsed.get(0));
        assertEquals("DIM 1", columnsUsed.get(0).getConcept());
        assertEquals("Column 1", "Column "+ columnsUsed.get(0).getUserFriendlyIdx());
        assertNotNull(columnsUsed.get(1)); 
        assertEquals("ATTR 2", columnsUsed.get(1).getConcept());
		assertEquals("Column 2", "Column "+ columnsUsed.get(1).getUserFriendlyIdx());
        assertNotNull(columnsUsed.get(2));
        assertEquals("ATTR 2", columnsUsed.get(2).getConcept());
		assertEquals("Column 7", "Column "+ columnsUsed.get(2).getUserFriendlyIdx());
        assertNotNull(columnsUsed.get(3));
        assertEquals("ATTR 2", columnsUsed.get(3).getConcept());
		assertEquals("Column 3", "Column "+ columnsUsed.get(3).getUserFriendlyIdx());
    }
	
	@Test
	public void initInputColumnMappingWithMoreColumnsThanComponents(){
	    List<String> componentsToMap = Arrays.asList("DIM1", "DIM2", "ATTR1", "ATTR2"); 
	    List<CsvColumn> columnHeaders = Arrays.asList(new CsvColumn(1, "Column 1","ATTR2"),
	                                                  new CsvColumn(2, "Column 2", "ATTR3"),
	                                                  new CsvColumn(3, "Column 3","ATTR4"),
	                                                  new CsvColumn(4, "Column 4","DIM1"),
	                                                  new CsvColumn(5, "Column 5","DIM2"),
	                                                  new CsvColumn(6, "Column 6","DIM3"),
	                                                  new CsvColumn(7, "Column 7","ATTR1"));
	    Map<String, CsvInColumnMapping> result = csvService.initInputColumnMappingWithCsvHeaders(componentsToMap, columnHeaders);
	    
	    assertNotNull(result);
	    assertEquals(componentsToMap.size(), result.size());
	    
	    CsvInColumnMapping mappingForDim1 = result.get("DIM1");
	    assertNotNull(mappingForDim1); 
	    assertTrue(!mappingForDim1.isFixed()); 
	    assertEquals(1, mappingForDim1.getLevel().intValue());
	    assertNotNull(mappingForDim1.getColumns());
	    assertEquals(1, mappingForDim1.getColumns().size());
	    assertEquals(new CsvColumn(4, "Column 4", "DIM1").toString(), mappingForDim1.getColumns().get(0).toString());
	    
	    CsvInColumnMapping mappingForDim2 = result.get("DIM2");
	    assertNotNull(mappingForDim2); 
        assertTrue(!mappingForDim2.isFixed()); 
        assertEquals(1, mappingForDim2.getLevel().intValue());
        assertNotNull(mappingForDim2.getColumns());
        assertEquals(1, mappingForDim2.getColumns().size());
        assertEquals(new CsvColumn(5, "Column 5","DIM2").toString(), mappingForDim2.getColumns().get(0).toString());
        
        CsvInColumnMapping mappingForAttr1 = result.get("ATTR1");
        assertNotNull(mappingForAttr1);
        assertTrue(!mappingForAttr1.isFixed()); 
        assertEquals(1, mappingForAttr1.getLevel().intValue());
        assertNotNull(mappingForAttr1.getColumns());
        assertEquals(1, mappingForAttr1.getColumns().size());
        assertEquals(new CsvColumn(7, "Column 7","ATTR1").toString(), mappingForAttr1.getColumns().get(0).toString());
        
        CsvInColumnMapping mappingForAttr2 = result.get("ATTR2");
        assertNotNull(mappingForAttr2);
        assertTrue(!mappingForAttr2.isFixed()); 
        assertEquals(1, mappingForAttr2.getLevel().intValue());
        assertNotNull(mappingForAttr2.getColumns());
        assertEquals(1, mappingForAttr2.getColumns().size());
        assertEquals(new CsvColumn(1, "Column 1","ATTR2").toString(), mappingForAttr2.getColumns().get(0).toString());
	}
	
	@Test
    public void initInputColumnMappingWithNonMatchingColumns(){
        List<String> componentsToMap = Arrays.asList("DIM1", "DIM2", "ATTR1", "ATTR2"); 
        List<CsvColumn> columnHeaders = Arrays.asList(new CsvColumn(1, "Column 1", "mama"),
                                                      new CsvColumn(2, "Column 2", "are"),
                                                      new CsvColumn(3, "Column 3", "mere"),
                                                      new CsvColumn(4, "Column 4", "iar"),
                                                      new CsvColumn(5, "Column 5", "ana"),
                                                      new CsvColumn(6, "Column 6", "are"),
                                                      new CsvColumn(7, "Column 7", "struguri"));
        Map<String, CsvInColumnMapping> result = csvService.initInputColumnMappingWithCsvHeaders(componentsToMap, columnHeaders);
        
        assertNotNull(result);
        
        assertEquals(4 ,result.size());
        
        CsvInColumnMapping mappingForDim1 = result.get("DIM1");
        assertNotNull(mappingForDim1); 
        assertTrue(!mappingForDim1.isFixed()); 
        assertEquals(1, mappingForDim1.getLevel().intValue());
        assertNotNull(mappingForDim1.getColumns());
        assertEquals(0, mappingForDim1.getColumns().size());
        
        CsvInColumnMapping mappingForDim2 = result.get("DIM2");
        assertNotNull(mappingForDim2); 
        assertTrue(!mappingForDim2.isFixed()); 
        assertEquals(1, mappingForDim2.getLevel().intValue());
        assertNotNull(mappingForDim2.getColumns());
        assertEquals(0, mappingForDim2.getColumns().size());
        
        CsvInColumnMapping mappingForAttr1 = result.get("ATTR1");
        assertNotNull(mappingForAttr1);
        assertTrue(!mappingForAttr1.isFixed()); 
        assertEquals(1, mappingForAttr1.getLevel().intValue());
        assertNotNull(mappingForAttr1.getColumns());
       assertEquals(0, mappingForAttr1.getColumns().size());
        
        CsvInColumnMapping mappingForAttr2 = result.get("ATTR2");
        assertNotNull(mappingForAttr2);
        assertTrue(!mappingForAttr2.isFixed()); 
        assertEquals(1, mappingForAttr2.getLevel().intValue());
        assertNotNull(mappingForAttr2.getColumns());
        assertEquals(0, mappingForAttr2.getColumns().size());
    }
	
	@Test
    public void initInputColumnMappingWithNoColumns(){
        List<String> componentsToMap = Arrays.asList("DIM1", "DIM2", "ATTR1", "ATTR2"); 
        List<CsvColumn> columnHeaders = new ArrayList<>(); 
        Map<String, CsvInColumnMapping> result = csvService.initInputColumnMappingWithCsvHeaders(componentsToMap, columnHeaders);
        
        assertNotNull(result);
        assertEquals(componentsToMap.size(), result.size());
        
        CsvInColumnMapping mappingForDim1 = result.get("DIM1");
        assertNotNull(mappingForDim1); 
        assertTrue(!mappingForDim1.isFixed()); 
        assertEquals(1, mappingForDim1.getLevel().intValue());
        assertNotNull(mappingForDim1.getColumns());
        assertEquals(0, mappingForDim1.getColumns().size());
        
        CsvInColumnMapping mappingForDim2 = result.get("DIM2");
        assertNotNull(mappingForDim2); 
        assertTrue(!mappingForDim2.isFixed()); 
        assertEquals(1, mappingForDim2.getLevel().intValue());
        assertNotNull(mappingForDim2.getColumns());
        assertEquals(0, mappingForDim2.getColumns().size());
        
        CsvInColumnMapping mappingForAttr1 = result.get("ATTR1");
        assertNotNull(mappingForAttr1);
        assertTrue(!mappingForAttr1.isFixed()); 
        assertEquals(1, mappingForAttr1.getLevel().intValue());
        assertNotNull(mappingForAttr1.getColumns());
        assertEquals(0, mappingForAttr1.getColumns().size());
        
        CsvInColumnMapping mappingForAttr2 = result.get("ATTR2");
        assertNotNull(mappingForAttr2);
        assertTrue(!mappingForAttr2.isFixed()); 
        assertEquals(1, mappingForAttr2.getLevel().intValue());
        assertNotNull(mappingForAttr2.getColumns());
        assertEquals(0, mappingForAttr2.getColumns().size());
    }
	
	@Test
	public void initInputColumnMappingWithPartialMatchingBetweenColumnsAndComponents(){
	    List<String> componentsToMap = Arrays.asList("DIM1", "DIM2", "ATTR1", "ATTR2"); 
	    List<CsvColumn> columnHeaders = Arrays.asList(new CsvColumn(1, "Column 1", "mama"),
	                                                  new CsvColumn(2, "Column 2", "ATTR3"),
	                                                  new CsvColumn(3, "Column 3", "ATTR4"),
	                                                  new CsvColumn(4, "Column 4", "DIM1"),
	                                                  new CsvColumn(5, "Column 5", "tata"),
	                                                  new CsvColumn(6, "Column 6", "DIM3"),
	                                                  new CsvColumn(7, "Column 7", "frati"));
      
	    Map<String, CsvInColumnMapping> result = csvService.initInputColumnMappingWithCsvHeaders(componentsToMap, columnHeaders);

        assertNotNull(result);
        assertEquals(componentsToMap.size(), result.size());
        
        CsvInColumnMapping mappingForDim1 = result.get("DIM1");
        assertNotNull(mappingForDim1); 
        assertTrue(!mappingForDim1.isFixed()); 
        assertEquals(1, mappingForDim1.getLevel().intValue());
        assertNotNull(mappingForDim1.getColumns());
        assertEquals(1, mappingForDim1.getColumns().size());
        assertEquals(new CsvColumn(4, "Column 4", "DIM1").toString(), mappingForDim1.getColumns().get(0).toString());
        
        CsvInColumnMapping mappingForDim2 = result.get("DIM2");
        assertNotNull(mappingForDim2); 
        assertTrue(!mappingForDim2.isFixed()); 
        assertEquals(1, mappingForDim2.getLevel().intValue());
        assertNotNull(mappingForDim2.getColumns());
        assertEquals(0, mappingForDim2.getColumns().size());
        
        CsvInColumnMapping mappingForAttr1 = result.get("ATTR1");
        assertNotNull(mappingForAttr1);
        assertTrue(!mappingForAttr1.isFixed()); 
        assertEquals(1, mappingForAttr1.getLevel().intValue());
        assertNotNull(mappingForAttr1.getColumns());
        assertEquals(0, mappingForAttr1.getColumns().size());
        
        CsvInColumnMapping mappingForAttr2 = result.get("ATTR2");
        assertNotNull(mappingForAttr2);
        assertTrue(!mappingForAttr2.isFixed()); 
        assertEquals(1, mappingForAttr2.getLevel().intValue());
        assertNotNull(mappingForAttr2.getColumns());
	    assertEquals(0, mappingForAttr2.getColumns().size());
		
	}
	
	@Test
	public void initInputColumnMappingWhenHeaderRowIsNotUsed(){
	    List<String> componentsToMap = Arrays.asList("DIM1", "DIM2", "ATTR1", "ATTR2"); 
	    Map<String, CsvInColumnMapping> result = csvService.initInputColumnMappingWithoutCsvHeaders(componentsToMap);

	    assertNotNull(result);
        assertEquals(componentsToMap.size(), result.size());
       
        CsvInColumnMapping mappingForDim1 = result.get("DIM1");
        assertNotNull(mappingForDim1); 
        assertTrue(!mappingForDim1.isFixed()); 
        assertEquals(1, mappingForDim1.getLevel().intValue());
        assertNotNull(mappingForDim1.getColumns());
        assertEquals(1, mappingForDim1.getColumns().size());
        assertEquals(new CsvColumn(0, "Column 1").toString(), result.get("DIM1").getColumns().get(0).toString());
       
        CsvInColumnMapping mappingForDim2 = result.get("DIM2");
        assertNotNull(mappingForDim2); 
        assertTrue(!mappingForDim2.isFixed()); 
        assertEquals(1, mappingForDim2.getLevel().intValue());
        assertNotNull(mappingForDim2.getColumns());
        assertEquals(1, mappingForDim2.getColumns().size());
        assertEquals(new CsvColumn(1, "Column 2").toString(), result.get("DIM2").getColumns().get(0).toString());

        CsvInColumnMapping mappingForAttr1 = result.get("ATTR1");
        assertNotNull(mappingForAttr1);
        assertTrue(!mappingForAttr1.isFixed()); 
        assertEquals(1, mappingForAttr1.getLevel().intValue());
        assertNotNull(mappingForAttr1.getColumns());
        assertEquals(1, mappingForAttr1.getColumns().size());
        assertEquals(new CsvColumn(2, "Column 3").toString(), result.get("ATTR1").getColumns().get(0).toString());

        CsvInColumnMapping mappingForAttr2 = result.get("ATTR2");
        assertNotNull(mappingForAttr2);
        assertTrue(!mappingForAttr2.isFixed()); 
        assertEquals(1, mappingForAttr2.getLevel().intValue());
        assertNotNull(mappingForAttr2.getColumns());
        assertEquals(new CsvColumn(3, "Column 4").toString(), result.get("ATTR2").getColumns().get(0).toString());

	}
	
	@Test
	public void initOutputColumnMapping(){
	    List<String> components = Arrays.asList("DIM1", "DIM2", "ATTR1", "ATTR2"); 
	    List<CsvOutColumnMapping> result = csvService.initOutputColumnMapping(components);
	    
	    assertNotNull(result);
	    assertEquals(4, result.size());
	    
	    CsvOutColumnMapping mapping = result.get(0);
	    assertNotNull(mapping); 
	    assertEquals(NumberUtils.INTEGER_ONE, mapping.getLevel());
	    assertEquals("DIM1", mapping.getComponent());
	    
	    mapping = result.get(1);
        assertNotNull(mapping); 
        assertEquals(NumberUtils.INTEGER_ONE, mapping.getLevel());
        assertEquals("DIM2", mapping.getComponent());
	    
        mapping = result.get(2);
        assertNotNull(mapping); 
        assertEquals(NumberUtils.INTEGER_ONE, mapping.getLevel());
        assertEquals("ATTR1", mapping.getComponent());
        
        mapping = result.get(3);
        assertNotNull(mapping); 
        assertEquals(NumberUtils.INTEGER_ONE, mapping.getLevel());
        assertEquals("ATTR2", mapping.getComponent());
	}

	@Test
	public void correctReadOfOutputColumnMappingFromAFileWithOneLevel() throws InvalidColumnMappingException{
        MultiLevelCsvOutColMapping mapping = csvService.buildOutputDimensionLevelMapping(new File("./test_files/UOE_NON_FINANCE/mappingWithFirstTwoColumnsSwitched.xml"));
        assertNotNull(mapping);
        assertFalse(mapping.isEmpty());
        assertNotNull(mapping.getMappedComponentsForLevel(1));
        assertFalse(mapping.getMappedComponentsForLevel(1).isEmpty());
        assertEquals("FREQ", mapping.getMappedComponentsForLevel(1).get(0));
        assertEquals("TABLE_IDENTIFIER", mapping.getMappedComponentsForLevel(1).get(1));
    }

    @Test
    public void correctReadOfOutputColumnMappingFromAFileWithThreeLevels() throws InvalidColumnMappingException, IOException {
        try (FileInputStream fileInputStream = new FileInputStream("./test_files/UOE_NON_FINANCE/mapping3Levels.xml")) {
            MultiLevelCsvOutColMapping mapping = csvService.buildOutputDimensionLevelMapping(fileInputStream);
            assertNotNull(mapping);
            assertFalse(mapping.isEmpty());
            assertNotNull(mapping.getMappedComponentsForLevel(1));
            assertNotNull(mapping.getMappedComponentsForLevel(2));
            assertNotNull(mapping.getMappedComponentsForLevel(3));
            assertFalse(mapping.getMappedComponentsForLevel(1).isEmpty());
            assertFalse(mapping.getMappedComponentsForLevel(2).isEmpty());
            assertFalse(mapping.getMappedComponentsForLevel(3).isEmpty());

            assertEquals("TABLE_IDENTIFIER", mapping.getMappedComponentsForLevel(1).get(0));
            assertEquals("INTENSITY", mapping.getMappedComponentsForLevel(2).get(0));
            assertEquals("COMMENT_OBS", mapping.getMappedComponentsForLevel(3).get(0));
        }
    }
}
