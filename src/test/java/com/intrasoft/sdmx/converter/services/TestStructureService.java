package com.intrasoft.sdmx.converter.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.exception.SdmxNotImplementedException;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestStructureService {
	
	@Autowired
	private StructureService structureService;

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
	
	@Test(expected = InvalidStructureException.class)
	public void testReadingStructureBeansWithInexistentFile() throws Exception{
	    File dataStructureFile = new File("inexistent_structure_file.xml");
	    SdmxBeans result = structureService.readStructuresFromFile(dataStructureFile);
	}
	
	@Rule
    public ExpectedException expectedException = ExpectedException.none(); 
	
	@Test
    public void testReadingStructureBeansWithNonSdmxFile() throws Exception{
	    expectedException.expect(SdmxNotImplementedException.class);
        expectedException.expectMessage("Can not parse structures.  Structure format is either not supported, or has an invalid syntax");
	    
        File dataStructureFile = new File("./test_files/UOE_NON_FINANCE/csv.csv");
        SdmxBeans result = structureService.readStructuresFromFile(dataStructureFile);
    }
	
	@Test
    public void testReadingStructureBeansWithNonStructureFile() throws Exception{
        expectedException.expect(InvalidStructureException.class);
        expectedException.expectMessage("Structure format is either not supported, or has an invalid syntax");
        
        File dataStructureFile = new File("./test_files/UOE_NON_FINANCE/generic.xml");
        SdmxBeans result = structureService.readStructuresFromFile(dataStructureFile);
    }
	
	@Test
	public void testReadingDataStructureStubsFromSingleStructureFile() throws Exception{
		File dataStructureFile = new File("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml"); 
		Set<StructureIdentifier> structureIdentifiers = structureService.readDataStructureStubsFromFile(dataStructureFile);
		assertNotNull(structureIdentifiers);
		assertFalse(structureIdentifiers.isEmpty());
		assertTrue(structureIdentifiers.size() == 1);
		StructureIdentifier theOnlyIdentifier = structureIdentifiers.iterator().next(); 
		assertEquals("ESTAT", theOnlyIdentifier.getAgency());
		assertEquals("UOE_NON_FINANCE", theOnlyIdentifier.getArtefactId());
		assertEquals("0.4", theOnlyIdentifier.getArtefactVersion());
	}
	
	@Test
	public void testReadingDataStructureStubsFromMultipleStructureFile() throws Exception{
		File dataStructureFile = new File("./test_files/ESTAT_STS/ESTAT_STS-MultipleDSDs.xml"); 
		Set<StructureIdentifier> structureIdentifiers = structureService.readDataStructureStubsFromFile(dataStructureFile);
		assertNotNull(structureIdentifiers);
		assertFalse(structureIdentifiers.isEmpty());
		assertEquals(3, structureIdentifiers.size());
		
		//structures sets don't have a guaranteed ordering
		assertTrue(structureIdentifiers.contains(new StructureIdentifier("ESTAT", "STS", "2.1")));
		assertTrue(structureIdentifiers.contains(new StructureIdentifier("ESTAT", "STS2", "0.8")));
		assertTrue(structureIdentifiers.contains(new StructureIdentifier("ESTAT", "STS1", "0.9")));
	}
	
	@Test
	public void testReadDataflowStubsFromSdmx21File() throws Exception{
		Set<StructureIdentifier> dataflowStubs = structureService.readDataflowStubsFromFile(new File("./test_files/dataflows/sdmx21MultipleDataflowsWithChildren.xml"));
		assertNotNull(dataflowStubs); 
		assertEquals(5, dataflowStubs.size());
		assertTrue(dataflowStubs.contains(new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.1")));
		assertTrue(dataflowStubs.contains(new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.2")));
		assertTrue(dataflowStubs.contains(new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.4")));
		assertTrue(dataflowStubs.contains(new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.5")));
		assertTrue(dataflowStubs.contains(new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.6")));
	}
	
	@Test
	public void testReadDataflowsFromSdmx21File() throws Exception {
		SdmxBeans structureBeans = structureService.readStructuresFromFile(new File("./test_files/dataflows/sdmx21MultipleDataflowsWithChildren.xml"));
		assertNotNull(structureBeans); 
		assertTrue(structureBeans.hasDataflows()); 
		assertTrue(structureBeans.hasDataStructures()); 
		assertEquals(5, structureBeans.getDataflows().size());
		assertEquals(5, structureBeans.getDataStructures().size()); 
	}
}
