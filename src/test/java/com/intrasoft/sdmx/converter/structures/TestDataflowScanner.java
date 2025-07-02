package com.intrasoft.sdmx.converter.structures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.exception.SdmxNoResultsException;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestDataflowScanner {
    
    private DataflowScanner classUnderTest;
    
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
    
    @Test
    public void testDataStructureBean() throws InvalidStructureException, IOException {
        SdmxBeans structureBeans = structureService.readStructuresFromFile(
              new File("./test_files/dataflows/sdmx21MultipleDataflowsWithChildren.xml"));
        assertNotNull(structureBeans); 
        
        classUnderTest = new DataflowScanner(structureBeans,
                new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.5"));
        
        DataStructureBean dataStructBean = classUnderTest.getDataStructure(); 
        assertNotNull(dataStructBean); 
        assertEquals("ESTAT", dataStructBean.getAgencyId());
        assertEquals("NA_MAIN", dataStructBean.getId());
        assertEquals("1.5", dataStructBean.getVersion());
    }
    

    @Test
    public void testGetDataflowById() throws Exception {
        SdmxBeans structureBeans = structureService.readStructuresFromFile(new File("./test_files/dataflows/sdmx21MultipleDataflowsWithChildren.xml"));
        assertNotNull(structureBeans);

        classUnderTest = new DataflowScanner(structureBeans,
                new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.5"));
        DataflowBean result = classUnderTest.getDataflow();
        assertNotNull(result);
        assertEquals("ESTAT", result.getAgencyId());
        assertEquals("NAMAIN_STRUVAL_N", result.getId());
        assertEquals("1.5", result.getVersion());
    }

    @Test(expected = SdmxNoResultsException.class)
    public void testGetDataflowByIdWithWrongIdentifier() throws Exception {
        SdmxBeans structureBeans = structureService.readStructuresFromFile(new File("./test_files/dataflows/sdmx21MultipleDataflowsWithChildren.xml"));
        assertNotNull(structureBeans);

        classUnderTest = new DataflowScanner(structureBeans,
                new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "wrongVersion"));
        DataflowBean result = classUnderTest.getDataflow();
    }

    @Test
    public void testFindingDatastructureForSpecificDataflow() throws Exception {
        SdmxBeans structureBeans = structureService.readStructuresFromFile(new File("./test_files/dataflows/sdmx21MultipleDataflowsWithChildren.xml"));
        assertNotNull(structureBeans);

        classUnderTest = new DataflowScanner(structureBeans,
                new StructureIdentifier("ESTAT", "NAMAIN_STRUVAL_N", "1.5"));
        DataStructureBean dataStructureFromDataflow = classUnderTest.getDataStructure();

        assertNotNull(dataStructureFromDataflow);
        assertEquals("ESTAT", dataStructureFromDataflow.getAgencyId());
        assertEquals("NA_MAIN", dataStructureFromDataflow.getId());
        assertEquals("1.5", dataStructureFromDataflow.getVersion());
    }
}
