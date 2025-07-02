package com.intrasoft.sdmx.converter.structures;

import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static org.junit.Assert.*;

/**
 * Created by dbalan on 6/28/2017.
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestDataStructureScanner {

    @Autowired
    private StructureService structureService;

    private DataStructureScanner classUnderTest;

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
    public void exceptionWhenUsingWrongDatastructId() throws InvalidStructureException, IOException {
        SdmxBeans structureBeans = structureService.readStructuresFromFile(
                new File("./test_files/dataflows/sdmx21MultipleDataflowsWithChildren.xml"));
        assertNotNull(structureBeans);

        classUnderTest = new DataStructureScanner(structureBeans,
                new StructureIdentifier("ESTAT", "NA_MAIN", "noSuchVersion"));
        classUnderTest.getDataStructure();
    }

    @Test
    public void testIsCrossSectionalStructureWithNonXsStructure() throws Exception {
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(
                new File("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml"));
        assertNotNull(sdmxBeans);
        classUnderTest = new DataStructureScanner(sdmxBeans,
                new StructureIdentifier("ESTAT", "UOE_NON_FINANCE", "0.4"));

        assertTrue(!classUnderTest.isCrossSectionalDataStructure());
    }

    @Test
    public void testIsCrossSectionalWithXsStructure() throws Exception{
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(
                new File("./test_files/USE_PESTICI/USE_PESTICI+ESTAT+1.1.xml"));
        assertNotNull(sdmxBeans);
        classUnderTest = new DataStructureScanner(sdmxBeans,
                new StructureIdentifier("ESTAT", "USE_PESTICI", "1.1"));
        assertTrue(classUnderTest.isCrossSectionalDataStructure());
    }

    @Test
    public void testHasCrossSectionalMeasureseWithNonXSStructure() throws Exception {
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(
                new File("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml"));
        assertNotNull(sdmxBeans);
        classUnderTest = new DataStructureScanner(sdmxBeans,
                new StructureIdentifier("ESTAT", "UOE_NON_FINANCE", "0.4"));
        assertTrue(!classUnderTest.hasCrossSectionalMeasures());
    }

    @Test
    public void testHasCrossSectionalMeasuresWithXSStructure() throws Exception{
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(
                new File("./test_files/USE_PESTICI/USE_PESTICI+ESTAT+1.1.xml"));
        assertNotNull(sdmxBeans);
        classUnderTest = new DataStructureScanner(sdmxBeans,
                new StructureIdentifier("ESTAT", "USE_PESTICI", "1.1"));
        assertTrue(classUnderTest.hasCrossSectionalMeasures());
    }

    @Test
    public void testGetCodedComponents() throws Exception{
        File dataStructureFile = new File("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml");
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dataStructureFile);
        assertNotNull(sdmxBeans);

        classUnderTest = new DataStructureScanner(sdmxBeans, new StructureIdentifier("ESTAT", "UOE_NON_FINANCE", "0.4"));
        List<String> codedComponents = classUnderTest.getCodedComponentNames();
        assertNotNull(codedComponents);
        assertEquals(27, codedComponents.size());
        assertTrue(codedComponents.contains("TABLE_IDENTIFIER"));
        assertTrue(codedComponents.contains("FREQ"));
        assertTrue(codedComponents.contains("REF_AREA"));
        assertTrue(codedComponents.contains("REF_SECTOR"));
        assertTrue(codedComponents.contains("EDU_TYPE"));
        assertTrue(codedComponents.contains("ISC11P_LEVEL"));
        assertTrue(codedComponents.contains("ISC11P_CAT"));
        assertTrue(codedComponents.contains("ISC11P_SUB"));
        assertTrue(codedComponents.contains("GRADE"));
        assertTrue(codedComponents.contains("FIELD"));
        assertTrue(codedComponents.contains("INTENSITY"));
        assertTrue(codedComponents.contains("COUNTRY_ORIGIN"));
        assertTrue(codedComponents.contains("COUNTRY_CITIZENSHIP"));
        assertTrue(codedComponents.contains("SEX"));
        assertTrue(codedComponents.contains("AGE"));
        assertTrue(codedComponents.contains("STAT_UNIT"));
        assertTrue(codedComponents.contains("UNIT_MEASURE"));
        assertTrue(codedComponents.contains("OBS_STATUS"));
        assertTrue(codedComponents.contains("DECIMALS"));
        assertTrue(codedComponents.contains("UNIT_MULT"));
        assertTrue(codedComponents.contains("ORIGIN_CRITERION"));
    }

    @Test
    public void testGetCodedComponentsForCross20DataStructure() throws Exception{
        File dataStructureFile = new File("./test_files/USE_PESTICI/USE_PESTICI+ESTAT+1.1.xml");
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dataStructureFile);
        assertNotNull(sdmxBeans);
        classUnderTest = new DataStructureScanner(sdmxBeans,
                new StructureIdentifier("ESTAT", "USE_PESTICI", "1.1"));
        List<String> codedComponents = classUnderTest.getCodedComponentNames();
        assertNotNull(codedComponents);
        assertEquals(10, codedComponents.size());
        assertTrue(codedComponents.contains("FREQ"));
        assertTrue(codedComponents.contains("REF_AREA"));
        assertTrue(codedComponents.contains("PESTICIDES"));
        assertTrue(codedComponents.contains("CROPS"));
        assertTrue(codedComponents.contains("MEASURE"));
        assertTrue(codedComponents.contains("OBS_STATUS"));
        assertTrue(codedComponents.contains("CONF_STATUS"));
        assertTrue(codedComponents.contains("UNIT"));
        assertTrue(codedComponents.contains("UNIT_MULT"));
        assertTrue(codedComponents.contains("DECIMALS"));
    }

    @Test
    public void testGetCodedComponentsForCross20DataStructureBisJoint() throws Exception{
        File dataStructureFile = new File("./test_files/BIS_JOINT_DEBT/BIS_JOINT_DEBT_v1.0.xml");
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dataStructureFile);
        assertNotNull(sdmxBeans);

        classUnderTest = new DataStructureScanner(sdmxBeans,
                new StructureIdentifier("BIS", "BIS_JOINT_DEBT", "1.0"));
        List<String> codedComponents = classUnderTest.getCodedComponentNames();
        assertNotNull(codedComponents);
        assertEquals(13, codedComponents.size());
        assertTrue(codedComponents.contains("FREQ"));
        assertTrue(codedComponents.contains("JD_TYPE"));
        assertTrue(codedComponents.contains("JD_CATEGORY"));
        assertTrue(codedComponents.contains("VIS_CTY"));
        assertTrue(codedComponents.contains("AVAILABILITY"));
        assertTrue(codedComponents.contains("TIME_FORMAT"));
        assertTrue(codedComponents.contains("COLLECTION"));
        assertTrue(codedComponents.contains("DECIMALS"));
        assertTrue(codedComponents.contains("OBS_CONF"));
        assertTrue(codedComponents.contains("OBS_STATUS"));
        assertTrue(codedComponents.contains("BIS_UNIT"));
        assertTrue(codedComponents.contains("UNIT_MULT"));
    }
    //SDMXCONV-1050
    // This method compares two strings
    // lexicographically without using
    // library functions
    public int stringCompare(String str1, String str2)
    {

        int l1 = str1.length();
        int l2 = str2.length();
        int lmin = Math.min(l1, l2);

        for (int i = 0; i < lmin; i++) {
            int str1_ch = (int)str1.charAt(i);
            int str2_ch = (int)str2.charAt(i);

            if (str1_ch != str2_ch) {
                return str1_ch - str2_ch;
            }
        }

        // Edge case for strings like
        // String 1="Geeks" and String 2="Geeksforgeeks"
        if (l1 != l2) {
            return l1 - l2;
        }

        // If none of the above conditions is true,
        // it implies both the strings are equal
        else {
            return 0;
        }
    }
    @Test
    public void testSortSet() throws Exception{
        File dataStructureFile = new File("./test_files/BIS_JOINT_DEBT/BIS_JOINT_DEBT_v1.0.xml");
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dataStructureFile);

        classUnderTest = new DataStructureScanner(sdmxBeans, new StructureIdentifier("BIS", "BIS_JOINT_DEBT", "1.0"));

        List<String> codedComponents = classUnderTest.getCodedComponentNames();
        Set<String> codedComponentsSet = structureService.convertToSet(codedComponents);

        Set<String> sortedCodedComponentsSet = structureService.sort(codedComponentsSet);
        //uncheck the line below if you want to make it fail, by comparing a not sorted list's values
//        sortedCodedComponentsSet = codedComponentsSet;

        List<String> sortedCodedComponentsList = structureService.convertToList(sortedCodedComponentsSet);

        String tmp = sortedCodedComponentsList.get(0);
        for (int i = 1; i < sortedCodedComponentsList.size(); i++) {
            assertTrue( stringCompare(tmp, sortedCodedComponentsList.get(i))<0);
            tmp = sortedCodedComponentsList.get(i);
        }

    }


}
