package com.intrasoft.sdmx.converter.io.data;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static org.junit.Assert.*;

/**
 * Created by dbalan on 6/13/2017.
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ComponentBufferWriterEngineTest {

    private MockComponentBufferWriterEngine classUnderTest;

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
    public void testWithoutGroups() throws InvalidStructureException, IOException {
        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        classUnderTest = new MockComponentBufferWriterEngine();
        classUnderTest.startDataset(null, dataStructure, null);
        classUnderTest.writeAttributeValue("TIME_PER_COLLECT", "TIME_PER_COLLECT_val1");
        classUnderTest.writeAttributeValue("ORIGIN_CRITERION", "ORIGIN_CRITERION_val1");
        classUnderTest.writeAttributeValue("REF_YEAR_AGES", "REF_YEAR_AGES_val1");
        classUnderTest.writeAttributeValue("REF_PER_START", "REF_PER_START_val1");
        classUnderTest.writeAttributeValue("REF_PER_END", "REF_PER_END_val1");
        classUnderTest.writeAttributeValue("COMPILING_ORG", "COMPILING_ORG_val1");

        classUnderTest.startSeries();
        classUnderTest.writeSeriesKeyValue("TABLE_IDENTIFIER", "TABLE_IDENTIFIER_val1");
        classUnderTest.writeAttributeValue("UNIT_MULT", "UNIT_MULT_val1");

        classUnderTest.writeObservation("TIME_PERIOD", "TIME_PERIOD_val1", "OBS_VAL_val1");
        classUnderTest.writeAttributeValue("OBS_STATUS", "OBS_STATUS_val1");
        classUnderTest.writeAttributeValue("COMMENT_OBS", "COMMENT_OBS_val1");

        classUnderTest.writeObservation("TIME_PERIOD", "TIME_PERIOD_val2","OBS_VAL_val2");
        classUnderTest.writeAttributeValue("OBS_STATUS", "OBS_STATUS_val2");
        classUnderTest.writeAttributeValue("COMMENT_OBS", "COMMENT_OBS_val2");

        classUnderTest.startSeries(null);
        classUnderTest.writeSeriesKeyValue("FREQ", "val2");
        classUnderTest.writeAttributeValue("DECIMALS", "DECIMALS_val2");

        classUnderTest.writeObservation("TIME_PERIOD", "TIME_PERIOD_val3", "OBS_VAL_val3");
        classUnderTest.writeAttributeValue("OBS_STATUS", "OBS_STATUS_val3");
        classUnderTest.writeAttributeValue("COMMENT_OBS", "COMMENT_OBS_val3");

        classUnderTest.close();

        List<Map<String, String>> valuesBuffer = classUnderTest.getValuesWritten();
        assertNotNull(valuesBuffer);
        assertEquals(3, valuesBuffer.size());
        Map<String, String> firstLine = valuesBuffer.get(0);
        assertEquals("TIME_PER_COLLECT_val1", firstLine.get("TIME_PER_COLLECT"));
        assertEquals("TABLE_IDENTIFIER_val1", firstLine.get("TABLE_IDENTIFIER"));
        assertEquals("TIME_PERIOD_val1", firstLine.get("TIME_PERIOD"));
        assertEquals("UNIT_MULT_val1", firstLine.get("UNIT_MULT"));
        assertEquals("OBS_VAL_val1", firstLine.get("OBS_VALUE"));

        Map<String, String> secondLine = valuesBuffer.get(1);
        assertEquals("TIME_PER_COLLECT_val1", secondLine.get("TIME_PER_COLLECT"));
        assertEquals("TABLE_IDENTIFIER_val1", secondLine.get("TABLE_IDENTIFIER"));
        assertEquals("TIME_PERIOD_val2", secondLine.get("TIME_PERIOD"));
        assertEquals("UNIT_MULT_val1", secondLine.get("UNIT_MULT"));
        assertEquals("OBS_VAL_val2", secondLine.get("OBS_VALUE"));

        Map<String, String> thirdLine = valuesBuffer.get(2);
        assertEquals("TIME_PER_COLLECT_val1", thirdLine.get("TIME_PER_COLLECT"));
        assertEquals("", thirdLine.get("TABLE_IDENTIFIER"));
        assertEquals("val2", thirdLine.get("FREQ"));
        assertEquals("TIME_PERIOD_val3", thirdLine.get("TIME_PERIOD"));
        assertEquals("", thirdLine.get("UNIT_MULT"));
        assertEquals("DECIMALS_val2", thirdLine.get("DECIMALS"));
        assertEquals("OBS_VAL_val3", thirdLine.get("OBS_VALUE"));
    }

}
