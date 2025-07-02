package com.intrasoft.sdmx.converter.io.data;

import com.intrasoft.sdmx.converter.io.data.UtilityDataWriterEngine;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.constants.DATASET_ACTION;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.util.Date;
import java.util.logging.Handler;
import java.util.logging.LogManager;

/**
 * Created by todoras on 30-May-17.
 */

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class UtilityDataWriterEngineTest {

    private DataWriterEngine classUnderTest = null;

    @Autowired
    private HeaderService headerService;

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
    public void writeDatasetWithoutGroupsAndObsAttributes() throws Exception {
        DataStructureBean dataStructureBean = structureService.readFirstDataStructure(new File("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml"));

        classUnderTest = new UtilityDataWriterEngine(new FileOutputStream("./target/utilityWithoutGroups.xml"));
        classUnderTest.writeHeader(new HeaderBeanImpl("test header", "test sender"));

        classUnderTest.startDataset(null, dataStructureBean, null, null);
        classUnderTest.writeAttributeValue("TIME_PER_COLLECT", "2014-07-01");
        classUnderTest.writeAttributeValue("ORIGIN_CRITERION", "_Z");
        classUnderTest.writeAttributeValue("REF_YEAR_AGES", "2013-01-01");
        classUnderTest.writeAttributeValue("REF_PER_START", "2012-09-22");
        classUnderTest.writeAttributeValue("REF_PER_END", "2013-06-15");
        classUnderTest.writeAttributeValue("COMPILING_ORG", "_T");

        classUnderTest.startSeries(null);
        classUnderTest.writeAttributeValue("DECIMALS", "0");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");

        classUnderTest.writeSeriesKeyValue("TABLE_IDENTIFIER", "PERS1");
        classUnderTest.writeSeriesKeyValue("FREQ", "A");
        classUnderTest.writeSeriesKeyValue("REF_AREA", "MT");
        classUnderTest.writeSeriesKeyValue("REF_SECTOR", "INST_PRIV_GOV");
        classUnderTest.writeSeriesKeyValue("EDU_TYPE", "_T");
        classUnderTest.writeSeriesKeyValue("ISC11P_LEVEL", "L5T8");
        classUnderTest.writeSeriesKeyValue("ISC11P_CAT", "C4");
        classUnderTest.writeSeriesKeyValue("ISC11P_SUB", "_T");
        classUnderTest.writeSeriesKeyValue("GRADE", "_T");
        classUnderTest.writeSeriesKeyValue("FIELD", "_T");
        classUnderTest.writeSeriesKeyValue("INTENSITY", "PT");
        classUnderTest.writeSeriesKeyValue("COUNTRY_ORIGIN", "W0");
        classUnderTest.writeSeriesKeyValue("COUNTRY_CITIZENSHIP", "W0");
        classUnderTest.writeSeriesKeyValue("SEX", "_T");
        classUnderTest.writeSeriesKeyValue("AGE", "_T");
        classUnderTest.writeSeriesKeyValue("STAT_UNIT", "STU");
        classUnderTest.writeSeriesKeyValue("UNIT_MEASURE", "PER");

        classUnderTest.writeObservation("TIME_PERIOD", "2013", "NaN");
        classUnderTest.writeAttributeValue("OBS_STATUS", "A");
        classUnderTest.writeAttributeValue("OBS_CONF", "F");
        classUnderTest.close(null);
    }

    @Test
    public void testWriteDatasetWithGroups() throws Exception {
        DataStructureBean dataStructureBean = structureService.readFirstDataStructure(new File("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml"));

        classUnderTest = new UtilityDataWriterEngine(   new FileOutputStream("./target/utilityWithGroups.xml"));
        classUnderTest.writeHeader(new HeaderBeanImpl("test", "test sender"));
        classUnderTest.startDataset(null, dataStructureBean,null, null);

        classUnderTest.startGroup("Group1", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "IT");
        classUnderTest.writeGroupKeyValue("FREQ", "M");
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "2");

        classUnderTest.startGroup("Group2", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "DE");
        classUnderTest.writeGroupKeyValue("FREQ", "M");
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "1");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "3");

        classUnderTest.startSeries(null);
        classUnderTest.writeSeriesKeyValue("REF_AREA", "IT");
        classUnderTest.writeSeriesKeyValue("ADJUSTMENT1", "C1");
        classUnderTest.writeSeriesKeyValue("STS_INDICATOR1", "TOVD1");
        classUnderTest.writeSeriesKeyValue("STS_ACTIVITY1", "NS00401");
        classUnderTest.writeSeriesKeyValue("STS_INSTITUTION1", "11");
        classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR1", "20001");
        classUnderTest.writeSeriesKeyValue("FREQ", "M");
        classUnderTest.writeAttributeValue("TIME_FORMAT", "P1M1");

        classUnderTest.writeObservation("TIME_PERIOD", "201306", "100" );
        classUnderTest.writeAttributeValue("OBS_STATUS", "A");
        classUnderTest.writeAttributeValue("OBS_CONF", "F");

        classUnderTest.writeObservation("TIME_PERIOD", "201307", "200");
        classUnderTest.writeAttributeValue("OBS_STATUS", "A");
        classUnderTest.writeAttributeValue("OBS_CONF", "F");

        classUnderTest.startSeries(null);
        classUnderTest.writeSeriesKeyValue("REF_AREA", "IT");
        classUnderTest.writeSeriesKeyValue("ADJUSTMENT2", "C2");
        classUnderTest.writeSeriesKeyValue("STS_INDICATOR2", "TOVD2");
        classUnderTest.writeSeriesKeyValue("STS_ACTIVITY2", "NS00402");
        classUnderTest.writeSeriesKeyValue("STS_INSTITUTION2", "12");
        classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR2", "20002");
        classUnderTest.writeSeriesKeyValue("FREQ", "M");
        classUnderTest.writeAttributeValue("TIME_FORMAT2", "P1M2");

        classUnderTest.writeObservation("TIME_PERIOD", "201306", "300" );
        classUnderTest.writeAttributeValue("OBS_STATUS2", "A2");
        classUnderTest.writeAttributeValue("OBS_CONF2", "F2");

        classUnderTest.writeObservation("TIME_PERIOD", "201306", "400" );
        classUnderTest.writeAttributeValue("OBS_STATUS22", "A2");
        classUnderTest.writeAttributeValue("OBS_CONF22", "F2");

        classUnderTest.startSeries(null);
        classUnderTest.writeSeriesKeyValue("REF_AREA", "DE");
        classUnderTest.writeSeriesKeyValue("ADJUSTMENT3", "C3");
        classUnderTest.writeSeriesKeyValue("STS_INDICATOR3", "TOVD3");
        classUnderTest.writeSeriesKeyValue("STS_ACTIVITY3", "NS00403");
        classUnderTest.writeSeriesKeyValue("STS_INSTITUTION3", "13");
        classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR3", "20003");
        classUnderTest.writeSeriesKeyValue("FREQ", "M");
        classUnderTest.writeAttributeValue("TIME_FORMAT3", "P1M3");

        classUnderTest.writeObservation("TIME_PERIOD", "201306", "500" );
        classUnderTest.writeAttributeValue("OBS_STATUS3", "A3");
        classUnderTest.writeAttributeValue("OBS_CONF3", "F3");

        classUnderTest.writeObservation("TIME_PERIOD", "201306", "600" );
        classUnderTest.writeAttributeValue("OBS_STATUS", "A3");
        classUnderTest.writeAttributeValue("OBS_CONF", "F3");

        classUnderTest.close(null);
    }

    @Test
    public void testWriteHeader() throws Exception {
        DataStructureBean dataStructureBean = structureService.readFirstDataStructure(new File("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml"));

        HeaderBean headerBean = new HeaderBeanImpl("JD014", "BIS");
        headerBean.setAction(DATASET_ACTION.INFORMATION);
        headerBean.setDatasetId("JD015");
        headerBean.setReportingBegin(new Date());
        headerBean.setReportingEnd(new Date());
        headerBean.setTest(true);

        classUnderTest = new UtilityDataWriterEngine(new FileOutputStream("./target/utilityWithHeader.xml"));
        classUnderTest.writeHeader(headerBean);
        classUnderTest.startDataset(null, dataStructureBean, null,null);
        classUnderTest.close();
    }
}
