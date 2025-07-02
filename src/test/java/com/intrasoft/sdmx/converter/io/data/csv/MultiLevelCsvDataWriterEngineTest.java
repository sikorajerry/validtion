package com.intrasoft.sdmx.converter.io.data.csv;

import com.intrasoft.sdmx.converter.services.StructureService;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class MultiLevelCsvDataWriterEngineTest {

	private MultilevelCsvDataWriterEngine classUnderTest = null;

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
    public void test() throws Exception{
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./test_files/ESTAT_STS/dsd.xml");
        String file = "./target/MultilevelCsvFromGeneric20WithGroups.txt";

        OutputStream outputStream =  new FileOutputStream(file);

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setLevels(3);
        csvOutputConfig.setDelimiter(";");
		
		LinkedHashMap<String, LinkedHashMap<String,String>> transcoding = new LinkedHashMap<String, LinkedHashMap<String,String>>();
        LinkedHashMap<String,String> valueForRefAreaCode = new LinkedHashMap<>();
	    valueForRefAreaCode.put("IT", "ITALY");
	    transcoding.put("REF_AREA", valueForRefAreaCode);
        LinkedHashMap<String,String> valueForDecimalsCode = new LinkedHashMap<>();
	    valueForDecimalsCode.put("TOVD", "STSTRANSVALUE");
	    transcoding.put("STS_INDICATOR", valueForDecimalsCode);
	    
	    csvOutputConfig.setTranscoding(transcoding);
				
        MultiLevelCsvOutColMapping mappings = new MultiLevelCsvOutColMapping();
        mappings.addMapping(1, 1, "REF_AREA");
        mappings.addMapping(1, 2, "ADJUSTMENT");
        mappings.addMapping(1, 3, "STS_INDICATOR");

        mappings.addMapping(2, 1, "STS_ACTIVITY");
        mappings.addMapping(2,2, "STS_INSTITUTION");
        mappings.addMapping(2,3, "STS_BASE_YEAR");

        mappings.addMapping(3,1, "UNIT");
        mappings.addMapping(3,2, "TIME_PERIOD");
        mappings.addMapping(3,3, "OBS_VALUE");

        csvOutputConfig.setColumnMapping(mappings);
                     		           
        classUnderTest = new MultilevelCsvDataWriterEngine(outputStream, csvOutputConfig);

        classUnderTest.startDataset(null,
                dataStructure,
                null,
                null);
        classUnderTest.startGroup("sibling", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "IT");
        classUnderTest.writeGroupKeyValue("ADJUSTMENT", "C" );
        classUnderTest.writeGroupKeyValue("STS_INDICATOR","TOVD" );
        classUnderTest.writeGroupKeyValue("STS_ACTIVITY", "NS0040" );
        classUnderTest.writeGroupKeyValue("STS_INSTITUTION","1");
        classUnderTest.writeGroupKeyValue("STS_BASE_YEAR","2000" );
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "2");

        classUnderTest.startGroup("sibling", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "IT");
        classUnderTest.writeGroupKeyValue("ADJUSTMENT", "C" );
        classUnderTest.writeGroupKeyValue("STS_INDICATOR","TOVT" );
        classUnderTest.writeGroupKeyValue("STS_ACTIVITY", "NS0040" );
        classUnderTest.writeGroupKeyValue("STS_INSTITUTION","1");
        classUnderTest.writeGroupKeyValue("STS_BASE_YEAR","2000" );
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "2");

        classUnderTest.startGroup("sibling", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "IT");
        classUnderTest.writeGroupKeyValue("ADJUSTMENT", "S" );
        classUnderTest.writeGroupKeyValue("STS_INDICATOR","PROD" );
        classUnderTest.writeGroupKeyValue("STS_ACTIVITY", "NS0021" );
        classUnderTest.writeGroupKeyValue("STS_INSTITUTION","1");
        classUnderTest.writeGroupKeyValue("STS_BASE_YEAR","2000" );
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "2");

        classUnderTest.startGroup("sibling", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "IT");
        classUnderTest.writeGroupKeyValue("ADJUSTMENT", "W" );
        classUnderTest.writeGroupKeyValue("STS_INDICATOR","PROD" );
        classUnderTest.writeGroupKeyValue("STS_ACTIVITY", "NS0020" );
        classUnderTest.writeGroupKeyValue("STS_INSTITUTION","1");
        classUnderTest.writeGroupKeyValue("STS_BASE_YEAR","2000" );
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "2");

        classUnderTest.startGroup("sibling", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "IT");
        classUnderTest.writeGroupKeyValue("ADJUSTMENT", "W" );
        classUnderTest.writeGroupKeyValue("STS_INDICATOR","TOVD" );
        classUnderTest.writeGroupKeyValue("STS_ACTIVITY", "NS0050" );
        classUnderTest.writeGroupKeyValue("STS_INSTITUTION","1");
        classUnderTest.writeGroupKeyValue("STS_BASE_YEAR","2000" );
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "2");

        classUnderTest.startGroup("sibling", null);
        classUnderTest.writeGroupKeyValue("REF_AREA", "IT");
        classUnderTest.writeGroupKeyValue("ADJUSTMENT", "W" );
        classUnderTest.writeGroupKeyValue("STS_INDICATOR","TOVT" );
        classUnderTest.writeGroupKeyValue("STS_ACTIVITY", "NS0030" );
        classUnderTest.writeGroupKeyValue("STS_INSTITUTION","1");
        classUnderTest.writeGroupKeyValue("STS_BASE_YEAR","2000" );
        classUnderTest.writeAttributeValue("UNIT", "PC");
        classUnderTest.writeAttributeValue("UNIT_MULT", "0");
        classUnderTest.writeAttributeValue("TITLE_COMPL", "Elements of the full national etc.");
        classUnderTest.writeAttributeValue("DECIMALS", "2");

        classUnderTest.startSeries(null);
        classUnderTest.writeSeriesKeyValue("REF_AREA", "IT");
        classUnderTest.writeSeriesKeyValue("ADJUSTMENT", "C");
        classUnderTest.writeSeriesKeyValue("STS_INDICATOR", "TOVD");
        classUnderTest.writeSeriesKeyValue("STS_ACTIVITY", "NS0040");
        classUnderTest.writeSeriesKeyValue("STS_INSTITUTION", "1");
        classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR", "2000");
        classUnderTest.writeSeriesKeyValue("FREQ", "M");
        classUnderTest.writeAttributeValue("TIME_FORMAT", "P1M");

        classUnderTest.writeObservation("TIME_PERIOD", "2005-01", "1.1", null);
        classUnderTest.writeAttributeValue("OBS_CONF", "F");
        classUnderTest.writeAttributeValue("OBS_STATUS", "E");

        classUnderTest.writeObservation("TIME_PERIOD", "2005-02", "2.2", null);
        classUnderTest.writeAttributeValue("OBS_CONF", "F");
        classUnderTest.writeAttributeValue("OBS_STATUS", "E");

        classUnderTest.close(null);
    }

  
}
