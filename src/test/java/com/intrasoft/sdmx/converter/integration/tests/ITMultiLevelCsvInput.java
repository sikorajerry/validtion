package com.intrasoft.sdmx.converter.integration.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.CsvService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;

import junitx.framework.FileAssert;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITMultiLevelCsvInput {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
	@Autowired
	private StructureService structureService;
	
    @Autowired
    private CsvService csvService; 
    
	@Autowired
	private HeaderService headerService; 
	
	@Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
	
    private final static String GENERATED_PATH = "multiCsv_input/";

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
    public void convertToCross() throws Exception{
		String resultFileName = "1_output_cross20.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/CrossPerLineCorrected.csv");
        //dsd
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");
        DataStructureBean dataStructure = (DataStructureBean) sdmxBeans.getDataStructures().iterator().next();


        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setMapCrossXMeasure(true);
        csvInputConfig.setHeader(new HeaderBeanImpl("IREF" + Calendar.getInstance().getTimeInMillis(), "TEST"));

        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
         // In XS FREQ and TIME_PERIOD appear in Group
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{2}, false, 1, ""));        
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{1}, false, 2, ""));
        csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));        
        csvMapping.put("STOCKS", new CsvInColumnMapping(new Integer[]{1}, false, 3, ""));
        csvMapping.put("FLOWS", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        
        TestConverterUtil.convert(  Formats.MULTI_LEVEL_CSV,
                            Formats.CROSS_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            csvInputConfig,
                            new SdmxOutputConfig(),
       		                dataStructure, 
       		                null,
       		                converterDelegatorService);
       
       // File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        //File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        //FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        //		" is different than what is expected at " + completeResultTestFilesFileName,
       // 		expectedFile, generatedFile);
    }	
    @Test
    public void convertToCompact20() throws Exception{
		String resultFileName = "1_output_compact20_ECB+ECB_EXR1+1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/multiCsv_input/1_input_multiLevelCsv_ECB+ECB_EXR1+1.0.csv");
         
        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        //csv output setup
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setLevelNumber("3");
		csvInputConfig.setHeader(headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/multiCsv_input/header.prop")));
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.EMPTY);
        csvInputConfig.setMapping(csvService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/multiCsv_input/mapping3Levels.xml"),3));
        // make the conversion
        TestConverterUtil.convert(  Formats.MULTI_LEVEL_CSV,
                            Formats.COMPACT_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            csvInputConfig,
                            new SdmxOutputConfig(),
       		                dataStructure, 
       		                dataflow,
       		                converterDelegatorService);
       
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }

    @Test
    public void convertToCompact20WithProductionFile() throws Exception{
		String resultFileName = "2_output_compact20_BIS_JOINT_DEBT_v1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/multiCsv_input/2_input_multilevelCsv_BIS_JOINT_DEBT_v1.0.csv");

        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml");

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);

        CsvInputConfig csvInputConfig = new CsvInputConfig();
        //csv output setup
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setHeader(headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/multiCsv_input/header.prop")));
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.EMPTY);
        csvInputConfig.setMapping(
                csvService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/multiCsv_input/mapping.xml"),3));
        // make the conversion
        TestConverterUtil.convert(  Formats.MULTI_LEVEL_CSV,
                Formats.COMPACT_SDMX,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                csvInputConfig,
                new SdmxOutputConfig(),
                dataStructure,
                null,
                converterDelegatorService);

        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }
}
