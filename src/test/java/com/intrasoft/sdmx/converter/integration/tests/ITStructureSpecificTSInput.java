package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Handler;
import java.util.logging.LogManager;


@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITStructureSpecificTSInput {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    private final static String GENERATED_PATH = "structurespecificTS21_input/";

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
    public void convertToSdmxCsv21() throws Exception{
		String resultFileName = "1_output_sdmxCsv_STS+ESTAT+2.0_withGroupsAndMultipleGroups.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/structurespecificTS21_input/1_input_structureSpecificTS21_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/STS+ESTAT+2.0.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/STS+ESTAT+2.0.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_TS_DATA_2_1, 
        			Formats.SDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
                	new SdmxInputConfig(),
	                new SdmxCsvOutputConfig(),
	                kf,
	                dataflow, 
		                converterDelegatorService);
       
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       		" is different than what is expected at " + completeResultTestFilesFileName,
	       		expectedFile, generatedFile);
    }
}
