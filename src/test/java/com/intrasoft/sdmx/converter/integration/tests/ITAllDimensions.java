package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.StructureService;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
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
public class ITAllDimensions {
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
   
    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    private final static String GENERATED_PATH = "structureSpecificAlldimensions/";

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
    public void convertToGeneric21() throws Exception{
		String resultFileName = "1_output_generic21_allDimensions.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/structureSpecificAlldimensions/1_input_structureSpecific_allDimensions.xml");
		    OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/structureSpecificAlldimensions/SDMX_21_ESTAT+EGR_IS_F1_ATT+2.0.xml");

			ConverterInput converterInput = new ConverterInput(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_DATA_2_1, outputStream, sdmxOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }

    @Test
    public void convertToStructureSpecific21() throws Exception {
		String resultFileName = "2_output_structureSpecific21_allDimensions.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile = new File(completeResultTargetFileName);

		try (InputStream inputStream = new FileInputStream("./testfiles/structureSpecificAlldimensions/2_input_generic21_allDimensions.xml");
			 OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/structureSpecificAlldimensions/SDMX_21_ESTAT+EGR_IS_F1_ATT+2.0.xml");

			ConverterInput converterInput = new ConverterInput(Formats.GENERIC_DATA_2_1,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

			ConverterOutput converterOutput = new ConverterOutput(Formats.STRUCTURE_SPECIFIC_DATA_2_1, outputStream, sdmxOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
	}
}
