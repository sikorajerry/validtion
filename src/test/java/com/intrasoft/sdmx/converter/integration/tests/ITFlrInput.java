package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.FlrService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITFlrInput {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
   
    @Autowired
    private StructureService structureService;
    
    @Autowired
    private HeaderService headerService;
       
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    @Autowired
    private FlrService flrService;
            
    private final static String GENERATED_PATH = "flr_input/";

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
	public void convertToCompact() throws Exception{
		String resultFileName = "1_output_compact_BIS_JOINT_DEBT_v1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_input/1_input_flr.txt");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml"));
		
		FLRInputConfig inputConfig = new FLRInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/flr_input/header.prop"));
		//TODO mapping for FLR
		inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_input/mapping.xml")));
		inputConfig.setTranscoding(structureService.readStructureSetMap(new FileInputStream("./testfiles/flr_input/transcoding.xml")));
		inputConfig.setHeader(header);
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.FLR,
                            Formats.COMPACT_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
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

	/**
	 * Having input transcoding from “null” to a default value.
	 * (see SDMXCONV-1164)
	 * @throws Exception
	 */
	@Test
	public void convertWithTranscoding() throws Exception{
		String resultFileName = "output-convertWithTranscoding.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + "transcoding/" + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/transcoding/input.flr");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/transcoding/dsd.xml"));
		FLRInputConfig inputConfig = new FLRInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/flr_input/header.prop"));
		inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/transcoding/mapping.xml")));
		inputConfig.setTranscoding(structureService.readStructureSetMap(new FileInputStream("./testfiles/transcoding/transcoding.xml")));
		inputConfig.setHeader(header);
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);

		TestConverterUtil.convert(  Formats.FLR,
				Formats.COMPACT_SDMX,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
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
	
	@Test
	public void convertToCompactWithObservationValueOnSeparateColumns() throws Exception{
		String resultFileName = "2_output_compact_BIS_JOINT_DEBT_v1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_input/2_input_flr.txt");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml"));
		
		FLRInputConfig inputConfig = new FLRInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/flr_input/header.prop"));
		inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_input/2_mapping.xml")));
		inputConfig.setTranscoding(structureService.readStructureSetMap(new FileInputStream("./testfiles/flr_input/transcoding.xml")));
		inputConfig.setHeader(header);
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.FLR,
                            Formats.COMPACT_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
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
	
	@Test
	public void convertToCsv() throws Exception {
		String resultFileName = "3_output_LFS_Q_A_C1_2018_0003.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		try(InputStream inputStream = new FileInputStream("./testfiles/flr_input/3_input_LFS_Q_A_C1_2018_0003.txt")) {
			DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/ESTAT+LFS+1.0_Extended.xml"));

			FLRInputConfig inputConfig = new FLRInputConfig();
			HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/flr_input/header.prop"));
			inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_input/3_MAPPING_FILE_OBS_VALUE_EMPTY_REFYEAR_2018.XML")));
			inputConfig.setHeader(header);
			inputConfig.setPadding(" ");
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);

			//outputFileName
			File outputFile = new File(completeResultTargetFileName);
			try(FileOutputStream outputStream = new FileOutputStream(outputFile)) {

				MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
				csvOutputConfig.setLevels(1);
				csvOutputConfig.setDelimiter(";");
				csvOutputConfig.setEscapeValues((EscapeCsvValues.DEFAULT));
				csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
				csvOutputConfig.setMapCrossXMeasure(true);
				TestConverterUtil.convert(Formats.FLR,
						Formats.CSV,
						readableDataLocationFactory.getReadableDataLocation(inputStream),
						outputStream,
						inputConfig,
						csvOutputConfig,
						dataStructure,
						null,
						converterDelegatorService);
				File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
							" is different than what is expected at " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
			}
		}
	}
	
	@Test
	public void convertToCsv2() throws Exception{
		String resultFileName = "4_output_Flr_Bis1.0.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_input/4_input_Flr_Bis1.0.txt");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml"));
		
		FLRInputConfig inputConfig = new FLRInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/flr_input/header.prop"));

		inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_input/4_mapping.xml")));
		inputConfig.setHeader(header);
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
		csvOutputConfig.setLevels(1);
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setEscapeValues((EscapeCsvValues.DEFAULT));
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		TestConverterUtil.convert(  Formats.FLR,
                            Formats.CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            csvOutputConfig,
                            dataStructure,
                            null, 
       		                converterDelegatorService);
		
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       		" is different than what is expected at " + completeResultTestFilesFileName,
	       		expectedFile, generatedFile);
	}

	//SDMXCONV-1074
	@Test
	public void convertToCsv3() throws Exception{
		String resultFileName = "5_output_Flr_LFS.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_input/5_input_LFS_Q_Q_BE.txt");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/ESTAT+LFS_Q_Q+1.0.xml"));

		FLRInputConfig inputConfig = new FLRInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/flr_input/header.prop"));

		inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_input/5_MAPPING_FILE_lfs.XML")));
		inputConfig.setHeader(header);
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);

		MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
		csvOutputConfig.setLevels(1);
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setEscapeValues((EscapeCsvValues.DEFAULT));
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		TestConverterUtil.convert(  Formats.FLR,
				Formats.CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
				csvOutputConfig,
				dataStructure,
				null,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}
}
