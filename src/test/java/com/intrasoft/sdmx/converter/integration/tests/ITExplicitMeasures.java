package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.*;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
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
import java.util.LinkedHashMap;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/test-spring-context.xml" })
public class ITExplicitMeasures {
	@Autowired
	private ConverterDelegatorService converterDelegatorService;

	@Autowired
	private StructureService structureService;

	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;

	@Autowired
	private HeaderService headerService;

	@Autowired
	private FlrService flrService;

	@Autowired
	private CsvService csvService;
	
	private final static String GENERATED_PATH = "explicitMeasures/";

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
	public void convertFlrToSS21() throws Exception {
		String resultFileName = "1_output_ss_explicit.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH
				+ IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH
				+ resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		try (InputStream inputStream = new FileInputStream(
				"./testfiles/explicitMeasures/LFS_test_file_17_concepts.txt")) {

			// keyFamily
			DataStructureBean dataStructure = structureService
					.readFirstDataStructure("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml");

			try (InputStream structureStream = new FileInputStream("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml")) {
				SdmxBeans structureBean = structureService.readStructuresFromStream(structureStream);
				InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBean);

				// configuration for flr input
				FLRInputConfig inputConfig = new FLRInputConfig();
				HeaderBean header = headerService
						.parseSdmxHeaderProperties(new FileInputStream("./testfiles/explicitMeasures/header.prop"));
				inputConfig.setHeader(header);
				inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
				inputConfig.setMapMeasure(true);
				inputConfig.setBeanRetrieval(retrievalManager);

				// set the mapping
				inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(
						new FileInputStream("./testfiles/explicitMeasures/mapping.xml")));
				inputConfig.setLevelNumber("1");

				// outputFileName
				File outputFile = new File(completeResultTargetFileName);
				FileOutputStream outputStream = new FileOutputStream(outputFile);

				TestConverterUtil.convert(Formats.FLR, Formats.STRUCTURE_SPECIFIC_DATA_2_1,
						readableDataLocationFactory.getReadableDataLocation(inputStream), outputStream, inputConfig,
						new SdmxOutputConfig(), dataStructure, null, converterDelegatorService);

				File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
				File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
				FileAssert.assertEquals(
						"the generated file at " + completeResultTargetFileName
								+ " is different than what is expected at " + completeResultTestFilesFileName,
						expectedFile, generatedFile);
			}
		}
	}

	/**
	 * From Flr to Csv.
	 * Explicit measures are only used for input.
	 * For the output the mapping hasn't got the necessary explicit measures.
	 * So the file correctly will have the values vertically. 
	 * @throws Exception
	 */
	@Test
	public void convertFlrToCsvWithoutMap() throws Exception {
		String resultFileName = "2_output_csv_explicit.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		try (InputStream inputStream = new FileInputStream("./testfiles/explicitMeasures/LFS_test_file_17_concepts.txt")) {

			// keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml");

			try (InputStream structureStream = new FileInputStream("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml")) {
				SdmxBeans structureBean = structureService.readStructuresFromStream(structureStream);
				InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBean);

				// configuration for flr input
				FLRInputConfig inputConfig = new FLRInputConfig();
				HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/explicitMeasures/header.prop"));
				inputConfig.setHeader(header);
				inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
				inputConfig.setMapMeasure(true);
				inputConfig.setBeanRetrieval(retrievalManager);

				// set the mapping
				inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(
						new FileInputStream("./testfiles/explicitMeasures/mapping.xml")));
				inputConfig.setLevelNumber("1");

				//outputFileName
				File outputFile =  new File (completeResultTargetFileName);
				FileOutputStream outputStream = new FileOutputStream(outputFile);
				
				// output configuration
				MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
				csvOutputConfig.setLevels(1);
				csvOutputConfig.setDelimiter(";");
				csvOutputConfig.setEscapeValues(EscapeCsvValues.DEFAULT);
				csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
				   
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

	/**
	 * From Flr to Csv.
	 * Explicit measures used for input and output.
	 * For the output the mapping has necessary explicit measures.
	 * @throws Exception
	 */
	@Test
	public void convertFlrToCsvWithMap() throws Exception {
		String resultFileName = "3_output_csv_explicit.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		try (InputStream inputStream = new FileInputStream("./testfiles/explicitMeasures/LFS_test_file_17_concepts.txt")) {

			// keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml");

			try (InputStream structureStream = new FileInputStream("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml")) {
				SdmxBeans structureBean = structureService.readStructuresFromStream(structureStream);
				InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBean);

				// configuration for flr input
				FLRInputConfig inputConfig = new FLRInputConfig();
				HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/explicitMeasures/header.prop"));
				inputConfig.setHeader(header);
				inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
				inputConfig.setMapMeasure(true);
				inputConfig.setBeanRetrieval(retrievalManager);

				// set the mapping
				inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/explicitMeasures/mapping.xml")));
				inputConfig.setLevelNumber("1");

				//outputFileName
				File outputFile =  new File (completeResultTargetFileName);
				FileOutputStream outputStream = new FileOutputStream(outputFile);
				
				// output configuration
				MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
				csvOutputConfig.setLevels(1);
				csvOutputConfig.setDelimiter(";");
				csvOutputConfig.setEscapeValues((EscapeCsvValues.DEFAULT));
				csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
				csvOutputConfig.setMapMeasure(true);
				csvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(new FileInputStream("./testfiles/explicitMeasures/mapping_out_csv.xml")));
				   
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
    public void convertFlrToCsvWithMapAndTranscoding() throws Exception {
        String resultFileName = "5_output_csv_explicit.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        
        try (InputStream inputStream = new FileInputStream("./testfiles/explicitMeasures/LFS_test_file_17_concepts.txt")) {
            
            // keyFamily
            DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml");
            
            try (InputStream structureStream = new FileInputStream("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml")) {
                SdmxBeans structureBean = structureService.readStructuresFromStream(structureStream);
                InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBean);

                // configuration for flr input
                FLRInputConfig inputConfig = new FLRInputConfig();
                HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/explicitMeasures/header.prop"));
                inputConfig.setHeader(header);
                inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
                inputConfig.setMapMeasure(true);
                inputConfig.setMapTranscoding(true);
                inputConfig.setBeanRetrieval(retrievalManager);

                // set the mapping
                inputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/explicitMeasures/mapping.xml")));
                inputConfig.setLevelNumber("1");

                // set the transcoding
                //set the transcoding
                InputStream transcodingIs = (InputStream) new FileInputStream("./testfiles/explicitMeasures/transcoding_test.xml");
				LinkedHashMap<String, LinkedHashMap<String, String>> structureSetMap = structureService.readStructureSetMap(transcodingIs);
                inputConfig.setTranscoding(structureSetMap);

                
                //outputFileName
                File outputFile = new File(completeResultTargetFileName);
                FileOutputStream outputStream = new FileOutputStream(outputFile);

                // output configuration
                MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
                csvOutputConfig.setLevels(1);
                csvOutputConfig.setDelimiter(";");
                csvOutputConfig.setEscapeValues((EscapeCsvValues.DEFAULT));
                csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
                csvOutputConfig.setMapMeasure(true);
                csvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(new FileInputStream("./testfiles/explicitMeasures/mapping_out_csv.xml")));

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
                FileAssert.assertEquals("the generated file at " + completeResultTargetFileName
                    + " is different than what is expected at " + completeResultTestFilesFileName,
                    expectedFile, generatedFile);
            }
        }
    }
	/**
	 * From Flr to Csv.
	 * Explicit measures used for input and output.
	 * For the output the mapping has necessary explicit measures.
	 * @throws Exception
	 */
	@Test
	public void convertSSToCsvWithMap() throws Exception {
		String resultFileName = "4_output_csv_explicit.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		try (InputStream inputStream = new FileInputStream("./testfiles/explicitMeasures/input_ss_explicit.xml")) {

			// keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml");
			try (InputStream structureStream = new FileInputStream("./testfiles/explicitMeasures/ESTAT+LFS+3.0.xml")) {

				//outputFileName
				File outputFile =  new File (completeResultTargetFileName);
				FileOutputStream outputStream = new FileOutputStream(outputFile);
				
				// output configuration
				MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
				csvOutputConfig.setLevels(1);
				csvOutputConfig.setDelimiter(";");
				csvOutputConfig.setEscapeValues((EscapeCsvValues.DEFAULT));
				csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
				csvOutputConfig.setMapMeasure(true);
				csvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(new FileInputStream("./testfiles/explicitMeasures/mapping_out_csv.xml")));
				   
			  TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1, 
					  					Formats.CSV,
					  					readableDataLocationFactory.getReadableDataLocation(inputStream),
					  					outputStream, 
					  					new SdmxInputConfig(), 
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
}
