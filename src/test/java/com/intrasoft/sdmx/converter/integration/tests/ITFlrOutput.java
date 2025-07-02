package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.FlrOutputConfig;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.FlrService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.FlrInColumnMapping;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
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
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITFlrOutput {
	
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
            
    private final static String GENERATED_PATH = "flr_output/";

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
	/**
	 * Test with mapping external file. From compact to flr.
	 */
	public void convertFromCompact() throws Exception {
		String resultFileName = "1_output_flr_BIS_JOINT_DEBT_v1.0.txt";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_output/1_compact.xml");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml"));
		FlrOutputConfig outputConfig = new FlrOutputConfig();
		outputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_output/1_mapping.xml")));
		outputConfig.setOutputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		outputConfig.setPadding("_");
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(  Formats.COMPACT_SDMX,
                            Formats.FLR,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
							new SdmxInputConfig(),
							outputConfig,
                            dataStructure,
                            null, 
       		                converterDelegatorService);
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	@Test
	/**
	 * Test without mapping external file. From compact to flr.
	 */
	public void convertFromCompactWithAuto() throws Exception{
		String resultFileName = "2_output_flr_BIS_JOINT_DEBT_v1.0.txt";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_output/1_compact.xml");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml"));
		FlrOutputConfig outputConfig = new FlrOutputConfig();
		outputConfig.setOutputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		outputConfig.setPadding("_");
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(  Formats.COMPACT_SDMX,
				Formats.FLR,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				new SdmxInputConfig(),
				outputConfig,
				dataStructure,
				null,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	@Test
	/**
	 * Test from sdmx_csv to flr for SDMXCONV-1036 */
	public void convertFromSdmxCsv() throws Exception {
		String resultFileName = "1036-output.txt";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_output/1036-input.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/flr_output/1036-ESTAT+LFS_2019+1.0.xml"));
		SdmxCsvInputConfig inputConfig = new SdmxCsvInputConfig();
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/flr_input/header.prop"));
		inputConfig.setHeader(header);
		FlrOutputConfig outputConfig = new FlrOutputConfig();
		outputConfig.setMapping(flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_output/1036-mapping.xml")));
		outputConfig.setOutputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		outputConfig.setPadding(" ");
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(  Formats.SDMX_CSV,
				Formats.FLR,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
				outputConfig,
				dataStructure,
				null,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * Conversion from SDMX_CSV_2_0 to FLR with complex components.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1558">SDMXCONV-1558</a>
	 */
	@Test
	public void convertFromSdmxCsv20withComplex() throws Exception {
		String resultFileName = "3_output.flr";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_output/3_SmdxCsv20.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/flr_output/3_TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml"));
		SdmxCsvInputConfig inputConfig = new SdmxCsvInputConfig();
		inputConfig.setDelimiter(";");
		inputConfig.setSubFieldSeparationChar("#");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		FlrOutputConfig outputConfig = new FlrOutputConfig();
		LinkedHashMap<String, FlrInColumnMapping> mappings = flrService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/flr_output/3_mapping.xml"), dataStructure, true);
		outputConfig.setMapping(mappings);
		outputConfig.setOutputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		outputConfig.setPadding(" ");
		outputConfig.setSubFieldSeparationChar("#");
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(  Formats.SDMX_CSV_2_0,
				Formats.FLR,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
				outputConfig,
				dataStructure,
				null,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * Conversion from SDMX_CSV_2_0 to FLR with complex components, without mapping.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1558">SDMXCONV-1558</a>
	 */
	@Test
	public void convertFromSdmxCsv20withComplexAutoMapping() throws Exception {
		String resultFileName = "3_auto_output.flr";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_output/3_SmdxCsv20.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/flr_output/3_TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml"));
		SdmxCsvInputConfig inputConfig = new SdmxCsvInputConfig();
		inputConfig.setDelimiter(";");
		inputConfig.setSubFieldSeparationChar("#");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		FlrOutputConfig outputConfig = new FlrOutputConfig();
		outputConfig.setOutputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		outputConfig.setPadding(" ");
		outputConfig.setSubFieldSeparationChar("#");
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(  Formats.SDMX_CSV_2_0,
				Formats.FLR,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
				outputConfig,
				dataStructure,
				null,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * Conversion from SDMX_CSV_2_0 to FLR with complex components, without mapping.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1558">SDMXCONV-1558</a>
	 */
	@Test
	public void convertFromSS30withComplexAutoMapping() throws Exception {
		String resultFileName = "3_auto_output.flr";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/flr_output/3_StrSpecOutput.xml");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/flr_output/3_TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml"));
		SdmxInputConfig inputConfig = new SdmxInputConfig();
		FlrOutputConfig outputConfig = new FlrOutputConfig();
		outputConfig.setOutputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		outputConfig.setPadding(" ");
		outputConfig.setSubFieldSeparationChar("#");
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(  Formats.STRUCTURE_SPECIFIC_DATA_3_0,
				Formats.FLR,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
				outputConfig,
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
