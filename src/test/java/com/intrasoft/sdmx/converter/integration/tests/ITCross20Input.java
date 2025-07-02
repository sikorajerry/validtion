package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.CsvService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
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
public class ITCross20Input {
	private static Logger logger = org.apache.logging.log4j.LogManager.getLogger(ITCross20Input.class);
	
	@Autowired
	private StructureService structureService;
	
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    @Autowired
    private ConverterDelegatorService converterDelegatorService;

    @Autowired
    private CsvService csvService;

    private final static String GENERATED_PATH = "cross20_input/";

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
    public void testConvertToGeneric20() throws Exception{
		String resultFileName = "12_output_generic20_USE_PESTICI+ESTAT+1.1.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		
		logger.info("From XS TO GENERIC20");
		try(InputStream inputStream = new FileInputStream(("./testfiles/cross20_input/1_input_cross20_USE_PESTICI+ESTAT+1.1.xml"));
			OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.GENERIC_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxOutputConfig(),
					dataStructure,
					null,
					converterDelegatorService);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }

	@Test
    public void testConvertFromCrossToGeneric() throws Exception{
		logger.info("From XS TO GENERIC20");
		
		String resultFileName = "1_output_generic20_USE_PESTICI+ESTAT+1.1.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/cross20_input/1_input_cross20_USE_PESTICI+ESTAT+1.1.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.GENERIC_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxOutputConfig(),
					dataStructure,
					null,
					converterDelegatorService);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }

    @Test
    public void testConvertFromCrossToUtility() throws Exception{
		String resultFileName = "1_output_utility20_USE_PESTICI+ESTAT+1.1.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		
    	logger.info("From XS TO UTILITY20");

		try(InputStream inputStream = new FileInputStream("./testfiles/cross20_input/1_input_cross20_USE_PESTICI+ESTAT+1.1.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {

			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.UTILITY_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxOutputConfig(),
					dataStructure,
					null,
					converterDelegatorService);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }

    @Test
    public void testConvertFromCrossToCompact() throws Exception{
    	logger.info("From XS TO COMPACT3");
    	
		String resultFileName = "1_output_compact20_USE_PESTICI+ESTAT+1.1.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try( InputStream inputStream = new FileInputStream("./testfiles/cross20_input/1_input_cross20_USE_PESTICI+ESTAT+1.1.xml");
			 OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.COMPACT_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxOutputConfig(),
					dataStructure,
					null,
					converterDelegatorService);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }
    //SDMXCONV-950
    @Test
    public void convertFromCrossToMultilevel() throws Exception{
        String resultFileName = "1_output_multilevel_csv_with_mappingCrossXmeasures_USE_PESTICI+ESTAT+1.1.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + IntegrationTestsUtils.GENERATED_NAME + resultFileName;//OUTPUT file, generated
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + "multiLevelCsvOutput/" + resultFileName;//expected

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + "cross20_input/"));

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/cross20_input/input_Cross_DSD_2_0_SDMXCONV-950.xml");
			FileInputStream configOutputStream = new FileInputStream("./testfiles/multiLevelCsvOutput/mappingSDMXCONV-950.xml");
			FileOutputStream outputStream = new FileOutputStream(outputFile)) {


			DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml"));
			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			MultiLevelCsvOutputConfig multiLevelCsvOutputConfig = new MultiLevelCsvOutputConfig();
			//csv output setup
			multiLevelCsvOutputConfig.setDelimiter(";");
			multiLevelCsvOutputConfig.setLevels(2);
			multiLevelCsvOutputConfig.setOutputHeader(CsvOutputColumnHeader.EMPTY);
			multiLevelCsvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(configOutputStream));
			multiLevelCsvOutputConfig.setMapCrossXMeasure(true);

			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.MULTI_LEVEL_CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					multiLevelCsvOutputConfig,
					dataStructure,
					dataflow,
					converterDelegatorService);

			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
							" is different than what is expected at " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
		}
    }

	@Test
    public void testConvertToCompact20() throws Exception{
		logger.info("From XS TO Structure Specific Data 21");
		
		String resultFileName = "2_output_cross20_demography.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream(("./testfiles/cross20_input/2_input_cross20_demography.xml"));
			OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/demography.xml");

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.STRUCTURE_SPECIFIC_DATA_2_1,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxOutputConfig(),
					dataStructure,
					null,
					converterDelegatorService);
			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }

	//@Test
	@Ignore(value="this is an performance test and should be run outside the integration tests")
    public void testConvertBigFileToCompact20() throws Exception{
		logger.info("From XS TO COMPACT20");
    	
		String resultFileName = "3_output_cross20_xs_demo_rq_dsd_big_file.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try( InputStream inputStream = new FileInputStream(("./testfiles/cross20_input/3_input_cross20_xs_demo_rq_dsd_big_file.xml"));
			 OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/xs_demo_rq_dsd.xml");
			DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.COMPACT_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxOutputConfig(),
					kf,
					null,
					converterDelegatorService);
			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }
	
	/**
	 * Changed the reader for cross when converting to SDMX_CSV.
	 * Now StruvalCrossSectionalDataReaderEngine is used.
	 * With this reader we check if section node is after a group node.
	 * The below test runs correctly with the old reader and the whereabouts of group/section is not taken into account.
	 * <p>To see the old have a look: 1_output_sdmxCsv_demography_withDefaultOutputConfig.csv</p>
	 * @throws Exception
	 */
	@Test
    public void testConvertToSdmxCsv() throws Exception{
		logger.info("From XS TO SDMX_CSV");
		
		String resultFileName = "1_output_sdmxCsv_demography_withDefaultOutputConfig-new.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try( InputStream inputStream = new FileInputStream(("./testfiles/cross20_input/2_input_cross20_demography.xml"));
			 OutputStream outputStream = new FileOutputStream(outputFile);) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/demography.xml");
			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/demography.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.SDMX_CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxCsvOutputConfig(),
					dataStructure,
					dataflow,
					converterDelegatorService);

			File expectedFile = new File(completeResultTestFilesFileName);
			//TODO the sdmx_csv will always have the header present so the CsvOutputConfig header config will not be taken into consideration
			File generatedFile = outputFile;

			FileAssert.assertEquals("the generated file " + completeResultTargetFileName +
							" is different than what is expected " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
		}
    }

	/**
	 * When user selects dataflow explicitly ignore DatasetHeader structure.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1407">https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1407</a>
	 * @throws Exception
	 */
	@Test
	public void testConvertToSdmxCsvWithDefaultDataflow() throws Exception{
		String resultFileName = "04_output_sdmxCsv_default_dataflow.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream(("./testfiles/cross20_input/04_CENSUS_GRID_N_MT_2021_0001.xml"));
			OutputStream outputStream = new FileOutputStream(outputFile);) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/cross20_input/04_ESTAT+DF_CENSUS_GRID_2021+2.0_Standard.xml");
			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/cross20_input/04_ESTAT+DF_CENSUS_GRID_2021+2.0_Standard.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			// make the conversion
			TestConverterUtil.convert(Formats.CROSS_SDMX,
					Formats.SDMX_CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxCsvOutputConfig(),
					dataStructure,
					dataflow,
					converterDelegatorService);

			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;

			FileAssert.assertEquals("the generated file " + completeResultTargetFileName +
							" is different than what is expected " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
		}
	}
}
