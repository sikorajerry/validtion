package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Handler;
import java.util.logging.LogManager;

/**
 * Tests from all inputs to Csv
 * that reports input's position of the observation.
 * @throws Exception
 */

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITCsvOutput {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;

    @Autowired
	private HeaderService headerService;
    
    private final static String GENERATED_PATH = "CsvOutput/";

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}

	/**
	 * Ensure that obs attributes are correctly erased if exists in one observation only.
	 * See also SDMXCONV-1175
	 * @throws Exception
	 */
	@Test
    public void convertFromCompact() throws Exception{
		String resultFileName = "01-STSCONS_PERM_Q_output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile = new File(completeResultTargetFileName);

		try( InputStream inputStream = new FileInputStream("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_input.xml");
			 OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");

			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			//output configuration
			MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			//make the conversion
			TestConverterUtil.convert(Formats.COMPACT_SDMX,
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					csvOutputConfig,
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

	/**
	 * If the input file has only a header without DataSet element then the converter produces empty file.
	 * SDMXCONV-1184
	 * @throws Exception
	 */
	@Test
	public void convertFromSSEmpty() throws Exception{
		String resultFileName = "01-output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/07-input-NAMAIN.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {

			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/07-dsd.xml");
			DataStructureBean kf = sdmxBeans.getDataStructures().iterator().next();
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			//output configuration
			MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
			csvOutputConfig.setDelimiter(";");

			//make the conversion
			TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					csvOutputConfig,
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

	/**
	 * If the input file has only a header without DataSet element
	 * and the dsd is passed as a configuration parameter then the converter produces file with only the header row.
	 * SDMXCONV-1184
	 * @throws Exception
	 */
	@Test
	public void convertFromSSEmptyWriteHeaderRow() throws Exception{
		String resultFileName = "01-headerRowOutput.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/07-input-NAMAIN.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {

			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/07-dsd.xml");
			DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			//output configuration
			MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDsd(dsd);
			csvOutputConfig.setDelimiter(";");
			csvOutputConfig.setLevels(1);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			//make the conversion
			TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					csvOutputConfig,
					dsd,
					dataflow,
					converterDelegatorService);

			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
							" is different than what is expected at " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
		}
	}
}
