package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.InputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.HeaderSDMXCsvValues;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;
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

/**
 * Tests from all inputs to Sdmx Csv
 * that reports input's position of the observation.
 * @throws Exception
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITSdmxCsvOutput {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    @Autowired
	private HeaderService headerService; 
    
    private final static String GENERATED_PATH = "sdmxCsvOutput/";

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
	 * Manual SDMX header added to output
	 * See also SDMXCONV-1292
	 * @throws Exception
	 */
	@Test
	public void testSdmxHeaderManual() throws Exception{
		String resultFileName = "03-STSCONS_PERM_Q_output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_input.xml");

		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
		InputConfig inputConfig = new SdmxInputConfig();
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setOutputHeader(org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader.USE_HEADER);
		csvOutputConfig.setHeaderSDMXCsvValue(HeaderSDMXCsvValues.DEFAULT);
		readHeader(csvOutputConfig,
				inputConfig,
				Formats.COMPACT_SDMX,
				Formats.SDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(new FileInputStream("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_input.xml")),
				outputStream,
				kf,
				dataflow,
				"Header/ID,Header/Sender@Id,Header/Sender/Name");
		//make the conversion
		TestConverterUtil.convert(Formats.COMPACT_SDMX,
				Formats.SDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
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

	/**
	 * Default SDMX header added to output
	 * See also SDMXCONV-1292
	 * @throws Exception
	 */
	@Test
	public void testSdmxHeaderTrue() throws Exception{
		String resultFileName = "02-STSCONS_PERM_Q_output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_input.xml");

		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
		InputConfig inputConfig = new SdmxInputConfig();
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		csvOutputConfig.setHeaderSDMXCsvValue(HeaderSDMXCsvValues.DEFAULT);
		readHeader(csvOutputConfig,
				inputConfig,
				Formats.COMPACT_SDMX,
				Formats.SDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(new FileInputStream("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_input.xml")),
				outputStream,
				kf,
				dataflow, "true");
		//make the conversion
		TestConverterUtil.convert(Formats.COMPACT_SDMX,
				Formats.SDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
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


	private HeaderBean readHeader(OutputConfig outputConfig,
								  InputConfig inputConfig,
								  Formats sourceFormat,
								  Formats destinationFormat,
								  ReadableDataLocation readableDataLocation,
								  OutputStream outputStream,
								  DataStructureBean dsd,
								  DataflowBean df,
								  String sdmxHeaderToSdmxCsv) throws IOException, WriteHeaderException {
		HeaderBean headerBean;
		ConverterOutput converterOutput = new ConverterOutput(destinationFormat, outputStream, outputConfig);
		ConverterStructure converterStructure = new ConverterStructure(dsd, df);

		try (InputStream inputStream = readableDataLocation.getInputStream();
			 ReadableDataLocation reader = readableDataLocationFactory.getReadableDataLocation(inputStream)) {
			ConverterInput converterNewInput = new ConverterInput(sourceFormat, reader, inputConfig);
			headerBean = headerService.getHeaderBean(converterNewInput, converterOutput, converterStructure, sdmxHeaderToSdmxCsv, null);
		}
		return headerBean;
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
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		File expectedFile = new File(completeResultTestFilesFileName);
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		try(InputStream inputStream = new FileInputStream("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_input.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {
			//keyFamily
			DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");

			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/sdmxCsvOutput/01-STSCONS_PERM_Q_DF.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			//output configuration
			SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
			csvOutputConfig.setInternalSdmxCsv(false);

			//make the conversion
			TestConverterUtil.convert(Formats.COMPACT_SDMX,
					Formats.SDMX_CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					csvOutputConfig,
					kf,
					dataflow,
					converterDelegatorService);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
							" is different than what is expected at " + completeResultTestFilesFileName,
					expectedFile, outputFile);
		} finally {
			outputFile.delete();
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

		InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/07-input-NAMAIN.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/07-dsd.xml");
		DataStructureBean kf = sdmxBeans.getDataStructures().iterator().next();
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);

		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(false);

		//make the conversion
		TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				Formats.SDMX_CSV,
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

		InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/07-input-NAMAIN.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/07-dsd.xml");
		DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);

		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setDsd(dsd);
		csvOutputConfig.setInternalSdmxCsv(false);

		//make the conversion
		TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				Formats.SDMX_CSV,
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
