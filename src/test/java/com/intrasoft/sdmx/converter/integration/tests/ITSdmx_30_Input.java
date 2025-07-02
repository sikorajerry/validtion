package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.*;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.config.SdmxMLInputConfig;
import org.estat.sdmxsource.util.csv.*;
import org.estat.struval.ValidationError;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
import org.sdmxsource.sdmx.structureretrieval.manager.SdmxSuperBeanRetrievalManagerImpl;
import org.sdmxsource.util.translator.PreferredLanguageTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Test converting Structure Specific 3.0 to CSVs
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITSdmx_30_Input {

	private final static String GENERATED_PATH = "structure_specific_3_0_input/";
	@Autowired
	private ConverterDelegatorService converterDelegatorService;
	@Autowired
	private StructureService structureService;
	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;
	@Autowired
	private HeaderService headerService;
	@Autowired
	private ValidationService validationService;
	@Autowired
	private CsvService csvService;

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
	 * Convert to old iSdmxCsv
	 *
	 * @throws Exception
	 */
	@Test
	public void convertToISdmxCsv() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "01_output_iSdmxCsv.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/input_STRUCTURE_SPECIFIC_3_0.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			//This needs to be added to be able to fix the header row
			csvOutputConfig.setRetrievalManager(superRetrievalManager);
			csvOutputConfig.setDsd(dataStructure);
			csvOutputConfig.setDataflow(dataflow);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.iSDMX_CSV, outputStream, csvOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file " + completeResultTargetFileName + " is different than what is expected " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Convert to old SdmxCsv
	 *
	 * @throws Exception
	 */
	@Test
	public void convertToSdmxCsv() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "02_output_SdmxCsv.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/input_STRUCTURE_SPECIFIC_3_0.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			//This needs to be added to be able to fix the header row
			csvOutputConfig.setRetrievalManager(superRetrievalManager);
			csvOutputConfig.setDsd(dataStructure);
			csvOutputConfig.setDataflow(dataflow);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV, outputStream, csvOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file " + completeResultTargetFileName + " is different than what is expected " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Convert to old Csv
	 *
	 * @throws Exception
	 */
	@Test
	public void convertToSingleLevelCsv() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "03_output_sinlgeCsv.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/input_STRUCTURE_SPECIFIC_3_0.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			//This needs to be added to be able to fix the header row
			csvOutputConfig.setRetrievalManager(superRetrievalManager);
			csvOutputConfig.setDsd(dataStructure);
			csvOutputConfig.setDataflow(dataflow);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.CSV, outputStream, csvOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file " + completeResultTargetFileName + " is different than what is expected " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Convert to old Csv
	 *
	 * @throws Exception
	 */

	@Test
	public void convertFromSingleLevelCsvTo30() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "09_output_30.xml";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/03_output_sinlgeCsv.csv");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			CsvInputConfig inputConfig = new CsvInputConfig();
			HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
			inputConfig.setHeader(header);
			inputConfig.setDelimiter(";");
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
			inputConfig.setIsEscapeCSV(false);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.STRUCTURE_SPECIFIC_DATA_3_0, outputStream, new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
							" is different than what is expected at " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Convert Structure Specific 3.0 to SDMX_CSV 2.0
	 */
	@Test
	public void convertToSDMXCSV20() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "04_output_sdmxCsv30.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/input_STRUCTURE_SPECIFIC_3_0.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();

			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			outputConfig.setCsvLabel(CsvLabel.ID);
			outputConfig.setDelimiter(";");
			outputConfig.setSubFieldSeparationChar(",");
			outputConfig.setDsd(dataStructure);
			outputConfig.setRetrievalManager(superRetrievalManager);
			List preferredLanguages = Arrays.asList(new Locale("en"));
			List availableLanguages = Arrays.asList(new Locale("en"));
			Locale defaultLanguage = new Locale("en");
			PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
			outputConfig.setTranslator(translator);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file "
					+ completeResultTargetFileName
					+ " is different than what is expected "
					+ completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}


	/**
	 * Test convert Structure Specific 3.0 to old CSV based on Multiple Attribute DSD
	 */

	@Test
	public void convertToCsv_MultipleDSD() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "06_output_Csv_Multiple.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/MULTIPLE_MEASURES_DATA.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/MULTIPLE_MEASURES_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			//This needs to be added to be able to fix the header row
			csvOutputConfig.setRetrievalManager(superRetrievalManager);
			csvOutputConfig.setDsd(dataStructure);
			csvOutputConfig.setDataflow(dataflow);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.CSV, outputStream, csvOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file " + completeResultTargetFileName + " is different than what is expected " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}


	/**
	 * Test convert Structure Specific 3.0 to SDMX_CSV_2_0 based on Multiple Attribute DSD
	 */

	@Test
	public void convertToSDMXCSV20_MultipleDSD() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "08_output_SdmxCsv_2_MultipleDSD.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/MULTIPLE_MEASURES_DATA.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/MULTIPLE_MEASURES_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();

			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			outputConfig.setCsvLabel(CsvLabel.ID);
			outputConfig.setDelimiter(";");
			outputConfig.setSubFieldSeparationChar(",");
			outputConfig.setDsd(dataStructure);
			outputConfig.setRetrievalManager(superRetrievalManager);
			List preferredLanguages = Arrays.asList(new Locale("en"));
			List availableLanguages = Arrays.asList(new Locale("en"));
			Locale defaultLanguage = new Locale("en");
			PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
			outputConfig.setTranslator(translator);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file "
					+ completeResultTargetFileName
					+ " is different than what is expected "
					+ completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Test convert Structure Specific 3.0 to iSDMX_CSV_2_0 based on Simple Attribute DSD
	 */
	@Test
	public void convertToISdmxCsv_2_0() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "13_output_iSdmxCsv_2_0.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/input_STRUCTURE_SPECIFIC_3_0.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			outputConfig.setCsvLabel(CsvLabel.ID);
			outputConfig.setDelimiter(";");
			outputConfig.setSubFieldSeparationChar(",");
			outputConfig.setDsd(dataStructure);
			outputConfig.setRetrievalManager(superRetrievalManager);
			List preferredLanguages = Arrays.asList(new Locale("en"));
			List availableLanguages = Arrays.asList(new Locale("en"));
			Locale defaultLanguage = new Locale("en");
			PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
			outputConfig.setTranslator(translator);
			outputConfig.setInternalSdmxCsvAdjustment(true);
			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.iSDMX_CSV_2_0, outputStream,outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file " + completeResultTargetFileName + " is different than what is expected " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Test convert Structure Specific 3.0 to iSDMX_CSV_2_0 based on Multiple Attribute DSD
	 */

	@Test
	public void convertToISdmxCsv_2_0_MultipleDSD() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "14_output_iSdmxCsv_2_MultipleDSD.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/MULTIPLE_MEASURES_DATA.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/MULTIPLE_MEASURES_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			outputConfig.setCsvLabel(CsvLabel.ID);
			outputConfig.setDelimiter(";");
			outputConfig.setSubFieldSeparationChar(",");
			outputConfig.setDsd(dataStructure);
			outputConfig.setRetrievalManager(superRetrievalManager);
			List preferredLanguages = Arrays.asList(new Locale("en"));
			List availableLanguages = Arrays.asList(new Locale("en"));
			Locale defaultLanguage = new Locale("en");
			PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
			outputConfig.setTranslator(translator);
			outputConfig.setInternalSdmxCsvAdjustment(true);
			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.iSDMX_CSV_2_0, outputStream,outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file " + completeResultTargetFileName + " is different than what is expected " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}


	/**
	 * Test convert Structure Specific 3.0 to old CSV based on Complex Attribute DSD
	 */

	@Test
	public void convertToCsv_ComplexDSD() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "10_output_Csv_Complex.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/ECB_EXR_CA.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_CA_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();
			MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			//This needs to be added to be able to fix the header row
			csvOutputConfig.setRetrievalManager(superRetrievalManager);
			csvOutputConfig.setDsd(dataStructure);
			csvOutputConfig.setDataflow(dataflow);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.CSV, outputStream, csvOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file " + completeResultTargetFileName + " is different than what is expected " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}


	/**
	 * Test convert Structure Specific 3.0 to SDMX_CSV_2_0 based on Complex Attribute DSD
	 */
	@Test
	public void convertToSDMXCSV20_CommplexDSD() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "11_output_SdmxCsv_2_0_Complex.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/ECB_EXR_CA.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/ECB_EXR_CA_DSD.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();

			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			outputConfig.setCsvLabel(CsvLabel.ID);
			outputConfig.setDelimiter(";");
			outputConfig.setSubFieldSeparationChar(",");
			outputConfig.setDsd(dataStructure);
			outputConfig.setRetrievalManager(superRetrievalManager);
			List preferredLanguages = Arrays.asList(new Locale("en"));
			List availableLanguages = Arrays.asList(new Locale("en"));
			Locale defaultLanguage = new Locale("en");
			PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
			outputConfig.setTranslator(translator);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file "
					+ completeResultTargetFileName
					+ " is different than what is expected "
					+ completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Test convert Structure Specific 3.0 to Compact 2.0
	 * SDMXCONV-1508
	 */
	@Test
	public void convertToCompactMissingDataset() throws Exception{
		String resultFileName = "Compact_MissingDataset.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		InputStream inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/SS_30_MissingDataset.xml");

		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/NAMAIN_IDC_N1.9_SDMX3.0.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/NAMAIN_IDC_N1.9_SDMX3.0.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);

		// make the conversion
		TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_3_0,
				Formats.COMPACT_SDMX,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				new SdmxInputConfig(),
				new SdmxOutputConfig(),
				kf,
				dataflow,
				converterDelegatorService);


		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * Test iSDMX_CSV_2_0 output. Quotes problem.
	 * And OBS_STATUS1 problem with values
	 */
	@Test
	public void convertToISDMXCSV20() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "12_output_ISdmxCsv_2_0.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "1530/" + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/1530/TEST_Input_Demobal_StructSpec.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/1530/SDMX_3.0_ESTAT+DEM_DEMOBAL+1.1.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();

			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			outputConfig.setCsvLabel(CsvLabel.ID);
			outputConfig.setDelimiter(";");
			outputConfig.setSubFieldSeparationChar(",");
			outputConfig.setDsd(dataStructure);
			outputConfig.setRetrievalManager(superRetrievalManager);
			List preferredLanguages = Arrays.asList(new Locale("en"));
			List availableLanguages = Arrays.asList(new Locale("en"));
			Locale defaultLanguage = new Locale("en");
			PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
			outputConfig.setTranslator(translator);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.iSDMX_CSV_2_0, outputStream, outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file "
					+ completeResultTargetFileName
					+ " is different than what is expected "
					+ completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Solved problem with two groups.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1577">SDMXCONV-1577</a>
	 */
	@Test
	public void convertWithTwoGroups() throws Exception {
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			String resultFileName = "16_output_ISdmxCsv_2_0.csv";
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "1577/" + resultFileName;
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/1577/56_259+1.0+IT_data_SS_3_0.xml");
			// dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/1577/56_259_xml_3_0.xml");
			InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
			SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
			DataStructureBean dataStructure = dataStructures.iterator().next();

			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			outputConfig.setCsvLabel(CsvLabel.ID);
			outputConfig.setDelimiter(";");
			outputConfig.setSubFieldSeparationChar(",");
			outputConfig.setDsd(dataStructure);
			outputConfig.setRetrievalManager(superRetrievalManager);
			List preferredLanguages = Arrays.asList(new Locale("en"));
			List availableLanguages = Arrays.asList(new Locale("en"));
			Locale defaultLanguage = new Locale("en");
			PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
			outputConfig.setTranslator(translator);

			// ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			outputStream = new FileOutputStream(outputFile);
			ConverterInput converterInput = new ConverterInput(
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.iSDMX_CSV_2_0, outputStream, outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file "
					+ completeResultTargetFileName
					+ " is different than what is expected "
					+ completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * Having input csv with complex and apply transcoding.
	 * (see SDMXCONV-1533)
	 * @throws Exception
	 */
	@Test
	public void convertToSS30WithTranscodingOutput() throws Exception {
		String resultFileName = "02-output-convertToCsvWithTranscoding.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME +  "transcoding/"+ resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/transcoding/02-multMeasuresComplexr-xl.xml");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(
				new File("./testfiles/transcoding/01-TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml"));

		MultiLevelCsvOutputConfig outputConfig = new MultiLevelCsvOutputConfig();
		outputConfig.setDelimiter(";");
		outputConfig.setSubFieldSeparationChar("#");
		outputConfig.setDsd(dataStructure);
		outputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		outputConfig.setTranscoding(structureService.readStructureSetMap(new FileInputStream("./testfiles/transcoding/02-transcoding.xml")));
		//set the mapping
		InputStream mappingIs = new FileInputStream("./testfiles/transcoding/02-mapping3.0.xml");
		MultiLevelCsvOutColMapping mappingMap = csvService.buildOutputDimensionLevelMapping(mappingIs);
		outputConfig.setColumnMapping(mappingMap);
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);

		ConverterInput converterInput = new ConverterInput(
				Formats.STRUCTURE_SPECIFIC_DATA_3_0,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				new SdmxInputConfig());
		ConverterOutput converterOutput = new ConverterOutput(Formats.CSV, outputStream, outputConfig);
		ConverterStructure converterStructure = new ConverterStructure(dataStructure, null, null);
		// make the conversion
		converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * <p>Header should not throw validation errors from validating against the xsds</p>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1507">SDMXCONV-1507</a>
	 * @throws IOException
	 */
	@Test
	public void validFile() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/01-ss-3.0.xml");
			 InputStream structureInputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/01-NAMAIN_IDC_N_1.9_sdmx2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), 10).getErrors();
			assertNotNull(errorList);
			assertEquals(0, errorList.size());
		}
	}
}
