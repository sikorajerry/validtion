package com.intrasoft.sdmx.converter.parameters;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.integration.tests.IntegrationTestsUtils;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.CsvService;
import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.exception.SdmxSyntaxException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataFormatException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Tests the parameter errorIfEmpty for Various Formats
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1205">SDMXCONV-1205</a>
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITParameterErrorIfEmpty {

	private final static String GENERATED_PATH = "errorIfEmptyFiles/";
	@Autowired
	private ConverterDelegatorService converterDelegatorService;
	@Autowired
	private StructureService structureService;
	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;
	@Autowired
	private HeaderService headerService;
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
	 * Conversion from compact file that contains only a dataset.
	 * <p>With error if empty parameter equals true</p>
	 * <p>We expect an error</p>
	 *
	 * @throws Exception Conversion can throw any kind of errors
	 */
	@Test
	public void convertCompactErrorIfEmptyTrue() throws Exception {
		String resultFileName = "01-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream("./testfiles/errorIfEmptyFiles/01-inputWithOnlyDatasetAndSeriesNode.xml")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/errorIfEmptyFiles/DataStructure_NA_REG.xml");
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config
			SdmxInputConfig inputConfig = new SdmxInputConfig();
			inputConfig.setErrorIfEmpty(true);
			ConverterInput converterInput = new ConverterInput(Formats.COMPACT_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream, new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			Exception exception = assertThrows(Exception.class, () -> {
				converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			});
			assertEquals(SdmxDataFormatException.class, exception.getClass());
			assertEquals(
					"No Observation is defined in the series FREQ:A,REF_AREA:LT,COUNTERPART_AREA:B0,REF_SECTOR:S1,COUNTERPART_SECTOR:S1,ACCOUNTING_ENTRY:L,STO:F,PRICES:V,UNIT_MEASURE:XDC,TRANSFORMATION:N.",
					exception.getMessage());
		}
	}

	/**
	 * Conversion from compact file that contains only a dataset.
	 * <p>With error if empty parameter equals false</p>
	 * <p>Conversion should proceed</p>
	 *
	 * @throws Exception Conversion can throw any kind of errors
	 */
	@Test()
	public void convertCompactErrorIfEmptyFalse() throws Exception {
		String resultFileName = "01-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream(
				"./testfiles/errorIfEmptyFiles/01-inputWithOnlyDatasetAndSeriesNode.xml")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure(
					"./testfiles/errorIfEmptyFiles/DataStructure_NA_REG.xml");
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config
			SdmxInputConfig inputConfig = new SdmxInputConfig();
			inputConfig.setErrorIfEmpty(false);
			ConverterInput converterInput = new ConverterInput(Formats.COMPACT_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream), inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream,
					new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals(
					"the generated file at " + completeResultTargetFileName + " is different than what is expected at " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
		}
	}
	@Test()
	public void convertStructureSpecificErrorIfEmptyTrue() throws Exception {
		String resultFileName = "sdmxcsv-output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream(
				//keyFamily
				"./testfiles/errorIfEmptyFiles/EBSFATS_T33_A_SI_2021_0000_V0004.xml")) {
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/errorIfEmptyFiles/ESTAT+EBSFATS_T33_A+1.1_20240430.xml");
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/errorIfEmptyFiles/ESTAT+EBSFATS_T33_A+1.1_20240430.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config

			SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
			SdmxInputConfig inputConfig = new SdmxInputConfig();
			inputConfig.setErrorIfEmpty(true);
			outputConfig.setDelimiter(";");
			ConverterInput converterInput = new ConverterInput(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
					readableDataLocationFactory.getReadableDataLocation(inputStream), inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV, outputStream,
					outputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow);
			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated file at "
					+ completeResultTargetFileName +
					" is different than what is expected at "
					+ completeResultTestFilesFileName, expectedFile, generatedFile);
		}
	}

	/**
	 * Conversion from Csv Flat file that contains only a dataset.
	 * <p>With error if empty parameter equals true</p>
	 * <p>Exception is expected</p>
	 *
	 * @throws Exception Conversion can throw any kind of errors
	 */
	@Test()
	public void convertCsvErrorIfEmptyTrue() throws Exception {
		String resultFileName = "02-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream("./testfiles/errorIfEmptyFiles/02-csvOnlyHeader.csv")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure(
					"./testfiles/errorIfEmptyFiles/DataStructure_NA_REG.xml");
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config
			CsvInputConfig inputConfig = new CsvInputConfig();
			inputConfig.setErrorIfEmpty(true);
			inputConfig.setLevelNumber("1");
			HeaderBean header = headerService.parseSdmxHeaderProperties(
					new FileInputStream("./testfiles/singleCsv_input/header.prop"));
			inputConfig.setHeader(header);
			inputConfig.setDelimiter(";");
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

			ConverterInput converterInput = new ConverterInput(Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream), inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream,
					new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			Exception exception = assertThrows(Exception.class, () -> {
				converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			});
			assertEquals(SdmxSyntaxException.class, exception.getClass());
			assertEquals("Data reader error. Collection is empty.", exception.getMessage());
		}
	}

	@Test()
	public void convertMultiLevelErrorIfEmptyTrue() throws Exception {
		String resultFileName = "02-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream("./testfiles/errorIfEmptyFiles/20230201.csv")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure(
					"./testfiles/errorIfEmptyFiles/SDMX_3.0_ESTAT+DEM_DEMOBAL+1.1.xml");
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config
			CsvInputConfig inputConfig = new CsvInputConfig();
			inputConfig.setErrorIfEmpty(true);
			inputConfig.setLevelNumber("2");
			inputConfig.setDelimiter(";");
			inputConfig.setMapping(csvService.buildInputDimensionLevelMapping(new FileInputStream("./testfiles/errorIfEmptyFiles/mappingMultiLevel.xml"),2));

			inputConfig.setHeader(headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/multiCsv_input/header.prop")));
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.EMPTY);
			ConverterInput converterInput = new ConverterInput(Formats.MULTI_LEVEL_CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream), inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.STRUCTURE_SPECIFIC_DATA_3_0, outputStream,
					new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			Exception exception = assertThrows(Exception.class, () -> {
				converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			});
			assertEquals(SdmxSyntaxException.class, exception.getClass());
			assertEquals("Data reader error. Collection is empty.", exception.getMessage());
		}
	}

	/**
	 * Conversion from CSV flat file that contains only a dataset.
	 * <p>With error if empty parameter equals false</p>
	 * <p>Conversion should proceed</p>
	 *
	 * @throws Exception Conversion can throw any kind of errors
	 */
	@Test()
	public void convertCsvErrorIfEmptyFalse() throws Exception {
		String resultFileName = "02-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream("./testfiles/errorIfEmptyFiles/02-csvOnlyHeader.csv")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/errorIfEmptyFiles/DataStructure_NA_REG.xml");
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config
			CsvInputConfig inputConfig = new CsvInputConfig();
			inputConfig.setErrorIfEmpty(false);
			inputConfig.setLevelNumber("1");
			HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
			inputConfig.setHeader(header);
			inputConfig.setDelimiter(";");
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

			ConverterInput converterInput = new ConverterInput(Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream, new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
	}

	/**
	 * Conversion from Sdmx_Csv file that contains only a dataset.
	 * <p>With error if empty parameter equals true</p>
	 * <p>Exception is expected</p>
	 *
	 * @throws Exception Conversion can throw any kind of errors
	 */
	@Test(expected = SdmxSyntaxException.class)
	public void convertSdmxCsvErrorIfEmptyFalse() throws Exception {
		String resultFileName = "03-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream("./testfiles/errorIfEmptyFiles/03-sdmxCsvOnlyHeader.csv")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/errorIfEmptyFiles/DataStructure_NA_REG.xml");
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config
			SdmxCsvInputConfig inputConfig = new CsvInputConfig();
			inputConfig.setErrorIfEmpty(true);
			HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
			inputConfig.setHeader(header);
			inputConfig.setDelimiter(";");
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

			ConverterInput converterInput = new ConverterInput(Formats.SDMX_CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream, new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
		}
	}

	/**
	 * Conversion from Sdmx_CSV file that contains only a dataset.
	 * <p>With error if empty parameter equals false</p>
	 * <p>Conversion should proceed</p>
	 *
	 * @throws Exception Conversion can throw any kind of errors
	 */
	@Test()
	public void convertSdmxCsvErrorIfEmptyTrue() throws Exception {
		String resultFileName = "03-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream("./testfiles/errorIfEmptyFiles/03-sdmxCsvOnlyHeader.csv")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/errorIfEmptyFiles/DataStructure_NA_REG.xml");
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Config
			SdmxCsvInputConfig inputConfig = new CsvInputConfig();
			inputConfig.setErrorIfEmpty(false);
			HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
			inputConfig.setHeader(header);
			inputConfig.setDelimiter(";");
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

			ConverterInput converterInput = new ConverterInput(Formats.SDMX_CSV,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream, new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
	}

	/**
	 * Conversion from Excel file that doesn't have any valid observations.
	 * <p>With error if empty parameter equals false</p>
	 *
	 * @throws Exception Conversion can throw any kind of errors
	 */
	@Test()
	public void convertExcelErrorIfEmptyFalse() throws Exception {
		String resultFileName = "04-output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		try (InputStream inputStream = new FileInputStream("./testfiles/errorIfEmptyFiles/04-ENERGY_NUCLEAR-input.xlsm")) {
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/errorIfEmptyFiles/04-ESTAT_ENERGY_1.2-dataflow.xml");
			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/errorIfEmptyFiles/04-ESTAT_ENERGY_1.2-dataflow.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
			//ouputFileName
			File outputFile = new File(completeResultTargetFileName);
			OutputStream outputStream = new FileOutputStream(outputFile);
			//Input Config
			ExcelInputConfigImpl inputConfig = new ExcelInputConfigImpl();
			inputConfig.setErrorIfEmpty(true);
			HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
			inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/errorIfEmptyFiles/04-JF_TEST_ENERGY-params.xlsx"), new FirstFailureExceptionHandler()));
			inputConfig.setHeader(header);
			//Output config
			SdmxCsvOutputConfig sdmxOutputConfig = new SdmxCsvOutputConfig();
			sdmxOutputConfig.setDelimiter(";");
			sdmxOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			ConverterInput converterInput = new ConverterInput(Formats.EXCEL,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					inputConfig);
			ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV, outputStream, sdmxOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow);

			// make the conversion
			Exception exception = assertThrows(Exception.class, () -> {
						converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
					});
			assertEquals(SdmxSyntaxException.class, exception.getClass());
			assertEquals("Data reader error. Collection is empty.", exception.getMessage());
		}
	}

}
