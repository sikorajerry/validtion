package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidColumnMappingException;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.estat.struval.ValidationError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/test-spring-context.xml" })

/**
 * Validation tests ensure the correct behavior of the 'Allow Additional Column' parameter, depending on client requirements.
 * The default value is false. This parameter is now valid for CSV, SDMX_CSV, and Multilevel formats.
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1528">SDMXCONV-1528</a>
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1622">SDMXCONV-1622</a>
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1458">SDMXCONV-1458</a>
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1506">SDMXCONV-1506</a>
 */

public class TestAllowAdditionalColumn {

	@Autowired
	private ValidationService validationService;
	@Autowired
	private CsvService csvService;

	private static final String DEFAULT_HEADER_ID = "ID1";
	private static final String DEFAULT_SENDER_ID = "ZZ9";
	private static final int DEFAUL_LIMIT_ERRORS = 100;

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
	public void testCsvAllowFalse() throws IOException, InvalidColumnMappingException {
		try (InputStream dataInputStream = new FileInputStream(
				"./test_files/allow_additional_column/COD_GEN_A_C1_2013_0000_V0002.csv");
				InputStream structureInputStream = new FileInputStream(
						"./test_files/allow_additional_column/COD_MICRO_Multiple.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream,
					configureParamsForCsvFalseAllow(CsvInputColumnHeader.NO_HEADER), 10).getErrors();
			Assert.assertTrue(errorList.size() == 2);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Collection is empty"));
			Assert.assertTrue(errorList.get(1).getMessage()
					.contains("Row 1 of file was found with number of values different from the number of columns;"));
		}
	}

	@Test
	public void testCsvAllowTrue() throws IOException, InvalidColumnMappingException {
		try (InputStream dataInputStream = new FileInputStream(
				"./test_files/allow_additional_column/COD_GEN_A_C1_2013_0000_V0002.csv");
				InputStream structureInputStream = new FileInputStream(
						"./test_files/allow_additional_column/COD_MICRO_Multiple.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream,
					configureParamsForCsvTrueAllow(CsvInputColumnHeader.NO_HEADER), 10).getErrors();
			Assert.assertEquals(0, errorList.size());
		}
	}

	/* See also SDMXCONV-1528
				 File with mapping (simple csv): will raise an error if they are more columns in the file than what is declared in the mapping.
				 In other words if a column is not used in the mapping.
				 This covers also the case of no mapping and no header where we build a default mapping */
	@Test
	public void testCsvWithMoreDataColumns() throws IOException, InvalidColumnMappingException {
		try (InputStream dataInputStream = new FileInputStream(
				"./test_files/allow_additional_column/valid_COD_GEN_A_C1_2013_0000_V0002.csv");
				InputStream structureInputStream = new FileInputStream(
						"./test_files/allow_additional_column/COD_MICRO_Multiple.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream,
					configureParamsForCsvTrueAllow(CsvInputColumnHeader.NO_HEADER), 10).getErrors();
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage()
					.contains("Data reader error. Row 4 of file was found with number of values different from the number of columns;"));
		}
	}

	@Test
	public void testSdmx_CsvAllowFalse() throws IOException {
		try (InputStream dataInputStream = new FileInputStream(
				"./test_files/allow_additional_column/sdmx_csv_allowAdditionCol.csv");
				InputStream structureInputStream = new FileInputStream(
						"./test_files/allow_additional_column/COD_MICRO_Multiple.xml")) {
			SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
			csvInputConfig.setDelimiter(";");
			csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
			csvInputConfig.setAllowAdditionalColumns(false);
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream,
					csvInputConfig, DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(10, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("INTRA"));

		}
	}

	@Test
	public void testSdmx_CsvAllowTrue() throws IOException {
		try (InputStream dataInputStream = new FileInputStream(
				"./test_files/allow_additional_column/sdmx_csv_allowAdditionCol.csv");
				InputStream structureInputStream = new FileInputStream(
						"./test_files/allow_additional_column/COD_MICRO_Multiple.xml")) {
			SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
			csvInputConfig.setDelimiter(";");
			csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
			csvInputConfig.setAllowAdditionalColumns(true);
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream,
					csvInputConfig, DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(9, errorList.size());
		}
	}

	@Test
	public void testMultilevelCsvAllowTrue() throws Exception {
		try (InputStream dataInputStream = new FileInputStream(
				"./test_files/allow_additional_column/COMEXT_EFTA_M_IS_2025_0001_V0001.csv");
				InputStream structureInputStream = new FileInputStream(
						"./test_files/allow_additional_column/ESTAT+COMEXT_EFTA_M+2025.0_20241118.xml")) {
			InputStream mapping = new FileInputStream("./test_files/allow_additional_column/mapping_ITGS_DET.xml");
			CsvInputConfig csvInputConfig = new CsvInputConfig();
			csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
			csvInputConfig.setLevelNumber("2");
			csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
			csvInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("SDMX"));
			csvInputConfig.setDelimiter(";");
			csvInputConfig.setInputOrdered(true);
			csvInputConfig.setIsEscapeCSV(false);
			csvInputConfig.setAllowAdditionalColumns(true);
			csvInputConfig.setMapping(csvService.buildInputDimensionLevelMapping(mapping, 2));
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream,
					csvInputConfig, DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}

	@Test
	public void testMultilevelCsvAllowFalse() throws Exception {
		try (InputStream dataInputStream = new FileInputStream(
				"./test_files/allow_additional_column/COMEXT_EFTA_M_IS_2025_0001_V0001.csv");
				InputStream structureInputStream = new FileInputStream(
						"./test_files/allow_additional_column/ESTAT+COMEXT_EFTA_M+2025.0_20241118.xml")) {
			InputStream mapping = new FileInputStream("./test_files/allow_additional_column/mapping_ITGS_DET.xml");
			CsvInputConfig csvInputConfig = new CsvInputConfig();
			csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
			csvInputConfig.setLevelNumber("2");
			csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
			csvInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("SDMX"));
			csvInputConfig.setDelimiter(";");
			csvInputConfig.setInputOrdered(true);
			csvInputConfig.setIsEscapeCSV(false);
			csvInputConfig.setAllowAdditionalColumns(false);
			csvInputConfig.setMapping(csvService.buildInputDimensionLevelMapping(mapping, 2));
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream,
					csvInputConfig, DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains(
					"Row 1 of level 1 of file was found with number of values different from the number of columns;"));
		}
	}

	private CsvInputConfig configureParamsForCsvFalseAllow(CsvInputColumnHeader columnHeader)
			throws FileNotFoundException, InvalidColumnMappingException {
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(columnHeader);
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setAllowAdditionalColumns(false);
		csvInputConfig.setIsEscapeCSV(false);
		csvInputConfig.setErrorIfEmpty(true);
		csvInputConfig.setMapping(csvService.buildInputDimensionLevelMapping(
				new FileInputStream("./test_files/allow_additional_column/COD_GEN_A-mapping.xml"), 1));
		return csvInputConfig;
	}

	private CsvInputConfig configureParamsForCsvTrueAllow(CsvInputColumnHeader columnHeader)
			throws FileNotFoundException, InvalidColumnMappingException {
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(columnHeader);
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setAllowAdditionalColumns(true);
		csvInputConfig.setIsEscapeCSV(false);
		csvInputConfig.setErrorIfEmpty(true);
		csvInputConfig.setMapping(csvService.buildInputDimensionLevelMapping(
				new FileInputStream("./test_files/allow_additional_column/COD_GEN_A-mapping.xml"), 1));
		return csvInputConfig;
	}
}
