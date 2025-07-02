package com.intrasoft.sdmx.converter.stress.tests;

import com.intrasoft.sdmx.converter.services.CsvService;
import com.intrasoft.sdmx.converter.services.ValidationService;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidColumnMappingException;
import org.apache.logging.log4j.core.config.Configurator;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.struval.ValidationError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * This tests are used to stress test struval for big amount of errors.
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
@Ignore
public class TestStressValidation {

	@Autowired
	private ValidationService validationService;

	@Autowired
	private CsvService csvService;

	private static final String DEFAULT_HEADER_ID = "ID1";
	private static final String DEFAULT_SENDER_ID = "ZZ9";
	private static final int DEFAUL_LIMIT_ERRORS = 10;

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
	public void testWithMultiCsv300KB() {
		Instant start = null;
		Instant end = null;
		try(InputStream dataInputStream = new FileInputStream("./test_files/stress_tests/multi/COMEXT_INTRA_M_NL_tiny.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/stress_tests/multi/ESTAT+COMEXT_INTRA_M.xml");
			InputStream mappingStream = new FileInputStream("./test_files/stress_tests/multi/mapping_ITGS_DET.xml")) {
			start = Instant.now();
			List<ValidationError> errorList = validationService.validate(dataInputStream,
					structureInputStream,
					configureValidationParamsForMutliCsvInput(mappingStream),
					500).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(261, errorList.size());
			end = Instant.now();
			System.out.println("Elapsed Time in milli seconds: "+ Duration.between(start, end).toString());
		} catch (InvalidColumnMappingException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCrossErrorsAtGroup() throws IOException {
		Instant start = null;
		Instant end = null;
		try (InputStream dataInputStream = new FileInputStream("./test_files/stress_tests/cross-495/1BadCode_groupLevel.XML");
			 InputStream structureInputStream = new FileInputStream("./test_files/stress_tests/cross-495/ESTAT+USE_PESTICI+1.1_Extended.xml");) {
			start = Instant.now();
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, null, 1000).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 1);
			assertEquals(errorList.get(0).getMessage(), "Dimension FREQ is reporting value which is not a valid representation in referenced codelist \"urn:sdmx:org.sdmx.infomodel.codelist.Codelist=SDMX:CL_FREQ(2.0)\".");
			assertEquals(errorList.get(0).getErrorDetails().getErrorPositions().size(), 1);
			end = Instant.now();
			System.out.println("Elapsed Time in seconds: "+ Duration.between(start, end).toString());
		}
	}

	@Test
	public void testCrossErrors3Bad() throws IOException {
		Instant start = null;
		Instant end = null;
		try (InputStream dataInputStream = new FileInputStream("./test_files/stress_tests/cross-495/3BadCodes.XML");
			 InputStream structureInputStream = new FileInputStream("./test_files/stress_tests/cross-495/ESTAT+USE_PESTICI+1.1_Extended.xml");) {
			start = Instant.now();
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, null, 1000).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 3);
			end = Instant.now();
			System.out.println("Elapsed Time in seconds: "+ Duration.between(start, end).toString());
		}
	}

	@Test
	public void testCrossErrorsMultipleBad() throws IOException {
		Instant start = null;
		Instant end = null;
		try (InputStream dataInputStream = new FileInputStream("./test_files/stress_tests/cross-495/MultipleBadCodesatgrouplevel.XML");
			 InputStream structureInputStream = new FileInputStream("./test_files/stress_tests/cross-495/ESTAT+USE_PESTICI+1.1_Extended.xml");) {
			start = Instant.now();
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, null, 5).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 5);
			end = Instant.now();
			System.out.println("Elapsed Time in seconds: "+ Duration.between(start, end).toString());
		}
	}

	@Test
	public void testWithMultiCsv5MB() {
		Instant start = null;
		Instant end = null;
		try(InputStream dataInputStream = new FileInputStream("./test_files/stress_tests/multi/COMEXT_INTRA_M_NL_smaller.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/stress_tests/multi/ESTAT+COMEXT_INTRA_M.xml");
			InputStream mappingStream = new FileInputStream("./test_files/stress_tests/multi/mapping_ITGS_DET.xml")) {
			start = Instant.now();
			List<ValidationError> errorList = validationService.validate(dataInputStream,
					structureInputStream,
					configureValidationParamsForMutliCsvInput(mappingStream),
					10).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(10, errorList.size());
			end = Instant.now();
			Assert.assertEquals( 6, errorList.get(1).getErrorDetails().getErrorPositions().size());
			Assert.assertEquals(7, errorList.get(errorList.size()-1).getErrorDetails().getErrorOccurrences());
/*			for(ValidationError error : errorList) {
				System.out.println("positions: ");
				System.out.println(error.getErrorDetails().getErrorPositions().size());
			}*/
			System.out.println("Elapsed Time in milli seconds: "+ Duration.between(start, end).toString());
		} catch (InvalidColumnMappingException | IOException e) {
			e.printStackTrace();
		}
	}

	private CsvInputConfig configureValidationParamsForMutliCsvInput(InputStream mappingStream) throws InvalidColumnMappingException {
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
		csvInputConfig.setAllowAdditionalColumns(false);
		csvInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("SDMX"));
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setLevelNumber("2");
		csvInputConfig.setInputOrdered(true);
		csvInputConfig.setIsEscapeCSV(false);
		csvInputConfig.setErrorIfEmpty(false);
		csvInputConfig.setErrorIfDataValuesEmpty(false);
		csvInputConfig.setMapping(csvService.buildInputDimensionLevelMapping(mappingStream,3));
		return csvInputConfig;
	}
}
