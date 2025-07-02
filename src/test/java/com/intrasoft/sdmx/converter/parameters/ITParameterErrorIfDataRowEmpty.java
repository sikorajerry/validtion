package com.intrasoft.sdmx.converter.parameters;

import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.ValidationService;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.SdmxMLInputConfig;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.estat.struval.ValidationError;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Tests the parameter errorIfDataValuesEmpty for SDMX_CSV format
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1194">SDMXCONV-1194</a>
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITParameterErrorIfDataRowEmpty {

	private final static String GENERATED_PATH = "errorIfDataValuesEmpty/";

	private static final int DEFAULT_LIMIT_ERRORS = 10;

	@Autowired
	private StructureService structureService;

	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;

	@Autowired
	private ValidationService validationService;

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
	 * If the errorIfDataValuesEmpty = true error for rows with all values = null must be thrown.
	 * (e.g.;;;;;;;;;)
	 *
	 * @throws FileNotFoundException
	 */
	@Test
	public void errorIfDataValuesEmptyTrue() throws FileNotFoundException {
		InputStream dataInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/inputSdmxCsv.csv");
		InputStream structureInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/ESTAT_DEM_URESPOP_1.0.xml");
		SdmxCsvInputConfig inputConfig = new SdmxCsvInputConfig();
		inputConfig.setErrorIfDataValuesEmpty(true);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, inputConfig, 20).getErrors();
		assertNotNull(errorList);
		assertEquals(2, errorList.size());
		assertEquals(errorList.get(0).getMessage(), "Data reader error. Row of data was found with no values present.");
	}

	/**
	 * If the errorIfDataValuesEmpty = false no errors for row with empty data will be thrown.
	 * (e.g.;;;;;;;;;)
	 *
	 * @throws FileNotFoundException
	 */
	@Test
	public void errorIfDataValuesEmptyFalse() throws FileNotFoundException {
		InputStream dataInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/inputSdmxCsv.csv");
		InputStream structureInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/ESTAT_DEM_URESPOP_1.0.xml");
		SdmxCsvInputConfig inputConfig = new SdmxCsvInputConfig();
		inputConfig.setErrorIfDataValuesEmpty(false);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, inputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
		assertNotNull(errorList);
		assertEquals(7, errorList.size());
	}

	@Test
	public void Compact_errorIfDataValuesEmptyTrue() throws FileNotFoundException {
		InputStream dataInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/01-inputWithOnlyDatasetAndSeriesNode.xml");
		InputStream structureInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/DataStructure_NA_REG.xml");
		SdmxMLInputConfig inputConfig = new SdmxMLInputConfig();
		inputConfig.setErrorIfEmpty(true);
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, inputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
		assertNotNull(errorList);
		assertEquals(4, errorList.size());
	}

	@Test
	public void Compact_errorIfDataValuesEmptyTrue_OnlyDataset() throws FileNotFoundException {
		InputStream dataInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/OnlyDataset.xml");
		InputStream structureInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/DataStructure_NA_REG.xml");
		SdmxMLInputConfig inputConfig = new SdmxMLInputConfig();
		inputConfig.setErrorIfEmpty(true);
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, inputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
		assertNotNull(errorList);
		assertEquals(1, errorList.size());
		assertEquals(errorList.get(0).getMessage(), "Data reader error. Collection is empty.");
	}
	@Test
	public void Compact_errorIfDataValuesEmptyTrue_OnlyHeader() throws FileNotFoundException {
		InputStream dataInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/OnlyHeader.xml");
		InputStream structureInputStream = new FileInputStream("./testfiles/errorIfDataValuesEmpty/DataStructure_NA_REG.xml");
		SdmxMLInputConfig inputConfig = new SdmxMLInputConfig();
		inputConfig.setErrorIfEmpty(true);
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, inputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
		assertNotNull(errorList);
		assertEquals(1, errorList.size());
		assertEquals(errorList.get(0).getMessage(), "Data reader error. Collection is empty.");
	}
}
