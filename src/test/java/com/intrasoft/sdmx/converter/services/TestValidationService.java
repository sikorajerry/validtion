package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.io.data.Formats;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.SdmxMLInputConfig;
import org.estat.sdmxsource.sdmx.structureparser.SdmxBeansSchemaDecorator;
import org.estat.struval.ValidationError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.manager.DataReaderManager;
import org.sdmxsource.sdmx.util.sdmx.SdmxMessageUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.*;


/**
 * These are just a few tests to see if the Validation Engine functionality has been correctly copied and integrated into converter api.
 * For more tests related to validation please check the Struval project (and tests)
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class TestValidationService {

	private static final int DEFAULT_LIMIT_ERRORS = 10;
	@Autowired
	private ValidationService validationService;

	@Autowired
	private DataReaderManager dataReaderManager;

	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;

	@Autowired
	private StructureParsingManager structureParsingManager;

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
	public void validFile() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/ESTAT_STS/test-data-ok.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/ESTAT_STS/ESTAT+STS+2.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(0, errorList.size());
		}
	}

	@Test
	public void invalidDataFile() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/ESTAT_STS/invalid-data-validation-case1.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/ESTAT_STS/ESTAT+STS+2.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(5, errorList.size());
		}
	}

	//SDMXCONV-773
	@Test //Testing of minValue property in DSD
	public void invalidSsDataFile() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/Ss_testing/SDMXCONV-812_COMEXT_AGG_M_DE_2017_0009_V0001.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/Ss_testing/SDMXCONV-812_ESTAT+AGG_ITGS+1.0.xml");) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(3, errorList.size());
		}
	}

	@Test
	public void invalidSyntaxFile() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/validation/invalid_syntax/test-data-syntax-case1.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/validation/invalid_syntax/ESTAT+STS+2.0.xml");) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();

			assertNotNull(errorList);
			assertEquals(1, errorList.size());
			assertTrue(errorList.get(0).getMessage().contains("Unexpected close tag </message:ID>; expected </message:Header>."));
		}


	}

	@Test
	public void testHeaderIdStartingWithNumber() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/UOE_NON_FINANCE/compactHeaderIdStartingWithNumber.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml");) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 0);
		}
	}

	/**
	 * Fix dimension missing values did not report any error.
	 * see SDMXCONV-1412
	 *
	 * @throws FileNotFoundException
	 */
	@Test
	public void testDimensionMissing() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/BOP/BPM6_BOP_Q_MK_2022_0003_V0002.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/BOP/ESTAT+BPM6_BOP_Q+2.0.xml");) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 1);
			assertEquals("The dataset contains a series with a missing mandatory concept TYPE_ENTITY is missing from series and is mandatory", errorList.get(0).getMessage());
		}
	}

	/**
	 * Fix optional Attribute should not throw any error.
	 * see SDMXCONV-1450
	 *
	 * @throws FileNotFoundException
	 */
	@Test
	public void testMissingAttributeNotMandatory() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/validation/emptyAttribute/BCS_QBD_M_HR_2023_0003_V0002.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/validation/emptyAttribute/ESTAT+BCS+1.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 0);
		}
	}

	/**
	 * Fix Mandatory Attribute should not throw any error.
	 * see SDMXCONV-1450
	 *
	 * @throws FileNotFoundException
	 */
	@Test
	public void testMissingAttributeMandatory() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./test_files/validation/emptyAttribute/BCS_QBD_M_HR_2023_0003_V0002.xml");
			 InputStream structureInputStream = new FileInputStream("./test_files/validation/emptyAttribute/ESTAT+BCS+1.1-Mandatory.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 1);
		}
	}

	//SDMXCONV-1502 throw error if we have input cross and dsd is SDMX 3.0
	@Test
	public void test_validation_1502() throws IOException {
		File dataFile = new File("./test_files/validation/1502/1_input_cross20_USE_PESTICI+ESTAT+1.1.xml");
		try (InputStream structureInputStream = new FileInputStream("./test_files/validation/1502/MULTIPLE_MEASURES_DSD.xml")) {
			ReadableDataLocation dsdLocation = readableDataLocationFactory.getReadableDataLocation(structureInputStream);
			StructureWorkspace parseStructures = structureParsingManager.parseStructures(dsdLocation);
			//SDMXCONV-792
			SDMX_SCHEMA beansVersion = SdmxMessageUtil.getSchemaVersion(dsdLocation);
			SdmxBeans structureBeans = parseStructures.getStructureBeans(false);
			SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(structureBeans, beansVersion);
			DataStructureBean dsdBean = null;
			if (beansSchemaDecorator.getDataStructures() != null && !beansSchemaDecorator.getDataStructures().isEmpty()) {
				dsdBean = beansSchemaDecorator.getDataStructures().iterator().next();
			}
			DataflowBean dataflowBean = null;
			if (beansSchemaDecorator.getDataflows() != null && !beansSchemaDecorator.getDataflows().isEmpty()) {
				dataflowBean = beansSchemaDecorator.getDataflows().iterator().next();
			}
			ValidationServiceReturn validationServiceReturn = validationService.validate(dataFile, beansSchemaDecorator, new SdmxMLInputConfig(), dsdBean, dataflowBean, DEFAULT_LIMIT_ERRORS, Formats.CROSS_SDMX, "latest");
			Assert.assertNotNull(validationServiceReturn.getErrors());
			Assert.assertEquals(1, validationServiceReturn.getErrors().size());
			Assert.assertEquals("Cross sectional format is not compatible with SDMX 3.0 DSD. Please provide Cross sectional DSD in SDMX 2.0 format.", validationServiceReturn.getErrors().get(0).getMessage());
		}
	}

	/**
	 * If there is an attribute with empty string, and it is under a codelist rule then we throw an error,
	 * even if the attribute is conditional.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1574">SDMXCONV-1574</a>
	 * @throws IOException
	 */
	@Test
	public void test_validation_of_empty_dataset_attr() throws IOException {
		File dataFile = new File("./test_files/validation/1574/1-POPSTAT_POP_1979.xml");
		try (InputStream structureInputStream = new FileInputStream("./test_files/validation/1574/1-ESTAT+DEM_POP+1.1.xml")) {
			ReadableDataLocation dsdLocation = readableDataLocationFactory.getReadableDataLocation(structureInputStream);
			StructureWorkspace parseStructures = structureParsingManager.parseStructures(dsdLocation);
			//SDMXCONV-792
			SDMX_SCHEMA beansVersion = SdmxMessageUtil.getSchemaVersion(dsdLocation);
			SdmxBeans structureBeans = parseStructures.getStructureBeans(false);
			SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(structureBeans, beansVersion);
			DataStructureBean dsdBean = null;
			if (beansSchemaDecorator.getDataStructures() != null && !beansSchemaDecorator.getDataStructures().isEmpty()) {
				dsdBean = beansSchemaDecorator.getDataStructures().iterator().next();
			}
			DataflowBean dataflowBean = null;
			if (beansSchemaDecorator.getDataflows() != null && !beansSchemaDecorator.getDataflows().isEmpty()) {
				dataflowBean = beansSchemaDecorator.getDataflows().iterator().next();
			}
			String expected = "Data Attribute DECIMALS is reporting value which is not a valid representation in referenced codelist \"urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESTAT:CL_DECIMALS(1.0)\".";
			ValidationServiceReturn validationServiceReturn = validationService.validate(dataFile, beansSchemaDecorator, new SdmxMLInputConfig(), dsdBean, dataflowBean, DEFAULT_LIMIT_ERRORS, Formats.COMPACT_SDMX, "latest");
			Assert.assertNotNull(validationServiceReturn.getErrors());
			Assert.assertEquals(1, validationServiceReturn.getErrors().size());
			Assert.assertEquals(expected, validationServiceReturn.getErrors().get(0).getMessage());
		}
	}
}
