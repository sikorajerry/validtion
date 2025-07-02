package com.intrasoft.sdmx.converter.services;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.SdmxMLInputConfig;
import org.junit.Assert;

import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.struval.ValidationError;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * These are just a few tests to see if the Validation Engine functionality has been correctly copied and integrated into converter api. 
 * For more tests related to validation please check the Struval project (and tests)
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestPeriodicityValidation {
	
	@Autowired
	private ValidationService validationService;
	
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
	public void testCsvWeekWithStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-week.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'yyyy-W01-9' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCsvWeekWithStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-week.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
		Assert.assertNotNull(errorList);
		Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'yyyy-W1-9' is not valid type for SDMX_2.1 version of Structure.");
		Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCompactWeekWithStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-week.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Www' is not valid type for SDMX v2.0.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSWeekWithStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-week.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-W1-9' is not valid type for SDMX v2.1.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCompactMonthStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-month.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Mmm' is not valid type for SDMX_2.0 version of Data.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCompactMonthStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-month.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Mmm' is not valid type for SDMX_2.0 version of Data.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSMonthStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-month.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Mmm' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSMonthStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-month.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	@Test
	public void testCsvMonthWithStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-month.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Mmm' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCsvMonthWithStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-month.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	/////////////////////////////
	@Test
	public void testCompactYearStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-year.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-A1' is not valid type for SDMX_2.0 version of Data.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCompactYearStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-year.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-A1' is not valid type for SDMX_2.0 version of Data.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSYearStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-year.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-A1' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSYearStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-year.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	@Test
	public void testCsvYearWithStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-year.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-A1' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCsvYearWithStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-year.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	///////////////////////
	@Test
	public void testCompactDStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-d.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Dddd' is not valid type for SDMX v2.0.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCompactDStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-d.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Dddd' is not valid type for SDMX v2.0.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSDStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-d.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Dddd' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSDStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-d.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	@Test
	public void testCsvDWithStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-d.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-Dddd' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCsvDWithStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-d.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	
	/*System.out.println("____________________");
	System.out.println(errorList.get(0).getMessage());
	System.out.println("____________________");*/
	
	@Test
	public void testCompactTimeRangeStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-time-range.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-MM-DD(Thh:mm:ss)/<duration>' or 'yyyy-mm-dd/duration' is not valid type for SDMX v2.0.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCompactTimeRangeStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/compact-time-range.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-MM-DD(Thh:mm:ss)/<duration>' or 'yyyy-mm-dd/duration' is not valid type for SDMX v2.0.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSTimeRangeStructure20() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-time-range.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-MM-DD(Thh:mm:ss)/<duration>' or 'yyyy-mm-dd/duration' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testSSTimeRangeStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/ss-time-range.xml");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, new SdmxMLInputConfig(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	@Test
	public void testCsvTimeRangeWithStructure20() throws IOException{
		try(	InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-time-range.csv");
				InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/ESTAT+STSALL+2.1_Extended.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(errorList.get(0).getMessage(), "Time Dimension TIME_PERIOD is reporting invalid value which 'YYYY-MM-DD(Thh:mm:ss)/<duration>' or 'yyyy-mm-dd/duration' is not valid type for SDMX_2.0 version of Structure.");
			Assert.assertEquals(1, errorList.size());
		}
	}
	
	@Test
	public void testCsvTimeRangeWithStructure21() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/periodicity_validation/csv-time-range.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/periodicity_validation/STSALL+2.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}

	private CsvInputConfig configureValidationParamsForCsvInput(CsvInputColumnHeader columnHeader) {		
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(columnHeader);    
		csvInputConfig.setDelimiter(";");    	
		csvInputConfig.setInputOrdered(true);
		csvInputConfig.setIsEscapeCSV(false);
		return csvInputConfig;
	}

}
