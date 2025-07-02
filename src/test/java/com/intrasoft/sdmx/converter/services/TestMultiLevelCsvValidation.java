package com.intrasoft.sdmx.converter.services;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
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
 * These are just a few tests to see if the Validaiton Engine functionality has been correctly copied and integrated into converter api. 
 * For more tests related to validation please check the Struval project (and tests)
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestMultiLevelCsvValidation {
	
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
	
	private static final String DEFAULT_HEADER_ID = "ID1";
	private static final String DEFAULT_SENDER_ID = "ZZ9";
	private static final int DEFAUL_LIMIT_ERRORS = 10;
	
	@Test
	public void testMultilevelCsvOk() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case1.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
					
    //Illegal values for coded components
	@Test
	public void test_validation_case2() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case2.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(2, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("TABLE_IDENTIFIER is reporting value which is not a valid representation"));
			Assert.assertTrue(errorList.get(1).getMessage().contains("COUNTRY_ORIGIN is reporting value which is not a valid representation"));
		}
	}
		
	//Illegal value for non-coded components
	@Test
	public void test_validation_case3() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case3.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4+Decimal_Obs_value.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			//SDMXCONV-1083
			Assert.assertTrue(errorList.get(0).getMessage().contains("Primary Measure OBS_VALUE is reporting invalid value which should be a numeric value('Decimal')"));
		}
	}
	
	//Duplicate obs
	@Test
	public void test_validation_case8() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case9.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Duplicate observation found"));
		}
	}
	
	//Invalid time period values
	//@Test
	public void test_validation_case4() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case5.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml");) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Invalid Date Format `ERROR`"));
		}
	}
	
	//Invalid level number negative number
	@Test
	public void test_validation_case5() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case6.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("First column of each row should be an integer greater than zero representing the level."));
		}
	}
	
	//Invalid level number , greater then csvinputconfig level provided
	@Test
	public void test_validation_case6() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case7.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("First column of each row represents the level and cannot be greater than the number of levels"));
		}
	}
	
	//Invalid level number , string instead of number
	@Test
	public void test_validation_case7() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case8.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("First column of each row should be an integer"));
		}
	}
	
	//Invalid number of columns in a certain level
	@Test
	public void test_validation_case9() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./test_files/multilevel_csv_validation/case10.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/multilevel_csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Level 2 was found with different number of columns."));
		}
	}
	
	private CsvInputConfig configureValidationParamsForCsvInput() throws Exception{		
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		try(InputStream mapping = new FileInputStream("./test_files/multilevel_csv_validation/mapping3Levels.xml")) {
			csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
			csvInputConfig.setLevelNumber("3");
			csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
			csvInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("SDMX"));
			csvInputConfig.setDelimiter(";");
			csvInputConfig.setInputOrdered(true);
			csvInputConfig.setIsEscapeCSV(false);
			csvInputConfig.setMapping(csvService.buildInputDimensionLevelMapping(mapping, 3));
		}
		return csvInputConfig;
	}
}
