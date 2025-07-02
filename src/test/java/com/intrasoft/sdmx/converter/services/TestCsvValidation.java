package com.intrasoft.sdmx.converter.services;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.estat.struval.ValidationError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.sdmx.converter.sdmxsource.MessageDecoderConverter;
import com.intrasoft.sdmx.converter.util.SpringContext;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;


/**
 * These are just a few tests to see if the Validaiton Engine functionality has been correctly copied and integrated into converter api. 
 * For more tests related to validation please check the Struval project (and tests)
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestCsvValidation {
	
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
	public void testCsvOkWithHeader() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-ok-header.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	@Test
	public void testCsvOkWithNoHeader() throws IOException {
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-ok-no-header.csv");
			 InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			 List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.NO_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			 Assert.assertNotNull(errorList);
		     Assert.assertEquals(0, errorList.size());
		}
	}
	
	// SDMXCONV-1318  End game validation to return no errors.
	@Test
	public void testSDMXCsvDisplayNoErrorMessage() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-output-sdmx-validation.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/dataflows/test-output-sdmx-validation-dataflow.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}
	
	// SDMXCONV-1318  End game validation to return errors if the sign '@' exists on the input file
	@Test
	public void testSDMXCsvWithFaultySign() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-output-sdmx-validation-with-@.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/dataflows/test-output-sdmx-validation-dataflow.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("is reporting value which is not a valid representation in referenced codelist"));
		}
	}
	
	// SDMXCONV-1318  End game validation to return no errors when for example instead of having (1.1) to have (1.1.0) on the name.
		@Test
		public void testSDMXCsvDisplayNoErrorMessageWithExtraValueOnName() throws IOException{
		    try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-output-sdmx-validation-extra-number-value-name.csv");
			   InputStream structureInputStream = new FileInputStream("./test_files/dataflows/test-output-sdmx-validation-dataflow.xml")) {
			   List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			   Assert.assertNotNull(errorList);
			   Assert.assertEquals(0, errorList.size());
		    }
	    }

	//SDMXCONV-1360 Check if the input file with SMDX_CSV/CSV format is empty
	@Test
	public void testSdmx_CsvEmptyInputFile() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/EmptyFile.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/dataflows/test-output-sdmx-validation-dataflow.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, true), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("The input file is empty, please check it and try to upload one more time."));
		}
	}

	/**
	 * Fix error message in case that Dataflow value is empty in SDMX_CSV file.
	 * see SDMXCONV-1452
	 *
	 * @throws IOException
	 */
	@Test
	public void testEmptyDataflowValue() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./testfiles/sdmxCsv_input/EGR_ISRLE_A_DE_2022_0000_V0003.csv");
			 InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/ESTAT+EGR_ISRLE_A+1.0_20230208.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 1);
			assertEquals("Data reader error. Dataflow value is empty." , errorList.get(0).getMessage());
		}
	}

	@Test
	public void testCsvEmptyInputFile() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/EmptyFile.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/dataflows/test-output-sdmx-validation-dataflow.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.NO_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("The input file is empty, please check it and try to upload one more time."));
		}
	}
		
		//SDMXCONV-1355 CSV contains duplicate header (HH_CHILD) 
		@Test
		public void test_validation_case_duplicate_header() throws IOException{
		    try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case_duplicate_header.csv");
			    InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+INFOSOC_HHUDHH_A+1.0_test-validation-case_duplicate_header.xml")) {
			    List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, true), DEFAUL_LIMIT_ERRORS).getErrors();
			    Assert.assertNotNull(errorList);
			    System.out.println(errorList);
			    Assert.assertEquals(1, errorList.size());
			    Assert.assertTrue(errorList.get(0).getMessage().contains("Duplicate columns/concepts"));
		    }
	    }

		// SDMXCONV-1355 CSV contains null header that doesn't return validating error.
		@Test
		public void test_validation_case_null_header() throws IOException{
		    try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case_null_header.csv");
			    InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+INFOSOC_HHUDHH_A+1.0_test-validation-case_duplicate_header.xml")) {
			    List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, true), DEFAUL_LIMIT_ERRORS).getErrors();
			    Assert.assertNotNull(errorList);
			   Assert.assertTrue(errorList.size() == 0);
		    }
	    }

	//csv with header contains row with different length then columns size
	@Test
	public void test_validation_case1() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case1.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
		    List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
		    Assert.assertNotNull(errorList);
		    Assert.assertEquals(1, errorList.size());
		    Assert.assertTrue(errorList.get(0).getMessage().contains("number of values different from the number of columns"));
		}
	}
	
	//csv with no header contains row with different length then columns size
	@Test
	public void test_validation_case2() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case2.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.NO_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("number of values different from the number of columns"));
		}
	}
	
	
	
	//csv contains component not found in the dsd
	@Test
	public void test_validation_case3() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case3.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("The dataset contains a concept ERROR that is not defined in DSD."));
		}
	}
	
    //Illegal values for coded components
	@Test
	public void test_validation_case4() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case4.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.NO_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(2, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("TABLE_IDENTIFIER is reporting value which is not a valid representation"));
			Assert.assertTrue(errorList.get(1).getMessage().contains("COUNTRY_ORIGIN is reporting value which is not a valid representation"));
		}
	}
	
	//Missing dimensions
	@Test
	public void test_validation_case5() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case5.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("The dataset contains a header row with a missing mandatory dimension: FREQ"));
			Assert.assertTrue(errorList.get(1).getMessage().contains("The dataset contains a header row with a missing mandatory dimension: REF_SECTOR"));
		}
	}
	
	//Illegal value for non-coded components
	@Test
	public void test_validation_case6() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case6.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4+Decimal_Obs_value.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.NO_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			//SDMXCONV-1083
			Assert.assertTrue(errorList.get(0).getMessage().contains("Primary Measure OBS_VALUE is reporting invalid value which should be a numeric value('Decimal')"));
		}
	}
	
	//Duplicate obs
	@Test
	public void test_validation_case7() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case7.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.NO_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Duplicate observation found"));
		}
	}

	//Invalid time period values
	//@Test
	public void test_validation_case8() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case8.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Invalid Date Format `ERROR`"));
		}
	}

	//Invalid OBS_VALUE from codelist see SDMXCONV-1006
	@Test
	public void test_validation_case10() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case10.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTATHICPAP2.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Primary Measure OBS_VALUE is reporting value which is not a valid representation in referenced codelist"));
		}
	}
	
	@Test
	public void test_validation_case9() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-ok-header.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInputWithWrongDelimiter(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("file was found with number of values different from the number of columns"));
		}
	}

	//Validation with different number of column in the header
	// at the first row of data
	//SDMXCONV-1458 (Case 1)
	@Test
	public void test_validation_case11() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./testfiles/sdmxCsv_input/SHA_NAT_test_1.csv");
			 InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/ESTAT+SHA+4.0-Copy.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInputWithDifferentDelimiter(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 1);
			assertEquals("Data reader error. Row 2 of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used." , errorList.get(0).getMessage());
		}
	}

	//Validation with different number of column in the header
	// from dara row >1
	//SDMXCONV-1458 (Case 2)
	@Test
	public void test_validation_case12() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./testfiles/sdmxCsv_input/SHA_NAT_test_2.csv");
			 InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/ESTAT+SHA+4.0-Copy.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInputWithDifferentDelimiter(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 3);
			assertEquals("Data reader error. Row 3 of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used." , errorList.get(1).getMessage());
		}
	}

	//Validation with wrong delimiter in the first row of data
	//SDMXCONV-1458 (Case 3)
	@Test
	public void test_validation_case13() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./testfiles/sdmxCsv_input/AIR_1_AIR_C1_A_IS_2022_0000_V0006.csv");
			 InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/ESTAT+AIR_C1_A+6.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 1);
			assertEquals("Data reader error. Row 2 of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used." , errorList.get(0).getMessage());
		}
	}

	//Validation with wrong delimiter in the first row of data
	//SDMXCONV-1458 (Case 4)
	@Test
	public void test_validation_case14() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./testfiles/sdmxCsv_input/AIR_2_AIR_C1_A_IS_2022_0000_V0006.csv");
			 InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/ESTAT+AIR_C1_A+6.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 7);
			assertEquals("Data reader error. Row 18 of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used." , errorList.get(0).getMessage());
		}
	}

	//SDMXCONV-1458 (Case 5)
	@Test
	public void test_validation_case15() throws IOException {
		try (InputStream dataInputStream = new FileInputStream("./testfiles/sdmxCsv_input/AIR_3_AIR_C1_A_new.csv");
			 InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/ESTAT+AIR_C1_A+6.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(errorList.size(), 1);
			assertEquals("Data reader error. Row 2 of file was found with number of values different from the number of columns; check the field separator, the number of fields in the row and if a non-authorized character was used." , errorList.get(0).getMessage());
		}
	}

	//SDMXCONV-1045
	@Test
	public void test_validation_ofQuotedFile() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test_Quoted.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+NA_MAIN+1.6.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertTrue(errorList.size() == 0);
		}
	}

	//SDMXCONV-1361

	//Invalid character 27 of ASCII code
	@Test
	public void test_StrictValidationCharacter_case1() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/Disallow27.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/EGR_LEL_A_C1_2020_0000_V0004(1).xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("The encoding of the file is not correct and contains invalid characters in line (2 ). Please correct the file and try again! {1} value contains invalid character."));
		}
	}

	//Valid characters 10_9_13 should be accepted
	@Test
	public void test_StrictValidationCharacter_case2() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/Allow10_9_13cases.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/EGR_LEL_A_C1_2020_0000_V0004(1).xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.size() == 0);
		}
	}

	//Valid character 160 should be accepted
	@Test
	public void test_StrictValidationCharacter_case3() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/Allow160.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/EGR_LEL_A_C1_2020_0000_V0004(1).xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.size() == 0);
		}
	}


	/**
	 *SDMXCONV-1171
     * Test that number of decimal places is calculated successfully
	 * Eg: 22839058.1 should be valid when number of decimal places is set to 2
     * 	   657535.4444 should throw an error in the above case
	 */
	@Test
	public void test_validation_of_decimal_places() throws  IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/AEI_PESTUSE_5_IT_2019_0000_V0001_decimal_places.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTATAEI_PESTUSE_51.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			// In the DSD number of decimal points is set to 2:
			// <str:TextFormat textType="Double" minValue="0.0" decimals="2" pattern="^.+$"/>
			// We expect error only in the values that have >2 decimal points
			Assert.assertTrue(errorList.size() == 3);
		}
	}

	@Test
	public void test_validation_of_decimal_places_with_zeros() throws  IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/decimal_places_with_zeros.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTATAEI_PESTUSE_51.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			// In the DSD number of decimal points is set to 2:
			// <str:TextFormat textType="Double" minValue="0.0" decimals="2" pattern="^.+$"/>
			// We expect error only in the values that have >2 decimal points
			Assert.assertTrue(errorList.size() == 5);
		}
	}

	private CsvInputConfig configureValidationParamsForCsvInput(CsvInputColumnHeader columnHeader) {
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(columnHeader);
		csvInputConfig.setAllowAdditionalColumns(false);
		csvInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("GESMES"));	        
		csvInputConfig.setDelimiter(";");    	
		csvInputConfig.setInputOrdered(true);
		csvInputConfig.setIsEscapeCSV(false);
		csvInputConfig.setErrorIfEmpty(false);
		csvInputConfig.setErrorIfDataValuesEmpty(false);
		return csvInputConfig;
	}
	
	private SdmxCsvInputConfig configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader columnHeader, boolean isAllowColumns) {
		SdmxCsvInputConfig sdmxCsvInputConfig = new SdmxCsvInputConfig();
		sdmxCsvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		sdmxCsvInputConfig.setInputColumnHeader(columnHeader);
		sdmxCsvInputConfig.setAllowAdditionalColumns(isAllowColumns);
		sdmxCsvInputConfig.setDelimiter(";");
		sdmxCsvInputConfig.setErrorIfEmpty(false);
		sdmxCsvInputConfig.setErrorIfDataValuesEmpty(false);
		return sdmxCsvInputConfig;
	}

	private CsvInputConfig configureValidationParamsForCsvInputVersion2(CsvInputColumnHeader columnHeader) {
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(columnHeader);
		csvInputConfig.setAllowAdditionalColumns(true);
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputOrdered(true);
		csvInputConfig.setIsEscapeCSV(true);
		csvInputConfig.setUseGesmesDateFormat(CsvDateFormat.SDMX);
		return csvInputConfig;
	}

	private CsvInputConfig configureValidationParamsForCsvInputWithWrongDelimiter(CsvInputColumnHeader columnHeader) {
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(columnHeader);
		csvInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("GESMES"));	        
		csvInputConfig.setDelimiter(",");
		csvInputConfig.setInputOrdered(true);
		csvInputConfig.setIsEscapeCSV(false);
		return csvInputConfig;
	}

	private SdmxCsvInputConfig configureValidationParamsForSDMXCsvInputWithDifferentDelimiter(CsvInputColumnHeader columnHeader, boolean isAllowColumns) {
		SdmxCsvInputConfig sdmxCsvInputConfig = new SdmxCsvInputConfig();
		sdmxCsvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		sdmxCsvInputConfig.setInputColumnHeader(columnHeader);
		sdmxCsvInputConfig.setAllowAdditionalColumns(isAllowColumns);
		sdmxCsvInputConfig.setDelimiter(",");
		sdmxCsvInputConfig.setErrorIfEmpty(false);
		sdmxCsvInputConfig.setErrorIfDataValuesEmpty(false);
		return sdmxCsvInputConfig;
	}

	private SdmxCsvInputConfig configureValidationWithDifferentSeparator(CsvInputColumnHeader columnHeader) {
		CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
		csvInputConfig.setInputColumnHeader(columnHeader);
		csvInputConfig.setSubFieldSeparationChar(",");
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputOrdered(true);
		csvInputConfig.setIsEscapeCSV(false);
		return csvInputConfig;
	}


	/**
	 * Test file created for reporting mandatory columns missing from header row when the file is empty
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1163">SDMXCONV-1163</a>
	 * @throws IOException
	 */
	@Test
	public void testHeaderRowColumns() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case9.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+DEM_URESPOP.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(2, errorList.size());
		}
	}

	/**
	 * Test file created for reporting duplicate columns from header row
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1355">SDMXCONV-1355</a>
	 * @throws IOException
	 */
	@Test
	public void testHeaderRowColumnsForDuplicates() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case11.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(2, errorList.size());
			Assert.assertEquals(errorList.get(0).getMessage(), "Duplicate columns/concepts found in Header Row: COMPILING_ORG, DECIMALS.");
		}
	}

	/**
	 * SDMXCONV-1185
	 * Test case to demonstrate that when a mandatory dimension is missing from the header row, the error is thrown
	 * only once, in the reader and never again in Struval for every data row.
	 * @throws IOException
	 */
	@Test
	public void test_validation_missing_dimension_from_header_row() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-missing-series-ref_area.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+RESPER_FRPS2_A+1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), 10).getErrors();
			Assert.assertNotNull(errorList);
			int initialNumOfErrors = errorList.size();
			for (ValidationError error : errorList) {
				if (error.getMessage().contains("The dataset contains a header row with a missing mandatory dimension: REF_AREA.")) {
					errorList.remove(error);
					break;
				}
			}
			int newNumOfErrors = errorList.size();
			// We make sure we deleted only one error
			Assert.assertTrue((initialNumOfErrors - newNumOfErrors) == 1);
			// Now we try to find the previous error in the errorList. If we find it we fail the test
			for (ValidationError error : errorList) {
				if (error.getMessage().contains("The dataset contains a header row with a missing mandatory dimension: REF_AREA.")) {
					Assert.fail("The above error is being reported more than once in Struval");
					return;
				}
			}
		}
	}

	/**
	 * SDMXCONV-1185
	 * Test case to demonstrate that when a mandatory attribute is missing from the header row, the error is thrown
	 * only once, in the reader and never again in Struval for every data row.
	 * @throws IOException
	 */
	@Test
	public void test_validation_missing_attribute_from_header_row() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-missing-attribute-obs_status.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+RESPER_FRPS2_A+1.0_obs_status_attribute_mandatory.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), 10).getErrors();
			Assert.assertNotNull(errorList);
			int initialNumOfErrors = errorList.size();
			for (ValidationError error : errorList) {
				if (error.getMessage().contains("The dataset contains a header row with a missing mandatory attribute: OBS_STATUS.")) {
					errorList.remove(error);
					break;
				}
			}
			int newNumOfErrors = errorList.size();
			// We make sure we deleted only one error
			Assert.assertEquals(1, (initialNumOfErrors - newNumOfErrors));
			// Now we try to find the previous error in the errorList. If we find it we fail the test
			for (ValidationError error : errorList) {
				if (error.getMessage().contains("The dataset contains a header row with a missing mandatory attribute: OBS_STATUS.")) {
					Assert.fail("The above error is being reported more than once in Struval");
					return;
				}
			}
		}
	}


	/**
	 * SDMXCONV-1185
	 * Test case to demonstrate that when a mandatory dimension is missing from the data rows, the error indeed
	 * is thrown multiple times, one for each data row,
	 * since the dimension exists in the headerRow but is missing from the data rows.
	 * @throws IOException
	 */
	@Test
	public void test_validation_missing_dimension_from_data_row() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-missing-dimension-values-ref_area.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+RESPER_FRPS2_A+1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), 10).getErrors();
			Assert.assertNotNull(errorList);
			int countOfOccurrences = 0;
			for (ValidationError error : errorList) {
				if (error.getMessage().contains("The dataset contains a series with a missing mandatory concept REF_AREA")) {
					countOfOccurrences++;
				}
			}
			Assert.assertTrue(countOfOccurrences == 1);
		}
	}

	@Test
	public void test_maxLenght_property_to_show_characters_in_error_instead_of_digits() throws IOException {
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/maxLengthDoubleNA_MAIN.csv");
		InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/ESTAT+NA_MAIN+1.6.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInputVersion2(CsvInputColumnHeader.USE_HEADER), 10).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.size() == 1);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Data Attribute PRE_BREAK_VALUE"));
		}
	}

	//Test Sdmx Data reader Engine performance in a large file
	@Test
	public void performanceCsvValidationTest() {
		List<ValidationError> errorList = validationService.validate(
				new File("./test_files/csv_validation/performance/IFS_Test_10K.csv"),
				new File("./test_files/csv_validation/performance/ESTAT+IFS.xml"),
				configureValidationParamsForCsvInputVersion2(CsvInputColumnHeader.USE_HEADER),10).getErrors();
		Assert.assertNotNull(errorList);
	}

	/**
	 * SDMXCONV-1399
	 * @throws IOException
	 */
	@Test
	public void test_validation_numberOfObsProcessed() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/SDMXCONV-1399/AIR_A1_M_BE_2019_0006_V0001.CSV");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/SDMXCONV-1399/ESTAT+AIR_A1_M+6.0.xml")) {
			ValidationServiceReturn validationServiceReturn = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), 10);
			Assert.assertNotNull(validationServiceReturn.getErrors());
			Assert.assertEquals(10, validationServiceReturn.getErrors().size());
			Assert.assertEquals("Number of Observations processed: " + validationServiceReturn.getObsCount(), 3, validationServiceReturn.getObsCount());
			Assert.assertNotNull(validationServiceReturn.getErrors().get(6).getErrorDetails().getErrorPositions().get(0).getCurrentRow());
			Assert.assertTrue(4 == validationServiceReturn.getErrors().get(6).getErrorDetails().getErrorPositions().get(0).getCurrentRow());
		}
	}


	@Test
	public void test_validation_mandatory_error_wrong_line() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/test-validation-case1.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml")) {
			ValidationServiceReturn validationServiceReturn = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS);
			Assert.assertNotNull(validationServiceReturn.getErrors());
			Assert.assertEquals(1, validationServiceReturn.getErrors().size());
			Assert.assertEquals("Number of Observations processed: " + validationServiceReturn.getObsCount(), 3, validationServiceReturn.getObsCount());
		}
	}

	//SDMXCONV-1494 missing TIME_PERIOD and before was nullPointer
	@Test
	public void test_validation_1494() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/1494/data_w_header_missing_time.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/1494/SDMX_3.0_NAMAIN_IDC_N_1.9_sdmx2.1.xml")) {
			ValidationServiceReturn validationServiceReturn = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForCsvInput(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS);
			Assert.assertNotNull(validationServiceReturn.getErrors());
			Assert.assertEquals(1, validationServiceReturn.getErrors().size());
			Assert.assertEquals("The observation does not have a value for its time dimension (i.e. the reference period is missing).", validationServiceReturn.getErrors().get(0).getMessage());
		}
	}

	/**
	 * SDMXCONV-1564
	 * Test file created for recognition only one complex value of component as complex.
	 * @throws IOException
	 */
	@Test
	public void testOnlyOneComplexValue() throws IOException{
		try(InputStream dataInputStream = new FileInputStream("./test_files/csv_validation/SDMXCONV-1564/NEW_ESSPROS_EARLY_sample_bug.csv");
			InputStream structureInputStream = new FileInputStream("./test_files/csv_validation/SDMXCONV-1564/NEW_ESTAT+DF_ESSPROS_TEST_EARLYES_A+1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationWithDifferentSeparator(CsvInputColumnHeader.USE_HEADER), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}

	@Test
	public void test_input_file_encoding() throws IOException{
		FileInputStream[] structureInputStreamArray = {
				new FileInputStream("./test_files/csv_validation/test-validation-case1.csv"),
				new FileInputStream("./test_files/csv_validation/UOE_NON_FINANCE+ESTAT+0.4.xml"),
				new FileInputStream("./test_files/BIS_JOINT_DEBT/flr.txt"),
				new FileInputStream("./test_files/BIS_JOINT_DEBT/gesmes21.ges"),
				new FileInputStream("./test_files/ECB_EXR/excel.xlsx"),
				};
		 
		for(FileInputStream structureInputStream: structureInputStreamArray)
			try {
				guessEncoding(structureInputStream);
			} finally {
				if(structureInputStream!=null)
				structureInputStream.close();
			}
	}
	
	public static String guessEncoding(InputStream input) throws IOException {
		LinkedHashMap<String, int[]> encodingsScores = new LinkedHashMap<>();
	    // Load input data
	   
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		org.apache.commons.io.IOUtils.copy(input, baos);
		byte[] bytes = baos.toByteArray();
		
		/// WAY 1
		long count = 0;
	    int n = 0, EOF = -1;
	    byte[] buffer = new byte[4*1024];
	    ByteArrayInputStream bais2 = new ByteArrayInputStream(bytes);
	    ByteArrayOutputStream output = new ByteArrayOutputStream();

	    while ((EOF != (n = bais2.read(buffer))) && (count <= (1*1024*1024))) {
	        output.write(buffer, 0, n);
	        count += n;
	    }
	    
	    if (count > Integer.MAX_VALUE) {
	        throw new RuntimeException("Inputstream too large.");
	    }
	    byte[] data = output.toByteArray();
	    // Detect encoding
	    // WAY 2
	    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		 // Only part read
	    ReadableByteChannel inputChannel = Channels.newChannel(bais);
		ByteBuffer buffer2 = ByteBuffer.wrap(new byte[1*1024*1024]);
		inputChannel.read(buffer2);
		byte[] readBytes = buffer2.array();
	    // * ICU4j
	    CharsetDetector charsetDetector = new CharsetDetector();
	    charsetDetector.setText(data);
	    charsetDetector.enableInputFilter(true);
	    CharsetMatch cm = charsetDetector.detect();
	    	System.out.println(cm.getConfidence());
	    	updateEncodingsScores(encodingsScores, "3) " + cm.getName());
	    // Find winning encoding
	    Map.Entry<String, int[]> maxEntry = null;
	    for (Map.Entry<String, int[]> e : encodingsScores.entrySet()) {
	        if (maxEntry == null || (e.getValue()[0] > maxEntry.getValue()[0])) {
	            maxEntry = e;
	        }
	    }

	    String winningEncoding = maxEntry.getKey();
	    dumpEncodingsScores(encodingsScores);
	    return winningEncoding;
	}

	private static void updateEncodingsScores(Map<String, int[]> encodingsScores, String encoding) {
	    String encodingName = encoding.toLowerCase();
	    int[] encodingScore = encodingsScores.get(encodingName);

	    if (encodingScore == null) {
	        encodingsScores.put(encodingName, new int[] { 1 });
	    } else {
	        encodingScore[0]++;
	    }
	}    

	private static void dumpEncodingsScores(Map<String, int[]> encodingsScores) {
	    System.out.println(toString(encodingsScores));
	}

	private static String toString(Map<String, int[]> encodingsScores) {
	    String GLUE = ",\t ";
	    StringBuilder sb = new StringBuilder();

	    for (Map.Entry<String, int[]> e : encodingsScores.entrySet()) {
	        sb.append("\t").append(e.getKey() + ":" + e.getValue()[0] + GLUE).append((e.getKey().length()<12?"\t":""));
	    }
	    int len = sb.length();
	    sb.delete(len - GLUE.length(), len);//.append("\t");

	    return "{ " + sb.toString() + " }";
	}
	
	@Test
	public void testResourceBundleMessageSourceBasenames() {
		ConfigService configService = SpringContext.INSTANCE.getBean(ConfigService.class);
		ReloadableResourceBundleMessageSource errorDetailsMessagesSource = new ReloadableResourceBundleMessageSource();
		errorDetailsMessagesSource.setCacheSeconds(1);
		errorDetailsMessagesSource.addBasenames("NON EXISTING URL - and last is default");																	// N/A Location
		if(configService.getErrorMessagePath().startsWith("http"))
			errorDetailsMessagesSource.addBasenames(configService.getErrorMessagePath() + MessageDecoderConverter.EXPLANATION_FILE_NAME);					// CUSTOM Location
		else
			errorDetailsMessagesSource.addBasenames(new String("file:" + configService.getErrorMessagePath() + MessageDecoderConverter.EXPLANATION_FILE_NAME));	// CUSTOM Location

		errorDetailsMessagesSource.addBasenames("test_default_error_explanation");																			// DEFAULT Location

		String errorMessage = null;

		while(!ObjectUtil.validString(errorMessage)) {
			errorMessage = errorDetailsMessagesSource.getMessage("059", null, new Locale("en"));
		}
		Assert.assertEquals(errorMessage, "DEFAULT - General request error check your request files and parameters.");
	}
}
