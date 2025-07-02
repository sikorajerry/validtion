package com.intrasoft.sdmx.converter.services;

import org.apache.logging.log4j.core.config.Configurator;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.FLRInputConfig;
import org.estat.sdmxsource.util.csv.FlrInColumnMapping;
import org.estat.struval.ValidationError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

/**
 * These are just a few tests to see if the Validaiton Engine functionality has been correctly copied and integrated into converter api. 
 * For more tests related to validation please check the Struval project (and tests)
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestFlrValidation {
	
	@Autowired
	private ValidationService validationService;
	
	@Autowired
	private FlrService flrService;
	
	@Autowired
	private StructureService structureService;

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
	private static final int DEFAULT_LIMIT_ERRORS = 100;
	
	@Test
	public void testFlrOk() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_validation/1_flr_ok.txt");
			InputStream structureInputStream = new FileInputStream("./testfiles/flr_validation/BIS_JOINT_DEBT_v1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForFlrInput(CsvInputColumnHeader.NO_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(0, errorList.size());
		}
	}

    //Illegal values for coded components
	@Test
	public void test_validation_case4() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_validation/3_flr_illegal_value.txt");
			InputStream structureInputStream = new FileInputStream("./testfiles/flr_validation/BIS_JOINT_DEBT_v1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForFlrInput(CsvInputColumnHeader.NO_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertEquals(1, errorList.size());
			Assert.assertTrue(errorList.get(0).getMessage().contains("FREQ is reporting value which is not a valid representation"));
		}
	}
		
	//Missing dimensions
	@Test
	public void test_validation_case5() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_validation/4_flr_missing_dimesnion.txt");
			InputStream structureInputStream = new FileInputStream("./testfiles/flr_validation/BIS_JOINT_DEBT_v1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForFlrInput(CsvInputColumnHeader.NO_HEADER, true), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("The dataset contains a header row with a missing mandatory dimension: FREQ."));
		}
	}

	//Illegal value for non-coded components
	@Test
	public void test_validation_case6() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_validation/5_flr_illegal_non_coded.txt");
			InputStream structureInputStream = new FileInputStream("./testfiles/flr_validation/BIS_JOINT_DEBT_v1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForFlrInput(CsvInputColumnHeader.NO_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			//SDMXCONV-1083
			Assert.assertTrue(errorList.get(0).getMessage().contains("Primary Measure OBS_VALUE is reporting invalid value which should be a floating point number with double precision('Double')"));
		}
	}

	//Duplicate obs
	@Test
	public void test_validation_case7() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_validation/6_flr_duplicate_obs.txt");
			InputStream structureInputStream = new FileInputStream("./testfiles/flr_validation/BIS_JOINT_DEBT_v1.0.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForFlrInput(CsvInputColumnHeader.NO_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Duplicate observation found"));
		}
	}
	@Test
	public void test_validation_case8() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_validation/Input_FLR.flr");
			InputStream structureInputStream = new FileInputStream("./testfiles/flr_validation/SDMX_3.0_ESTAT+DEM_DEMOBAL+1.1.xml")) {
			List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsErrorIfEmptyTrueForFlr(CsvInputColumnHeader.NO_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
			Assert.assertNotNull(errorList);
			Assert.assertTrue(errorList.get(0).getMessage().contains("Collection is empty."));
		}
	}

	//Flr with incorrect mapping file
	@Test
	public void test_validation_case9() throws Exception{
		try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_validation/LFS_A_A_RO_2023_0000_V0003_short.txt");
			InputStream structureInputStream = new FileInputStream("./testfiles/flr_validation/ESTAT+LFS_A_A+4.0 (1).xml")) {
			int obsCount = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForFlrWithIncorrectMapping(CsvInputColumnHeader.NO_HEADER, false), DEFAULT_LIMIT_ERRORS).getObsCount();
			Assert.assertEquals(obsCount, 10);
		}
	}

	private FLRInputConfig configureValidationParamsForFlrInput(CsvInputColumnHeader columnHeader, boolean isMissingConceptCase) throws Exception {		
		FLRInputConfig flrInputConfig = new FLRInputConfig();
		InputStream mapping = null;
		InputStream transcodingFile = null;
		try {
			if (isMissingConceptCase) {
				mapping = new FileInputStream("./testfiles/flr_validation/mapping2.xml");
			} else {
				mapping = new FileInputStream("./testfiles/flr_validation/mapping.xml");
			}
			transcodingFile = new FileInputStream("./testfiles/flr_validation/transcoding.xml");
			Map<String, FlrInColumnMapping> flrMap = flrService.buildInputDimensionLevelMapping(mapping);
			LinkedHashMap<String, LinkedHashMap<String, String>> transcoding = structureService.readStructureSetMap(transcodingFile);
			flrInputConfig.setTranscoding(transcoding);
			flrInputConfig.setMapping(flrMap);
			flrInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
			flrInputConfig.setInputColumnHeader(columnHeader);
			flrInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("GESMES"));
			flrInputConfig.setInputOrdered(true);
		} finally {
			mapping.close();
			transcodingFile.close();
		}
		return flrInputConfig;
	}

	private FLRInputConfig configureValidationParamsErrorIfEmptyTrueForFlr(CsvInputColumnHeader columnHeader, boolean isMissingConceptCase) throws Exception {
		FLRInputConfig flrInputConfig = new FLRInputConfig();
        try (InputStream mapping = new FileInputStream("./testfiles/flr_validation/mapping.xml")) {
            Map<String, FlrInColumnMapping> flrMap = flrService.buildInputDimensionLevelMapping(mapping);
            flrInputConfig.setMapping(flrMap);
            flrInputConfig.setErrorIfEmpty(true);
            flrInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
            flrInputConfig.setInputColumnHeader(columnHeader);
            flrInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("GESMES"));
            flrInputConfig.setInputOrdered(true);
        }
		return flrInputConfig;
	}

	private FLRInputConfig configureValidationParamsForFlrWithIncorrectMapping(CsvInputColumnHeader columnHeader, boolean isMissingConceptCase) throws Exception {
		FLRInputConfig flrInputConfig = new FLRInputConfig();
        try (InputStream mapping = new FileInputStream("./testfiles/flr_validation/MappingFile_LFS_Yearly_2023.xml");
			 InputStream transcodingFile = new FileInputStream("./testfiles/flr_validation/LFS_Transcoding_FLR_to_SDMX_CSV_Yearly_LFS_2023_230210.xml")) {
            Map<String, FlrInColumnMapping> flrMap = flrService.buildInputDimensionLevelMapping(mapping);
            LinkedHashMap<String, LinkedHashMap<String, String>> transcoding = structureService.readStructureSetMap(transcodingFile);
            flrInputConfig.setTranscoding(transcoding);
            flrInputConfig.setMapping(flrMap);
            flrInputConfig.setErrorIfEmpty(true);
            flrInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
            flrInputConfig.setInputColumnHeader(columnHeader);
            flrInputConfig.setUseGesmesDateFormat(CsvDateFormat.forDescription("GESMES"));
            flrInputConfig.setInputOrdered(true);
        }
		return flrInputConfig;
	}
}
