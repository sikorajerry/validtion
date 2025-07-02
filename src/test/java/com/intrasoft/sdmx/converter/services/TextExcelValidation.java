package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.io.data.excel.ExcelConfigurer;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.sdmxsource.util.excel.ExcelInputConfig;
import org.estat.struval.ValidationError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.validation.model.MultipleFailureHandlerEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

/**
 * These are just a few tests to see if the Validaiton Engine functionality has been correctly copied and integrated into converter api.
 * For more tests related to validation please check the Struval project (and tests)
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class TextExcelValidation {

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

    @Test
    public void testExcelWithParameters() throws FileNotFoundException, Exception {
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(false);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(50 + 1);
        try(   InputStream inputFile = new FileInputStream("./test_files/test_excel/REGWEB_IND_A_C1_2018_Witherrors.xls");
               InputStream dsd = new FileInputStream("./test_files/test_excel/ESTATREGWEB_IND_A1.0.xml")) {
            File excelConfig = new File("./test_files/test_excel/External_parameters_REGWEB.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(false);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsd, excelInputConfig, 1000).getErrors();
            Assert.assertNotNull(errorList);
            Assert.assertTrue(errorList.size() == 58);
            Assert.assertTrue(errorList.get(5).getMessage().contains("Observation missing time dimension for time series data"));
        }
    }
	@Test
	public void testExcelWithObs_StatusSkipColumns() throws FileNotFoundException, Exception{
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(true);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(10);
        try(InputStream inputFile = new FileInputStream("./test_files/test_excel/SkipColumns.xls");
            InputStream dsd = new FileInputStream("./test_files/test_excel/DSD_HCNE.xml")) {
            File excelConfig = new File("./test_files/test_excel/SkipColumnsParameters.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(true);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsd, excelInputConfig, 10).getErrors();
            Assert.assertTrue(!errorList.isEmpty());
            Assert.assertTrue(!allExcelParameters.get(0).getSkipColumns().isEmpty());
        }
	}

	@Test
	public void testExcelWithEndData() throws FileNotFoundException, Exception{
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(true);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(100);
		try(InputStream inputFile = new FileInputStream("./test_files/test_excel/HCNE_DSD_MLT_WorkforceMigration_2022.xls");
            InputStream dsd = new FileInputStream("./test_files/test_excel/DSD_HCNE-1.xml")) {
            File excelConfig = new File("./test_files/test_excel/HCNE_Parameters_WorkforceMigr_v1.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(false);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsd, excelInputConfig, 50).getErrors();
            Assert.assertNotNull(errorList);
            Assert.assertTrue(errorList.size() == 33);
            Assert.assertTrue(errorList.get(6).getMessage().contains("Duplicate observation found"));
        }
	}

    @Test
    public void testExcelWithParametersSkipColumnsAndRows() throws FileNotFoundException, Exception {
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(false);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(50 + 1);
        try(InputStream inputFile = new FileInputStream("./test_files/test_excel/DSD_BEL_HealthEmployment_2022_test.xls");
            InputStream dsd = new FileInputStream("./test_files/test_excel/TEST_Multiple.xml")) {
            File excelConfig = new File("./test_files/test_excel/HCNE_Parameters_v1.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(false);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsd, excelInputConfig, 1000).getErrors();
            Assert.assertTrue(errorList.isEmpty());
            Assert.assertTrue(!allExcelParameters.get(1).getSkipColumns().isEmpty());
        }
    }

    @Test
    public void testExcelWithParametersSkipColumnsAndRowsFix() throws FileNotFoundException, Exception {
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(true);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(50 + 1);
        try(InputStream inputFile = new FileInputStream("./test_files/test_excel/Sample_SkipColumns.xlsx");
            InputStream dsd = new FileInputStream("./test_files/test_excel/NA_MAIN+ESTAT+1.2_RI.xml")) {
            File excelConfig = new File("./test_files/test_excel/Sample_SkipColumns.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(true);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsd, excelInputConfig, 1000).getErrors();
            Assert.assertTrue(!errorList.isEmpty());
            Assert.assertTrue(!allExcelParameters.get(0).getSkipColumns().isEmpty());
        }
    }

    @Test
    public void testRequiredAttribute() throws FileNotFoundException, Exception {
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(true);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(5);
        try(InputStream inputFile = new FileInputStream("./test_files/RequiredAttribute/HCSHA_2011NAT_A_AT_2020_0000_V0002.xls");
            InputStream dsdFile = new FileInputStream("./test_files/RequiredAttribute/ESTAT+HCSHA_2011NAT_A+4.2_obs_status.xml")) {
            File excelConfig = new File("./test_files/RequiredAttribute/01-External_parameters_SHA_2022.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(true);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsdFile, excelInputConfig, 5).getErrors();
            Assert.assertTrue(errorList.isEmpty());
        }
    }

	/**
	 * For more info see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1396">SDMXCONV-1396</a>
	 * @throws FileNotFoundException
	 * @throws Exception
	 */
	@Test
	public void testValidationResultPositionsOfObsLevelAtt() throws FileNotFoundException, Exception {
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(false);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(250);
		try(InputStream inputFile = new FileInputStream("./test_files/ValidationPositions/ESTAMOD.xls");
            InputStream dsdFile = new FileInputStream("./test_files/ValidationPositions/ESTAT+EFA_ACC_A+1.0.xml")) {
            File excelConfig = new File("./test_files/ValidationPositions/Parameters_ADD_Mod.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(false);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsdFile, excelInputConfig, 250).getErrors();
            Assert.assertTrue(!errorList.isEmpty());
            Assert.assertTrue(errorList.size() == 73);
            Assert.assertTrue(errorList.get(31).getMessage().contains("Data Attribute OBS_STATUS is reporting value which is not a valid representation in referenced codelist"));
            Assert.assertTrue(errorList.get(31).getErrorDetails().getErrorPositions().get(0).getCurrentCell().equals("StaffRegional!B11"));
        }
	}

	@Test
	public void testExcelErrorIfEmpty() throws FileNotFoundException, Exception{
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(true);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(100);
		try(InputStream inputFile = new FileInputStream("./test_files/test_excel/emptyInput.xlsx");
            InputStream dsd = new FileInputStream("./test_files/test_excel/ESTATANI_SLAUGHT_M.xml")) {
            File excelConfig = new File("./test_files/test_excel/parametersEmpty.xlsx");
            excelInputConfig.setErrorIfEmpty(true);
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }

            excelInputConfig.setConfigInsideExcel(false);
            excelInputConfig.setConfiguration(allExcelParameters);

            List<ValidationError> errorList = validationService.validate(inputFile, dsd, excelInputConfig, 50).getErrors();
            Assert.assertNotNull(errorList);
            Assert.assertTrue(errorList.size() == 3);
            Assert.assertTrue(errorList.get(3).getMessage().contains("Collection is Empty"));
        }
	}

    //SDMXCONV-1618
    @Test
    public void testExcelErrorOccurrences() throws Exception{
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(false);
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(1000);
        try(InputStream inputFile = new FileInputStream("./test_files/ValidationPositions/01-ESSPROS_UBR_A_SK_2019_0000_V9011.xlsx");
                InputStream dsdFile = new FileInputStream("./test_files/ValidationPositions/01-ESTAT+UBR_A+1.0.xml")) {
            File excelConfig = new File("./test_files/ValidationPositions/01-ESSPROS_UBR_PARAM_2023_v1.5.xlsx");
            LinkedHashMap<String, ArrayList<String>> mapping;
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
            }
            excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
            //The names of the param sheets taken from the mapping
            List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
            List<ExcelConfiguration> allExcelParameters = null;
            //Read the parameter sheets with the help of the mapping from the external file
            try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
                allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
            }
            excelInputConfig.setConfigInsideExcel(false);
            excelInputConfig.setConfiguration(allExcelParameters);
            ValidationServiceReturn validated = validationService.validate(inputFile, dsdFile, excelInputConfig, 2000);
            Assert.assertTrue(validated!=null);
            Assert.assertEquals( validated.getNumberOfErrorsFound(), 900);
           }
    }
}
