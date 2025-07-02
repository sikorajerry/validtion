package com.intrasoft.sdmx.converter.services;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * These are just a few tests to see if the Validaiton Engine functionality has been correctly copied and integrated into converter api.
 * For more tests related to validation please check the Struval project (and tests)
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestSdmxCsv2Validation {

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
    public void testValidation() throws IOException {
        try (InputStream dataInputStream = new FileInputStream("./testfiles/Sdmx_Csv_2_input/sdmx_csv_2.csv");
             InputStream structureInputStream = new FileInputStream("./testfiles/Sdmx_Csv_2_input/TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml")) {
            List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
            assertNotNull(errorList);
            assertEquals(errorList.size(), 6);
            Assert.assertTrue(errorList.get(1).getMessage().contains("Attribute OBS_STATUS defined as mandatory in DSD"));
            Assert.assertTrue(errorList.get(0).getMessage().contains("Measure MEASURE_1 is reporting invalid value"));
        }
    }

    @Test
    public void testValidationWithNoError() throws IOException {
        try (InputStream dataInputStream = new FileInputStream("./testfiles/Sdmx_Csv_2_input/sdmx_csv_2_no_errors.csv");
             InputStream structureInputStream = new FileInputStream("./testfiles/Sdmx_Csv_2_input/NAMAIN_IDC_N1.9_SDMX3.0.xml")) {
            List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader.USE_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
            assertNotNull(errorList);
            assertEquals(errorList.size(), 0);
        }
    }

    private SdmxCsvInputConfig configureValidationParamsForSDMXCsvInput(CsvInputColumnHeader columnHeader, boolean isAllowColumns) {
        SdmxCsvInputConfig sdmxCsvInputConfig = new SdmxCsvInputConfig();
        sdmxCsvInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
        sdmxCsvInputConfig.setInputColumnHeader(columnHeader);
        sdmxCsvInputConfig.setAllowAdditionalColumns(isAllowColumns);
        sdmxCsvInputConfig.setDelimiter(";");
        sdmxCsvInputConfig.setSubFieldSeparationChar("#");
        sdmxCsvInputConfig.setErrorIfEmpty(false);
        sdmxCsvInputConfig.setErrorIfDataValuesEmpty(false);
        sdmxCsvInputConfig.setSdmxCsv20(true);
        return sdmxCsvInputConfig;
    }

}
