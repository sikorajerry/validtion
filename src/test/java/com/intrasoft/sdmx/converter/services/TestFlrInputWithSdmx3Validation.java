package com.intrasoft.sdmx.converter.services;

import org.apache.logging.log4j.core.config.Configurator;
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

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestFlrInputWithSdmx3Validation {

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


    /**
     * SDMXCONV-1564
     * Test file created for recognition only one complex value of component as complex.
     */
    @Test
    public void test_validation_flr() throws Exception {
        try(InputStream dataInputStream = new FileInputStream("./testfiles/flr_input_SDMX3.0/flrWithComplex.flr");
            InputStream structureInputStream = new FileInputStream("./testfiles/flr_input_SDMX3.0/DSD3.0_FLR_INPUT.xml")) {
            List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, configureValidationParamsForFlrInput(CsvInputColumnHeader.NO_HEADER, false), DEFAUL_LIMIT_ERRORS).getErrors();
            Assert.assertNotNull(errorList);
        }
    }

    private FLRInputConfig configureValidationParamsForFlrInput(CsvInputColumnHeader columnHeader, boolean isMissingConceptCase) throws Exception {
        FLRInputConfig flrInputConfig = new FLRInputConfig();
        try(InputStream mapping = new FileInputStream("./testfiles/flr_input_SDMX3.0/mapping.xml")) {
            Map<String, FlrInColumnMapping> flrMap = flrService.buildInputDimensionLevelMapping(mapping);
            flrInputConfig.setMapping(flrMap);
            flrInputConfig.setHeader(new HeaderBeanImpl(DEFAULT_HEADER_ID, DEFAULT_SENDER_ID));
            flrInputConfig.setInputColumnHeader(columnHeader);
        }
        return flrInputConfig;
    }

}
