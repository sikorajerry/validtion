package com.intrasoft.sdmx.converter.services;

import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.commons.ui.services.JsonEndpointsParserService;
import com.intrasoft.sdmx.converter.io.data.Formats;
import junit.framework.Assert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.SdmxMLInputConfig;
import org.estat.struval.ValidationError;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestConfigService {
    
    @Autowired
    private ConfigService configService;

    @Autowired
    private JsonEndpointsParserService endpointsParserService;

    @Autowired
    private ValidationService validationService;

    private static final int DEFAULT_LIMIT_ERRORS = 10;

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
    public void testGetDefaultRestEndPoint() {
        Assert.assertEquals(8, endpointsParserService.getEndpoints().getEndpoints().size());
    }

    @Test
    public void testGetDefaultRestEndpoint() {
        Assert.assertEquals("c:/temp/SDMX_CONV_API/", configService.getLocalFileStorageConversion());
    }
    
    @Test
    public void testGetOutputFormat() {
    	Assert.assertEquals(Formats.GESMES_TS, configService.getDefaultOutputFormat());
    }

    /**
     * If the input includes bom character and the property validation.check.bom.error=true
     * the file is not accepted and an error is raised.
     * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1189">SDMXCONV-1198</a>
     */
    @Test
    public void testBomParameterTrue() {
        configService.setCheckBomError(true);
        List<ValidationError> errorList = validationService.validate(
                new File("./test_files/configuration_tests/crossWithBom.xml"),
                new File("./test_files/configuration_tests/ESTAT+USE_PESTICI+1.1_Extended.xml"),
                null, DEFAULT_LIMIT_ERRORS).getErrors();
        assertNotNull(errorList);
        assertEquals(1, errorList.size());
    }
    /**
     * If the input includes bom character and the property validation.check.bom.error=false
     * the file is accepted and the bom character is removed
     * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1189">SDMXCONV-1198</a>
     */
    @Test
    public void testBomParameterFalse() {
        configService.setCheckBomError(false);
        List<ValidationError> errorList = validationService.validate(
                new File("./test_files/configuration_tests/crossWithBom.xml"),
                new File("./test_files/configuration_tests/ESTAT+USE_PESTICI+1.1_Extended.xml"),
                new SdmxMLInputConfig(), DEFAULT_LIMIT_ERRORS).getErrors();
        assertNotNull(errorList);
        assertEquals(0, errorList.size());
    }
}
