package com.intrasoft.sdmx.converter.services;

import java.io.File;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.MessageGroupService;

import junitx.framework.FileAssert;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestMessageGroupService {
    
    @Autowired
    private MessageGroupService messageGroupService;

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
    public void testDetectFormatForMessageGroupFileFile() {
        File inputMessageGroupFile = new File("./test_files/STLABOUR/Data_STLABOUR.xml"); 
        Assert.assertEquals(Formats.GENERIC_SDMX, 
                     messageGroupService.detectFormatForMessageGroupFile(inputMessageGroupFile)); 
    }

    @Test
    public void testGetXmlDiscriminatorFor() {
        Assert.assertEquals("GenericData", 
                            messageGroupService.getXmlDiscriminatorFor(Formats.GENERIC_SDMX));
    }

    @Test
    public void testReplaceMessageGroup() throws Exception {
        File inputMessageGroupFile = new File("./test_files/STLABOUR/Data_STLABOUR.xml"); 
        File destination = new File("./target/fileWithMessageGroupReplaced.xml");  
        Formats result = messageGroupService.processMessageGroupFile(inputMessageGroupFile, destination);
        
        Assert.assertEquals(Formats.GENERIC_SDMX, result);
        
        File expected = new File("./test_files/STLABOUR/fileWithMessageGroupReplaced.xml");
        FileAssert.assertEquals(expected, destination);
    }

    @Test
    public void testCreateTemporaryReplacementFile() throws Exception {
        String fileSeparator = System.getProperty("file.separator"); 
        String mockMessageGroupFileName = "." +fileSeparator+ "target" + fileSeparator + "caca.xml";
        File result = messageGroupService.createTemporaryReplacementFile(mockMessageGroupFileName); 
        Assert.assertTrue(result.getName().startsWith("sdmx_message_temp"));
        Assert.assertTrue(result.getPath().contains("target"));
    }

}
