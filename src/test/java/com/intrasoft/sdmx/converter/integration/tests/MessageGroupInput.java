package com.intrasoft.sdmx.converter.integration.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.MessageGroupService;
import com.intrasoft.sdmx.converter.services.MsgGroupReplacement;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;

import junitx.framework.FileAssert;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class MessageGroupInput {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
    
    @Autowired
    private StructureService structureService;  
    
    @Autowired
    private MessageGroupService messageGroupService; 
    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    private final static String GENERATED_PATH = "messagegroup_input/";

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
    
    @Ignore(value="Utility writer does not support message group yet")
    public void convertToUtility() throws Exception{
        File inputMessageGroupFile = new File("./test_files/STLABOUR/Data_STLABOUR.xml");
          
        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./test_files/STLABOUR/STLABOUR.xml");

        //outputFileName
        File outputFile =  new File ("./target/utilityFromMessageGroup.xml");
        OutputStream outputStream = new FileOutputStream(outputFile);
       
        MsgGroupReplacement msgGroupReplacement = messageGroupService.processMessageGroupFile(inputMessageGroupFile);
       
        InputStream inputStream = new FileInputStream(msgGroupReplacement.getReplacementFile());
       
        // make the conversion
        TestConverterUtil.convert(msgGroupReplacement.getRealFormat(),
    		             Formats.UTILITY_SDMX, 
    		             readableDataLocationFactory.getReadableDataLocation(inputStream), 
    		             outputStream, 
    		             new SdmxInputConfig(),
    		             new SdmxOutputConfig(),
    		             dataStructure,
    		             null,
    		             converterDelegatorService);
       
       FileAssert.assertEquals("the generated utility file is different than what is expected", 
                               new File("./test_files/STLABOUR/utilityFromMessageGroup.xml"), 
                               outputFile);
    }
    
    @Test
    public void convertToGeneric20() throws Exception{
		String resultFileName = "1_output_generic20_STLABOUR_OECD.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        File inputMessageGroupFile = new File("./testfiles/messagegroup_input/1_input_messagegroup_STLABOUR_OECD.xml");
     
        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/STLABOUR.xml");

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
       
        MsgGroupReplacement msgGroupReplacement = messageGroupService.processMessageGroupFile(inputMessageGroupFile);
       
        InputStream inputStream = new FileInputStream(msgGroupReplacement.getReplacementFile());
     
        // make the conversion
        TestConverterUtil.convert(msgGroupReplacement.getRealFormat(),
    		             Formats.COMPACT_SDMX, 
    		             readableDataLocationFactory.getReadableDataLocation(inputStream), 
    		             outputStream, 
    		             new SdmxInputConfig(),
    		             new SdmxOutputConfig(),
    		             dataStructure,
    		             null,
    		             converterDelegatorService);
       
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }

}
