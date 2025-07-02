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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.config.GesmesInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;

import junitx.framework.FileAssert;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITGesmesTSInput {
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
    
	@Rule
	public final ExpectedException expectedException = ExpectedException.none();
   
    @Autowired
    private StructureService structureService;  
    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    private final static String GENERATED_PATH = "gesmesTS_input/";

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
	
    /** negative test, the input file is not Gesmes TS compliant */
    @Test
    public void convertToCompact20() throws Exception{
       expectedException.expect(IllegalArgumentException.class);
		
       String resultFileName = "2_compact20FromGesmesTS21.xml"; 
       String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
       Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
       InputStream inputStream = new FileInputStream("./testfiles/gesmesTS_input/2_input_gesmesTS21_BIS_JOINT+DEBT+1.0.ges");
         
       //keyFamily
       SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml");
       DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
     
       //ouputFileName
       File outputFile =  new File (completeResultTargetFileName);
       OutputStream outputStream = new FileOutputStream(outputFile);
     
       // make the conversion
       TestConverterUtil.convert(   Formats.GESMES_TS,
                            Formats.COMPACT_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new GesmesInputConfig(),
       		                new SdmxOutputConfig(),
       		                kf,
       		                null,
       		                converterDelegatorService);
    }
	
	@Test
	public void convertToGeneric20() throws Exception{
		String resultFileName = "1_output_generic20_UE_NON_FINANCE.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		InputStream inputStream = new FileInputStream("./testfiles/gesmesTS_input/1_input_gesmesTS21_UE_NON_FINANCE.xml");
     
		//keyFamily
		SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
		DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
     
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
     
		// make the conversion
		TestConverterUtil.convert(   Formats.GESMES_TS,
                            Formats.GENERIC_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new GesmesInputConfig(),
       		                new SdmxOutputConfig(),
       		                kf,
       		                null,
       		                converterDelegatorService);
       
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
	}
	
	@Test
	public void convertToCompact20_3() throws Exception{
		String resultFileName = "3_output_compact_ESTAT_STS_v2.1.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		InputStream inputStream = new FileInputStream("./testfiles/gesmesTS_input/3_input_gesmesTS21_ESTAT_STS_v2.1.ges");
     
		//keyFamily
		SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/ESTAT_STS_v2.1.xml");
		DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
     
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
     
		//configuration
		GesmesInputConfig gesmesInputConfig = new GesmesInputConfig();
		gesmesInputConfig.setSdmxBeans(structure);
		
		// make the conversion
		TestConverterUtil.convert(Formats.GESMES_TS,
                            Formats.COMPACT_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            gesmesInputConfig,
       		                new SdmxOutputConfig(),
       		                kf,
       		                null,
       		                converterDelegatorService);
		
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
	}
	
	@Test
	public void convertToCross20DimensionWildcard() throws Exception{
		String resultFileName = "4_output_cross20FromGesmesTS_BIS_JOINT_DEBT_v1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		InputStream inputStream = new FileInputStream("./testfiles/gesmesTS_input/4_input_gesmesTS21_BIS_JOINT_DEBT_v1.0.ges");
     
		//keyFamily
		SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml");
		DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
     
		//configuration
		GesmesInputConfig gesmesInputConfig = new GesmesInputConfig();
		gesmesInputConfig.setSdmxBeans(structure);
		
		// make the conversion
		TestConverterUtil.convert(   Formats.GESMES_TS,
                            Formats.CROSS_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            gesmesInputConfig,
       		                new SdmxOutputConfig(),
       		                kf,
       		                null,
       		                converterDelegatorService, 
       		                null);
		
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
	}
	
	@Test
	public void convertToCompact20DimensionWildcard() throws Exception{
		String resultFileName = "4_compact20FromGesmesTS_BIS_JOINT_DEBT_v1.0_2.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		InputStream inputStream = new FileInputStream("./testfiles/gesmesTS_input/4_input_gesmesTS21_BIS_JOINT_DEBT_v1.0.ges");
     
		//keyFamily
		SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/BIS_JOINT_DEBT_v1.0.xml");
		DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
     
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
     
		//configuration
		GesmesInputConfig gesmesInputConfig = new GesmesInputConfig();
		gesmesInputConfig.setSdmxBeans(structure);
		
		// make the conversion
		TestConverterUtil.convert(   Formats.GESMES_TS,
                            Formats.COMPACT_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            gesmesInputConfig,
       		                new SdmxOutputConfig(),
       		                kf,
       		                null,
       		                converterDelegatorService);
	}
}
