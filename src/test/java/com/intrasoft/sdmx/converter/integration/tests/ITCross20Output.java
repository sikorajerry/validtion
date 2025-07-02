package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITCross20Output {
   
    @Autowired
    private StructureService structureService;  
    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
    
    private final static String GENERATED_PATH = "cross20_output/";

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
    public void convertFromGeneric() throws Exception{
		String resultFileName = "1_output_cross20_EDUCATION+ESTAT+1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);

        try(InputStream inputStream = new FileInputStream("./testfiles/cross20_output/1_input_generic20_EDUCATION+ESTAT+1.0.xml");
            OutputStream outputStream = new FileOutputStream(outputFile)) {

            //keyFamily
            SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/EDUCATION+ESTAT+1.0.xml");
            DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();
            String xsdPath = "cross20_output\\xsd\\2_0";

            // make the conversion
            TestConverterUtil.convert(Formats.GENERIC_SDMX,
                    Formats.CROSS_SDMX,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    outputStream,
                    new SdmxInputConfig(),
                    new SdmxOutputConfig(),
                    kf,
                    null,
                    converterDelegatorService);


            File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
            File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                    " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
        }
    }
    
	@Test
    public void convertFromCompact20() throws Exception{
		String resultFileName = "2_output_cross20_from_compact20_demography.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);

        try(InputStream inputStream = new FileInputStream(("./testfiles/cross20_output/2_input_compact20_demography.xml"));
            OutputStream outputStream = new FileOutputStream(outputFile)) {

            //keyFamily
            SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/demography.xml");
            DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

            // make the conversion
            TestConverterUtil.convert(Formats.COMPACT_SDMX,
                    Formats.CROSS_SDMX,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    outputStream,
                    new SdmxInputConfig(),
                    new SdmxOutputConfig(),
                    kf,
                    null,
                    converterDelegatorService);

            File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
            File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                    " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
        }
    }
	
	@Test
    public void convert2FromCompact20() throws Exception{
		
		String resultFileName = "output_cross20_from_compact20_ESTAT+DEMOGRAPHY+2.1.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        //ouputFileName
        File outputFile = new File(completeResultTargetFileName);

        try(InputStream inputStream = new FileInputStream(("./testfiles/cross20_output/3_input_compact20_ESTAT+DEMOGRAPHY+2.1.xml"));
            OutputStream outputStream = new FileOutputStream(outputFile)) {

            //keyFamily
            SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/ESTAT+DEMOGRAPHY+2.1.xml");
            DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

            // make the conversion
            TestConverterUtil.convert(Formats.COMPACT_SDMX,
                    Formats.CROSS_SDMX,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    outputStream,
                    new SdmxInputConfig(),
                    new SdmxOutputConfig(),
                    kf,
                    null,
                    converterDelegatorService);
            File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
            File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                    " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
        }
    }
	
	@Ignore(value="this is an performance test and should be run outside the integration tests")
	//@Test
    public void convert4FromCompact20() throws Exception{
		
		String resultFileName = "4_output_cross20_xs_demo_rq_dsd_big_file.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);

        try(InputStream inputStream = new FileInputStream(("./testfiles/cross20_output/4_input_compact20_xs_demo_rq_dsd_big_file.xml"));
            OutputStream outputStream = new FileOutputStream(outputFile)) {

            //keyFamily
            SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/xs_demo_rq_dsd.xml");
            DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

            // make the conversion
            TestConverterUtil.convert(Formats.COMPACT_SDMX,
                    Formats.CROSS_SDMX,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    outputStream,
                    new SdmxInputConfig(),
                    new SdmxOutputConfig(),
                    kf,
                    null,
                    converterDelegatorService);
            File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
            File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                    " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
        }
    }
}
