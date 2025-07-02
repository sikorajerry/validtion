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
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.config.GesmesOutputConfig;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.TsTechnique;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.CsvService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;

import junitx.framework.FileAssert;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITGeneric20Input {
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
   
    @Autowired
    private StructureService structureService;  
    
    @Autowired
    private CsvService csvService; 
    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    private final static String GENERATED_PATH = "generic20_input/";

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}

    @Ignore(value="header information is not passed correctly from the generic header to utility header")
    public void convertToUtility() throws Exception{
		String resultFileName = "1_output_utility20_UOE_NON_FINANCE+ESTAT+0.4.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
          
        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        //outputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
     
        // make the conversion
        TestConverterUtil.convert(   Formats.GENERIC_SDMX,
                            Formats.UTILITY_SDMX,
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


    @Test
    public void convertToCompact() throws Exception{
		String resultFileName = "1_output_compact20_UOE_NON_FINANCE+ESTAT+0.4.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
     
        //data structure
        DataStructureBean dataSructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
     
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.COMPACT_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            new SdmxOutputConfig(),
                            dataSructure,
                            null, 
                            converterDelegatorService);
       
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }


//    @Ignore(value="This test should fail because the cross message has OBS_VALUE instead cross measures.")
	@Test
    public void convertToCross20() throws Exception {
		String resultFileName = "3_output_cross20_USE_PESTICI+ESTAT+1.1.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
    	InputStream inputStream = new FileInputStream("./testfiles/generic20_input/3_input_generic20_USE_PESTICI+ESTAT+1.1.xml");
     
       //keyFamily
       SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");
       DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 

       //ouputFileName
       File outputFile =  new File (completeResultTargetFileName);
       OutputStream outputStream = new FileOutputStream(outputFile);
     
       // make the conversion
       TestConverterUtil.convert(   Formats.GENERIC_SDMX,
                            Formats.CROSS_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            new SdmxOutputConfig(),
                            kf,
                            null, 
                            converterDelegatorService);

       //no assertion until cross sectional writer is fixed (all mechanism)
//       FileAssert.assertEquals("the generated cross file is different than what is expected",
//               new File("./testfiles/generic20_input/3_output_cross20_USE_PESTICI+ESTAT+1.1.xml.xml"),
//               outputFile);
    }
    
    @Test
    public void convert3ToCSV() throws Exception{
		String resultFileName = "3_output_csv_USE_PESTICI+ESTAT+1.1.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
    	InputStream inputStream = new FileInputStream("./testfiles/generic20_input/3_input_generic20_USE_PESTICI+ESTAT+1.1.xml");
     
       //keyFamily
       SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");
       DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 

       //ouputFileName
       File outputFile =  new File (completeResultTargetFileName);
       OutputStream outputStream = new FileOutputStream(outputFile);
       
       MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
       csvOutputConfig.setDelimiter(";");
       csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
     
       // make the conversion
       TestConverterUtil.convert(   Formats.GENERIC_SDMX,
                            Formats.CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            csvOutputConfig,
                            kf,
                            null, 
                            converterDelegatorService);

       //no assertion until cross sectional writer is fixed (all mechanism)
//       FileAssert.assertEquals("the generated cross file is different than what is expected",
//               new File("./testfiles/generic20_input/3_output_cross20_USE_PESTICI+ESTAT+1.1.xml.xml"),
//               outputFile);
    }

    @Test
    public void convertToGeneric21() throws Exception{
		String resultFileName = "2_output_generic21_UOE_NON_FINANCE+ESTAT+0.4_small.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/2_input_generic20_UOE_NON_FINANCE+ESTAT+0.4_small.xml");
     
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
      
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.GENERIC_DATA_2_1,
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
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }


    @Test
    public void convertToGenericTS21() throws Exception{
		String resultFileName = "2_output_genericTS21_UOE_NON_FINANCE+ESTAT+0.4_small.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/2_input_generic20_UOE_NON_FINANCE+ESTAT+0.4_small.xml");
                    
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
      
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.GENERIC_TS_DATA_2_1,
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
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }


    @Test
    public void convertToStructure21() throws Exception{
		String resultFileName = "2_output_structure21_UOE_NON_FINANCE+ESTAT+0.4_small.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/2_input_generic20_UOE_NON_FINANCE+ESTAT+0.4_small.xml");
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
      
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        
        SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
        sdmxOutputConfig.setUseReportingPeriod(true);
        sdmxOutputConfig.setReportingStartYearDate("--01-01");;
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.STRUCTURE_SPECIFIC_DATA_2_1,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            sdmxOutputConfig,
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
    public void convertToStructureTS21() throws Exception{
		String resultFileName = "2_output_structureTS21_UOE_NON_FINANCE+ESTAT+0.4_small.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/2_input_generic20_UOE_NON_FINANCE+ESTAT+0.4_small.xml");
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
      
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        
        SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
        sdmxOutputConfig.setUseReportingPeriod(true);
        sdmxOutputConfig.setReportingStartYearDate("--03-07");;
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.STRUCTURE_SPECIFIC_TS_DATA_2_1,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            sdmxOutputConfig,
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
    public void convertToCsv() throws Exception{
		String resultFileName = "1_output_csv_UOE_NON_FINANCE+ESTAT+0.4.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
              
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        //csv output setup
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            csvOutputConfig,
                            kf,
                            null, 
                            converterDelegatorService);
        
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }
    
    @Test
    public void convertToSdmxCsv() throws Exception{
		String resultFileName = "1_output_sdmxCsv_UOE_NON_FINANCE+ESTAT+0.4.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
              
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/STS+ESTAT+2.0.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        //csv output setup
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.SDMX_CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            csvOutputConfig,
                            kf,
                            dataflow, 
                            converterDelegatorService);
        
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }

    @Test
    public void convertToCsvWithSimpleOutputColumnMapping() throws Exception{
		String resultFileName = "1_output_csv_UOE_NON_FINANCE+ESTAT+0.4.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
                    
        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        
        //csv output setup
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		csvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(new FileInputStream("./testfiles/generic20_input/mappingSimple.xml")));
      
        //outputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            csvOutputConfig,
                            dataStructure,
                            null, 
                            converterDelegatorService);
        
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }


    @Test
    public void convertToCsvWithSimpleOutColumnMappingHavingFirstTwoColumnsSwitched() throws Exception{
		String resultFileName = "1_output_csvWithFirstTwoColumnsSwitched.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
              
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        //csv output setup
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

		csvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(new FileInputStream("./testfiles/generic20_input/mappingWithFirstTwoColumnsSwitched.xml")));
      
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            csvOutputConfig,
                            kf,
                            null, 
                            converterDelegatorService);
        
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }

    @Test
    public void convertToCsvWithMappingHavingTheFirstStructureDimensionPositionedLastInMappingFile() throws Exception{
		String resultFileName = "1_output_csv_UOE_NON_FINANCE+ESTAT+0.4.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
                    
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        //csv output setup
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		csvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(new FileInputStream("./testfiles/generic20_input/mappingWithFirstDimensionInStructurePositionedLastInMapping.xml")));
      
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            csvOutputConfig,
                            kf,
                            null, 
                            converterDelegatorService);
        
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }


    @Test
    public void convertToCsvWithOutputMappingHaving3Levels() throws Exception{
		String resultFileName = "1_output_csvWith3Levels.txt"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic20_input/1_input_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml");
              
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        //csv output setup
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        csvOutputConfig.setLevels(3);
		csvOutputConfig.setColumnMapping(
		        csvService.buildOutputDimensionLevelMapping(new FileInputStream("./testfiles/generic20_input/mapping3Levels.xml")));
		      
        //outputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.MULTI_LEVEL_CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            csvOutputConfig,
                            kf,
                            null, 
                            converterDelegatorService);
        
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }
        
    @Ignore(value="test fails because of some limitations of the Gesmes TS writer (see readme.md for details)")
    public void convertToGesmesTS() throws Exception{
		String resultFileName = "gesmesTS.ges"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./test_files/UOE_NON_FINANCE/generic.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml");

        //output configuration
        GesmesOutputConfig gesmesOutputConfig = new GesmesOutputConfig();
        gesmesOutputConfig.setGesmeswritingtechnique(TsTechnique.TIME_RANGE);
        
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(  Formats.GENERIC_SDMX,
                            Formats.GESMES_TS,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            gesmesOutputConfig,
                            kf,
                            null, 
                            converterDelegatorService);
        
        FileAssert.assertEquals("the generated gesmes 21 file is different than what is expected",
                                new File("./test_files/UOE_NON_FINANCE/gesmesTS.ges"),
                                outputFile);
    }

   /*
    @Test
    public void convertSimpleGenericToExcel() throws Exception{
        Converter converter = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/ESTAT_STS/in_generic20.xml");
                    
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./test_files/ESTAT_STS/ESTAT+STS+2.0.xml");

        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(
                new FileInputStream("./test_files/ESTAT_STS/excel.xlsx"),
                ExcelUtils.readExcelConfig(new FileInputStream("./test_files/ESTAT_STS/excel.xlsx"), false, ExcelUtils.XLSX_EXCEL).get(0));

        File outputFile =  new File ("./target/excelFromGeneric20.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        converter.convert(  Formats.GENERIC_SDMX,
                            Formats.EXCEL,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            excelOutputConfig,
                            kf);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ESTAT_STS/outputExcelFromGeneric20.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }


    @Test
    public void convertComplexGenericToExcelWithTimeAsRowHeaders() throws Exception{
        Converter converter = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/ESTAT_STS/in_generic20_multipleValues.xml");
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream("./test_files/ESTAT_STS/ESTAT_STS_v2.1.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        
        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx"), excelService.readExcelConfig(new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx"), false, ExcelUtils.XLSX_EXCEL).get(0));
				
		//ouputFileName
        File outputFile =  new File ("./target/excelWithTimeAsRowHeadersFromGeneric20WithGroups.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        converter.convert(  Formats.GENERIC_SDMX,
                            Formats.EXCEL,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            excelOutputConfig,
                            kf);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ESTAT_STS/outputExcelWithTimeAsRowHeadersFromGeneric20WithGroups.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    
    @Test
    public void convertComplexGenericToExcelWithTimeAsColHeaders() throws Exception{
        Converter converter = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/ESTAT_STS/in_generic20_multipleValues.xml");
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream("./test_files/ESTAT_STS/ESTAT_STS_v2.1.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        
        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/ESTAT_STS/in_excelTemplateWithTimeAsColHeaders.xlsx"), excelService.readExcelConfig(new FileInputStream("./test_files/ESTAT_STS/in_excelTemplateWithTimeAsColHeaders.xlsx"), false, ExcelUtils.XLSX_EXCEL).get(0));
				
		//ouputFileName
        File outputFile =  new File ("./target/excelFromGeneric20WithTimeAsColumnHeaders.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        converter.convert(  Formats.GENERIC_SDMX,
                            Formats.EXCEL,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            excelOutputConfig,
                            kf);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ESTAT_STS/outputExcelFromGeneric20WithTimeAsColumnHeaders.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    
    @Test
    public void convertGenericWithGroupsToExcel() throws Exception{
        Converter converter = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/ESTAT_STS/in_sts-generic-with-group.xml");
      
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream("./test_files/ESTAT_STS/ESTAT_STS_v2.1.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        
        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx"), excelService.readExcelConfig(new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx"), false, ExcelUtils.XLSX_EXCEL).get(0));
				
		//ouputFileName
        File outputFile =  new File ("./target/excelFromGeneric20WithGroups.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        converter.convert(  Formats.GENERIC_SDMX,
                            Formats.EXCEL,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            excelOutputConfig,
                            kf);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ESTAT_STS/outputExcelFromGeneric20WithGroups.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    
    @Test
    public void convertComplexGenericToExcelWithoutDimensionsOnRows() throws Exception{
        Converter converter = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/ESTAT_STS/in_generic20_multipleValues.xml");
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream("./test_files/ESTAT_STS/ESTAT_STS_v2.1.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        
        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/ESTAT_STS/in_excelTemplateWithoutDimensionsOnRows.xlsx"), excelService.readExcelConfig(new FileInputStream("./test_files/ESTAT_STS/in_excelTemplateWithoutDimensionsOnRows.xlsx"), false, ExcelUtils.XLSX_EXCEL).get(0));
				
		//ouputFileName
        File outputFile =  new File ("./target/excelFromGeneric20WithoutDimensionsOnRows.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        converter.convert(  Formats.GENERIC_SDMX,
                            Formats.EXCEL,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            excelOutputConfig,
                            kf);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ESTAT_STS/outputExcelFromGeneric20WithoutDimensionsOnRows.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    
    @Test
    public void convertComplexGenericToExcelWithoutDimensionsOnColumns() throws Exception{
        Converter converter = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/ESTAT_STS/in_generic20_multipleValues.xml");

        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream("./test_files/ESTAT_STS/ESTAT_STS_v2.1.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        
        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/ESTAT_STS/in_excelTemplateWithoutDimensionsOnColumns.xlsx"), excelService.readExcelConfig(new FileInputStream("./test_files/ESTAT_STS/in_excelTemplateWithoutDimensionsOnColumns.xlsx"), false, ExcelUtils.XLSX_EXCEL).get(0));
				
		//ouputFileName
        File outputFile =  new File ("./target/excelFromGeneric20WithoutDimensionsOnCols.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        converter.convert(  Formats.GENERIC_SDMX,
                            Formats.EXCEL,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            excelOutputConfig,
                            kf);

        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ESTAT_STS/outputExcelFromGeneric20WithoutDimensionsOnCols.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    
    @Test
    public void convertGeneric20ToExcelUsingATemplateWithObsLevelValues() throws Exception{
        Converter converter = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/UOE_NON_FINANCE/in_generic20.xml");
      
             
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        
        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/UOE_NON_FINANCE/in_excelTemplateWith2ObsLevelAttributes.xlsx"), excelService.readExcelConfig(new FileInputStream("./test_files/UOE_NON_FINANCE/in_excelTemplateWith2ObsLevelAttributes.xlsx"), false, ExcelUtils.XLSX_EXCEL).get(0));
				
		//ouputFileName
        File outputFile =  new File ("./target/excelFromGeneric20WithATemplateContainingObsLevelValues.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        converter.convert(  Formats.GENERIC_SDMX,
                            Formats.EXCEL,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
                            excelOutputConfig,
                            kf);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/UOE_NON_FINANCE/outputExcelFromGeneric20WithATemplateContainingObsLevelValues.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    
    @Test
    public void convertToExcelWithInputGenericWithGroup() throws Exception{
    	Converter conv = new Converter();
        InputStream inputStream = new FileInputStream("./test_files/ESTAT_STS/in_sts-generic-with-group.xml");
      
        // initialize the Input Params
        InputParams params = new InputParams();
              
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream("./test_files/ESTAT_STS/dsd.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        params.setKeyFamilyBean(kf);
        
        //excel output setup
        params.setExcelType(ExcelUtils.XLSX_EXCEL);
		params.setAllExcelParameters(excelService.readExcelConfig(new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx"), false, params.getExcelType()));
		params.setExcelOutputTemplate(new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx"));
				
		//ouputFileName
        File outputFile =  new File ("./target/excelFromGenericWithGroup.xlsx");
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        conv.convert(Formats.GENERIC_SDMX, Formats.EXCEL, inputStream, outputStream, params);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ESTAT_STS/outputExcelFromGenericWithGroup.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        
       //Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    */
}
