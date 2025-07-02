package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterException;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelOutputConfig;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.poi.xssf.extractor.XSSFExcelExtractor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.estat.sdmxsource.config.SdmxMLInputConfig;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.estat.sdmxsource.util.csv.SingleLevelCsvOutColMapping;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
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
public class ITGeneric21Input {
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
	@Rule
	public final ExpectedException expectedException = ExpectedException.none();
	
    private final static String GENERATED_PATH = "generic21_input/";

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
    public void convertToGeneric20() throws Exception{
		String resultFileName = "1_output_generic20_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/1_input_generic21_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/STS+ESTAT+2.0.xml");

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, 
        			Formats.GENERIC_SDMX, 
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
    public void convertToExcel() throws Exception{
		String resultFileName = "excelFromGeneric21withGroupsAndMultipleObservations.xlsx"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/1_input_generic21_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/STS+ESTAT+2.0.xml");

        //excel output setup
        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(
        			new FileInputStream("./testfiles/generic21_input/1_excel_parameter_template.xlsx"), 
        			ExcelUtils.readExcelConfig(
        						new FileInputStream("./testfiles/generic21_input/1_excel_parameter_template.xlsx"), 
        						false, new FirstFailureExceptionHandler()).get(0));
				
		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, 
        		Formats.EXCEL, 
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                new SdmxInputConfig(),
                excelOutputConfig,
                kf,
                null,
                converterDelegatorService);
        
        XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
        String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./testfiles/generic21_input/1_output_excel_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
 
    @Test
    public void convertToCsv() throws Exception{
		String resultFileName = "2_output_csv_UE_NON_FINANCE+ESTAT+0.4.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/2_input_generic21_UE_NON_FINANCE+ESTAT+0.4.xml");

        MultiLevelCsvOutputConfig outputConfig = new MultiLevelCsvOutputConfig();
        
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

        //csv output setup
        outputConfig.setDelimiter(";");
        outputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);;
        outputConfig.setLevels(1);


        SingleLevelCsvOutColMapping colMapping = new SingleLevelCsvOutColMapping();
        colMapping.addMapping(0, "TABLE_IDENTIFIER");
        colMapping.addMapping(1, "FREQ");
        colMapping.addMapping(2, "REF_AREA");
        colMapping.addMapping(3, "REF_SECTOR");
        colMapping.addMapping(4, "EDU_TYPE");
        colMapping.addMapping(5, "ISC11P_LEVEL");
        colMapping.addMapping(6, "ISC11P_CAT");
        colMapping.addMapping(7, "ISC11P_SUB");
        colMapping.addMapping(8, "GRADE");
        colMapping.addMapping(9, "FIELD");
        colMapping.addMapping(10, "INTENSITY");
        colMapping.addMapping(11, "COUNTRY_ORIGIN");
        colMapping.addMapping(12, "COUNTRY_CITIZENSHIP");
        colMapping.addMapping(13, "SEX");
        colMapping.addMapping(14, "AGE");
        colMapping.addMapping(15, "STAT_UNIT");
        colMapping.addMapping(16, "UNIT_MEASURE");
        colMapping.addMapping(17, "TIME_PERIOD");
        colMapping.addMapping(18, "OBS_VALUE");
        colMapping.addMapping(19, "OBS_STATUS");
        colMapping.addMapping(20, "COMMENT_OBS");
        colMapping.addMapping(21, "TIME_PER_COLLECT");
        colMapping.addMapping(22, "ORIGIN_CRITERION");
        colMapping.addMapping(23, "REF_YEAR_AGES");
        colMapping.addMapping(24, "REF_PER_START");
        colMapping.addMapping(25, "REF_PER_END");
        colMapping.addMapping(26, "COMPILING_ORG");
        colMapping.addMapping(27, "DECIMALS");
        colMapping.addMapping(28, "UNIT_MULT");

        outputConfig.setColumnMapping(colMapping);
		
      
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        
        ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(inputStream);
        
        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, Formats.CSV, dataLocation, outputStream, new SdmxMLInputConfig(), outputConfig, kf, null,
        		converterDelegatorService);
        
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }
    
    /**
     * This is a negative test 
     * @throws Exception
     */
    @Test
    public void convertToCross20() throws Exception{
		String resultFileName = "cross20FromGeneric21WithGroups"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
    	expectedException.expect(ConverterException.class);
		
        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/1_input_generic21_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/STS+ESTAT+2.0.xml");

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, 
        			Formats.CROSS_SDMX, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
                	new SdmxInputConfig(),
	                new SdmxOutputConfig(),
	                kf,
	                null,
	                converterDelegatorService);
    }
    
    /**
     * This is a negative test 
     * @throws Exception
     */
    @Test
    public void convertToSdmxCsv() throws Exception{
		String resultFileName = "1_output_sdmxCsv_STS+ESTAT+2.0_withGroupsAndMultipleObservations.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/1_input_generic21_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/STS+ESTAT+2.0.xml");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/STS+ESTAT+2.0.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, 
        			Formats.SDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
                	new SdmxInputConfig(),
	                new SdmxCsvOutputConfig(),
	                kf,
	                dataflow,
	                converterDelegatorService);
        File expectedFile = new File(completeResultTestFilesFileName);
        //TODO the sdmx_csv will always have the header present so the CsvOutputConfig header config will not be taken into cosideration
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
    }
    
	@Test
    public void convertToStructureSpecific21() throws Exception{
		String resultFileName = "1_output_structureSpecific21_STS+ESTAT+2.0_withGroupsAndMultipleGroups.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/1_input_generic21_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/STS+ESTAT+2.0.xml");

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
        sdmxOutputConfig.setUseReportingPeriod(true);
        sdmxOutputConfig.setReportingStartYearDate("--01-01");
        
        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, 
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
    public void convertToStructureSpecificTS21() throws Exception{
		String resultFileName = "1_output_structureSpecificTS21_STS+ESTAT+2.0_withGroupsAndMultipleGroups.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/1_input_generic21_STS+ESTAT+2.0_withGroupsAndMultipleObservations.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/STS+ESTAT+2.0.xml");

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        
        SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
        sdmxOutputConfig.setUseReportingPeriod(true);
        sdmxOutputConfig.setReportingStartYearDate("--07-01");
      
        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, 
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


    //SDMXCONV-1562
    @Test
    public void convertToSDMX_CSV() throws Exception{
        String resultFileName = "output_sdmx_csv_1562.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/NAMAIN_T01EMP_A_BE_2020_0000_V0003.xml");

        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/CR_NAMAIN_T01EMP_A_1.0_20231009.xml");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/CR_NAMAIN_T01EMP_A_1.0_20231009.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1,
                Formats.SDMX_CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                new SdmxInputConfig(),
                csvOutputConfig,
                kf,
                dataflow,
                converterDelegatorService);

        File expectedFile = new File(completeResultTestFilesFileName);

        //TODO the sdmx_csv will always have the header present so the CsvOutputConfig header config will not be taken into cosideration
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName,
                expectedFile, generatedFile);
    }

    //SDMXCONV-1562

    @Test
    public void convertToStructureSpecific() throws Exception{
        String resultFileName = "output_ss21_1562.xml";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/generic21_input/NAMAIN_T01EMP_A_BE_2020_0000_V0003.xml");

        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/CR_NAMAIN_T01EMP_A_1.0_20231009.xml");
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/CR_NAMAIN_T01EMP_A_1.0_20231009.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);

        // make the conversion
        TestConverterUtil.convert(Formats.GENERIC_SDMX,
                Formats.STRUCTURE_SPECIFIC_DATA_2_1,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                new SdmxInputConfig(),
                new SdmxOutputConfig(),
                kf,
                dataflow,
                converterDelegatorService);


        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName,
                expectedFile, generatedFile);
    }
}
