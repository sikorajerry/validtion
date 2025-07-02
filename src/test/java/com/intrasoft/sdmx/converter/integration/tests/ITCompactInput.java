package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelOutputConfig;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.poi.xssf.extractor.XSSFExcelExtractor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
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

import static com.intrasoft.sdmx.converter.services.ExcelUtils.readExcelConfig;
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITCompactInput {
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
   
    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    private final static String GENERATED_PATH = "compact20_input/";

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
    public void convertToGenericTS21Data() throws Exception{
		String resultFileName = "3_output_genericTS21Data_UOE_NON_FINANCE_ESTAT_0.4.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		 try(InputStream inputStream = new FileInputStream("./testfiles/compact20_input/3_input_compact20_UOE_NON_FINANCE_ESTAT_0.4.xml");
			 OutputStream outputStream = new FileOutputStream(outputFile)) {

			 //keyFamily
			 DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
			 //dataflow
			 //SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
			 //DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			 ConverterInput converterInput = new ConverterInput(Formats.COMPACT_SDMX,
					 readableDataLocationFactory.getReadableDataLocation(inputStream),
					 new SdmxInputConfig());
			 ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream, new SdmxOutputConfig());
			 ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			 // make the conversion
			 converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);

			 File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			 File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			 FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					 " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		 }
    }
    
    @Test
    public void convertToGenericTS21DataWithReportingPeriod() throws Exception{
		String resultFileName = "3_output_genericTS21DataWithReportingPeriod_UOE_NON_FINANCE_ESTAT_0.4.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/compact20_input/3_input_compact20_UOE_NON_FINANCE_ESTAT_0.4.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {


			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
			//dataflow
			//SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
			//DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();


			ConverterInput converterInput = new ConverterInput(Formats.COMPACT_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
			sdmxOutputConfig.setReportingStartYearDate("--05-06");
			sdmxOutputConfig.setUseReportingPeriod(true);

			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_TS_DATA_2_1, outputStream, sdmxOutputConfig);
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }

    @Test
    public void convertToGeneric20() throws Exception{
		String resultFileName = "2_output_generic20_ECB+ECB_EXR1+1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/compact20_input/2_input_compact20_ECB+ECB_EXR+1.0.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			ConverterInput converterInput = new ConverterInput(Formats.COMPACT_SDMX,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					new SdmxInputConfig());
			ConverterOutput converterOutput = new ConverterOutput(Formats.GENERIC_SDMX, outputStream, new SdmxOutputConfig());
			ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow);

			// make the conversion
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }
            
    @Test
    public void convertToSdmxCsv() throws Exception{
		String resultFileName = "2_output_sdmxCsv_ECB+ECB_EXR1+1.0.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/compact20_input/2_input_compact20_ECB+ECB_EXR+1.0.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {

        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();




        // make the conversion
        TestConverterUtil.convert(  Formats.COMPACT_SDMX,
                            Formats.SDMX_CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
       		                new SdmxCsvOutputConfig(),
       		                dataStructure,
       		                dataflow,
                            converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file " + completeResultTargetFileName +
						" is different than what is expected " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
		}
    }
    
    @Ignore(value="it seems that the input file is invalid")
    private void convertToExcel() throws Exception{
		String resultFileName = "excelFromCompact.xlsx"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/compact20_input/1_input_compact20_ECB+ECB_EXR1+1.0.xml");

        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromFile("./testfiles/dsd/ECB+ECB_EXR1+1.0.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 


        //excel output setup
        ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/ECB_EXR/excel.xlsx"),
                readExcelConfig(new FileInputStream("./test_files/ECB_EXR/excel.xlsx"),
                        false, new FirstFailureExceptionHandler()).get(0));

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);

        // make the conversion
        TestConverterUtil.convert(  Formats.COMPACT_SDMX,
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
        XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./test_files/ECB_EXR/outputExcelFromCompact.xlsx")));
        String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
        Assert.assertTrue(outputExcelText.equals(actualExcelText));
    }
    
    @Test
    public void convertToUtility() throws Exception{
		String resultFileName = "2_output_utility20_ECB+ECB_EXR1+1.0.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);

		try(InputStream inputStream = new FileInputStream("./testfiles/compact20_input/2_input_compact20_ECB+ECB_EXR+1.0.xml");
			OutputStream outputStream = new FileOutputStream(outputFile)) {

        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/ECB+ECB_EXR1+1.0.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

        // make the conversion
        TestConverterUtil.convert(  Formats.COMPACT_SDMX,
                            Formats.UTILITY_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            new SdmxInputConfig(),
       		                new SdmxOutputConfig(),
       		                dataStructure,
       		                dataflow,
       		                converterDelegatorService);

        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
    }

	@Test
	public void convertStructureSpecific30() throws Exception {
		String resultFileName = "SS_30_MissingDataset.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);

		try (InputStream inputStream = new FileInputStream("./testfiles/compact20_input/Compact_MissingDataset.xml");
			 OutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/NAMAIN_IDC_N1.9_SDMX3.0.xml");
			//dataflow
			SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/NAMAIN_IDC_N1.9_SDMX3.0.xml");
			DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

			// make the conversion
			TestConverterUtil.convert(Formats.COMPACT_SDMX,
					Formats.STRUCTURE_SPECIFIC_DATA_3_0,
					readableDataLocationFactory.getReadableDataLocation(inputStream),
					outputStream,
					new SdmxInputConfig(),
					new SdmxOutputConfig(),
					dataStructure,
					dataflow,
					converterDelegatorService);

			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		}
	}
}
