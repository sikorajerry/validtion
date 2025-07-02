package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelConfigurer;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelSheetUtils;
import com.intrasoft.sdmx.converter.services.*;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.struval.ValidationError;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITExcelInput {
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
	@Autowired
	private StructureService structureService; 
	
	@Autowired
	private HeaderService headerService; 
	
	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;

	@Autowired
	private ValidationService validationService;
	
    private final static String GENERATED_PATH = "excel_input/";
    
	@Rule
	public final ExpectedException expectedException = ExpectedException.none();

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
     
    @Test(expected = SdmxSemmanticException.class)
	public void testCrossOutput() throws Exception {
		String resultFileName = "2_output_corss_UOE_NON_FINANCE+ESTAT+0.4.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/2_excel.xlsx"), new FirstFailureExceptionHandler()));
		excelInputConfig.setExternalParamsFileName("externalParamsFileStream");
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
		
		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
		sdmxOutputConfig.setNamespaceprefix("default_namespace");
		sdmxOutputConfig.setNamespaceuri("default_uri");

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		File expectedFile = null;
		File generatedFile = null;
		try(FileOutputStream outputStream = new FileOutputStream(outputFile)) {
			TestConverterUtil.convert(Formats.EXCEL,
					Formats.CROSS_SDMX,
					readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/2_excel.xlsx"),
					outputStream,
					excelInputConfig,
					sdmxOutputConfig,
					dataStructure,
					null,
					converterDelegatorService);

			expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
					" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
		} finally {
			if(outputFile!=null)
				outputFile.delete();
			if(generatedFile!=null)
				generatedFile.delete();
		}
	}

	//test case for SDMXCONV-1007, excel to compact
    @Test
	public void testCompactOutput4() throws Exception {
		String generatedFileName = "2_output_compact_SDG_DSD.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + generatedFileName;
		String expectedFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + generatedFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/TajikistanSDGs.MAPPED.TEST.xlsx"), new FirstFailureExceptionHandler()));
		excelInputConfig.setExternalParamsFileName("externalParamsFileStream");
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/SDG_DSD.xml");

		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
		sdmxOutputConfig.setNamespaceprefix("default_namespace");
		sdmxOutputConfig.setNamespaceuri("default_uri");

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
							Formats.COMPACT_SDMX,
							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/TajikistanSDGs.MAPPED.TEST.xlsx"),
							outputStream,
							excelInputConfig,
							sdmxOutputConfig,
							dataStructure,
							null,
                            converterDelegatorService);

		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(expectedFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + expectedFileName, expectedFile, generatedFile);
	}

	/**
	 * Skip observations with zeros
	 * @throws Exception
	 */
    @Test
	public void testEnergyTemplateOutputSdmxCsv() throws Exception {
		String resultFileName = "1_output_energy_sdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/energy/CoalQues_2017.08_with_DSD.xlsm"), new FirstFailureExceptionHandler()));
		excelInputConfig.setExternalParamsFileName("externalParamsFileStream");
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/energy/ESTAT+ENERGY+1.1_DF.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/energy/ESTAT+ENERGY+1.1_DF.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		SdmxCsvOutputConfig sdmxOutputConfig = new SdmxCsvOutputConfig();
		sdmxOutputConfig.setDelimiter(";");
		sdmxOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		//outputFileName
		File outputFile = new File(completeResultTargetFileName); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL, 
							Formats.SDMX_CSV,
							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/energy/CoalQues_2017.08_with_DSD.xlsm"),
							outputStream, 
							excelInputConfig,
							sdmxOutputConfig,
							dataStructure, dataflow,
                            converterDelegatorService);
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = (outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}
    
    @Test
	public void testCompactOutput() throws Exception {
		String resultFileName = "BE_AEA_2017_out.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"compact_output/" + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/compact_output/ESTAT+SEEAAIR+1.0.xml");
		
		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
		sdmxOutputConfig.setNamespaceprefix("default_namespace");
		sdmxOutputConfig.setNamespaceuri("default_uri");

		//outputFileName
		File outputFile = new File(completeResultTargetFileName); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL, 
							Formats.COMPACT_SDMX,
							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/compact_output/BE_AEA_2017_with_params_transcoding_test.xlsm"),
							outputStream, 
							excelInputConfig,
							sdmxOutputConfig,
							dataStructure,
							null, 
                            converterDelegatorService);

        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName +
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
	}
    
    @Test
	public void testSSOutput() throws Exception {
		String resultFileName = "1_output_ESSPROS_SCHEME_bg.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"structure_specific_out/" + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/structure_specific_out/ESTAT+ESSPROS_SCHEME+1.1_Extended.xml");
		
		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

		//outputFileName
		File outputFile = new File(completeResultTargetFileName); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL, 
							Formats.STRUCTURE_SPECIFIC_DATA_2_1,
							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/structure_specific_out/1_input_ESSPROS_SCHEME_bg.xlsx"),
							outputStream, 
							excelInputConfig,
							sdmxOutputConfig,
							dataStructure,
							null, 
                            converterDelegatorService);
		
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}
    
    @Test
	public void testCompactOutput2() throws Exception {
		String resultFileName = "output_BE_AEA_2017_2.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"compact_output_2/" + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/compact_output_2/ESTAT+ENVDATA_AEA_A+1.0.xml");
		
		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

		//outputFileName
		File outputFile = new File(completeResultTargetFileName); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL, 
							Formats.COMPACT_SDMX,
							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/compact_output_2/BE_AEA_2017_2.xlsm"),
							outputStream, 
							excelInputConfig,
							sdmxOutputConfig,
							dataStructure,
							null, 
                            converterDelegatorService);
		
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}
    //SDMXCONV-952, convert excel to compact
    @Test
	public void testCompactOutput3() throws Exception {
		String resultFileName = "ENERGY_PETRO_A_CH_2018_0000_V0006.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"compact_output_3/" + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_3/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        //external parameters
        File excelConfig = new File("./testfiles/excel_input/compact_output_3/ENERGY_PETRO_SDMXConverterParameters-export0s.xlsx"); 
        LinkedHashMap<String, ArrayList<String>> mapping;
        //Read the mapping from the external parameter file
        try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        }
        excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        excelInputConfig.setMappingInsideExcel(false);

        //The names of the param sheets taken from the mapping
        List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
        List<ExcelConfiguration> allExcelParameters = null;
        //Read the parameter sheets with the help of the mapping from the external file
        try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
            allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
        }
 		
    	excelInputConfig.setConfigInsideExcel(false);
    	excelInputConfig.setConfiguration(allExcelParameters);
        
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/compact_output_3/ESTAT+ENERGY+1.2+DFs.xml");
		
		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

		//outputFileName
		File outputFile = new File(completeResultTargetFileName); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL, 
							Formats.COMPACT_SDMX,
							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/compact_output_3/ENERGY_PETRO_A_CH_2018_0000_V0006.xlsm"),
							outputStream, 
							excelInputConfig,
							sdmxOutputConfig,
							dataStructure,
							null, 
                            converterDelegatorService);
        File expFile = new File(completeResultTestFilesFileName);
        expFile = IntegrationTestsUtils.prettyPrint(expFile);
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expFile, generatedFile);
	}
    
    @Test
 	public void testGeneric() throws Exception {
 		String resultFileName = "output_ENERGY_PETRO_SDMX.xml"; 
 		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
 		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"energy/" + resultFileName;
 		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
 		
 		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
 		excelInputConfig.setHeader(header);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
 		File excelConfig = new File("./testfiles/excel_input/energy/ENERGY_PETRO_SDMXConverterParameters.xlsx"); 
 			LinkedHashMap<String, ArrayList<String>> mapping;
			//Read the mapping from the external parameter file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
        		mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        	}
        	excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        	excelInputConfig.setMappingInsideExcel(false);
    		
    		//The names of the param sheets taken from the mapping
    		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
    		List<ExcelConfiguration> allExcelParameters = null;
    		//Read the parameter sheets with the help of the mapping from the external file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
				allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
        	}
 		
    		excelInputConfig.setConfigInsideExcel(false);
        	excelInputConfig.setErrorIfEmpty(false);
    		excelInputConfig.setConfiguration(allExcelParameters);
			//keyFamily
			DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/energy/ESTAT+ENERGY+1.2 + DFs.xml");
			SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

			//outputFileName
			File outputFile = new File(completeResultTargetFileName);
			FileOutputStream outputStream = new FileOutputStream(outputFile);
			TestConverterUtil.convert(Formats.EXCEL,
								Formats.GENERIC_DATA_2_1,
								readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/energy/ENERGY_PETRO_SDMX.xlsx"),
								outputStream,
								excelInputConfig,
								sdmxOutputConfig,
								dataStructure,
								null,
								converterDelegatorService);
			assertEquals(Integer.toString(TestConverterUtil.getProccessedCount()), "364");
			assertEquals(Integer.toString(TestConverterUtil.getIgnoredCount()), "52");
			File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
			File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
			FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
 	}
    
    /**
     * External Parameter file provided.
     * Transcoding sheet is included inside the params file only.
     * @throws Exception
     */
    @Test
 	public void testTranscodingInExternalParamsFile() throws Exception {
 		String resultFileName = "output_BE_AEA_2017.xml"; 
 		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
 		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"transcoding/" + resultFileName;
 		
 		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
 		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
 		excelInputConfig.setHeader(header);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
 		File excelConfig = new File("./testfiles/excel_input/transcoding/params_mapping_transcoding.xlsm"); 
 			LinkedHashMap<String, ArrayList<String>> mapping;
			//Read the mapping from the external parameter file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
        		mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        	}
        	excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        	excelInputConfig.setMappingInsideExcel(false);
    		
    		//The names of the param sheets taken from the mapping
    		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
    		List<ExcelConfiguration> allExcelParameters = null;
    		//Read the parameter sheets with the help of the mapping from the external file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
				allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
        	}
 		
    	excelInputConfig.setConfigInsideExcel(false);
    	excelInputConfig.setConfiguration(allExcelParameters);
 		//keyFamily
 		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/transcoding/ESTAT+SEEAAIR+1.0.xml");
 		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

 		//outputFileName
 		File outputFile = new File(completeResultTargetFileName); 
 		FileOutputStream outputStream = new FileOutputStream(outputFile);
 		TestConverterUtil.convert(Formats.EXCEL, 
 							Formats.STRUCTURE_SPECIFIC_DATA_2_1,
 							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/transcoding/BE_AEA_2017.xlsm"),
 							outputStream, 
 							excelInputConfig,
 							sdmxOutputConfig,
 							dataStructure,
 							null, 
                            converterDelegatorService);

		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
 	}
    
    /**
     * External Parameter file provided.
     * Transcoding sheet is included inside the input file only.
     * There is no transcoding sheet inside parameter sheet.
     * @throws Exception
     */
    @Test
 	public void testTranscodingInternalFile() throws Exception {
 		String resultFileName = "2_output_BE_AEA_2017.xml"; 
 		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
 		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"transcoding/" + resultFileName;
 		
 		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
 		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
 		excelInputConfig.setHeader(header);
		excelInputConfig.setInflateRatio(0.00001);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
 		File excelConfig = new File("./testfiles/excel_input/transcoding/2_params_mapping_transcoding.xlsm"); 
 			LinkedHashMap<String, ArrayList<String>> mapping;
			//Read the mapping from the external parameter file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
        		mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        	}
        	excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        	excelInputConfig.setMappingInsideExcel(false);
    		
    		//The names of the param sheets taken from the mapping
    		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
    		List<ExcelConfiguration> allExcelParameters = null;
    		//Read the parameter sheets with the help of the mapping from the external file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
				allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
        	}
 		
    	excelInputConfig.setConfigInsideExcel(false);
    	excelInputConfig.setConfiguration(allExcelParameters);
 		//keyFamily
 		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/transcoding/ESTAT+SEEAAIR+1.0.xml");
 		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

 		//outputFileName
 		File outputFile = new File(completeResultTargetFileName); 
 		FileOutputStream outputStream = new FileOutputStream(outputFile);
 		TestConverterUtil.convert(Formats.EXCEL, 
 							Formats.STRUCTURE_SPECIFIC_DATA_2_1,
 							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/transcoding/2_BE_AEA_2017.xlsm"),
 							outputStream, 
 							excelInputConfig,
 							sdmxOutputConfig,
 							dataStructure,
 							null, 
                            converterDelegatorService);

         File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
         File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
 	}
    
    /**
     * External Parameter file provided.
     * Transcoding sheet is included inside the input file and there is another one inside input file.
     * We test the priority of the two.
     * @see 'SDMXCONV-963'
     * @throws Exception
     */
    @Test
 	public void testTranscodingInternalExternalParamsFile() throws Exception {
 		String resultFileName = "3_output_BE_AEA_2017.xml"; 
 		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
 		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"transcoding/" + resultFileName;
 		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
 		
 		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
 		excelInputConfig.setHeader(header);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
 		File excelConfig = new File("./testfiles/excel_input/transcoding/3_params_mapping_transcoding.xlsm");
 			LinkedHashMap<String, ArrayList<String>> mapping;
			//Read the mapping from the external parameter file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
        		mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        	}
        	excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        	excelInputConfig.setMappingInsideExcel(false);
    		
    		//The names of the param sheets taken from the mapping
    		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
    		List<ExcelConfiguration> allExcelParameters = null;
    		//Read the parameter sheets with the help of the mapping from the external file
        	try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
				allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
        	}
 		
    	excelInputConfig.setConfigInsideExcel(false);
    	excelInputConfig.setConfiguration(allExcelParameters);
 		//keyFamily
 		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/transcoding/ESTAT+SEEAAIR+1.0.xml");
 		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

 		//outputFileName
 		File outputFile = new File(completeResultTargetFileName); 
 		FileOutputStream outputStream = new FileOutputStream(outputFile);
 		TestConverterUtil.convert(Formats.EXCEL, 
 							Formats.STRUCTURE_SPECIFIC_DATA_2_1,
 							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/transcoding/2_BE_AEA_2017.xlsm"),
 							outputStream, 
 							excelInputConfig,
 							sdmxOutputConfig,
 							dataStructure,
 							null, 
                            converterDelegatorService);

         File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
         File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
 	}
    
    /**
     * Test created for SDMXCONV-975.
     * Testing the effectiveness of MaxEmptyRows, MaxEmptyColumns
     * @throws Exception
     */
    @Test
 	public void testMaxEmptyColumns() throws Exception {
 		String resultFileName = "out_MaxEmptyColumns_2.xml"; 
 		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
 		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"975-MaxEmptyColumns/" + resultFileName;
 		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
 		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
 		excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(true);
    	excelInputConfig.setConfigInsideExcel(true);
		excelInputConfig.setInflateRatio(0.00001);
 		//keyFamily
 		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/975-MaxEmptyColumns/ESTAT+REGIO_2015+1.1_Extended.xml");
 		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();

 		//outputFileName
 		File outputFile = new File(completeResultTargetFileName); 
 		FileOutputStream outputStream = new FileOutputStream(outputFile);
 		TestConverterUtil.convert(Formats.EXCEL, 
 							Formats.COMPACT_SDMX,
 							readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/975-MaxEmptyColumns/MaxEmptyColumns_2.xlsx"),
 							outputStream, 
 							excelInputConfig,
 							sdmxOutputConfig,
 							dataStructure,
 							null, 
                            converterDelegatorService);

         File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
         File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
 	}

	/**
	 * External Parameter file provided.
	 * There was an error with the list in which
	 * we store the columns we found empty.
	 * We <b>must</b> clear this for every sheet.
	 * @see 'SDMXCONV-1013'
	 * @throws Exception
	 */
	@Test
	public void testEmptyColumnsList() throws Exception {
		String resultFileName = "04_ENERGY_1013.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"energy/" + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		File excelConfig = new File("./testfiles/excel_input/energy/ENERGY_1013_params.xlsx");
		LinkedHashMap<String, ArrayList<String>> mapping;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setConfigInsideExcel(false);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;

		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setConfiguration(allExcelParameters);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/energy/ESTAT+ENERGY_NTGAS_A+1.0.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/energy/ESTAT+ENERGY_NTGAS_A+1.0.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		//output configuration
		MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.CSV,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/energy/ENERGY_1013.xlsm"),
				outputStream,
				excelInputConfig,
				csvOutputConfig,
				dataStructure,
				dataflow,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
						expectedFile, generatedFile);
	}

	/**
	 * There was an error with group dimensions were repeated in every row,
	 * and in some rows they should be empty.
	 * External Parameter file provided.
	 * @see 'SDMXCONV-1013'
	 * @throws Exception
	 */
	@Test
	public void testSdmxCsvSeriesAttributes() throws Exception {
		String resultFileName = "05_ENERGY_1013.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+"energy/" + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_2/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		File excelConfig = new File("./testfiles/excel_input/energy/Param_Table_5b.xlsx");
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);

		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;

		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
		}

		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/energy/ESTAT+ENERGY_NTGAS_A+1.0.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/energy/ESTAT+ENERGY_NTGAS_A+1.0.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		//output configuration
		MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.CSV,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/energy/ENERGY_1013.xlsm"),
				outputStream,
				excelInputConfig,
				csvOutputConfig,
				dataStructure,
				dataflow,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile =outputFile;
		FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * Round to fit to dsd maxLength is performed after rounding precision.
	 * SDMXCONV-1075
	 * @throws Exception
	 */
	@Test
	public void testRoundingMechanism() throws Exception {
		String resultFileName = "output_roundToFit.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(true);
		excelInputConfig.setConfigInsideExcel(true);
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/roundToFit/ESTAT+COSAEA_REGION+1.0_all.xml");
		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
		sdmxOutputConfig.setNamespaceprefix("default_namespace");
		sdmxOutputConfig.setNamespaceuri("default_uri");
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/roundToFit/roundToFit-inputFile.xlsx"),
				outputStream,
				excelInputConfig,
				sdmxOutputConfig,
				dataStructure,
				null,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * Trim leading or trailing whitespace from values that are numbers in the dsd.
	 * SDMXCONV-1066
	 * @throws Exception
	 */
	@Test
	public void testTrimWhitespaces() throws Exception {
		String resultFileName = "output-trimWhite.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(true);
		excelInputConfig.setConfigInsideExcel(true);
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/roundToFit/ESTAT+COSAEA_REGION+1.0_all.xml");
		SdmxOutputConfig sdmxOutputConfig = new SdmxOutputConfig();
		sdmxOutputConfig.setNamespaceprefix("default_namespace");
		sdmxOutputConfig.setNamespaceuri("default_uri");
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/roundToFit/trimWhite.xlsx"),
				outputStream,
				excelInputConfig,
				sdmxOutputConfig,
				dataStructure,
				null,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * No data is generated only header must be present in the output file.
	 * see also SDMXCONV-1174
	 * @throws Exception
	 */
	@Test
	public void testEmptySdmxCsvOutputFile() throws Exception {
		String resultFileName = "7_output_sdmxCsv.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/7_ENERGY_MOSGAS_M_CY_2020_0011_V0001.xlsx"), new FirstFailureExceptionHandler()));
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/7_ENERGY_MOSGAS_M1.0_201221.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/7_ENERGY_MOSGAS_M1.0_201221.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		SdmxCsvOutputConfig sdmxOutputConfig = new SdmxCsvOutputConfig();
		sdmxOutputConfig.setDelimiter(";");
		sdmxOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.SDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/7_ENERGY_MOSGAS_M_CY_2020_0011_V0001.xlsx"),
				outputStream,
				excelInputConfig,
				sdmxOutputConfig,
				dataStructure, dataflow,
				converterDelegatorService);
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = (outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * <h1>Validation test</h1>
	 * if dataEnd has value  and SkipIncompleteKeys = false and the row is not included in the skiprows list,
	 * then all the empty rows should be checked and reported (both the type of the row and every cell's empty value).
	 * When MaxEmptyRows is used then again to determine if a row is empty we will have to check the type if empty and if not check every cell's value.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1176">SDMXCONV-1176</a>
	 * @throws Exception
	 */
	@Test
	public void testEmptyRows1() throws Exception {
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		File excelConfig = new File("./testfiles/excel_input/1176-EmptyRows/parameters-credMob.xlsx");
		LinkedHashMap<String, ArrayList<String>> mapping;
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setInflateRatio(0.00001);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);

		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);

		InputStream structureInputStream = new FileInputStream("./testfiles/excel_input/1176-EmptyRows/df-credMob.xml");
		InputStream dataInputStream = new FileInputStream("./testfiles/excel_input/1176-EmptyRows/input-credMob.xlsx");
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 10).getErrors();
		assertNotNull(errorList);
		assertEquals(5, errorList.size());
		assertTrue(errorList.get(3).getMessage().contains("257")); //This is an empty line appeared in the error
		assertTrue(errorList.get(4).getMessage().contains("484")); //This is an empty line appeared in the errors
	}

	/**
	 * <h1>Validation test</h1>
	 * if dataEnd has value  and SkipIncompleteKeys = true and the row is not included in the skiprows list,
	 * then all the empty rows should be ignored and not reported (both the type of the row and every cell's empty value).
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1176">SDMXCONV-1176</a>
	 * @throws Exception
	 */
	@Test
	public void testEmptyRows2() throws Exception {
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setInflateRatio(0.00001);
		File excelConfig = new File("./testfiles/excel_input/1176-EmptyRows/parameters-credMob.xlsx");
		LinkedHashMap<String, ArrayList<String>> mapping;
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
			allExcelParameters.get(0).setSkipIncompleteKey(true);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);

		InputStream structureInputStream = new FileInputStream("./testfiles/excel_input/1176-EmptyRows/df-credMob.xml");
		InputStream dataInputStream = new FileInputStream("./testfiles/excel_input/1176-EmptyRows/input-credMob.xlsx");
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 500).getErrors();
		assertNotNull(errorList);
		assertEquals(3, errorList.size());
		assertTrue(errorList.get(0).getMessage().contains("TIME_PER_COLLECT")); //This is an empty line appeared in the error
		assertTrue(errorList.get(1).getMessage().contains("REPYEAREND"));
	}

	/**
	 * <h1>Validation test for position of the errors</h1>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1566">SDMXCONV-1566</a>
	 * @throws Exception
	 */
	@Test
	public void testMissingErrorPositions() throws Exception {
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setInflateRatio(0.00001);
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setFormulaErrorsReported(true);
		File excelConfig = new File("./testfiles/excel_input/1566/ESSPROS_QUALI_BENEFIT_Parameters_2023_06_15.xlsx");
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		InputStream structureInputStream = new FileInputStream("./testfiles/excel_input/1566/ESTAT+ESSPROS_QUALI_BENEFIT_A+1.0_20240314.xml");
		InputStream dataInputStream = new FileInputStream("./testfiles/excel_input/1566/ESSPROS_QUALI_BENEFIT.xlsx");
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 100).getErrors();
		assertNotNull(errorList);
		assertEquals(11, errorList.size());
		assertEquals(14, errorList.get(0).getErrorOccurrences());
	}

	/**
	 * <h1>Validation test for formula errors</h1>
	 * <p>Report all formula errors even those the Cell type is ERROR</p>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1239">SDMXCONV-1239</a>
	 * @throws Exception
	 */
/*	@Test
	public void testFormulaErrors() throws Exception {
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setInflateRatio(0.00001);
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setFormulaErrorsReported(true);
		File excelConfig = new File("./testfiles/excel_input/1239-FormulaErrors/parameters.xlsx");
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		InputStream structureInputStream = new FileInputStream("./testfiles/excel_input/1239-FormulaErrors/ESTATANI_SLAUGHT_M.xml");
		InputStream dataInputStream = new FileInputStream("./testfiles/excel_input/1239-FormulaErrors/ANI_SLAUGHT_M_NL_2020_0003_V1-Formula_error_values.xlsx");
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 100).getErrors();
		assertNotNull(errorList);

		for(ValidationError error : errorList) {
			System.out.println(error.getMessage());
			System.out.println(error.getErrorDetails().getErrorPositions());
		}
		assertEquals(28, errorList.size()); //changed the number of errors after SDMXCONV-1563 for duplicate errors.
	}*/

	/**
	 * The whole data column is empty but there is a default value present.
	 * All cells should be parsed.
	 * see also SDMXCONV-1278
	 * @throws Exception
	 */
	@Test
	public void testEmptyValuesColumn() throws Exception {
		String resultFileName = "EmptyColumnWithDefaultValue.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		File excelConfig = new File("./testfiles/excel_input/1278-EmptyColumn/EmptyColumnWithDefaultValue-Parameters.xlsx");
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		excelInputConfig.setMappingInsideExcel(false);

		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
		}

		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setErrorIfEmpty(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/1278-EmptyColumn/ESTATANI_LSCATMJ.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/1278-EmptyColumn/ESTATANI_LSCATMJ.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1278-EmptyColumn/EmptyColumnWithDefaultValue.xlsx"),
				outputStream,
				excelInputConfig,
				new SdmxOutputConfig(),
				dataStructure, dataflow,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint((outputFile));
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * Test the error positions reported.
	 * see also SDMXCONV-1224
	 * @throws Exception
	 */
	@Test
	public void testValidationPosition() throws Exception {
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setInflateRatio(0.00001);
		File excelConfig = new File("./testfiles/excel_input/1224-errorPosition/params-EDUCAT_ENTR.xlsx");
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		InputStream structureInputStream = new FileInputStream("./testfiles/excel_input/1224-errorPosition/UISEDUCAT_ENTR_A1.0.xml");
		InputStream dataInputStream = new FileInputStream("./testfiles/excel_input/1224-errorPosition/input-EDUCAT_ENTR.xlsx");
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 50).getErrors();
		assertNotNull(errorList);
		assertEquals(10, errorList.size());
		assertEquals(errorList.get(7).getErrorInfo().getErrorPositions().get(0).getCurrentCell(), "ENTR2-Mobile&Age");
	}

	/**
	 * When an observation value is only spaces we need to trim it,
	 * so the excel treat it as empty and add the default value.
	 * see also SDMXCONV-1269
	 * @throws Exception
	 */
	@Test
	public void testTrimValues() throws Exception {
		String resultFileName = "Sample_whitespaces7.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(true);
		excelInputConfig.setConfigInsideExcel(true);
		excelInputConfig.setErrorIfEmpty(false);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/1269-trimSpaces/NA_MAIN+ESTAT+1.2_RI.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/1269-trimSpaces/NA_MAIN+ESTAT+1.2_RI.xml");
		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1269-trimSpaces/SDMX-NA_MAIN_T0101_V1.2c_Sample_whitespaces7.xlsx"),
				outputStream,
				excelInputConfig,
				new SdmxOutputConfig(),
				dataStructure, null,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint((outputFile));
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * Test for preprocessing sheet given in the parameters sheet.
	 * <p>CodesFromExternalFile searches in the external parameter file.</p>
	 * @see "SDMXCONV-1286"
	 */
	@Test
	public void testForCodesFromExternalFile() throws Exception {
		String resultFileName = "1286-SS-output.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+ "1286-CodesFromExternalFile/" + resultFileName;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setErrorIfEmpty(false);
		File excelConfig = new File("./testfiles/excel_input/1286-CodesFromExternalFile/paramsWithCodes.xlsx");
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			final LinkedHashMap<String, ExcelSheetUtils> codeSheetUtils = ExcelUtils.readCodes(excelExternalParamIs,
					allExcelParameters,
					excelInputConfig.getInflateRatio(),
					new FirstFailureExceptionHandler());
			//SDMXCONV-1286, after the configurations are set we set the codes sheet values
			excelInputConfig.setCodesSheetUtils(codeSheetUtils);
		}
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/1286-CodesFromExternalFile/Structures.xml");
		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1286-CodesFromExternalFile/SDMX-compatible-Template.xlsx"),
				outputStream,
				excelInputConfig,
				new SdmxOutputConfig(),
				dataStructure, null,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint((outputFile));
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * Test for preprocessing sheet given in the parameters sheet.
	 * <p>CodesFromExternalFile searches in the external parameter file.</p>
	 * @see "SDMXCONV-1286"
	 */
	@Test
	public void testWithoutCodesFromExternalFile() throws Exception {
		String resultFileName = "1286-SS-output-2.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+ "1286-CodesFromExternalFile/" + resultFileName;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setErrorIfEmpty(false);
		File excelConfig = new File("./testfiles/excel_input/1286-CodesFromExternalFile/paramsWithoutCodes.xlsx");
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			final LinkedHashMap<String, ExcelSheetUtils> codeSheetUtils = ExcelUtils.readCodes(excelExternalParamIs,
					allExcelParameters,
					excelInputConfig.getInflateRatio(),
					new FirstFailureExceptionHandler());
			//SDMXCONV-1286, after the configurations are set we set the codes sheet values
			excelInputConfig.setCodesSheetUtils(codeSheetUtils);
		}
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/1286-CodesFromExternalFile/Structures.xml");
		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1286-CodesFromExternalFile/SDMX-compatible-Template.xlsx"),
				outputStream,
				excelInputConfig,
				new SdmxOutputConfig(),
				dataStructure, null,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint((outputFile));
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * SDMXCONV-1432 Wont read empty columns without DataEnd.
	 * @throws Exception
	 */
	@Test
	public void testEmptyColumnWithDataEnd() throws Exception {
		String resultFileName = "1432-output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+ "1432-EmptyColumn/" + resultFileName;
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setErrorIfEmpty(false);
		File excelConfig = new File("./testfiles/excel_input/1432-EmptyColumn/METR_Parameters_ticket.xlsx");
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/1432-EmptyColumn/EARNINGS_Multiple.xml");
		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/excel_input/1432-EmptyColumn/EARNINGS_Multiple.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/1432-EmptyColumn/EARNINGS_Multiple.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.iSDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1432-EmptyColumn/METR_2015.xlsx"),
				outputStream,
				excelInputConfig,
				csvOutputConfig,
				dataStructure, dataflow,
				converterDelegatorService);
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * SDMXCONV-1432 Wont read empty columns without DataEnd.
	 * @throws Exception
	 */
	@Test
	public void testEmptyColumnWithNoDataEnd() throws Exception {
		String resultFileName = "1432-output-noEnd.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH+ "1432-EmptyColumn/" + "1432-output.csv";
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setErrorIfEmpty(false);
		File excelConfig = new File("./testfiles/excel_input/1432-EmptyColumn/METR_Parameters_ticket-withNoDataEnd.xlsx");
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		LinkedHashMap<String, ArrayList<String>> mapping;
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
			assertNotNull(allExcelParameters);
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/excel_input/1432-EmptyColumn/EARNINGS_Multiple.xml");
		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/excel_input/1432-EmptyColumn/EARNINGS_Multiple.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/excel_input/1432-EmptyColumn/EARNINGS_Multiple.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(Formats.EXCEL,
				Formats.iSDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1432-EmptyColumn/METR_2015.xlsx"),
				outputStream,
				excelInputConfig,
				csvOutputConfig,
				dataStructure, dataflow,
				converterDelegatorService);
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
				" is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
	}

	/**
	 * When no parameter sheet is found, check the error.
	 * see also SDMXCONV-1414
	 * @throws Exception
	 */
	@Test
	public void testParamsExistence() throws Exception {
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		File excelConfig = new File("./testfiles/excel_input/1414-NoParams/NASEC_T0620_A_V1.9_MissingParameters.xlsx");
		LinkedHashMap<String, ArrayList<String>> mapping;
		excelInputConfig.setHeader(header);
		excelInputConfig.setMappingInsideExcel(false);
		excelInputConfig.setInflateRatio(0.00001);
		ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
		//Read the mapping from the external parameter file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
			mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);

		//The names of the param sheets taken from the mapping
		List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
		List<ExcelConfiguration> allExcelParameters = null;
		//Read the parameter sheets with the help of the mapping from the external file
		try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)){
			allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
		}
		excelInputConfig.setConfigInsideExcel(false);
		excelInputConfig.setConfiguration(allExcelParameters);

		InputStream structureInputStream = new FileInputStream("./testfiles/dsds/NASEC_T0620.xml");
		InputStream dataInputStream = new FileInputStream("./testfiles/excel_input/1414-NoParams/NASEC_T0620_A_V1.9_MissingParameters.xlsx");
		List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 10).getErrors();
		assertNotNull(errorList);
		assertEquals(1, errorList.size());
		assertEquals("Invalid Parameters detected. EXCEL Parameters sheet missing, cannot read the data..", errorList.get(0).getMessage());
		assertEquals(" The Dataset requires an embedded or external Parameters sheet to process the data.", errorList.get(0).getErrorDetails().getUserFriendlyString());
	}


//	@Test
//	public void testGenericOutput() throws Exception {
//		String resultFileName = "2_output_generic20_UOE_NON_FINANCE+ESTAT+0.4.xml"; 
//		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
//		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
//		
//		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
//		
//		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
//		ExcelInputConfig excelInputConfig = new ExcelInputConfig();
//		excelInputConfig.setHeader(header);
//		excelInputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/2_excel.xlsx")));
//
//		//keyFamily
//		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
//
//		//outputFileName
//		File outputFile = new File(completeResultTargetFileName); 
//		FileOutputStream outputStream = new FileOutputStream(outputFile);
//		
//		TestConverterUtil.convert(Formats.EXCEL, 
//				Formats.GENERIC_SDMX,
//				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/2_excel.xlsx"),
//				outputStream, 
//				excelInputConfig,
//				new SdmxOutputConfig(),
//				dataStructure,
//				null, 
//                converterDelegatorService);
//		
//        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
//        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
//        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName + 
//        		" is different than what is expected at " + completeResultTestFilesFileName,
//        		expectedFile, generatedFile);
//	}
	
//	@Test
//	public void testGenericOutputWithProductionFile() throws Exception {
//		
//		String resultFileName = "1_output_generic20_ESTAT_STS_2.1.xml"; 
//		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
//		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
//		
//		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
//		
//		//keyFamily
//        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/ESTAT_STS_v2.1.xml");
//
//        //set header
//        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
//
//        ExcelInputConfig inputConfig = new ExcelInputConfig();
//        inputConfig.setHeader(header);
//        inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/1_excel.xlsx")));
//
//		//outputFileName
//		File outputFile = new File(completeResultTargetFileName);
//		FileOutputStream outputStream = new FileOutputStream(outputFile);
//		TestConverterUtil.convert(Formats.EXCEL, 
//					Formats.GENERIC_SDMX,
//                                        readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1_excel.xlsx"),
//                                        outputStream, 
//                                        inputConfig,
//                                        new SdmxOutputConfig(),
//                                        kf,
//                                        null,
//                                        converterDelegatorService);
//		
//        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
//        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
//        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName + 
//        		" is different than what is expected at " + completeResultTestFilesFileName,
//        		expectedFile, generatedFile);
//	}
/*
	@Test
	public void testGenericOutputWithExcelInputHavingDimensionsMissingFromConfiguration() throws Exception {
		String resultFileName = "4_output_generic20_NASU_1.8.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
	    //the input file has the following dimensions not configured ( in the parameter sheet)
        //REPYEARSTART, REPYEAREND, DATA_COMP, CURRENCY, DISS_ORG, PRE_BREAK_VALUE

        //keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/NASU_1_8.xml");

		//set header
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");

		ExcelInputConfig inputConfig = new ExcelInputConfig();
		inputConfig.setHeader(header);
		inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/4_input_NASU_1.8.xls")));

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(	Formats.EXCEL,
				Formats.GENERIC_SDMX,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/4_input_NASU_1.8.xls"),
				outputStream,
				inputConfig,
				new SdmxOutputConfig(),
				kf,
				null);

        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        
        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
	}
	
	@Test
	public void testSdmxCsvOutputWithProductionFile() throws Exception {
		String resultFileName = "1_output_sdmxCsv_ESTAT_STS_2.1.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/ESTAT_STS_v2.1.xml");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/ESTAT_STS_v2.1.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

        //set header
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");

        ExcelInputConfig inputConfig = new ExcelInputConfig();
        inputConfig.setHeader(header);
        inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/1_excel.xlsx")));

		CsvOutputConfig csvOutputConfig = new CsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setEscapeCsvValues(false);
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(	Formats.EXCEL, 
							Formats.SDMX_CSV,
                            readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/1_excel.xlsx"),
							outputStream, 
							inputConfig,
							csvOutputConfig,
                            kf,
                            dataflow);

        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);
		
	}

	@Test
	public void testCsvOutputWithProductionFile() throws Exception {
		String resultFileName = "3_output_csv_ENERGY_ESTAT_1.1.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/ENERGY+ESTAT+1.1.xml");

		//set header
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");

		ExcelInputConfig inputConfig = new ExcelInputConfig();
		inputConfig.setHeader(header);
		inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/3_input_ENERGY_ESTAT_1.1.xls")));

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
		csvOutputConfig.setDelimiter(";");
		csvOutputConfig.setEscapeCsvValues(false);
		csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(	Formats.EXCEL,
				Formats.CSV,
				readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/3_input_ENERGY_ESTAT_1.1.xls"),
				outputStream,
				inputConfig,
				csvOutputConfig,
				kf,
				null);

        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName + 
        		" is different than what is expected at " + completeResultTestFilesFileName,
        		expectedFile, generatedFile);

	}

	@Ignore(value="the assert does not work and it seems to be a bug either in the old converter or int he new reader")
	public void testGeneric21OutputWithParametersInsideAndExternalSheetMapping() throws Exception {
		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./test_files/test_excel/ESTAT_STS_v2.2.xml");

		//set header
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./test_files/test_excel/header.prop"));
        Map<String, String> mappingBetweenDataSheetsAndParamSheets = ExcelUtils.getParametersMap(new File("./test_files/test_excel/SheetMapping.txt"));
		System.out.println("mapping : "+ mappingBetweenDataSheetsAndParamSheets);

		ExcelInputConfig inputConfig = new ExcelInputConfig();
		inputConfig.setHeader(header);
		inputConfig.setConfiguration(ExcelUtils.readExcelConfig(new File("./test_files/test_excel/excelWithParametersInside.xlsx")));
		inputConfig.setMapping(mappingBetweenDataSheetsAndParamSheets);

		//outputFileName
		File outputFile = new File("./target/generic21FromExcel.xml");
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		TestConverterUtil.convert(  Formats.EXCEL,
				Formats.GENERIC_DATA_2_1,
                readableDataLocationFactory.getReadableDataLocation(new FileInputStream("./test_files/test_excel/excelWithParametersInside.xlsx")),
				outputStream,
				inputConfig,
				new SdmxOutputConfig(),
				kf,
				null);

		FileAssert.assertEquals("the generated generic 21 file is different than what is expected",
				new File("./test_files/test_excel/out_generic21.xml"),
				outputFile);
	}
	
	 @Test
	 public void testExcelWithDefaultValueParameterMissingToCsv() throws Exception {
	    	String resultFileName = "4_output_csv_ENERGY_ESTAT_1.1_default_parameter_missing.csv"; 
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			
			//keyFamily
			DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/ENERGY+ESTAT+1.1.xml");

			//set the default header
			HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
			
			ExcelInputConfig inputConfig = new ExcelInputConfig();
			inputConfig.setHeader(header);
			inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/4_input_ENERGY_ESTAT_1.1.xlsm")));

	        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			csvOutputConfig.setEscapeCsvValues(false);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			//outputFileName
			File outputFile = new File(completeResultTargetFileName);
			FileOutputStream outputStream = new FileOutputStream(outputFile);
			TestConverterUtil.convert(	Formats.EXCEL,
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/4_input_ENERGY_ESTAT_1.1.xlsm"),
					outputStream,
					inputConfig,
					csvOutputConfig,
					kf,
					null);

	        File expectedFile = new File(completeResultTestFilesFileName);
	        File generatedFile = outputFile;
	        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName + 
	        		" is different than what is expected at " + completeResultTestFilesFileName,
	        		expectedFile, generatedFile);

	    }
	 
	 @Test
	 public void testExcelWithDefaultValueParameterNaNToCsv() throws Exception {
	    	String resultFileName = "5_output_csv_UOE_NON_FINANCE+ESTAT+0.4_default_parameter_NaN.csv"; 
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			
			//keyFamily
			DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

			//set the default header
			HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
			
			ExcelInputConfig inputConfig = new ExcelInputConfig();
			inputConfig.setHeader(header);
			inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/5_input_UOE_NON_FINANCE+ESTAT+0.4_default_value_NaN.xlsx")));

	        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			csvOutputConfig.setEscapeCsvValues(false);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			//outputFileName
			File outputFile = new File(completeResultTargetFileName);
			FileOutputStream outputStream = new FileOutputStream(outputFile);
			TestConverterUtil.convert(	Formats.EXCEL,
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/5_input_UOE_NON_FINANCE+ESTAT+0.4_default_value_NaN.xlsx"),
					outputStream,
					inputConfig,
					csvOutputConfig,
					kf,
					null);

	        File expectedFile = new File(completeResultTestFilesFileName);
	        File generatedFile = outputFile;
	        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName + 
	        		" is different than what is expected at " + completeResultTestFilesFileName,
	        		expectedFile, generatedFile);

	    }
	 
	 @Test
	 public void testExcelWithDefaultValueParameterToCsv() throws Exception {
	    	String resultFileName = "6_output_csv_UOE_NON_FINANCE+ESTAT+0.4_default_value.csv"; 
			String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
			String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
			
			Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
			
			//keyFamily
			DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");

			//set the default header
			HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
			
			ExcelInputConfig inputConfig = new ExcelInputConfig();
			inputConfig.setHeader(header);
			inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/excel_input/6_input_UOE_NON_FINANCE+ESTAT+0.4_default_value.xlsx")));

	        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			csvOutputConfig.setEscapeCsvValues(false);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);

			//outputFileName
			File outputFile = new File(completeResultTargetFileName);
			FileOutputStream outputStream = new FileOutputStream(outputFile);
			TestConverterUtil.convert(	Formats.EXCEL,
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/6_input_UOE_NON_FINANCE+ESTAT+0.4_default_value.xlsx"),
					outputStream,
					inputConfig,
					csvOutputConfig,
					kf,
					null);

	        File expectedFile = new File(completeResultTestFilesFileName);
	        File generatedFile = outputFile;
	        FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName + 
	        		" is different than what is expected at " + completeResultTestFilesFileName,
	        		expectedFile, generatedFile);

	    }

*/

	/*
	@Test
	public void testCompactOutput() throws Exception {
		Converter converter = new Converter(); 
		
		// initialize the Input Params
		InputParams params = new InputParams();
		params.setFlatFileEncoding("UTF-8");
		
		//keyFamily
		SdmxBeans structure = structureService.readStructuresFromStream("./test_files/ECB_EXR/ECB+ECB_EXR1+1.0.xml");
		
		DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();	
		params.setKeyFamilyBean(kf);
		
		//set header
		HeaderBean header = headerService.parseHeaderPropertyFile(new FileInputStream("./test_files/ECB_EXR/header.prop"));
		params.setHeaderBean(header);
		
		//excel parameters
		params.setExcelType(ExcelUtils.XLSX_EXCEL);
		params.setAllExcelParameters(excelService.readExcelConfig(
				new FileInputStream("./test_files/ECB_EXR/excel.xlsx"), 
				false, 
				params.getExcelType()));
		
		//ouputFileName
		File outputFile = new File("./target/compactFromExcel.xml"); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		converter.convert(	Formats.EXCEL, 
							Formats.COMPACT_SDMX, 
							new FileInputStream("./test_files/ECB_EXR/excel.xlsx"), 
							outputStream, 
							params);
		
	FileAssert.assertEquals("the generated generic file is different than what is expected", 
                new File("./test_files/ECB_EXR/outputCompactFromExcel_7_8.xml"),
                outputFile);
	}
	
	
	@Test
	public void testCompactOutputWithCustomNamespace() throws Exception {
		Converter converter = new Converter(); 
		
		// initialize the Input Params
		InputParams params = new InputParams();
		params.setFlatFileEncoding("UTF-8");
		
		//keyFamily
		SdmxBeans structure = structureService.readStructuresFromStream("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml");
		
		DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();	
		params.setKeyFamilyBean(kf);
		
		//set header
		HeaderBean header = headerService.parseHeaderPropertyFile(new FileInputStream("./test_files/UOE_NON_FINANCE/header.prop"));
		params.setHeaderBean(header);
		params.setNamespaceUri("cacaNamespace");
		params.setNamespacePrefix("cacaPrefix");
		
		//excel parameters
		params.setExcelType(ExcelUtils.XLSX_EXCEL);
				 
		params.setAllExcelParameters(excelService.readExcelConfig(new FileInputStream("./test_files/UOE_NON_FINANCE/excel.xlsx"), false, params.getExcelType()));
		
		//ouputFileName
		File outputFile = new File("./target/compactFromExcelWithCustomNamespace.xml"); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		converter.convert(	Formats.EXCEL, 
							Formats.COMPACT_SDMX, 
							new FileInputStream("./test_files/UOE_NON_FINANCE/excel.xlsx"), 
							outputStream, 
							params);
		
		FileAssert.assertEquals("the generated generic file is different than what is expected", 
                new File("./test_files/UOE_NON_FINANCE/out_compactWithCustomNamespace.xml"),
                outputFile);
	}
	
	@Test
	public void testCsvOutput() throws Exception {
		Converter converter = new Converter(); 
		
		// initialize the Input Params
		InputParams params = new InputParams();
				
		//keyFamily
		SdmxBeans structure = structureService.readStructuresFromStream("./test_files/NA_SEC/ESTAT+NA_SEC+1.2.xml");
		
		DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();	
		params.setKeyFamilyBean(kf);
		
		//set header
		HeaderBean header = headerService.parseHeaderPropertyFile(new FileInputStream("./test_files/NA_SEC/header.prop"));
		params.setHeaderBean(header);
		
		//excel parameters
		params.setExcelType(ExcelUtils.XLSX_EXCEL);
		params.setAllExcelParameters(excelService.readExcelConfig(new FileInputStream("./test_files/NA_SEC/excel.xlsx"),
																	false, 
																	params.getExcelType()));
		//csv output setup
		params.setDelimiter(";");
		params.setFlatFileEncoding("UTF-8");
		params.setHeaderRow(Defs.NO_COLUMN_HEADERS);
		InputStream mappingIs = new FileInputStream("./test_files/NA_SEC/mapping.xml");
		Map<String, CsvInColumnMapping> mappingMap = csvService.buildInputDimensionLevelMapping(mappingIs);	
		params.setComponentsInputMapping(mappingMap);
		params.setLevelNumber("1");
		
		//ouputFileName
		File outputFile = new File("./target/csvFromExcel.csv"); 
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		converter.convert(	Formats.EXCEL, 
							Formats.CSV, 
							new FileInputStream("./test_files/NA_SEC/excel.xlsx"), 
							outputStream, 
							params);
		
		FileAssert.assertEquals("the generated csv file is different than what is expected", 
                new File("./test_files/NA_SEC/out_csv.csv"),
                outputFile);
	}
	
	
*/
	
/*	@Test
    public void testGesmesOutputWithParametersOutsideAndNoSheetMapping() throws Exception {
        Converter converter = new Converter(); 
        
        // initialize the Input Params
        InputParams params = new InputParams();
                
        //keyFamily
        SdmxBeans structure = structureService.readStructuresFromStream(new FileInputStream("./test_files/test_excel/ESTAT_STS_v2.2.xml"));
        
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next(); 
        params.setKeyFamilyBean(kf);
        
        //set header
        HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./test_files/test_excel/header.prop"));
        params.setHeaderBean(header);
        
        //excel parameters
        params.setExcelType(ExcelUtils.XLSX_EXCEL);
        List<com.intrasoft.sdmx.converter.ex.io.data.excel.ExcelConfiguration> paramsLs = new ArrayList<com.intrasoft.sdmx.converter.ex.io.data.excel.ExcelConfiguration>();
		com.intrasoft.sdmx.converter.ex.io.data.excel.ExcelConfiguration excelConfig = ExcelIOUtils.readExcelConfigFromText(new File("./test_files/test_excel/externalParams.txt")));
		paramsLs.add(excelConfig);
		params.setExternalExcelParameter(excelConfig);
		params.setAllExcelParameters(paramsLs);
        //params.setExcelParameterMap(excelService.getParametersMap(new File("./test_files/test_excel/SheetMapping.txt")));
        
        //output setup
        params.setFlatFileEncoding("UTF-8");
        
        //ouputFileName
        File outputFile = new File("./target/gesmes21FromExcel.ges"); 
        FileOutputStream outputStream = new FileOutputStream(outputFile);
        TestConverterUtil.convert(  
        					Formats.EXCEL, 
                            Formats.GESMES_TS, 
                            new FileInputStream("./test_files/test_excel/noParamsInside.xlsx"), 
                            outputStream, 
                            params,
                            structure,
                            null,
                            converterDelegatorService);

        
     FileAssert.assertEquals("the generated generic 21 file is different than what is expected", 
                new File("./test_files/test_excel/out_gesmes21.ges"),
                outputFile);
    }*/		
}
