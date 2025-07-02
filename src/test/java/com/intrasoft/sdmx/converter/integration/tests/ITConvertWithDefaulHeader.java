package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelOutputConfig;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.CsvService;
import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.poi.xssf.extractor.XSSFExcelExtractor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.estat.sdmxsource.util.csv.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
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
public class ITConvertWithDefaulHeader {
	
	@Autowired
	private CsvService csvService;
	
	@Autowired
	private StructureService structureService; 
			
	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;

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
	public void testExcelToCsvOutputWithDefaultHeader() throws Exception {
		String resultFileName = "1_output_csv_with_default_header_ENERGY_ESTAT_1.1.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + "excel_input/" + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + "excel_input/"));

		//outputFileName
		File outputFile = new File(completeResultTargetFileName);

		try(InputStream configInputStream = new FileInputStream("./testfiles/excel_input/3_input_ENERGY_ESTAT_1.1.xls");
		FileOutputStream outputStream = new FileOutputStream(outputFile)) {

			//keyFamily
			DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/ENERGY+ESTAT+1.1.xml");

			//set the default header
			HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");

			ExcelInputConfigImpl inputConfig = new ExcelInputConfigImpl();
			inputConfig.setHeader(header);
			inputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(configInputStream, new FirstFailureExceptionHandler()));
			inputConfig.setExternalParamsFileName("externalParamsFileStream");
			MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
			csvOutputConfig.setDelimiter(";");
			csvOutputConfig.setEscapeValues(EscapeCsvValues.DEFAULT);
			csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);


			TestConverterUtil.convert(Formats.EXCEL,
					Formats.CSV,
					readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/3_input_ENERGY_ESTAT_1.1.xls"),
					outputStream,
					inputConfig,
					csvOutputConfig,
					kf,
					null,
					converterDelegatorService);

			File expectedFile = new File(completeResultTestFilesFileName);
			File generatedFile = outputFile;
			FileAssert.assertEquals("the generated generic file at " + completeResultTargetFileName +
							" is different than what is expected at " + completeResultTestFilesFileName,
					expectedFile, generatedFile);
		}

	}
	
	@Test
	public void convertCsvToMultilevelCsvNoTranscodingNoColumnMappingWithDefaultHeader() throws Exception{
		String resultFileName = "2_output_multilevel_csv_with_default_header_UOE_NON_FINANCE+ESTAT+0.4.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + "singleCsv_input/" + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + "singleCsv_input/"));

		try(InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/1_input_csv.csv");
		FileInputStream configOutputStream = new FileInputStream("./testfiles/multiCsv_input/mapping3Levels.xml")) {


		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml"));
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		CsvInputConfig inputConfig = new CsvInputConfig();
		//set the default header
		HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);

		MultiLevelCsvOutputConfig multiLevelCsvOutputConfig = new MultiLevelCsvOutputConfig();
        //csv output setup
		multiLevelCsvOutputConfig.setDelimiter(";");
		multiLevelCsvOutputConfig.setLevels(3);
		multiLevelCsvOutputConfig.setOutputHeader(CsvOutputColumnHeader.EMPTY);
		multiLevelCsvOutputConfig.setColumnMapping(csvService.buildOutputDimensionLevelMapping(configOutputStream));

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);

		TestConverterUtil.convert(  Formats.CSV,
                            Formats.MULTI_LEVEL_CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            multiLevelCsvOutputConfig,
                            dataStructure,
                            dataflow,
                            converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
	       		" is different than what is expected at " + completeResultTestFilesFileName,
	       		expectedFile, generatedFile);
		}
	}

	@Test
	public void convertSdmxCsvToExcelWithDefaultHeader() throws Exception{
		String resultFileName = "3_output_excel_with_default_header.xlsx";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + IntegrationTestsUtils.GENERATED_NAME + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + "sdmxCsv_input/"));

		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/sdmxCsv.csv");

		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
		//dataflow
		SdmxBeans beansResult = structureService.readStructuresFromFile("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
		DataflowBean df = beansResult.getDataflows().iterator().next();

		SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
		csvInputConfig.setDelimiter(";");
		//set the default header
		HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
		csvInputConfig.setHeader(header);
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

		//excel output setup
		ExcelOutputConfig excelOutputConfig = new ExcelOutputConfig(new FileInputStream("./test_files/ECB_EXR/excel.xlsx"),
				readExcelConfig(new FileInputStream("./test_files/ECB_EXR/excel.xlsx"),
						false, new FirstFailureExceptionHandler()).get(0));
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
		// make the conversion
		TestConverterUtil.convert(   Formats.SDMX_CSV,
				Formats.EXCEL,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				csvInputConfig,
				excelOutputConfig,
				kf,
				df,
				converterDelegatorService);

		XSSFWorkbook outputExcel = new XSSFWorkbook(new FileInputStream(outputFile));
		String outputExcelText = new XSSFExcelExtractor(outputExcel).getText();
		outputExcel.close();
		XSSFWorkbook actualExcel = new XSSFWorkbook(new FileInputStream(new File("./testfiles/sdmxCsv_input/3_output_excel_with_default_header.xlsx")));
		String actualExcelText = new XSSFExcelExtractor(actualExcel).getText();
		actualExcel.close();
		Assert.assertEquals(outputExcelText, actualExcelText);
	}
}
