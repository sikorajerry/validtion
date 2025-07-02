package com.intrasoft.sdmx.converter.stress.tests;

import com.intrasoft.sdmx.converter.integration.tests.IntegrationTestsUtils;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelConfigurer;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

/**
 * <strong>Part of the analysis of SDMXCONV-1557</strong>
 * <p>Four test cases that we will count the duration of the conversion before and after the optimization.</p>
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
@Ignore
public class TestStressConversion {

    @Autowired
    private ConverterDelegatorService converterDelegatorService;

    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;

    @Autowired
    private HeaderService headerService;

    private final static String GENERATED_PATH = "testStressConversion/";
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
    public void testSize01Excel() throws Exception {
        String resultFileName = "01_output.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        InputStream inputStream = new FileInputStream("./testfiles/testStressConversion/01-NASU_16BP_A_V1.8_2011.xlsx");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/testStressConversion/01-NASU_1_8.xml");
        Instant start = Instant.now();
        DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setMappingInsideExcel(true);
        //output configuration
        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
                dsd,
                null,
                converterDelegatorService);
        Instant end = Instant.now();
        System.out.println("01 Elapsed Time in seconds: "+ Duration.between(start, end).getSeconds() + " sec. " + "Obs: " + TestConverterUtil.getProccessedCount());
        /*  String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH  + resultFileName;
            File expectedFile = new File(completeResultTestFilesFileName);
            File generatedFile = outputFile;
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile); */
    }

    @Test
    public void testSize02Excel() throws Exception {
        String resultFileName = "02_output.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        InputStream inputStream = new FileInputStream("./testfiles/testStressConversion/02-Energy-Coal.xlsm");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/testStressConversion/02-ESTAT+ENERGY+1.1+DF.xml");
        Instant start = Instant.now();
        DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setMappingInsideExcel(true);
        //output configuration
        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
                dsd,
                dataflow,
                converterDelegatorService);
        Instant end = Instant.now();
        System.out.println("02 Elapsed Time in seconds: "+ Duration.between(start, end).getSeconds() + " sec. " + "Obs: " + TestConverterUtil.getProccessedCount());
        /*  String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH  + resultFileName;
            File expectedFile = new File(completeResultTestFilesFileName);
            File generatedFile = outputFile;
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile); */
    }

    @Test
    public void testSize03Excel() throws Exception {
        String resultFileName = "03_output.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        InputStream inputStream = new FileInputStream("./testfiles/testStressConversion/03-SDMX-NA_MAIN_T0101_V1.2c_Sample_conceptSeparator.xlsx");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/testStressConversion/03-SDMX_3.0_NA_MAIN+ESTAT+1.2_RI.xml");
        Instant start = Instant.now();
        DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setMappingInsideExcel(true);
        //output configuration
        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
                dsd,
                dataflow,
                converterDelegatorService);
        Instant end = Instant.now();
        System.out.println("03 Elapsed Time in seconds: "+ Duration.between(start, end).getSeconds() + " sec. " + "Obs: " + TestConverterUtil.getProccessedCount());
        /*  String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH  + resultFileName;
            File expectedFile = new File(completeResultTestFilesFileName);
            File generatedFile = outputFile;
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile); */
    }

    @Test
    public void testSize04Excel() throws Exception {
        String resultFileName = "04_output.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        InputStream inputStream = new FileInputStream("./testfiles/testStressConversion/04-EDUCAT_ENRL_A_NO_2020_210924_v2.xlsx");
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/testStressConversion/04-SDMX_3.0_UISEDUCAT_ENRL_A1.0_with_XK.xml");
        Instant start = Instant.now();
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        LinkedHashMap<String, ArrayList<String>> mapping;
        File excelConfig = new File("./testfiles/testStressConversion/04-ENRL_Parameters_2020.xlsx");
        //Read the mapping from the external parameter file
        try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        }
        excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        excelInputConfig.setHeader(header);
        excelInputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(
                new FileInputStream("./testfiles/testStressConversion/04-ENRL_Parameters_2020.xlsx"),
                new FirstFailureExceptionHandler()));
        excelInputConfig.setConfigInsideExcel(false);
        excelInputConfig.setMappingInsideExcel(false);
        //output configuration
        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
                dsd,
                dataflow,
                converterDelegatorService);
        Instant end = Instant.now();
        System.out.println("04 Elapsed Time in seconds: "+ Duration.between(start, end).getSeconds() + " sec. " + "Obs: " + TestConverterUtil.getProccessedCount());
    }

    @Test
    public void testSize05Excel() throws Exception {
        String resultFileName = "ENERGY_PETRO_A_CH_2018_0000_V0006.xml";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/compact_output_3/header.prop");
        Instant start = Instant.now();
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
        //output configuration
        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        //outputFileName
        File outputFile = new File(completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation("./testfiles/excel_input/compact_output_3/ENERGY_PETRO_A_CH_2018_0000_V0006.xlsm"),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
                dataStructure,
                null,
                converterDelegatorService);

        Instant end = Instant.now();
        System.out.println("05 Elapsed Time in seconds: "+ Duration.between(start, end).getSeconds() + " sec. " + "Obs: " + TestConverterUtil.getProccessedCount());
    }

    @Test
    public void testSize06AExcel() throws Exception {
        String resultFileName = "06_A_output.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        InputStream inputStream = new FileInputStream("./testfiles/testStressConversion/06-ENERGY_PETRO_A_AT_2020.xlsm");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/testStressConversion/06-dsd.xml");
        Instant start = Instant.now();
        DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setMappingInsideExcel(true);
        //output configuration
        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
                dsd,
                dataflow,
                converterDelegatorService);
        Instant end = Instant.now();
        System.out.println("06 a Elapsed Time in seconds: "+ Duration.between(start, end).getSeconds() + " sec. " + "Obs: " + TestConverterUtil.getProccessedCount());
    }

    @Test
    public void testSize06BExcel() throws Exception {
        String resultFileName = "06_B_output.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        InputStream inputStream = new FileInputStream("./testfiles/testStressConversion/06-ENERGY_PETRO_A_AL_2021.xlsm");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/testStressConversion/06-dsd.xml");
        Instant start = Instant.now();
        DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setMappingInsideExcel(true);
        //output configuration
        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
                dsd,
                dataflow,
                converterDelegatorService);
        Instant end = Instant.now();
        System.out.println("06 b Elapsed Time in seconds: "+ Duration.between(start, end).getSeconds() + " sec. " + "Obs: " + TestConverterUtil.getProccessedCount());

    }
}
