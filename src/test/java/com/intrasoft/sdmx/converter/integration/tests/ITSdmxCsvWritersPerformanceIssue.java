package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelConfigurer;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITSdmxCsvWritersPerformanceIssue {

    @Autowired
    private ConverterDelegatorService converterDelegatorService;

    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;

    @Autowired
    private HeaderService headerService;

    private final static String GENERATED_PATH = "sdmxCsvOutput/";

    @BeforeClass
    public static void testSetup() {
        Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
        java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(java.util.logging.Level.OFF);
        for (Handler h : rootLogger.getHandlers()) {
            h.setLevel(java.util.logging.Level.OFF);
        }
    }

    //SDMXCONV-725
    @Test
    public void convertFromExcelWithoutExternalParameters() throws Exception{
        String resultFileName = "02_fromExcel_sdmxCsv.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        // File to be compared against
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMXCONV-725/" + resultFileName;

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/sdmxCsvOutput/SDMXCONV-725/GasQues_2017BDN20180726TESTforSDMXConverter.xlsm");

        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsvOutput/SDMXCONV-725/ESTAT+ENERGY+1.1+DF.xml");

        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/sdmxCsvOutput/SDMXCONV-725/ESTAT+ENERGY+1.1+DF.xml");
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
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");

        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.SDMX_CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
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
    // SDMXCONV-779
    @Test
    public void convertFromExcelWithExternalParameters() throws Exception {
        String resultFileName = "01_fromExcel_sdmxCsv.csv";
        // Generated output file
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        // File to be compared against
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMXCONV-779/" + resultFileName;

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/sdmxCsvOutput/SDMXCONV-779/ENERGY_NTGAS_A_IE_2017_0000_V0003.xlsm");

        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsvOutput/SDMXCONV-779/ESTAT+ENERGY+1.2+DFs.xml");

        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/sdmxCsvOutput/SDMXCONV-779/ESTAT+ENERGY+1.2+DFs.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);

        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        LinkedHashMap<String, ArrayList<String>> mapping;
        File excelConfig = new File("./testfiles/sdmxCsvOutput/SDMXCONV-779/ENERGY_NTGAS_SDMXConverterParameters.xlsx");
        //Read the mapping from the external parameter file
        try(InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        }
        excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        excelInputConfig.setHeader(header);
        excelInputConfig.setConfiguration(ExcelUtils.readExcelConfigFromXlsx(new FileInputStream("./testfiles/sdmxCsvOutput/SDMXCONV-779/ENERGY_NTGAS_SDMXConverterParameters.xlsx"), new FirstFailureExceptionHandler()));
        excelInputConfig.setExternalParamsFileName("externalParamsFileStream");
        excelInputConfig.setConfigInsideExcel(false);
        excelInputConfig.setMappingInsideExcel(false);

        //output configuration
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");

        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL,
                Formats.SDMX_CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
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

    //SDMXCONV-714
    @Test
    public void checkRowInFirstLine3() throws Exception {
        String resultFileName = "3-energy-coal-sdmx-csv.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        InputStream inputStream = new FileInputStream("./testfiles/sdmxCsvOutput/3-Energy-Coal.xlsm");
        //keyFamily
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/sdmxCsvOutput/3-ESTAT+ENERGY+1.1+DF.xml");
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/sdmxCsvOutput/3-ESTAT+ENERGY+1.1+DF.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(true);
        excelInputConfig.setConfigInsideExcel(true);
        //output configuration
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setInternalSdmxCsvAdjustment(true);
        csvOutputConfig.setInternalSdmxCsv(true);
        // make the conversion
        TestConverterUtil.convert(  Formats.EXCEL,
                Formats.iSDMX_CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                excelInputConfig,
                csvOutputConfig,
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
