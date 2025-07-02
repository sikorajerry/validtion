package com.intrasoft.sdmx.converter.integration.tests;


import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.ValidationService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;

import org.sdmxsource.util.translator.PreferredLanguageTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITSdmxCsv20 {

    @Autowired
    private ConverterDelegatorService converterDelegatorService;

    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;

    @Autowired
    private HeaderService headerService;

    private final static String GENERATED_PATH = "Sdmx_Csv_2_input/";

    @Autowired
    private ValidationService validationService;

    private static final int DEFAULT_LIMIT_ERRORS = 10;

    @BeforeClass
    public static void testSetup() {
        Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
        java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(java.util.logging.Level.OFF);
        for (Handler h : rootLogger.getHandlers()) {
            h.setLevel(java.util.logging.Level.OFF);
        }
    }

    /**
     * Tests to verify that all complex values and measures were converted correctly
     *
     * @throws Exception
     */
    @Test
    public void convertToSdmxCsv2() throws Exception {
        String resultFileName = "sdmx_csv_2.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/Sdmx_Csv_2_input/multmeasures.csv");

        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/Sdmx_Csv_2_input/TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml");

        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/Sdmx_Csv_2_input/TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml");
        DataflowBean df = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(df.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();

        HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setHeader(header);
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setSubFieldSeparationChar("#");
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

        SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
        outputConfig.setDelimiter(";");
        outputConfig.setSubFieldSeparationChar("#");
        outputConfig.setDsd(dataStructure);
        List preferredLanguages = Arrays.asList(new Locale("en"));
        List availableLanguages = Arrays.asList(new Locale("en"));
        Locale defaultLanguage = new Locale("en");
        PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
        outputConfig.setTranslator(translator);


        File outputFile =  new File (completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);

        // make the conversion
        TestConverterUtil.convert(Formats.CSV,
                Formats.SDMX_CSV_2_0,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                csvInputConfig,
                outputConfig,
                kf,
                df,
                converterDelegatorService);
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName,
                expectedFile, generatedFile);
    }

    @Test
    public void convertToCsv() throws Exception {
        String resultFileName = "multmeasures.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/Sdmx_Csv_2_input/sdmx_csv_2.csv");

        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/Sdmx_Csv_2_input/TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml");

        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/Sdmx_Csv_2_input/TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml");
        DataflowBean df = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(df.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();

        HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setHeader(header);
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setSubFieldSeparationChar("#");
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

        MultiLevelCsvOutputConfig outputConfig = new MultiLevelCsvOutputConfig();
        outputConfig.setDelimiter(";");
        outputConfig.setSubFieldSeparationChar("#");
        outputConfig.setDsd(dataStructure);
        outputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);


        File outputFile =  new File (completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);

        // make the conversion
        TestConverterUtil.convert(Formats.SDMX_CSV_2_0,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                csvInputConfig,
                outputConfig,
                kf,
                df,
                converterDelegatorService);
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName,
                expectedFile, generatedFile);
    }

    @Test
    public void convertToCsvCheckingSeparator() throws Exception {
        String resultFileName = "output_Csv.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

        InputStream inputStream = new FileInputStream("./testfiles/Sdmx_Csv_2_input/Input_SDMX_CSV_2_0.csv");

        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/Sdmx_Csv_2_input/SDMX_3.0_ESTAT+DEM_DEMOBAL+1.1.xml");

        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/Sdmx_Csv_2_input/SDMX_3.0_ESTAT+DEM_DEMOBAL+1.1.xml");
        DataflowBean df = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(df.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();

        HeaderBean header = new HeaderBeanImpl("IREF123", "ZZ9");
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setHeader(header);
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setSubFieldSeparationChar(",");
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

        MultiLevelCsvOutputConfig outputConfig = new MultiLevelCsvOutputConfig();
        outputConfig.setDelimiter(";");
        outputConfig.setSubFieldSeparationChar(",");
        outputConfig.setDsd(dataStructure);
        outputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);


        File outputFile =  new File (completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);

        // make the conversion
        TestConverterUtil.convert(Formats.SDMX_CSV_2_0,
                Formats.CSV,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                outputStream,
                csvInputConfig,
                outputConfig,
                kf,
                df,
                converterDelegatorService);
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                        " is different than what is expected at " + completeResultTestFilesFileName,
                expectedFile, generatedFile);
    }

}



