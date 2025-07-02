package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import junitx.framework.FileAssert;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
import org.sdmxsource.sdmx.structureretrieval.manager.SdmxSuperBeanRetrievalManagerImpl;
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

/**
 * Test converting old files to SDMX_CSV_2.0.
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1523">SDMXCONV-1523</a>
 */

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITSdmx_30_Output {
    private final static String GENERATED_PATH = "sdmx30_output/";
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
    @Autowired
    private StructureService structureService;
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    @Autowired
    private HeaderService headerService;

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
     * Input with multiple measures. Convert to iSdmxCsv 20
     *
     * @throws Exception
     */
    @Test
    public void convertSS30ToISdmxCsv20() throws Exception {
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            String resultFileName = "01-output.csv";
            String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
            String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
            Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
            inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "01-Inputfile_mult_measures_test.xml");
            // dataflow
            SdmxBeans sdmxBeans = structureService.readStructuresFromFile(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "01-DF_mult_measures_test.xml");
            InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
            SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
            DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
            Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
            DataStructureBean dataStructure = dataStructures.iterator().next();
            SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
            csvOutputConfig.setDelimiter(";");
            csvOutputConfig.setInternalSdmxCsvAdjustment(true);
            csvOutputConfig.setSubFieldSeparationChar(",");
            List preferredLanguages = Arrays.asList(new Locale("en"));
            List availableLanguages = Arrays.asList(new Locale("en"));
            Locale defaultLanguage = new Locale("en");
            PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
            csvOutputConfig.setTranslator(translator);
            //This needs to be added to be able to fix the header row
            csvOutputConfig.setRetrievalManager(superRetrievalManager);
            csvOutputConfig.setDsd(dataStructure);
            File outputFile = new File(completeResultTargetFileName); // output FileName
            outputStream = new FileOutputStream(outputFile);
            ConverterInput converterInput = new ConverterInput(
                    Formats.STRUCTURE_SPECIFIC_DATA_3_0,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    new SdmxInputConfig());
            ConverterOutput converterOutput = new ConverterOutput(Formats.iSDMX_CSV_2_0, outputStream, csvOutputConfig);
            ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
            // make the conversion
            converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
            File expectedFile = new File(completeResultTestFilesFileName);
            File generatedFile = outputFile;
            FileAssert.assertEquals("the generated file " + completeResultTargetFileName +
                                             " is different than what is expected " + completeResultTestFilesFileName,
                                             expectedFile,
                                             generatedFile);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }

    /**
     * Input Structure Specific 2.1. Convert to iSdmxCsv 20
     *
     * @throws Exception
     */
    @Test
    public void convertSS21ToISdmxCsv20() throws Exception {
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            String resultFileName = "02-output.csv";
            String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
            String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
            Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
            inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-Input_Demobal_StructSpec.xml");
            SdmxBeans sdmxBeans = structureService.readStructuresFromFile(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-SDMX_3.0_ESTAT+DEM_DEMOBAL+1.0.xml");
            InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
            SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
            DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
            Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
            DataStructureBean dataStructure = dataStructures.iterator().next();
            SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
            csvOutputConfig.setDelimiter(";");
            csvOutputConfig.setInternalSdmxCsvAdjustment(true);
            csvOutputConfig.setSubFieldSeparationChar(",");
            List preferredLanguages = Arrays.asList(new Locale("en"));
            List availableLanguages = Arrays.asList(new Locale("en"));
            Locale defaultLanguage = new Locale("en");
            PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
            csvOutputConfig.setTranslator(translator);
            //This needs to be added to be able to fix the header row
            csvOutputConfig.setRetrievalManager(superRetrievalManager);
            csvOutputConfig.setDsd(dataStructure);
            File outputFile = new File(completeResultTargetFileName); // output FileName
            outputStream = new FileOutputStream(outputFile);
            ConverterInput converterInput = new ConverterInput(
                    Formats.STRUCTURE_SPECIFIC_DATA_2_1,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    new SdmxInputConfig());
            ConverterOutput converterOutput = new ConverterOutput(Formats.iSDMX_CSV_2_0, outputStream, csvOutputConfig);
            ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
            // make the conversion
            converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
            File expectedFile = new File(completeResultTestFilesFileName);
            File generatedFile = outputFile;
            FileAssert.assertEquals("the generated file " + completeResultTargetFileName +
                            " is different than what is expected " + completeResultTestFilesFileName,
                    expectedFile,
                    generatedFile);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }

    /**
     * SDMXCONV-1527
     * StructureUsage should be used when referring to a dataflow only, not for dataStructure.
     * @throws Exception
     */
    @Test
    public void convertCsvToISS30() throws Exception {
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            String resultFileName = "03-output.xml";
            String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
            String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
            Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
            inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH +"03-single-test.csv");
            // dataflow
            DataStructureBean dataStructureBean = structureService.readFirstDataStructure(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH +"03-DSD_TEST_1.0.xml");
            CsvInputConfig inputConfig = new CsvInputConfig();
            HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
            inputConfig.setHeader(header);
            inputConfig.setDelimiter(";");
            inputConfig.setSubFieldSeparationChar("#");
            inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
            inputConfig.setIsEscapeCSV(false);
            inputConfig.setMapCrossXMeasure(true);
            inputConfig.setStructureSchemaVersion(SDMX_SCHEMA.VERSION_THREE);
            // ouputFileName
            File outputFile = new File(completeResultTargetFileName);
            outputStream = new FileOutputStream(outputFile);
            ConverterInput converterInput = new ConverterInput(
                    Formats.CSV,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    inputConfig);
            ConverterOutput converterOutput = new ConverterOutput(Formats.STRUCTURE_SPECIFIC_DATA_3_0, outputStream, new SdmxOutputConfig());
            ConverterStructure converterStructure = new ConverterStructure(dataStructureBean, null, null);
            // make the conversion
            converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
            File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
            File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
            FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
                            " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }
}
