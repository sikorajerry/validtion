package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import junitx.framework.FileAssert;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
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
 * Test converting Structure Specific 3.0 to SDMX_CSV_2_0 and check supporting of Semantic Versioning
 */

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITStructureSpecific_3_0SemanticVersioning {

    private final static String GENERATED_PATH = "structure_specific_3_0_input/";
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
    @Autowired
    private StructureService structureService;
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
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
     * Convert to SDMX_CSV_2_0. Checking that Converter/Struval can find the version
     * from DATAFLOW/DATASET that input file provides
     *
     * @throws Exception
     */
    @Test
    public void convertSS3_0ToSdmx_CsvUsingDataflow() throws Exception {
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            String resultFileName = "15_output_SdmxCsv_MultipleDataflows.csv";
            String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
            String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
            Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
            inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/MULTIPLE_DATAFLOWS_input.xml");
            // dataflow
            SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/MULTIPLE_DATAFLOWS_AND_MEASURES_DSD.xml");
            InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
            SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
            DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
            Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
            //Set only one datastructure, first priority
            DataStructureBean dataStructure = dataStructures.iterator().next();

            SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
            outputConfig.setCsvLabel(CsvLabel.ID);
            outputConfig.setDelimiter(";");
            outputConfig.setSubFieldSeparationChar(";");
            outputConfig.setDsd(dataStructure);
            outputConfig.setRetrievalManager(superRetrievalManager);
            List preferredLanguages = Arrays.asList(new Locale("en"));
            List availableLanguages = Arrays.asList(new Locale("en"));
            Locale defaultLanguage = new Locale("en");
            PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
            outputConfig.setTranslator(translator);
            // ouputFileName
            File outputFile = new File(completeResultTargetFileName);
            outputStream = new FileOutputStream(outputFile);
            ConverterInput converterInput = new ConverterInput(
                    Formats.STRUCTURE_SPECIFIC_DATA_3_0,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    new SdmxInputConfig());
            ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, outputConfig);
            ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
            // make the conversion
            converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
            File expectedFile = new File(completeResultTestFilesFileName);
            File generatedFile = outputFile;
            FileAssert.assertEquals("the generated file at "
                            + completeResultTargetFileName +
                            " is different than what is expected at "
                            + completeResultTestFilesFileName, expectedFile, generatedFile);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }


    @Test
    public void convertSS3_0ToSdmx_CsvUsingHeaderbean() throws Exception {
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            String resultFileName = "15_output_SdmxCsv_MultipleDataflows.csv";
            String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
            String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
            Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
            inputStream = new FileInputStream("./testfiles/structure_specific_3_0_input/MULTIPLE_MEASURES_HeaderBean_input.xml");
            // dataflow
            SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/structure_specific_3_0_input/MULTIPLE_DATAFLOWS_AND_MEASURES_DSD.xml");
            InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
            SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
            SdmxCsvOutputConfig outputConfig = new SdmxCsvOutputConfig();
            outputConfig.setCsvLabel(CsvLabel.ID);
            outputConfig.setDelimiter(";");
            outputConfig.setSubFieldSeparationChar(";");
            outputConfig.setRetrievalManager(superRetrievalManager);
            List preferredLanguages = Arrays.asList(new Locale("en"));
            List availableLanguages = Arrays.asList(new Locale("en"));
            Locale defaultLanguage = new Locale("en");
            PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
            outputConfig.setTranslator(translator);

            // ouputFileName
            File outputFile = new File(completeResultTargetFileName);
            outputStream = new FileOutputStream(outputFile);
            ConverterInput converterInput = new ConverterInput(
                    Formats.STRUCTURE_SPECIFIC_DATA_3_0,
                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                    new SdmxInputConfig());
            ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, outputConfig);
            ConverterStructure converterStructure = new ConverterStructure(null, null, retrievalManager);
            // make the conversion
            converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
            File expectedFile = new File(completeResultTestFilesFileName);
            File generatedFile = outputFile;
            FileAssert.assertEquals("the generated file at "
                    + completeResultTargetFileName +
                    " is different than what is expected at "
                    + completeResultTestFilesFileName, expectedFile, generatedFile);
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }
    }

}
