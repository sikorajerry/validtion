package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelConfigurer;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.services.*;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.struval.ValidationError;
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
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
import org.sdmxsource.sdmx.structureretrieval.manager.SdmxSuperBeanRetrievalManagerImpl;
import org.sdmxsource.sdmx.validation.model.MultipleFailureHandlerEngine;
import org.sdmxsource.util.translator.PreferredLanguageTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITExcelInputSDMX30 {
    private final static String GENERATED_PATH = "excel_input_SDMX3.0/";
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
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
     * <h1>Validation test for excel with complex.</h1>
     * @throws Exception
     */
    @Test
    public void testValidationForExcelWithComplex() throws Exception {
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(true);
        excelInputConfig.setInflateRatio(0.00001);
        excelInputConfig.setConfigInsideExcel(true);

        InputStream structureInputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-SDMX_3.0_NA_MAIN+ESTAT+1.2_RI.xml");
        InputStream dataInputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-SDMX-NA_MAIN_T0101_V1.2c_Sample_conceptSeparator.xlsx");
        List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 10).getErrors();
        assertNotNull(errorList);
        assertEquals(1, errorList.size());
        assertTrue(errorList.get(0).getMessage().contains("Data Attribute TABLE_IDENTIFIER is reporting value which is not a valid representation in referenced codelist"));
    }

    /**
     * Measures present and complex values, also subfield separator is present with also concept separator.
     * <p>This test was made to make sure that mixed concepts are working with the new column occurrences
     * and, subfield separator didnt mess up with concept separator.</p>
     *
     * @throws Exception
     * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1514">SDMXCONV-1514</a>
     */
    @Test
    public void testComplexWithSeparatorToSS30() throws Exception {
        String resultFileName = "02-output-1514.xml";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        //Parameters are inside the Excel data file
        FileInputStream inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-SDMX-NA_MAIN_T0101_V1.2c_Sample_conceptSeparator.xlsx");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(true);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setErrorIfEmpty(false);
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-SDMX_3.0_NA_MAIN+ESTAT+1.2_RI.xml");
        InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();
        //outputFileName
        File outputFile = new File(completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);
        ConverterInput converterInput = new ConverterInput(
                Formats.EXCEL,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                excelInputConfig);
        ConverterOutput converterOutput = new ConverterOutput(Formats.STRUCTURE_SPECIFIC_DATA_3_0, outputStream, new SdmxOutputConfig());
        ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
        // make the conversion
        converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
        File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
        File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
    }

    /**
     * Measures present and complex values, also subfield separator is present with also concept separator.
     * <p>This test was made to make sure that mixed concepts are working with the new column occurrences
     * and, subfield separator didnt mess up with concept separator.</p>
     *
     * @throws Exception
     * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1514">SDMXCONV-1514</a>
     */
    @Test
    public void testComplexWithSeparatorToSDMXCSV20() throws Exception {
        String resultFileName = "02-output-1514.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        //Parameters are inside the Excel data file
        FileInputStream inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-SDMX-NA_MAIN_T0101_V1.2c_Sample_conceptSeparator.xlsx");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(true);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setErrorIfEmpty(false);
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "02-SDMX_3.0_NA_MAIN+ESTAT+1.2_RI.xml");
        InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
        SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setSubFieldSeparationChar("#");
        csvOutputConfig.setCsvLabel(CsvLabel.ID);
        csvOutputConfig.setDsd(dataStructure);
        csvOutputConfig.setRetrievalManager(superRetrievalManager);
        List preferredLanguages = Arrays.asList(new Locale("en"));
        List availableLanguages = Arrays.asList(new Locale("en"));
        Locale defaultLanguage = new Locale("en");
        PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
        csvOutputConfig.setTranslator(translator);

        //outputFileName
        File outputFile = new File(completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);


        ConverterInput converterInput = new ConverterInput(
                Formats.EXCEL,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                excelInputConfig);
        ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, csvOutputConfig);
        ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
        // make the conversion
        converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
    }

    /**
     * Measures present and complex values.
     * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1514">SDMXCONV-1514</a>
     * @throws Exception
     */
    @Test
    public void testComplexValuesAtObsLevel() throws Exception {
        String resultFileName = "01-output-1514.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        FileInputStream inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "01-HCSHA_2011NAT_A_AT_2020_0000_V0002.xlsx");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(false);
        excelInputConfig.setConfigInsideExcel(false);
        excelInputConfig.setErrorIfEmpty(false);
        File excelConfig = new File(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "01-External_parameters_SHA_2022.xlsx");
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        LinkedHashMap<String, ArrayList<String>> mapping;
        //Read the mapping from the external parameter file
        try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        }
        excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        //The names of the param sheets taken from the mapping
        List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
        List<ExcelConfiguration> allExcelParameters = null;
        //Read the parameter sheets with the help of the mapping from the external file
        try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
            assertNotNull(allExcelParameters);
        }
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "01-SDMX_3.0_ESTAT+HCSHA_2011NAT_A+4.2_obs_status.xml");
        InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
        SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();
        excelInputConfig.setConfigInsideExcel(false);
        excelInputConfig.setConfiguration(allExcelParameters);
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setSubFieldSeparationChar("#");
        csvOutputConfig.setCsvLabel(CsvLabel.ID);
        csvOutputConfig.setDsd(dataStructure);
        csvOutputConfig.setRetrievalManager(superRetrievalManager);
        List preferredLanguages = Arrays.asList(new Locale("en"));
        List availableLanguages = Arrays.asList(new Locale("en"));
        Locale defaultLanguage = new Locale("en");
        PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
        csvOutputConfig.setTranslator(translator);

        //outputFileName
        File outputFile = new File(completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);


        ConverterInput converterInput = new ConverterInput(
                Formats.EXCEL,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                excelInputConfig);
        ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, csvOutputConfig);
        ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
        // make the conversion
        converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
    }


    /**
     * Verify that xml passes
     * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1493">SDMXCONV-1493</a>
     * @throws Exception
     */
    @Test
    public void testInvalidXml() throws Exception {
        String resultFileName = "InvalidXml-output.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        FileInputStream inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "EDUCAT_ENRL_A_NO_2020_210924_v2.xlsx");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(false);
        excelInputConfig.setConfigInsideExcel(false);
        File excelConfig = new File(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "ENRL_Parameters_2020.xlsx");
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        LinkedHashMap<String, ArrayList<String>> mapping;
        //Read the mapping from the external parameter file
        try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, new FirstFailureExceptionHandler());
        }
        excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        //The names of the param sheets taken from the mapping
        List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
        List<ExcelConfiguration> allExcelParameters = null;
        //Read the parameter sheets with the help of the mapping from the external file
        try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, new FirstFailureExceptionHandler());
            assertNotNull(allExcelParameters);
        }
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMX_3.0_UISEDUCAT_ENRL_A1.0_with_XK.xml");
        InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
        SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();
        excelInputConfig.setConfigInsideExcel(false);
        excelInputConfig.setConfiguration(allExcelParameters);
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setSubFieldSeparationChar("#");
        csvOutputConfig.setCsvLabel(CsvLabel.ID);
        csvOutputConfig.setDsd(dataStructure);
        csvOutputConfig.setRetrievalManager(superRetrievalManager);

        //outputFileName
        File outputFile = new File(completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);


        ConverterInput converterInput = new ConverterInput(
                Formats.EXCEL,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                excelInputConfig);
        ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, csvOutputConfig);
        ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
        // make the conversion
        converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
    }

    @Test
    public void testExcelWithInvalidXml() throws Exception{
        InputStream inputFile = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "EDUCAT_ENRL_A_NO_2020_210924_v2.xlsx");
        InputStream dsd = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMX_3.0_UISEDUCAT_ENRL_A1.0_with_XK.xml");
        File excelConfig = new File(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "ENRL_Parameters_2020.xlsx");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setMappingInsideExcel(true);
        LinkedHashMap<String, ArrayList<String>> mapping;
        ExcelConfigurer excelConfigurer = new ExcelConfigurer(excelInputConfig);
        MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(50 + 1);
        try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            mapping = excelConfigurer.readMappingSheets(excelExternalParamIs, exceptionHandler);
        }
        excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
        //The names of the param sheets taken from the mapping
        List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);
        List<ExcelConfiguration> allExcelParameters = null;
        //Read the parameter sheets with the help of the mapping from the external file
        try (InputStream excelExternalParamIs = new FileInputStream(excelConfig)) {
            allExcelParameters = excelConfigurer.readExcelParameters(excelExternalParamIs, paramSheetNames, exceptionHandler);
        }

        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setConfiguration(allExcelParameters);

        List<ValidationError> errorList = validationService.validate(inputFile, dsd, excelInputConfig, 5).getErrors();
        Assert.assertTrue(errorList.size() == 4);
    }

    /**
     * SDMXCONV-1564
     * Test file created for recognition only one complex value of component as complex.
     */
    @Test
    public void testValidationForExcelWithOnlyOneComplexValue() throws Exception {
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(true);
        excelInputConfig.setInflateRatio(0.00001);
        excelInputConfig.setConfigInsideExcel(true);

        InputStream structureInputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMXCONV-1564/02-SDMX_3.0_NA_MAIN+ESTAT+1.2_RI.xml");
        InputStream dataInputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMXCONV-1564/02-SDMX-NA_MAIN_T0101_V1.2c_Sample_conceptSeparator.xlsx");
        List<ValidationError> errorList = validationService.validate(dataInputStream, structureInputStream, excelInputConfig, 10).getErrors();
        assertNotNull(errorList);
        assertEquals(0, errorList.size());
    }

    @Test
    public void testOnlyOneComplexValueToSDMXCSV20() throws Exception {
        String resultFileName = "output_only_one_complex_value.csv";
        String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
        Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
        String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMXCONV-1564/" +resultFileName;
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        //Parameters are inside the Excel data file
        FileInputStream inputStream = new FileInputStream(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMXCONV-1564/02-SDMX-NA_MAIN_T0101_V1.2c_Sample_conceptSeparator.xlsx");
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setHeader(header);
        excelInputConfig.setMappingInsideExcel(true);
        excelInputConfig.setConfigInsideExcel(true);
        excelInputConfig.setErrorIfEmpty(false);
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile(IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + "SDMXCONV-1564/02-SDMX_3.0_NA_MAIN+ESTAT+1.2_RI.xml");
        InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
        SdmxSuperBeanRetrievalManagerImpl superRetrievalManager = new SdmxSuperBeanRetrievalManagerImpl(retrievalManager);
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
        Set<DataStructureBean> dataStructures = sdmxBeans.getDataStructures(dataflow.getDataStructureRef());
        DataStructureBean dataStructure = dataStructures.iterator().next();
        SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setSubFieldSeparationChar("#");
        csvOutputConfig.setCsvLabel(CsvLabel.ID);
        csvOutputConfig.setDsd(dataStructure);
        csvOutputConfig.setRetrievalManager(superRetrievalManager);
        List preferredLanguages = Arrays.asList(new Locale("en"));
        List availableLanguages = Arrays.asList(new Locale("en"));
        Locale defaultLanguage = new Locale("en");
        PreferredLanguageTranslator translator = new PreferredLanguageTranslator(preferredLanguages, availableLanguages, defaultLanguage);
        csvOutputConfig.setTranslator(translator);

        //outputFileName
        File outputFile = new File(completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);


        ConverterInput converterInput = new ConverterInput(
                Formats.EXCEL,
                readableDataLocationFactory.getReadableDataLocation(inputStream),
                excelInputConfig);
        ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV_2_0, outputStream, csvOutputConfig);
        ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, retrievalManager);
        // make the conversion
        converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
        File expectedFile = new File(completeResultTestFilesFileName);
        File generatedFile = outputFile;
        FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + " is different than what is expected at " + completeResultTestFilesFileName, expectedFile, generatedFile);
    }


}
