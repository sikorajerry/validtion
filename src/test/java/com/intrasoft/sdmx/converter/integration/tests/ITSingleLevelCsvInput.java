package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.CsvService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.constants.BASE_DATA_FORMAT;
import org.sdmxsource.sdmx.api.constants.DATASET_ACTION;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.writer.CompactDataWriterEngine;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetHeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;


@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITSingleLevelCsvInput {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
	@Autowired
	private StructureService structureService; 
	
	@Autowired
	private HeaderService headerService;

	@Autowired
	private CsvService csvService;
	
	@Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
	
	@Rule
	public final ExpectedException expectedException = ExpectedException.none();
	
    private final static String GENERATED_PATH = "singleCsv_input/";

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
	//Test the CompactDataWriterEngine for StructureSpecific with no Time dimension
	public void testWriterToStuctureSpecific21WithNoTimePeriod() throws Exception {
		String resultFileName = "writer_test_structureSpecificWithNoTimePeriodFromCsv.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/SDMX21_ESTAT+EGR_CORE_ENT+1.0.xml"));
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(true);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		DataWriterEngine dataWriterEngine = new CompactDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE, 
				BASE_DATA_FORMAT.STRUCTURE_SPECIFIC_DATA, outputStream);
		
		dataWriterEngine.writeHeader(header);

		DatasetHeaderBean datasetHeader = new DatasetHeaderBeanImpl("SDMX21_EGR_CORE_ENT", 
				DATASET_ACTION.INFORMATION, 
				new DatasetStructureReferenceBeanImpl("3c16c75d-4d96-4166-ba52-8a8dd99e4c8d",
						new StructureReferenceBeanImpl("ESTAT", "SDMX21_EGR_CORE_ENT", "1.0", SDMX_STRUCTURE_TYPE.DSD), 
						null, null, "ENT_NAME"));

		dataWriterEngine.startDataset(null, dataStructure, datasetHeader);
		
		dataWriterEngine.startSeries();
		
		dataWriterEngine.writeSeriesKeyValue("ENT_FRAME_RYEAR", "2015");
		dataWriterEngine.writeSeriesKeyValue("ENT_COUNTRY_CODE", "EE");
		dataWriterEngine.writeSeriesKeyValue("ENT_NSA_ID", "10000330");

		dataWriterEngine.writeObservation("ENTEPRISE 330" , "4399");

		dataWriterEngine.writeAttributeValue("ENT_TURNOV_CUR_CODE", "3300");
		dataWriterEngine.writeAttributeValue("ENT_STA_CODE", "A");
		dataWriterEngine.writeAttributeValue("ENT_END_DATE", "");
		dataWriterEngine.writeAttributeValue("ENT_START_DATE", "18/12/2003");
		dataWriterEngine.writeAttributeValue("ENT_PERS_EMPL_FTE", "330");
		dataWriterEngine.writeAttributeValue("ENT_INST_CODE", "");
		dataWriterEngine.writeAttributeValue("ENT_TURNOV", "330");
		
		dataWriterEngine.close();
	}

	@Test(expected = SdmxSemmanticException.class)
	public void convertToCross20DSD21() throws Exception{
		String resultFileName = "cross20Dsd21WithNoTimePeriodFromCsv.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/3_EGR_ENT_A_EE_2015_0000_V0003_15112016_NSA_BR.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/SDMX21_ESTAT+EGR_CORE_ENT+1.0.xml"));
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);
		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.CROSS_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxOutputConfig(),
                            dataStructure,
                            null, 
       		                converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
			" is different than what is expected at " + completeResultTestFilesFileName,
			expectedFile, generatedFile);
	}
	
	@Test
	public void convertToStructureSpecificNoTimeDimension() throws Exception{
		String resultFileName = "4_structureSpecific_EGR_ENT_WithNoTimePeriodFromCsv.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/3_EGR_ENT_A_EE_2015_0000_V0003_15112016_NSA_BR.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/SDMX21_ESTAT+EGR_CORE_ENT+1.0.xml"));
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.STRUCTURE_SPECIFIC_DATA_2_1,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxOutputConfig(),
                            dataStructure,
                            null, 
       		                converterDelegatorService);
		
	   File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
	   File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
	   FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
		" is different than what is expected at " + completeResultTestFilesFileName,
		expectedFile, generatedFile);
	}
	
	@Test
	public void convertToGeneric21NoTimeDimension() throws Exception{
		String resultFileName = "3_generic21WithNoTimePeriodFromCsv.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/3_EGR_ENT_A_EE_2015_0000_V0003_15112016_NSA_BR.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/SDMX21_ESTAT+EGR_CORE_ENT+1.0.xml"));
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.GENERIC_DATA_2_1,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxOutputConfig(),
                            dataStructure,
                            null, 
       		                converterDelegatorService);
	}
	
	@Test
	public void convertToGenericNoTranscodingNoColumnMapping() throws Exception{
		String resultFileName = "1_output_generic20.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/1_input_csv.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml"));
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.GENERIC_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxOutputConfig(),
                            dataStructure,
                            null, 
       		                converterDelegatorService);
		
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       		" is different than what is expected at " + completeResultTestFilesFileName,
	       		expectedFile, generatedFile);
	}
	
	@Test
	public void convertToGenericCsvWithDisregardHeader() throws Exception{
		String resultFileName = "1_output_generic20_UOE_NON_FINANCE+ESTAT+0.4_diregardHeader.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/1_input_csv.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml"));
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.DISREGARD_HEADER);
		inputConfig.setIsEscapeCSV(false);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.GENERIC_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxOutputConfig(),
                            dataStructure,
                            null, 
       		                converterDelegatorService);
		
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       		" is different than what is expected at " + completeResultTestFilesFileName,
	       		expectedFile, generatedFile);
	}

    @Test
    public void convertToGenericWithColumnMappingAndConcatColumns() throws Exception{
		String resultFileName = "2_output_genericFromCsvWithMappingWithConcatColumns.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        //input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/2_input_csvWithFirstDimensionSplitInTwoColumns.csv");
        DataStructureBean kf = structureService.readFirstDataStructure(new File("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml"));

        CsvInputConfig inputConfig = new CsvInputConfig();
        HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
        inputConfig.setHeader(header);
        inputConfig.setDelimiter(";");
        inputConfig.setInputOrdered(true);
        inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);
        inputConfig.setIsEscapeCSV(false);

        //set the mapping
        InputStream mappingIs = new FileInputStream("./testfiles/singleCsv_input/2_mappingWithFirstDimConcatFromTwoColumns.xml");
        Map<String, CsvInColumnMapping> mappingMap = csvService.buildInputDimensionLevelMapping(mappingIs,1);
        inputConfig.setMapping(mappingMap);
        inputConfig.setLevelNumber("1");

        //ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        FileOutputStream outputStream = new FileOutputStream(outputFile);

        TestConverterUtil.convert(  Formats.CSV,
                                    Formats.GENERIC_SDMX,
                                    readableDataLocationFactory.getReadableDataLocation(inputStream),
                                    outputStream,
                                    inputConfig,
                                    new SdmxOutputConfig(),
                                    kf,
                                    null, 
               		                converterDelegatorService );
        
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       		" is different than what is expected at " + completeResultTestFilesFileName,
	       		expectedFile, generatedFile);
    }


	@Test
	public void convertToSdmxCsvNoTranscodingNoColumnMapping() throws Exception{
		String resultFileName = "1_output_sdmxCsv_UOE_NON_FINANCE+ESTAT+0.4.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/1_input_csv.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml"));
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.SDMX_CSV,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxCsvOutputConfig(),
                            dataStructure,
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
	public void convert5ToCross20() throws Exception{
		
		String resultFileName = "5_generated_cross20WithTimePeriodFromCsv.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/5_input_csv_EDUCATION_ESTAT.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/dsds/EDUCATION+ESTAT+1.0.xml"));
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.CROSS_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxOutputConfig(),
                            dataStructure,
                            null, 
       		                converterDelegatorService);
	}

	/**
	 * Having input transcoding from “null” to a default value.
	 * (see SDMXCONV-1164)
	 * @throws Exception
	 */
	@Test
	public void convert6ToCompact() throws Exception{
		String resultFileName = "6_output-convertWithTranscoding.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME +  "transcoding/"+ resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//input
		InputStream inputStream = new FileInputStream("./testfiles/transcoding/input.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/transcoding/dsd.xml"));

		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);
		inputConfig.setTranscoding(structureService.readStructureSetMap(new FileInputStream("./testfiles/transcoding/transcoding.xml")));

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);

		TestConverterUtil.convert(  Formats.CSV,
				Formats.COMPACT_SDMX,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				inputConfig,
				new SdmxOutputConfig(),
				dataStructure,
				null,
				converterDelegatorService);

		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * Having input csv with complex and apply transcoding.
	 * (see SDMXCONV-1513)
	 * @throws Exception
	 */
	@Test
	public void convertToSS30WithTranscodingInput() throws Exception {
		String resultFileName = "01-output-convertSS30WithTranscoding.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME +  "transcoding/"+ resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/transcoding/01-SingleCSV3.0.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(
				new File("./testfiles/transcoding/01-TEST_XL_COMPL+STS+2.0-DF-DSD-full.xml"));

		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setSubFieldSeparationChar("#");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);
		inputConfig.setMapCrossXMeasure(true);
		inputConfig.setStructureSchemaVersion(SDMX_SCHEMA.VERSION_THREE);
		inputConfig.setTranscoding(structureService.readStructureSetMap(new FileInputStream("./testfiles/transcoding/01-transcoding.xml")));

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);

		ConverterInput converterInput = new ConverterInput(
				Formats.CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				inputConfig);
		ConverterOutput converterOutput = new ConverterOutput(Formats.STRUCTURE_SPECIFIC_DATA_3_0, outputStream, new SdmxOutputConfig());
		ConverterStructure converterStructure = new ConverterStructure(dataStructure, null, null);
		// make the conversion
		converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	@Test
	public void testCrossSectionalMeasures() throws Exception {
		
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/USE_PESTICI+ESTAT+1.1.xml");
        String resultFileName = "6_generated_CrossWithCrossSectionalMeasures.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/useXsMeasures.csv");
		
		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(",");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		inputConfig.setIsEscapeCSV(false);
		inputConfig.setMapCrossXMeasure(true);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		
		TestConverterUtil.convert(  Formats.CSV,
                            Formats.CROSS_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            inputConfig,
                            new SdmxOutputConfig(),
                            dataStructure,
                            null, 
       		                converterDelegatorService);
	}

	@Test
	public void convertCsvWithoutHeaderToCross() throws Exception{

		String resultFileName = "convertCsvWithoutHeaderToCross.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/SSTSCONS_PROD_M_TUTORIAL+2.0+ESTAT_data 1.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(new File("./testfiles/singleCsv_input/SSTSCONS_PROD_M_TUTORIAL 5.xml"));

		CsvInputConfig inputConfig = new CsvInputConfig();
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/singleCsv_input/header.prop"));
		inputConfig.setHeader(header);
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);
		ConverterInput converterInput = new ConverterInput(Formats.CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream), inputConfig);
		ConverterOutput converterOutput = new ConverterOutput(Formats.CROSS_SDMX, outputStream,
				new SdmxOutputConfig());
		ConverterStructure converterStructure = new ConverterStructure(dataStructure, null);
		Exception exception = assertThrows(Exception.class, () -> {
			converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
		});
		assertEquals(SdmxSemmanticException.class, exception.getClass());
		assertEquals("Unsupported The Structure is not CrossSectional compatible, and the CROSS_SDMX format is not supported", exception.getMessage());
	}

	@Test
	public void convertToSdmxCsvWithGroupObs() throws Exception {
		String resultFileName = "convertToSdmxCsvWithGroupObs";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		//input
		InputStream inputStream = new FileInputStream("./testfiles/singleCsv_input/SSTSCONS_PROD_M_TUTORIAL+2.0+ESTAT_data 1.csv");
		DataStructureBean dataStructure = structureService.readFirstDataStructure(
				new File("./testfiles/singleCsv_input/SSTSCONS_PROD_M_TUTORIAL 5.xml"));

		CsvInputConfig inputConfig = new CsvInputConfig();
		inputConfig.setDelimiter(";");
		inputConfig.setInputColumnHeader(CsvInputColumnHeader.NO_HEADER);

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		FileOutputStream outputStream = new FileOutputStream(outputFile);

		ConverterInput converterInput = new ConverterInput(
				Formats.CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				inputConfig);
		SdmxCsvOutputConfig sdmxCsvOutputConfig = new SdmxCsvOutputConfig();
		sdmxCsvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
		sdmxCsvOutputConfig.setDelimiter(";");
		ConverterOutput converterOutput = new ConverterOutput(Formats.SDMX_CSV, outputStream, sdmxCsvOutputConfig);

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/singleCsv_input/SSTSCONS_PROD_M_TUTORIAL 5.xml");
		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/singleCsv_input/SSTSCONS_PROD_M_TUTORIAL 5.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow, null);
		readHeader(sdmxCsvOutputConfig,
				inputConfig,
				Formats.COMPACT_SDMX,
				Formats.SDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(new FileInputStream("./testfiles/singleCsv_input/SSTSCONS_PROD_M_TUTORIAL 5.xml")),
				outputStream,
				kf,
				dataflow, "true");

		// make the conversion
		converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
		File expectedFile = new File(completeResultTargetFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTargetFileName,
				expectedFile, generatedFile);
	}

	private HeaderBean readHeader(OutputConfig outputConfig,
			InputConfig inputConfig,
			Formats sourceFormat,
			Formats destinationFormat,
			ReadableDataLocation readableDataLocation,
			OutputStream outputStream,
			DataStructureBean dsd,
			DataflowBean df,
			String sdmxHeaderToSdmxCsv) throws IOException, WriteHeaderException {
		HeaderBean headerBean;
		ConverterOutput converterOutput = new ConverterOutput(destinationFormat, outputStream, outputConfig);
		ConverterStructure converterStructure = new ConverterStructure(dsd, df);

		try (InputStream inputStream = readableDataLocation.getInputStream();
				ReadableDataLocation reader = readableDataLocationFactory.getReadableDataLocation(inputStream)) {
			ConverterInput converterNewInput = new ConverterInput(sourceFormat, reader, inputConfig);
			headerBean = headerService.getHeaderBean(converterNewInput, converterOutput, converterStructure, sdmxHeaderToSdmxCsv, null);
		}
		return headerBean;
	}
}
