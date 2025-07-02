package com.intrasoft.sdmx.converter.integration.tests;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import com.intrasoft.sdmx.converter.services.ValidationService;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.junit.Assert;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.estat.struval.ValidationError;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;

import junitx.framework.FileAssert;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITSdmxCsvInput {
    
	@Autowired
    private ConverterDelegatorService converterDelegatorService;
    
	@Autowired
	private StructureService structureService;  
	    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    @Autowired
	private HeaderService headerService; 
	
    private final static String GENERATED_PATH = "sdmxCsv_input/";

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
    
	@Test
    public void convertToGeneric20() throws Exception{
		String resultFileName = "output_generic20FromSdmxCsv.xml"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
       InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/sdmxCsv.csv");
             
       //keyFamily
       DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
       //dataflow
       SdmxBeans beansResult = structureService.readStructuresFromFile("./testfiles/dsds/UOE_NON_FINANCE+ESTAT+0.4.xml");
       DataflowBean df = beansResult.getDataflows().iterator().next();

       SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
       csvInputConfig.setDelimiter(";");
       HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/sdmxCsv_input/header.prop"));
       csvInputConfig.setHeader(header);
       csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
       
     
       //ouputFileName
       File outputFile =  new File (completeResultTargetFileName);
       OutputStream outputStream = new FileOutputStream(outputFile);
     
       // make the conversion
       TestConverterUtil.convert(   Formats.SDMX_CSV,
                            Formats.GENERIC_SDMX,
                            readableDataLocationFactory.getReadableDataLocation(inputStream),
                            outputStream,
                            csvInputConfig,
       		                new SdmxOutputConfig(),
       		                kf,
       		                df, 
       		                converterDelegatorService);
       
       File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
       File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
       FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
       		" is different than what is expected at " + completeResultTestFilesFileName,
       		expectedFile, generatedFile);
    }

	/**
	 * Test to verify that optional attributes are not required.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXRI-1410">SDMXRI-1410</a>
	 * @throws Exception
	 */
	@Test
	public void convertToCompactSdmx() throws Exception{
		String resultFileName = "03-outputFromSdmxCsv.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/03-inputCoal-sdmxCsv.csv");
		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsv_input/03-energy-df-MandatoryAttribute.xml");
		//dataflow
		SdmxBeans beansResult = structureService.readStructuresFromFile("./testfiles/sdmxCsv_input/03-energy-df-MandatoryAttribute.xml");
		DataflowBean df = beansResult.getDataflows().iterator().next();
		SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
		csvInputConfig.setDelimiter(";");
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/sdmxCsv_input/header.prop"));
		csvInputConfig.setHeader(header);
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
		// make the conversion
		TestConverterUtil.convert(   Formats.SDMX_CSV,
				Formats.COMPACT_SDMX,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				csvInputConfig,
				new SdmxOutputConfig(),
				kf,
				df,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	//SDMXRI-1420
	@Test
	public  void convertToCompactSdmxFromSdmxCsvIncludingIdsAndLabels() throws Exception {
		String resultFileName = "compactFromSdmxFromDS_POP_DENSITY_TEST.xml";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/DS_POP_DENSITY_TEST.csv");
		//keyFamily
		DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/sdmxCsv_input/SPC-DF_POP_DENSITY_TEST-1.0-all.xml");
		//dataflow
		SdmxBeans beansResult = structureService.readStructuresFromFile("./testfiles/sdmxCsv_input/SPC-DF_POP_DENSITY_TEST-1.0-all.xml");
		DataflowBean df = beansResult.getDataflows().iterator().next();
		SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
		csvInputConfig.setDelimiter(";");
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/sdmxCsv_input/header.prop"));
		csvInputConfig.setHeader(header);
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setCsvLabel(CsvLabel.BOTH);
		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);
		// make the conversion
		TestConverterUtil.convert(   Formats.SDMX_CSV,
				Formats.COMPACT_SDMX,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				csvInputConfig,
				new SdmxOutputConfig(),
				kf,
				df,
				converterDelegatorService);
		File expectedFile = IntegrationTestsUtils.prettyPrint(new File(completeResultTestFilesFileName));
		File generatedFile = IntegrationTestsUtils.prettyPrint(outputFile);
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is exactly the same as the expected one at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * Test to verify the number of errors found. Because we lose the series dimension validation errors.
	 * @see <a href="https://jira.intrasoft-intl.com/browse/SDMX2-6443">SDMX2-6443</a>
	 * @throws Exception
	 */
	@Test
	public void validateSdmxCsvFromFlr() throws Exception{
		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/SDMX2-6443/1_input_sdmx.csv");
		InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/SDMX2-6443/BIS_JOINT_DEBT_v1.0.xml");
		SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		List<ValidationError> errorList = validationService.validate(inputStream, structureInputStream, csvInputConfig, 50).getErrors();
		assertNotNull(errorList);
		assertEquals(8, errorList.size());
	}

	/**
	 * Test to verify that optional attributes are not required.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXRI-1410">SDMXRI-1410</a>
	 * @throws Exception
	 */
	@Test
	public void validateMandatoryAttributeError() throws Exception{
		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/03-inputCoal-sdmxCsv.csv");
		InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/03-energy-df-MandatoryAttribute.xml");
		SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
		csvInputConfig.setDelimiter(";");
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/sdmxCsv_input/header.prop"));
		csvInputConfig.setHeader(header);
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		List<ValidationError> errorList = validationService.validate(inputStream, structureInputStream, csvInputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
		assertNotNull(errorList);
		assertEquals(2, errorList.size());
	}

	/**
	 * Test to verify that optional attributes are not required.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXRI-1410">SDMXRI-1410</a>
	 * @throws Exception
	 */
	@Test
	public void validateNoNMandatoryAttributeNoError() throws Exception{
		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/03-inputCoal-sdmxCsv.csv");
		InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/03-energy-df-NoMandatoryAttribute.xml");
		SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
		csvInputConfig.setDelimiter(";");
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/sdmxCsv_input/header.prop"));
		csvInputConfig.setHeader(header);
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		List<ValidationError> errorList = validationService.validate(inputStream, structureInputStream, csvInputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
		assertNotNull(errorList);
		assertEquals(0, errorList.size());
	}

	/**
	 * Test to verify that optional attributes are not required.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXRI-1410">SDMXRI-1410</a>
	 * @throws Exception
	 */
	@Test
	public void validateEmptyColAttributeNoError() throws Exception{
		InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/03-inputCoal-emptyCol-sdmxCsv.csv");
		InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/03-energy-df-NoMandatoryAttribute.xml");
		SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
		csvInputConfig.setDelimiter(";");
		HeaderBean header = headerService.parseSdmxHeaderProperties(new FileInputStream("./testfiles/sdmxCsv_input/header.prop"));
		csvInputConfig.setHeader(header);
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		List<ValidationError> errorList = validationService.validate(inputStream, structureInputStream, csvInputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
		assertNotNull(errorList);
		assertEquals(0, errorList.size());
	}

	//SDMXCONV-1505
	@Test
	public  void convertToCsvWithCommaInVersion() throws Exception {
			InputStream inputStream = new FileInputStream("./testfiles/sdmxCsv_input/EBSFATS_T33_A_EL_2021_0000_V0004_comma_in_version.csv");
			InputStream structureInputStream = new FileInputStream("./testfiles/sdmxCsv_input/ESTAT+EBSFATS_T33_A+1.0_20230315.xml");
			SdmxCsvInputConfig csvInputConfig = new SdmxCsvInputConfig();
			csvInputConfig.setDelimiter(";");
			csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
			List<ValidationError> errorList = validationService.validate(inputStream, structureInputStream, csvInputConfig, DEFAULT_LIMIT_ERRORS).getErrors();
			assertNotNull(errorList);
			assertEquals(10, errorList.size());
		Assert.assertTrue(errorList.get(0).getMessage().contains("The DATAFLOW version has invalid string."));
		}
}
