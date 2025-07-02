package com.intrasoft.sdmx.converter.io.data.excel;

import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.sdmxsource.util.excel.InvalidExcelParamsException;
import org.estat.sdmxsource.util.excel.ValueShouldBeSkippedException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static org.junit.Assert.*;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/test-spring-context.xml" })
public class ExcelDataReaderEngineTestParameters {

    @Qualifier("readableDataLocationFactory")
    @Autowired
    private ReadableDataLocationFactory dataLocationFactory;

    @Autowired
    private StructureParsingManager parsingManager;
    
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

    private ExcelDataReaderEngine classUnderTest = null;

    private ReadableDataLocation excelLocation = null;
    private File excelInputFile = null;
    private LinkedHashMap<String, ArrayList<String>> mapping = null;
    private List<String> paramSheetNames = null;
    private List<ExcelConfiguration> allExcelParameters = null;
	private ExcelConfigurer excelConfigurer;
    private Observation obs = null;
    private Keyable key = null;
    private ExcelInputConfigImpl excelInputConfig = null;

    private DataStructureBean prepareDataStructure(String filename) {
	// prepare the dsd
	ReadableDataLocation dsdLocation = dataLocationFactory.getReadableDataLocation(filename);
	StructureWorkspace parseStructures = parsingManager.parseStructures(dsdLocation);
	SdmxBeans structureBeans = parseStructures.getStructureBeans(false);
	return structureBeans.getDataStructures().iterator().next();
    }

    // Method for setting up and initialize the excel environment
    public void prepareTest(String fileName, String dsdFilename)
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	excelLocation = dataLocationFactory.getReadableDataLocation(fileName);
	// First we read the mapping sheet inside the excel
	excelInputFile = new File(fileName);
	excelInputConfig = new ExcelInputConfigImpl();
	//set header
	HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
	excelInputConfig.setHeader(header);
	excelInputConfig.setConfiguration(allExcelParameters);
	excelInputConfig.setDataSheetWithParamSheetsMapping(mapping);
	excelConfigurer = new ExcelConfigurer(excelInputConfig);
	// Get mapping
	mapping = excelConfigurer.readMappingSheets(null, excelInputFile, new FirstFailureExceptionHandler());
	// get parameter sheets names
	paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(mapping);

	// From parameter sheet names we read all param sheets and set the list
	// of excel Configuration
	allExcelParameters = excelConfigurer.readExcelParameters(excelInputFile, paramSheetNames, new FirstFailureExceptionHandler());
	// We have only one parameter sheet
	assertEquals(allExcelParameters.size(), 1);


	classUnderTest = new ExcelDataReaderEngine(excelLocation, prepareDataStructure(dsdFilename), null,
		excelInputConfig);

	// Move one step and read first
	classUnderTest.reset();
	classUnderTest.moveNextDataset();
    }

    // Method for reading the Input file from DataStart to DataEnd
    public void parseInput(String fileName, String dsdFilename)
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	prepareTest(fileName, dsdFilename);

	// looping rows
	while (classUnderTest.moveNextKeyable()) {
	    key = classUnderTest.getCurrentKey();
	    while (classUnderTest.moveNextObservation()) {
		obs = classUnderTest.getCurrentObservation();
	    }
	}
    }

    @Test
    public void testMappingFromInput() throws ValueShouldBeSkippedException, InvalidExcelParamsException, IOException {
	try {
	    // First we read the mapping sheet inside the excel
	    File excelInputFile = new File("./test_files/excel_testing/DataStart/compatible_no_formula.xlsm");
	    Map<String, ArrayList<String>> mapping = excelConfigurer.readMappingSheets(null, excelInputFile, new FirstFailureExceptionHandler());
	    assertEquals("Parameters", mapping.get("Table 10").get(0));
	} finally {
	    if (classUnderTest != null) {
		classUnderTest.close();
	    }
	}
    }

    @Test
    public void testDataStart() throws ValueShouldBeSkippedException, InvalidExcelParamsException, IOException,
	    EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    prepareTest("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete2.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
	    classUnderTest.moveNextKeyable();
	    classUnderTest.moveNextObservation();
	    // Get first Observation
	    Observation observation = classUnderTest.getCurrentObservation();
	    assertEquals(excelInputConfig.getConfigurations().get(0).getDataStart(), "C7");
	    assertEquals(observation.getObservationValue(), "10");
	} finally {
	    classUnderTest.close();
	}
    }

    @Test
    public void testDataEnd()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete2.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
	    assertEquals(excelInputConfig.getConfigurations().get(0).getDataEnd(), "E25");
	    assertEquals(obs.getObservationValue(), "70");// D25
	} finally {
	    classUnderTest.close();
	}
    }

    @Test
    public void testSkipIncompleteKeys()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete2.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
	    // if you execute the following assertion before parseInput
	    // getSkipIncompleteKey returns the default value of false
	    assertTrue(excelInputConfig.getConfigurations().get(0).getSkipIncompleteKey());
	    // case where SkipIncopleteKeys==true
	    assertEquals(0, classUnderTest.getIngoredObsCount());
	} finally {
	    classUnderTest.close();
	}

    }

    @Test
    public void testSkipRows()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete3.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
	    List<Integer> skipedRows = excelInputConfig.getConfigurations().get(0).getSkipRows();
	    int numOfSkipedRows = skipedRows.size();
	    System.out.println(
		    "*******************************************************************************************************");
	    System.out.println("Number of observations processed: " + classUnderTest.getObsCount());
	    System.out.println(
		    "*******************************************************************************************************");
	    System.out.println("Number of skiped lines: " + numOfSkipedRows);
	    assertEquals(Integer.toString(classUnderTest.getObsCount()), "30");
	} finally {
	    classUnderTest.close();
	}
    }

    @Test
    public void testSkipObservationWithValue()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete4.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
	    String skipValue = excelInputConfig.getConfigurations().get(0).getSkipObservationWithValue();
	    // looping rows
	    boolean result = false;
	    while (classUnderTest.moveNextKeyable()) {
		Keyable key = classUnderTest.getCurrentKey();
		while (classUnderTest.moveNextObservation()) {
		    obs = classUnderTest.getCurrentObservation();
		    if (obs.getObservationValue().equals(skipValue)) {
			result = true;
		    }
		}
	    }
	    assertEquals(Integer.toString(classUnderTest.getObsCount()), "22");
	    assertFalse(result);
	    System.out.println("SUCCESS");
	} finally {
	    classUnderTest.close();
	}
    }

    @Test
    public void testTranscoding()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/BE_AEA_2017_with_params_transcodingCopy.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+SEEAAIR+1.0.xml");
	    System.out
		    .println("**************************************************************************************");
	    System.out.println("VALUE: " + obs.getAttribute("COMMENT_OBS").getCode());
	    System.out
		    .println("**************************************************************************************");
	    assertEquals(obs.getAttribute("COMMENT_OBS").getCode(), "PAOK");
	} finally {
	    classUnderTest.close();
	}
    }

    @Test
    public void testDefaultValue()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete5.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
	    assertEquals(classUnderTest.getFoundEmpty(), 0);
	} finally {
	    classUnderTest.close();
	}
    }

    @Test
    public void testConceptSeparator()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/SDMX-NA_MAIN_T0101_V1.2c_Sample_conceptSeparator.xlsx",
		    "./test_files/excel_testing/DataStart/ESTAT+NAMAIN+1.2.xml");
	    // UNIT_MULT component has parameter ATT CELL H8/1
	    assertEquals("6", key.getAttribute("UNIT_MULT").getCode());
	    // PRE_BREAK_VALUE component has parameter ATT CELL H8/2
	    assertEquals("0", obs.getAttribute("PRE_BREAK_VALUE").getCode());
	    // Test mixed Obs Level
	    assertEquals("N", obs.getAttribute("CONF_STATUS").getCode());
	    // Test mixed cell
	    assertEquals("A", obs.getAttribute("OBS_STATUS").getCode());
	} finally {
	    classUnderTest.close();
	}
    }

    @Test
    public void testMissingObsCharacter()
	    throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
	try {
	    parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete6.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
	    assertEquals(classUnderTest.getFoundEmpty(), 0);
	} finally {
            classUnderTest.close();
	}
    }
    
    @Test
    public void testMaxNumOfEmptyColums() throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
        try {
            parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete7.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
            assertEquals(classUnderTest.getObsCount(), 30);
            Observation observation = classUnderTest.getCurrentObservation();
            assertEquals(observation.getObservationValue(), "35000");
        } finally {
            classUnderTest.close();
        }
    }
    
    
    @Test
    public void testMaxEmptyRows() throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
        try {
            parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete7.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
            assertEquals(classUnderTest.getObsCount(), 30);
        } finally {
            classUnderTest.close();
        }
    }
    
    
    @Test
    public void testNumColumns() throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
        try {
            parseInput("./test_files/excel_testing/DataStart/compatible_no_formulaSkipIncomplete8.xlsm",
		    "./test_files/excel_testing/DataStart/ESTAT+ENERGY+1.2+DFs.xml");
            assertEquals(classUnderTest.getObsCount(), 20);
        } finally {
            classUnderTest.close();
        }
    }
    
    
    @Test
    public void testFormatValues() throws InvalidExcelParamsException, IOException, EncryptedDocumentException, InvalidFormatException, WriteHeaderException {
        try {
            //FormatValues=actualValue
            parseInput("./test_files/excel_testing/DataStart/formatValues/excelWithParametersInside-data1.xlsx",
		    "./test_files/excel_testing/DataStart/formatValues/ESTAT_STS_v2.2.xml");
            Observation observation = classUnderTest.getCurrentObservation();
            assertEquals(observation.getObservationValue(), "104.6");
            
            //FormatValues=customFormat
            parseInput("./test_files/excel_testing/DataStart/formatValues/excelWithParametersInside-data2.xlsx",
		    "./test_files/excel_testing/DataStart/formatValues/ESTAT_STS_v2.2.xml");
            observation = classUnderTest.getCurrentObservation();
            assertEquals(observation.getObservationValue(), "0");
            
            //FormatValues=asDisplayed
            parseInput("./test_files/excel_testing/DataStart/formatValues/excelWithParametersInside-data3.xlsx",
		    "./test_files/excel_testing/DataStart/formatValues/ESTAT_STS_v2.2.xml");
            observation = classUnderTest.getCurrentObservation();
            assertEquals(observation.getObservationValue(), "-");
        } finally {
            classUnderTest.close();
        }
        
    }
    
    @Test
    public void testInflateRatio() throws ValueShouldBeSkippedException, InvalidExcelParamsException, IOException,
	    EncryptedDocumentException, InvalidFormatException, WriteHeaderException{
        try {
            excelInputConfig.setInflateRatio(0.008);
		    parseInput("./test_files/excel_testing/DataStart/Class_2019.xlsx",
			    "./test_files/excel_testing/DataStart/UIS+UOE_NON_FINANCE+1.0_Extended.xml");
		    
		    classUnderTest.moveNextKeyable();
		    classUnderTest.moveNextObservation();
		    // Get first Observation
		    Observation observation = classUnderTest.getCurrentObservation();
		    assertEquals(excelInputConfig.getConfigurations().get(0).getDataStart(), "AA31");
		    //assertEquals(observation.getObservationValue(), "1");
        }catch(Exception ex) {
        	ex.printStackTrace();
		} finally {
		    classUnderTest.close();
		}
    }

	@Ignore("MARIOSSSS")
	@Test
	public void testParsing() throws FileNotFoundException, IOException {
		FileInputStream file = new FileInputStream(new
				File("./test_files/excel_testing/DataStart/compatible_no_formula.xlsm"));
		System.out.println("found file");
		XSSFWorkbook workbook = new XSSFWorkbook(file);
		System.out.println("in workbook");
		XSSFSheet sheet = workbook.getSheet("Table 10");
		System.out.println("got sheet: " + sheet.getSheetName());
		Iterator<Row> rowIterator = sheet.rowIterator();
		while (rowIterator.hasNext() && rowIterator.next().getRowNum() <= 8) {
			System.out.println("Row Num: " + rowIterator.next().getRowNum());
		}
	}

    /*
     * @Test public void testDetectDimensionValuesForCoordinates2() throws
     * FileNotFoundException, ValueShouldBeSkippedException {
     * ReadableDataLocation excelLocation =
     * dataLocationFactory.getReadableDataLocation(
     * "./test_files/test_excel/timeOnColumns.xlsx"); ExcelConfiguration
     * mockExcelConfiguration = new ExcelConfiguration();
     * mockExcelConfiguration.setDataStart("C24");
     * mockExcelConfiguration.setNumberOfEmptyColumns(3);
     * mockExcelConfiguration.setDimensionConfig( Arrays.asList( new
     * ExcelDimAttrConfig("TIME_PERIOD", ExcelElementType.DIM,
     * ExcelPositionType.COLUMN, "A"), new ExcelDimAttrConfig("OBS_STATUS",
     * ExcelElementType.ATT, ExcelPositionType.OBS_LEVEL, "1")
     * 
     * )); ExcelInputConfig excelInputConfig = new ExcelInputConfig();
     * excelInputConfig.setConfiguration(mockExcelConfiguration); classUnderTest
     * = new ExcelDataReaderEngine( excelLocation, prepareDataStructure(), null,
     * excelInputConfig); classUnderTest.reset();
     * classUnderTest.moveNextDataset(); assertEquals("",
     * classUnderTest.detectComponentValueForObs(new CellReference(23, 2),
     * "OBS_STATUS")); }
     * 
     * @Test public void testDetectAllDimensionValues() throws IOException,
     * InvalidExcelParamsException { ReadableDataLocation excelLocation =
     * dataLocationFactory.getReadableDataLocation(
     * "./test_files/test_excel/timeOnRows.xlsx"); classUnderTest = new
     * ExcelDataReaderEngine( excelLocation, prepareDataStructure(), null,
     * prepareExcelConfiguration("./test_files/test_excel/timeOnRows.xlsx"));
     * classUnderTest.reset(); classUnderTest.moveNextDataset();
     * 
     * System.out.println(classUnderTest.detectSeriesValuesRelativeTo(new
     * CellReference(23, 3))); }
     */
}
