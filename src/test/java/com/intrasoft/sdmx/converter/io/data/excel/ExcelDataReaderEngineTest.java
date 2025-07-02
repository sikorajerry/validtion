package com.intrasoft.sdmx.converter.io.data.excel;

import static com.intrasoft.sdmx.converter.services.ExcelUtils.readExcelConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.util.CellReference;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.sdmxsource.util.excel.ExcelDimAttrConfig;
import org.estat.sdmxsource.util.excel.ExcelElementType;
import org.estat.sdmxsource.util.excel.ExcelPositionType;
import org.estat.sdmxsource.util.excel.ExcelReaderException;
import org.estat.sdmxsource.util.excel.InvalidExcelParamsException;
import org.estat.sdmxsource.util.excel.ValueShouldBeSkippedException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ExcelDataReaderEngineTest {

    @Qualifier("readableDataLocationFactory")
    @Autowired
    private ReadableDataLocationFactory dataLocationFactory;

    @Autowired
    private StructureParsingManager parsingManager;

    private ExcelDataReaderEngine classUnderTest = null;

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
    public void moveNextDatasetReturnsFalseWhenCalledMoreThanOnce() throws IOException, InvalidExcelParamsException, InvalidFormatException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRows.xlsx");
        classUnderTest = new ExcelDataReaderEngine(excelLocation,
                                                   prepareDataStructure(),
                                                   null,
                                                   prepareExcelConfiguration("./test_files/test_excel/timeOnRows.xlsx"));
        classUnderTest.reset();
        assertTrue(classUnderTest.moveNextDataset());
        assertFalse(classUnderTest.moveNextDataset());
        assertFalse(classUnderTest.moveNextDataset());
        assertFalse(classUnderTest.moveNextDataset());
    }


   /* @Test
    public void computeNextObsForLeftToRightReadingDirection() throws InvalidExcelParamsException, IOException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRows.xlsx");

        classUnderTest = new ExcelDataReaderEngine(excelLocation,
                                                   prepareDataStructure(),
                                                   null,
                                                   prepareExcelConfiguration("./test_files/test_excel/timeOnRows.xlsx"));
        classUnderTest.reset();
        classUnderTest.moveNextDataset();

        classUnderTest.moveNextKeyable();

        CellReference cellRef = classUnderTest.computeNextObsCoordinatesFromLeftToRight(new CellReference(23, 3));
        assertEquals(23, cellRef.getRow());
        assertEquals(3, cellRef.getCol());

        cellRef = classUnderTest.computeNextObsCoordinatesFromLeftToRight(new CellReference(23, 9));
        assertEquals(24, cellRef.getRow());
        assertEquals(3, cellRef.getCol());

        //checks for the last row of observations
        cellRef = classUnderTest.computeNextObsCoordinatesFromLeftToRight(new CellReference(31,3));
        assertEquals(31, cellRef.getRow());
        assertEquals(3, cellRef.getCol());

        //returns null if no other observations can be found (and the max allowed empty rows limit is reached)
        cellRef = classUnderTest.computeNextObsCoordinatesFromLeftToRight(new CellReference(31, 9));
        assertNull(cellRef);

    }*/

/*    @Test
    public void testMoveNextObservationLeftToRight() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRows.xlsx");
        classUnderTest = new ExcelDataReaderEngine(excelLocation,
                                                   prepareDataStructure(),
                                                   null,
                                                   prepareExcelConfiguration("./test_files/test_excel/timeOnRows.xlsx"));
        classUnderTest.reset();
        classUnderTest.moveNextDataset();
        classUnderTest.moveNextKeyable();

        int obsCount = 0;
        while(classUnderTest.moveNextObservation()){
            obsCount++;
        }

        assertEquals(6, obsCount);
    }*/

/*    @Test
    public void testMoveNextObservationTopToBottom() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnColumns.xlsx");

        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                                                    null,
                                                    prepareExcelConfiguration("./test_files/test_excel/timeOnColumns.xlsx"));
        classUnderTest.reset();
        classUnderTest.moveNextDataset();
        classUnderTest.moveNextKeyable();

        int obsCount = 0;
        while(classUnderTest.moveNextObservation()){
            obsCount++;
        }

        assertEquals(9, obsCount);
    }*/


/*    @Test
    public void computeNextObsForTopToBottomReadingDirection() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnColumns.xlsx");

        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                                                    null,
                                                    prepareExcelConfiguration("./test_files/test_excel/timeOnColumns.xlsx"));
        classUnderTest.reset();
        classUnderTest.moveNextDataset();
        classUnderTest.moveNextKeyable();

        CellReference cellRef = classUnderTest.computeNextObsCoordinatesFromTopToBottom(new CellReference(23, 2));
        assertEquals(23, cellRef.getRow());
        assertEquals(2, cellRef.getCol());

        cellRef = classUnderTest.computeNextObsCoordinatesFromTopToBottom(new CellReference(32, 2));
        assertEquals(23, cellRef.getRow());
        assertEquals(4, cellRef.getCol());
    }*/

    @Test
    public void testDetectDimensionValuesForCoordinates() throws FileNotFoundException, ValueShouldBeSkippedException, ExcelReaderException{
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRows.xlsx");
        ExcelConfiguration mockExcelConfiguration = new ExcelConfiguration();
        mockExcelConfiguration.setDataStart("D24");
        mockExcelConfiguration.setNumberOfEmptyColumns(3);
        mockExcelConfiguration.setParameterElements(
                Arrays.asList(
                        new ExcelDimAttrConfig("TIME_PERIOD", ExcelElementType.DIM, ExcelPositionType.ROW, "23", null, 0),
                        new ExcelDimAttrConfig("STS_ACTIVITY", ExcelElementType.DIM, ExcelPositionType.COLUMN, "2", null, 0),
                        new ExcelDimAttrConfig("REF_AREA", ExcelElementType.DIM, ExcelPositionType.CELL, "B1", null, 0)
                ));
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setConfiguration(mockExcelConfiguration);
        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                                                    null,
                                                    excelInputConfig);
        classUnderTest.reset();
        classUnderTest.moveNextDataset();
        assertEquals("200610", classUnderTest.detectComponentValue(new CellReference(23, 3), "TIME_PERIOD").firstEntry().getValue());
        assertEquals("200610", classUnderTest.detectComponentValue(new CellReference(24, 3), "TIME_PERIOD").firstEntry().getValue());
        assertEquals("N100CO", classUnderTest.detectComponentValue(new CellReference(23, 3), "STS_ACTIVITY").firstEntry().getValue());
        assertEquals("DE", classUnderTest.detectComponentValue(new CellReference(23, 3), "REF_AREA").firstEntry().getValue());
    }

    @Test
    public void testDetectDimensionValuesForCoordinates2() throws FileNotFoundException, ValueShouldBeSkippedException, ExcelReaderException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnColumns.xlsx");
        ExcelConfiguration mockExcelConfiguration = new ExcelConfiguration();
        mockExcelConfiguration.setDataStart("C24");
        mockExcelConfiguration.setNumberOfEmptyColumns(3);
        mockExcelConfiguration.setParameterElements(
                Arrays.asList(
                        new ExcelDimAttrConfig("TIME_PERIOD", ExcelElementType.DIM, ExcelPositionType.COLUMN, "A", null, 0),
                        new ExcelDimAttrConfig("OBS_STATUS", ExcelElementType.ATT, ExcelPositionType.OBS_LEVEL, "1", null, 0)

                ));
        ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
        excelInputConfig.setConfiguration(mockExcelConfiguration);
        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                                                    null,
                                                    excelInputConfig);
        classUnderTest.reset();
        classUnderTest.moveNextDataset();
        assertEquals(null, classUnderTest.detectComponentValue(new CellReference(23, 2), "OBS_STATUS"));
    }

/*    @Test
    public void testDetectAllDimensionValues() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRows.xlsx");
        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                                                    null,
                                                    prepareExcelConfiguration("./test_files/test_excel/timeOnRows.xlsx"));
        classUnderTest.reset();
        classUnderTest.moveNextDataset();

        System.out.println(classUnderTest.detectSeriesValuesRelativeTo(new CellReference(23, 3)));
    }*/
/*
    @Test
    public void testMoveNextKeyableLeftToRight() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRows.xlsx");
        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                                                    null,
                                                    prepareExcelConfiguration("./test_files/test_excel/timeOnRows.xlsx"));
        classUnderTest.reset();
        assertTrue(classUnderTest.moveNextDataset());
        int seriesCount = 0;
        while(classUnderTest.moveNextKeyable()){
            seriesCount++;
        }
        assertEquals(9, seriesCount);
    }*/

/*    @Test
    public void testMoveNextKeyableTopToBottom() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnColumns.xlsx");
        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                                                    prepareExcelConfiguration("./test_files/test_excel/timeOnColumns.xlsx"));
        classUnderTest.reset();
        assertTrue(classUnderTest.moveNextDataset());

        int seriesCount = 0;
        while(classUnderTest.moveNextKeyable()){
            seriesCount++;
        }
        assertEquals(7, seriesCount);
    }*/

/*    @Test
    public void testMoveNextKeyableFromOneSheetToAnother() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/twoSheetsWithData.xlsx");
        classUnderTest = new ExcelDataReaderEngine( excelLocation,
                                                    prepareDataStructure(),
                        prepareExcelConfiguration("./test_files/test_excel/twoSheetsWithData.xlsx"));
        classUnderTest.reset();
        assertTrue(classUnderTest.moveNextDataset());

        int seriesCount = 0;
        while(classUnderTest.moveNextKeyable()){
            seriesCount++;
        }
        assertEquals(18, seriesCount);
    }*/

/*    @Test
    public void testSeekObservationHorizontallyWhenObsHasOneAttribute() throws IOException, InvalidExcelParamsException {
        try (InputStream excelInputStream = new FileInputStream("./test_files/test_excel/firstRowNotEmpty.xlsx")) {
            Workbook excelWorkbook = new XSSFWorkbook(excelInputStream);
            Sheet sheet = excelWorkbook.getSheet("Data");
            classUnderTest = new ExcelDataReaderEngine(null, null, null);
            assertEquals(0, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 0), 2, 3));
            assertEquals(2, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 2), 2, 3));
            assertEquals(4, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 4), 2, 3));
            assertEquals(-1, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 6), 2, 3));
        }
    }*/

/*    @Test
    public void testSeekObservationHorizontallyWhenObsHasTwoAttributes() throws IOException, InvalidExcelParamsException {
        try (InputStream excelInputStream = new FileInputStream("./test_files/test_excel/twoObsAttributes.xlsx")) {
            Workbook excelWorkbook = new XSSFWorkbook(excelInputStream);
            Sheet sheet = excelWorkbook.getSheet("Data");
            classUnderTest = new ExcelDataReaderEngine(null, null, null);
            assertEquals(0, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 0), 3, 3));
            assertEquals(3, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 3), 3, 3));
            assertEquals(6, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 6), 3, 3));
            assertEquals(-1, classUnderTest.seekFirstNonEmptyObsHorizontally(sheet, new CellReference(0, 9), 3, 3));
        }
    }*/

/*    @Test
    public void testSeekObservationVertically() throws IOException {
        try (InputStream excelInputStream = new FileInputStream("./test_files/test_excel/firstColumnNotEmpty.xlsx")) {
            Workbook excelWorkbook = new XSSFWorkbook(excelInputStream);
            Sheet sheet = excelWorkbook.getSheet("Data");

            classUnderTest = new ExcelDataReaderEngine(null, null, null);
            assertEquals(0, classUnderTest.seekFirstNonEmptyObsVertically(sheet, new CellReference(0, 0), 3));
            assertEquals(1, classUnderTest.seekFirstNonEmptyObsVertically(sheet, new CellReference(1, 0), 3));
            assertEquals(2, classUnderTest.seekFirstNonEmptyObsVertically(sheet, new CellReference(2, 0), 3));
            assertEquals(-1, classUnderTest.seekFirstNonEmptyObsVertically(sheet, new CellReference(3, 0), 3));
        }
    }*/

/*    @Test
    public void integrationTestWhenReadingLeftToRightForXLSX() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRows.xlsx");

        classUnderTest = new ExcelDataReaderEngine(excelLocation,
                prepareDataStructure(),
                prepareExcelConfiguration("./test_files/test_excel/timeOnRows.xlsx"));
        classUnderTest.reset();

        int datasetCount = 0;
        while(classUnderTest.moveNextDataset()){
            int seriesCount = 0;
            datasetCount++;
            while(classUnderTest.moveNextKeyable()){
                seriesCount++;
                assertNotNull(classUnderTest.getCurrentKey());
                int obsCount = 0;
                while(classUnderTest.moveNextObservation()){
                    obsCount++;
                    assertNotNull(classUnderTest.getCurrentObservation());
                }
                assertEquals(6, obsCount);
            }
            assertEquals(9, seriesCount);
        }
        assertEquals(1, datasetCount);
    }*/

/*    @Test
    public void integrationTestWhenReadingTopToBottomForXLSX() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnColumns.xlsx");

        classUnderTest = new ExcelDataReaderEngine(excelLocation,
                prepareDataStructure(),
                prepareExcelConfiguration("./test_files/test_excel/timeOnColumns.xlsx"));
        classUnderTest.reset();
        //assertNotNull(classUnderTest.getCurrentObservation());
        //assertEquals("NaN", classUnderTest.getCurrentObservation());
        int datasetCount = 0;
        int seriesCount = 0;
        while(classUnderTest.moveNextDataset()){
            datasetCount++;
            while(classUnderTest.moveNextKeyable()){
                seriesCount++;
                assertNotNull(classUnderTest.getCurrentKey());
                int obsCount = 0;
                while(classUnderTest.moveNextObservation()){
                    assertNotNull(classUnderTest.getCurrentObservation());
                    assertEquals("NaN", classUnderTest.getCurrentObservation().getAttribute("OBS_STATUS").getCode());
                    obsCount++;
                }
                assertEquals(9, obsCount);
            }
        }
        assertEquals(1, datasetCount);
        assertEquals(7, seriesCount);
    }*/

/*    @Test
    public void integrationTestWhenReadingLeftToRightForXLS() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnRowsXLS.xls");

        classUnderTest = new ExcelDataReaderEngine(excelLocation,
                prepareDataStructure(),
                prepareExcelConfiguration("./test_files/test_excel/timeOnRowsXLS.xls"));
        classUnderTest.reset();

        int datasetCount = 0;
        while(classUnderTest.moveNextDataset()){
            int seriesCount = 0;
            datasetCount++;
            while(classUnderTest.moveNextKeyable()){
                seriesCount++;
                assertNotNull(classUnderTest.getCurrentKey());
                int obsCount = 0;
                while(classUnderTest.moveNextObservation()){
                    obsCount++;
                    assertNotNull(classUnderTest.getCurrentObservation());
                }
                assertEquals(6, obsCount);
            }
            assertEquals(9, seriesCount);
        }
        assertEquals(1, datasetCount);
    }*/

/*    @Test
    public void integrationTestWhenReadingTopToBottomForXLS() throws IOException, InvalidExcelParamsException {
        ReadableDataLocation excelLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/timeOnColumnsXLS.xls");

        classUnderTest = new ExcelDataReaderEngine(excelLocation,
                prepareDataStructure(),
                prepareExcelConfiguration("./test_files/test_excel/timeOnColumnsXLS.xls"));
        classUnderTest.reset();
        int datasetCount = 0;
        int seriesCount = 0;
        while(classUnderTest.moveNextDataset()){
            datasetCount++;
            while(classUnderTest.moveNextKeyable()){
                seriesCount++;
                assertNotNull(classUnderTest.getCurrentKey());
                int obsCount = 0;
                while(classUnderTest.moveNextObservation()){
                    assertNotNull(classUnderTest.getCurrentObservation());
                    obsCount++;
                }
                assertEquals(9, obsCount);
            }
        }
        assertEquals(1, datasetCount);
        assertEquals(7, seriesCount);
    }*/

    private DataStructureBean prepareDataStructure(){
        //prepare the dsd
        ReadableDataLocation dsdLocation = dataLocationFactory.getReadableDataLocation("./test_files/test_excel/ESTAT_STS_v2.2.xml");
        StructureWorkspace parseStructures = parsingManager.parseStructures(dsdLocation);
        SdmxBeans structureBeans = parseStructures.getStructureBeans(false);
        return structureBeans.getDataStructures().iterator().next();
    }

    private ExcelInputConfigImpl prepareExcelConfiguration(String configFileName) throws IOException, InvalidExcelParamsException, InvalidFormatException {
        //prepare the configuration
        ExcelConfiguration config = readExcelConfig(new FileInputStream(configFileName), false, new FirstFailureExceptionHandler()).get(0);
        config.setMaxEmptyRows(3);
        config.setNumberOfEmptyColumns(3);

        ExcelInputConfigImpl result = new ExcelInputConfigImpl();
        result.setConfiguration(config);
        return result;
    }
}
