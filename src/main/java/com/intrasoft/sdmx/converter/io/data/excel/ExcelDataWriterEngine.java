/**
 * 
 */
package com.intrasoft.sdmx.converter.io.data.excel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.intrasoft.sdmx.converter.ComponentValuesBuffer;
import com.intrasoft.sdmx.converter.io.data.ComponentBufferWriterEngine;
import com.intrasoft.sdmx.converter.services.ExcelUtils;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellReference;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.sdmxsource.util.excel.ExcelDimAttrConfig;
import org.estat.sdmxsource.util.excel.InvalidExcelParamsException;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.util.MultiValueMap;

/**
 * Excel Data writer using the new Sdmx based reader / writer framework. 
 * 
 * @author dragos balan
 */
public class ExcelDataWriterEngine extends ComponentBufferWriterEngine {

	private static Logger logger = LogManager.getLogger(ExcelDataWriterEngine.class);
	
	public final static String DATA_SHEET_NAME_PREFIX = "Data_";
	
	private Workbook workbook; 
	private Sheet currentSheet; 
	private int currentSheetNbr = 0; 
	private int dataStartColumn = -1;
	private int dataStartRow = -1;
	
	/**
	 * column header
	 */
	private ExcelRowColumnHeader columnHeaderValues = null; 
	
	/**
	 * row header
	 */
	private ExcelRowColumnHeader rowHeaderValues = null; 
	
	/**
	 * wrapper over the parameter sheet in the excel template
	 */
	private ExcelOutputConfigParser excelConfigParser = null;
	
	/**
	 * the excel output template
	 */
	private ExcelOutputConfig outputConfig;
	private OutputStream outputStream;

	/**
	 * the constructor of this writer
	 * @param outputStream
	 * @param excelOutputConfig
	 */
	public ExcelDataWriterEngine(OutputStream outputStream,
								 ExcelOutputConfig excelOutputConfig){
		this.outputConfig = excelOutputConfig;
		this.outputStream = outputStream; 
		//this.excelConfigParser = new ExcelOutputConfigParser(outputConfig.getExcelConfig());
	}

	public void openWriter(){
		    try {
				workbook = WorkbookFactory.create(outputConfig.getExcelOutputTemplate());
				 List<ExcelConfiguration> excelTemplate = ExcelUtils.readExcelConfigFromWorkbook(workbook, new FirstFailureExceptionHandler());
				 this.excelConfigParser = new ExcelOutputConfigParser(excelTemplate.get(0));
				 setDataStart();
			} catch (EncryptedDocumentException | IOException | InvalidExcelParamsException e) {
				throw new RuntimeException(e);
			}
	}
	
	public void setDataStart() {
		CellReference dataStartCell = null;
		if(excelConfigParser.getDataStartCell()!=null && !"".equals(excelConfigParser.getDataStartCell())) {
			dataStartCell = new CellReference(excelConfigParser.getDataStartCell());
		}
		
		dataStartColumn = -1;
		dataStartRow = -1;
		if(dataStartCell!=null) {
			dataStartColumn = dataStartCell.getCol();
			dataStartRow = dataStartCell.getRow();
		}
		

		if(dataStartColumn==-1 || dataStartRow==-1) logger.error("Data Start Cell is not defined properly. Check the parameters and try again.");
	}

	public void closeWriter(){
		try {
			workbook.write(outputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void openHeader(){

	}

	public void doWriteHeader(HeaderBean header){

	}

	public void closeHeader(){

	}

	public void openSeries(AnnotationBean... annotations){
		super.openSeries(annotations);
		newSheet(DATA_SHEET_NAME_PREFIX+currentSheetNbr);
		columnHeaderValues = new ExcelRowColumnHeader(excelConfigParser.getDimensionsPerRows());
		rowHeaderValues = new ExcelRowColumnHeader(excelConfigParser.getDimensionsPerColumns());
	}
	
	public void closeSeries(){
		super.closeSeries();
		writeColumnHeadersToExcel();
        writeRowHeadersToExcel();
		writeIndependentCellsToExcel();
	}
	
	private void newSheet(String sheetName){
		logger.debug("creating a new excel sheet {}", sheetName);
		currentSheet = workbook.createSheet(sheetName); 
		currentSheetNbr++;
	}
	
	@Override
	protected void doWriteComponentsValues(ComponentValuesBuffer componentValues) {
		List<String> valuesForRows = getComponentValuesForRows(componentValues);
		if(!columnHeaderValues.containsValues(valuesForRows)){
			columnHeaderValues.addValues(valuesForRows);
		}
		int indexInColHeader = columnHeaderValues.getIndexForValues(valuesForRows);

		List<String> valuesForCols = getComponentValuesForCols(componentValues);
		if(!rowHeaderValues.containsValues(valuesForCols)){
			rowHeaderValues.addValues(valuesForCols);
		}
		int indexInRowHeader = rowHeaderValues.getIndexForValues(valuesForCols);

		
		
		List<String> obsLevelAttributes = excelConfigParser.getAttributesAtObsLevel();
		int obsLevelAttrsCount = excelConfigParser.getAttributesAtObsLevelCount();

		logger.debug("writing obs {} at row {} and col {} ", componentValues.getObsValue(), indexInRowHeader, indexInColHeader);
		writeValueInCell(   componentValues.getObsValue(),
				            dataStartRow + indexInRowHeader,
				            dataStartColumn + indexInColHeader + (indexInColHeader * obsLevelAttrsCount));

		//write obs level attributes
		if(obsLevelAttrsCount > 0){
			for(int obsLevelIndex = 0; obsLevelIndex < obsLevelAttributes.size(); obsLevelIndex++){
				String obsLevelAttrValue = obsLevelAttributes.get(obsLevelIndex);
				String obsValue = componentValues.getValueFor(obsLevelAttrValue);
				writeValueInCell(	obsValue,
						dataStartRow + indexInRowHeader,
						dataStartColumn + indexInColHeader + (obsLevelAttrsCount * indexInColHeader) + obsLevelIndex+1);
			}
		}
	}

    /**
	 * writes the row header (marked as LEFT_TO_RIGHT) and the column header ( marked as TOP_TO_BOTTOM) to excel
	 * based on the columnHeaderValues and rowHeaderValues
	 */
	private void writeColumnHeadersToExcel() {
        int obsLevelAttrCount = excelConfigParser.getAttributesAtObsLevelCount();

        //first we write the column headers
        logger.info("writing table column headers for dimensions {}", excelConfigParser.getDimensionsPerRows());
        for (String dim : excelConfigParser.getDimensionsPerRows()) {
            List<String> columnHeaders = columnHeaderValues.getValuesForDimension(dim);
            int currentCol = dataStartColumn;
            for (String header : columnHeaders) {
                writeValueInCell(header,
                        excelConfigParser.getConfiguredPositionForDimension(dim) - 1,
                        currentCol);
                currentCol = currentCol + obsLevelAttrCount + 1;
            }
        }
    }

    private void writeRowHeadersToExcel(){
		logger.info("writing table row headers for dimensions {}", excelConfigParser.getDimensionsPerColumns());
		for(String dim: excelConfigParser.getDimensionsPerColumns()){
			List<String> rowHeaders = rowHeaderValues.getValuesForDimension(dim); 
			int currentRow = dataStartRow;
			for(String header: rowHeaders){
				writeValueInCell(header, 
								currentRow, 
								excelConfigParser.getConfiguredPositionForDimension(dim)-1);
				currentRow++;
			}
		}
	}
	
	/**
	 * writes the cells marked as CELL in the parameters excel sheet
	 */
	private void writeIndependentCellsToExcel(){
	    Map<String, String> bufferedValues = getBufferedValues();
		for(String dim: excelConfigParser.getDimensionsPerCells()){
			String value = bufferedValues.get(dim);
			CellReference cellRef = getConfiguredCellReferenceForDimension(dim);
			logger.info("writing {} in cell {}", value, cellRef);
			writeValueInCell(value, cellRef);
			if (cellRef.getCol()-1 >= 0) {
				CellReference leftCellReference = new CellReference(cellRef.getRow(), cellRef.getCol()-1);
				writeValueInCell(dim, leftCellReference);
			}						
		}
	}
	
	
	/**
	 * todo: improve
	 * @param dimension
	 * @return
	 */
	private CellReference getConfiguredCellReferenceForDimension(String dimension){
		MultiValuedMap<String, ExcelDimAttrConfig> dimensionsConfig = excelConfigParser.getDimensionsConfig();
		String position = dimensionsConfig.get(dimension).stream().findFirst().get().getPosition();
		return new CellReference(position);
	}
	
	/**
	 * writes the given value in a particula excel cell
	 * 
	 * @param value
	 * @param cellRef
	 */
	private void writeValueInCell(String value, CellReference cellRef){
		writeValueInCell(value, cellRef.getRow(), cellRef.getCol());
	}
	
	/**
	 * writes the given value on the particular row-column cell
	 * 
	 * @param value
	 * @param row
	 * @param col
	 */
	private void writeValueInCell(String value, int row, int col){
		Row r = currentSheet.getRow(row); 
		if (r == null) {
		   r = currentSheet.createRow(row);
		}
		Cell c = r.getCell(col); 
		if (c == null) {
		    c = r.createCell(col);
		}
		c.setCellValue(value);
	}

    private List<String> getComponentValuesForRows(ComponentValuesBuffer dataRow){
        List<String> result = new ArrayList<String>();
        for(String dimDisplayedOnRows: excelConfigParser.getDimensionsPerRows()){
            result.add(dataRow.getValueFor(dimDisplayedOnRows));
        }
        return result;
    }

    private List<String> getComponentValuesForCols(ComponentValuesBuffer dataRow){
        List<String> result = new ArrayList<String>();
        for(String dimDisplayedOnCols: excelConfigParser.getDimensionsPerColumns()){
            result.add(dataRow.getValueFor(dimDisplayedOnCols));
        }
        return result;
    }

	@Override
	public void writeComplexAttributeValue(KeyValue keyValue) {

	}

	@Override
	public void writeComplexMeasureValue(KeyValue keyValue) {

	}

	@Override
	public void writeMeasureValue(String id, String value) {

	}

	@Override
	public void writeObservation(String obsConceptValue, AnnotationBean... annotations) {

	}

	//SDMXRI-1166
	@Override
	public void close() {
		close(new FooterMessage[]{});
		//closeWriter();
	}
}
