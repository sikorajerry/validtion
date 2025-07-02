/**
 * Copyright (c) 2015 European Commission.
 * <p>
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl5
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.io.data.excel;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellReference;
import org.estat.sdmxsource.util.excel.ExcelInputConfigParser;
import org.estat.sdmxsource.util.excel.FormatValues;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils that Stores current sheet and holds the list of rows of the sheet.
 * Methods regarding Sheet of excel that we are currently processing reside here.
 */
public class ExcelSheetUtils {

	private Sheet currentSheet;

	private List<Row> rows;

	public ExcelSheetUtils(Sheet currentSheet) {
		this.currentSheet = currentSheet;
	}

	public ExcelSheetUtils() {
	}

	/**
	 * <h2>Alternative method for checking a row if it is empty.</h2>
	 * Determines if the given row is empty, except from the cell type the method checks if the cells of a row are all empty.
	 * Used for reading with xlsx-streamer.
	 * @param row the row we check if it is empty
	 * @param decimalFormat from parameters sheet the user can select number formatting
	 * @param formatValues from parameters sheet the user can select which format to use when reading cell data.
	 * @param displayCellErrors if cell formula errors will be displayed or not
	 * @return boolean
	 *
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1176">SDMXCONV-1176</a>
	 */
	public static boolean isEmptyRow(Row row, DecimalFormat decimalFormat, FormatValues formatValues, boolean displayCellErrors) {
		if (row == null) {
			return true;
		}
		if (row.getLastCellNum() == -1) {
			return true;
		}
		/* for (Cell c : row) {
			if (CellType.BLANK == c.getCellTypeEnum()) {
				return false;
			}
		}*/
		for (Cell c : row) {
			if (!ExcelCellUtils.isBlankCell(c)) {
				String obsValue = ExcelCellUtils.getCellValue(c, decimalFormat, formatValues, displayCellErrors);
				if (obsValue != null && !"".equals(obsValue)) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Determines if the given row is empty. Used for reading with xlsx-streamer.
	 * The older XLS format is not capable of being streamed.
	 *
	 * @param row Row
	 * @return true if the row does not contain info.
	 */
	public static boolean isNotEmptyRow(Row row) {
		if (row == null) {
			return false;
		}
		if (row.getLastCellNum() == -1) {
			return false;
		}
		for (Cell c : row) {
			if (CellType.BLANK != c.getCellType()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Method to return a single row given the list of rows and the index(position
	 * of the row) of the list Alternative method because of the streaming reading
	 * of an xlsx
	 *
	 * @param rows List<Row> rows that we read through streaming parsing
	 * @param indx int       position of the row inside the list
	 * @return Row
	 */
	public static Row getRow(List<Row> rows, int indx) {
		Row row = null;
		// If index out of bounds then return empty row
		if (rows.size() > indx) {
			if (isNotEmptyRow(rows.get(indx))) {
				row = rows.get(indx);
			}
		}
		return row;
	}

	public Sheet getCurrentSheet() {
		return currentSheet;
	}

	public void setCurrentSheet(Sheet currentSheet) {
		this.currentSheet = currentSheet;
	}

	public List<Row> getRows() {
		return rows;
	}

	public void setRows(List<Row> rows) {
		this.rows = rows;
	}

	/**
	 * Method to store rows because of the streaming reading of an xlsx we need to
	 * store rows of the sheet and fetch data from there
	 *
	 * @param sheet Sheet
	 * @return List<Row>
	 */
	public List<Row> getRows(Sheet sheet) {
		List<Row> rows = new ArrayList<>();
		int prevRow = 0;
		for (Row r : sheet) {
			// SDMXCONV-703
			// Because iterator ignores empty rows and just skip it
			// and can't even recognize that are empty
			// we have to add them manually to achieve
			// having a list of rows with correct indexing
			while (prevRow != r.getRowNum()) {
				rows.add(null);
				prevRow = prevRow + 1;
			}
			rows.add(r);
			prevRow = prevRow + 1;
		}
		return rows;
	}


	/**
	 * Determines if the given column is empty.
	 * For efficiency we check 10 cells above and 10 cells bellow only.
	 * If we found them empty we assume the column as empty.
	 * @see 'SDMXCONV-975'
	 *
	 * @param rows
	 * @return true if the column does not contain info.
	 */
	public boolean isEmptyColumn(List<Integer> emptyColumnsLs, List<Row> rows, ExcelInputConfigParser configParser, int column, boolean displayCellErrors) {
		boolean emptyColumn = true;

		if(configParser.getDataStartCell()!=null && configParser.getDataEnd()!=null && configParser.getDefaultObsValue()!=null) {
			CellReference startCell = new CellReference(configParser.getDataStartCell());
			int startColumn = startCell.getCol();
			CellReference endCell = new CellReference(configParser.getDataEndCell());
			int endColumn = endCell.getCol();
			if(column>=startColumn && column<=endColumn) {
				return false;
			}
		}
		if (emptyColumnsLs.contains(column)) {
			return true;
		}

		int rIndex = 0;
		for (int i = rIndex; i < rows.size(); i++) {
			Row r = rows.get(i);
			if (r != null) {
				Cell c = r.getCell(column);
				if (!ExcelCellUtils.isBlankCell(c)) {
					String obsValue = ExcelCellUtils.getCellValue(c, configParser.getDecimalFormat(), configParser.getFormatValues(), displayCellErrors);
					if (obsValue != null && !"".equals(obsValue)) {
						return false;
					}
					if (CellType.BLANK != c.getCellType()) {
						return false;
					}
				}
			}
		}
		return emptyColumn;
	}

	/**
	 * Gets from a given String the Letter(s) value for the column.
	 * <p>
	 * For example: <br>
	 * if the position(the given stringNumber parameter) of the column is "B" the
	 * returned value is also "B" <br>
	 * if the position(the given stringNumber parameter) of the column is "2" the
	 * returned value is "B" <br>
	 * It is useful when in parameters file for the data sheets has the value of the
	 * columns using numbers instead of letters.
	 * </p>
	 *
	 * @param stringNumber
	 * @return the letter(s) value of the column
	 */
	public String getColumnStringFromStringNumber(String stringNumber) {
		for (int i = 0; i < stringNumber.length(); i++) {
			if (!Character.isDigit(stringNumber.charAt(i))) {
				return stringNumber;
			}
		}
		return CellReference.convertNumToColString(Integer.parseInt(stringNumber) - 1);
	}

	@Deprecated
	public int getRowFromCellRef(String cellReference) {
		CellReference cellRef = new CellReference(cellReference);
		return cellRef.getRow();
	}

	@Deprecated
	public int getColFromCellRef(String cellReference) {
		CellReference cellRef = new CellReference(cellReference);
		return cellRef.getCol();
	}
	/**
	 * Checks if the given row is empty
	 *
	 * @param sheet     Sheet the parsing sheet object
	 * @param rowNumber int the number of row to check
	 * @return true if the row does not contain info.
	 */
	@Deprecated
	public boolean isEmptyRow(Sheet sheet, int rowNumber) {
		boolean rowIsEmpty = true;
		Row row = sheet.getRow(rowNumber);

		if (row != null) {
			for (Cell c : row) {
				if (CellType.BLANK != c.getCellType()) {
					rowIsEmpty = false;
					break;
				}
			}
		}
		return rowIsEmpty;
	}

}
