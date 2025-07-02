package com.intrasoft.sdmx.converter.io.data.excel;

import com.intrasoft.sdmx.converter.services.ExcelUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.format.CellFormat;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellReference;
import org.estat.sdmxsource.util.excel.FormatValues;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utils that fetches and finds the cell we are currently accessing.
 * Methods regarding Cell of excel that we are currently processing reside here.
 */
public final class ExcelCellUtils {

	public static boolean isBlankCell(Sheet sheet, int row, int col) {
		return isBlankCell(getCell(sheet, row, col));
	}

	/**
	 * Checks if the given row is empty
	 *
	 * @param cell Cell the actual cell to check
	 * @return true if the cell does not contain info.
	 */
	public static boolean isBlankCell(Cell cell) {
		return cell == null || cell.getCellType() == CellType.BLANK;
	}

	public static boolean isBlankCell(List<Row> rows, int row, int col) {
		return isBlankCell(getCell(rows, row, col));
	}

	/**
	 * Get the cell, given the sheet, the row and the column.
	 * Iterating through the input file rows and columns till the row/column given is found.
	 * <p>This is not used during reading the dataSheets. Is used for transcoding sheets only.</p>
	 * @see 'SDMXCONV-963'
	 * @param sheet
	 * @param row
	 * @param column
	 * @return
	 */
	public static Cell getCell(Sheet sheet, int row, int column) {
		Cell cell = null;
		for (Row r : sheet) {
			if(row == r.getRowNum()) {
				for (Cell c : r) {
					if(column == c.getColumnIndex()) {
						return c;
					}
				}
			}
		}
		return cell;
	}

	/**
	 * Method to return a cell given the cellPosition Alternative method because of
	 * the streaming reading of an xlsx
	 *
	 * @param rows         List<Row> rows that we read through streaming parsing
	 * @param cellPosition position  of the cell
	 * @return Cell
	 */
	public static Cell getCell(List<Row> rows, String cellPosition, ExceptionHandler exceptionHandler) {
		Cell cell = null;
		try {
			CellReference cellReference = new CellReference(cellPosition);
			Row row = ExcelSheetUtils.getRow(rows, cellReference.getRow());
			if (row == null) {
				return null;
			}
			cell = row.getCell(cellReference.getCol());
		} catch (Exception e) {
			String msg = "Error during reading cell " + cellPosition + ".";
			ExcelUtils.handleExceptionSilently(msg, exceptionHandler, new ErrorPosition(cellPosition));
		}
		return cell;
	}

	/**
	 * Fetches the cell. Alternative implementation with rows for xlsx/xlsm
	 * @param rows
	 * @param row
	 * @param col
	 * @return
	 */
	public static Cell getCell(List<Row> rows, int row, int col) {
		Cell result = null;
		if (row < rows.size()) {
			if (ExcelSheetUtils.isNotEmptyRow(rows.get(row))) {
				result = rows.get(row).getCell(col);
			}
		}
		return result;
	}

	public static Cell getCell(List<Row> rows, CellReference cellReference) {
		return getCell(rows, cellReference.getRow(), cellReference.getCol());
	}

	public static String getCellValue(List<Row> rows, String cellReference, DecimalFormat decimalFormat, FormatValues formatValue, boolean displayCellErrors) {
		CellReference cellRef = new CellReference(cellReference);
		return getCellValue(rows, cellRef.getRow(), cellRef.getCol(), decimalFormat, formatValue, displayCellErrors);
	}

	public static String getCellValue(List<Row> rows, CellReference cellRef, DecimalFormat decimalFormat, FormatValues formatValue, boolean displayCellErrors) {
		if (cellRef != null && cellRef.getRow() < rows.size()) {
			return getCellValue(rows, cellRef.getRow(), cellRef.getCol(), decimalFormat, formatValue, displayCellErrors);
		} else {
			return null;
		}
	}

	public static String getCellValue(List<Row> rows, int row, int col, DecimalFormat decimalFormat, FormatValues formatValue, boolean displayCellErrors) {
		return getCellValue(getCell(rows, row, col), decimalFormat, formatValue, displayCellErrors);
	}

	/**
	 * Returns the String value of the value contained in the given Cell.
	 * (functionality of formatValues Parameter is implemented here)
	 * @param cell
	 * @param decimalFormat
	 * @param formatValues
	 * @param displayCellErrors Boolean to determine if the value is computed during sneak pick or
	 *                          user selected not to throw cell formula errors. This is needed to avoid double errors
	 * @return the String value of the value contained by the Cell
	 */
	public static String getCellValue(Cell cell, DecimalFormat decimalFormat, FormatValues formatValues, boolean displayCellErrors) {
		if (cell == null) {
			return null;
		}

		CellType cellType = (cell.getCellTypeEnum() == CellType.FORMULA) ? cell.getCachedFormulaResultTypeEnum()
				: cell.getCellTypeEnum();

		switch (cellType) {
			// In stream processing excel if a formula cell is found returns the value
			// inside "", so we need to do some trimming
			// Although we shouldn't have formula type at this point. Has to do something
			// with the cache
			case FORMULA:
				String value = null;
				try {
					value = StringUtils.strip(String.valueOf(cell.getStringCellValue()).trim(), "\"");
				} catch (Exception e1) {
					value = null;
				}
				return value;
			case BLANK:
				return null;
			case BOOLEAN:
				return String.valueOf(cell.getBooleanCellValue());
			case NUMERIC:
				String retValue = null;
				if (DateUtil.isCellDateFormatted(cell)) {
					try {
						Date d = cell.getDateCellValue();
						SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
						retValue = fmt.format(d);
					} catch (Exception e) {
						cell.setCellType(CellType.STRING);
						retValue = cell.getRichStringCellValue().toString();
					}
				} else {
					retValue = getFormattedValue(cell, formatValues, decimalFormat);
				}

				if (retValue != null) {
					retValue = retValue.replaceAll(",", "");
				}
				return retValue;
			case STRING:
				return cell.getStringCellValue();
			case ERROR:
				//SDMXCONV-1239
				//In case of an error in formula computation we want Validation catch the error and inform the user
				if(displayCellErrors) {
					return "'" + cell.getStringCellValue() + "'";
				} else {
					//If we are in the process of sneaking then we want to avoid reporting the error
					return null;
				}
			default:
				return null;
		}
	}

	/**
	 * Returns the String value of the value contained in the given Cell.
	 * (functionality of formatValues Parameter is implemented here)
	 * (functionality of cell separator is implemented here)
	 * @param cell
	 * @param cellInsidePosition Position if there is a concept separator find the correct value inside cell
	 * @param separator  from parameter sheet
	 * @param decimalFormat
	 * @param formatValues
	 * @param displayCellErrors Boolean to determine if the value is computed during sneak pick or
	 * 	 *                      user selected not to throw cell formula errors. This is needed to avoid double errors
	 * @return the String value of the value contained by the Cell
	 */
	public static String getCellValue(Cell cell, int cellInsidePosition, String separator, DecimalFormat decimalFormat, FormatValues formatValues, boolean displayCellErrors) {
		if (cell == null) {
			return null;
		}
		CellType cellType = (cell.getCellTypeEnum() == CellType.FORMULA) ? cell.getCachedFormulaResultTypeEnum()
				: cell.getCellTypeEnum();

		switch (cellType) {
			// In stream processing excel if a formula cell is found returns the value
			// inside "", so we need to do some trimming
			// Although we shouldn't have formula type at this point. Has to do something
			// with the cached file types
			case FORMULA:
				String value = null;
				try {
					value = StringUtils.strip(String.valueOf(cell.getStringCellValue()).trim(), "\"");
				} catch (Exception e1) {
					value = null;
				}
				return value;
			case BLANK:
				return null;
			case BOOLEAN:
				return String.valueOf(cell.getBooleanCellValue());
			case NUMERIC:
				String retValue = null;
				if (DateUtil.isCellDateFormatted(cell)) {
					// solution for SDMXCONV-64
					try {
						Date d = cell.getDateCellValue();
						SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
						retValue = fmt.format(d);
					} catch (Exception e) {
						cell.setCellType(CellType.STRING);
						retValue = cell.getRichStringCellValue().toString();
					}
				} else {
					retValue = getFormattedValue(cell, formatValues, decimalFormat);
				}
				if (retValue != null) {
					retValue = retValue.replaceAll(",", "");
				}
				return retValue;
			case STRING:
				// If the cell value contains separator
				// we need the position inside cell to parse it
				if (cellInsidePosition != 0) {
					String cellValueSplit = cell.getStringCellValue();

					// To accept "(" and ")" as separators we need Pattern.quote
					String[] strArray = cellValueSplit.split(Pattern.quote(separator));

					// if strArray has smaller size than the position we gave in the parameter file
					if (strArray.length < cellInsidePosition) {
						return null;
					}
					return strArray[cellInsidePosition - 1];
				} else {
					return cell.getStringCellValue();
				}
			case ERROR:
				//SDMXCONV-1239
				//In case of an error in formula computation we want Validation catch the error and inform the user
				if(displayCellErrors) {
					return "'" + cell.getStringCellValue() + "'";
				} else {
					//If we are in the process of sneaking then we want to avoid reporting the error
					return null;
				}
			default:
				return null;
		}
	}

	/**
	 * Output value depends on the parameter formatValues
	 * <ul>
	 *	<li>option 1: actual values (disregard every format and take into
	 *	consideration only the actual value without exception).</li>
	 *	<li>option 2: custom formatting (if custom formatting exists in a cell we
	 *	keep the formatted value of the cell else we take the actual value) as it is
	 *	now.</li>
	 *	<li>option 3: as displayed (only the formatted value of the cell is used
	 *	everywhere).</li>
	 * </ul>
	 *
	 * @See issues SDMXCONV-744, SDMXCONV-200, SDMXCONV-927
	 *
	 * @param cell
	 * @param formatValues  - parameter formatValues
	 * @param decimalFormat
	 * @return cellValue
	 */
	private static String getFormattedValue(Cell cell, FormatValues formatValues, DecimalFormat decimalFormat) {
		String cellValue = null;

		if (cell != null && !isBlankCell(cell)) {
			if (formatValues==FormatValues.DISPLAYED) {
				CellFormat cellFormat = CellFormat.getInstance(cell.getCellStyle().getDataFormatString());
				cellValue = cellFormat.apply(cell).text;
			} else if (formatValues==FormatValues.ACTUAL) {
				cellValue = decimalFormat.format(cell.getNumericCellValue());
			} else { //FormatValues.CUSTOM
				CellStyle style = cell.getCellStyle();
				short formatIndex = style.getDataFormat();
				String formatString = style.getDataFormatString();
				if (formatString == null) {
					formatString = BuiltinFormats.getBuiltinFormat(formatIndex);
				}
				// Solution for SDMXCONV-200
				if (formatIndex >= 164) { // custom format
					CellFormat cellFormat = CellFormat.getInstance(formatString);
					cellValue = cellFormat.apply(cell).text;
				} else {
					cellValue = decimalFormat.format(cell.getNumericCellValue());
				}
			}
		}
		return cellValue;
	}
}
