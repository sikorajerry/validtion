package com.intrasoft.sdmx.converter.io.data.excel;


import org.apache.poi.ss.util.CellReference;

/**
 * Current cell's attributes.
 * @author tkaraisku
 *
 */
public class CurrentCellAttributes {
	/**
	 * Represents cell's attributes.
	 * Used while reading inside {@link ExcelDataReaderEngine#moveNextCellHorizontal(CellReference) moveNextCellHorizontal}
	 * to store various attributes of the cell.
	 * @param cellValue
	 * @param row
	 * @param col
	 * @param indexRow
	 * @param indexColumn
	 */
	public CurrentCellAttributes(String cellValue,
								int row,
								int col,
								int indexRow,
								int indexColumn) {
		this.cellValue = cellValue;
		this.row = row;
		this.col = col;
		setIndexs(indexRow, indexColumn);
	}
	
	public CurrentCellAttributes(String cellValue, int row, int col) {
		this.cellValue = cellValue;
		this.row = row;
		this.col = col;
	}
	
	/**
	 * The result for next indexes of the cell.
	 * <p>int[0] is the row Index 0-based;</p>
	 * <p>int[1] is the column Index 0-based;</p>
	 */
	private int[] indexs = new int[2];
	/**
	 * Current cell's value
	 */
	private String cellValue;
	/**
	 * Current cell's row.	
	 */
	private int row;
	/**
	 * Current cell's column.
	 */
	private int col;

	public int[] getIndexs() {
		return indexs;
	}

	public void setIndexs(int[] indexs) {
		this.indexs = indexs;
	}
	
	public void setIndexs(int rowIndex, int colIndex) {
		this.indexs[0] = rowIndex;
		this.indexs[1] = colIndex;
	}

	/**
	 * @return the row
	 */
	public int getRow() {
		return row;
	}

	/**
	 * @param row the row to set
	 */
	public void setRow(int row) {
		this.row = row;
	}
	
	/**
	 * @return the col
	 */
	public int getCol() {
		return col;
	}

	/**
	 * @param col the col to set
	 */
	public void setCol(int col) {
		this.col = col;
	}
	
	/**
	 * @return the cellValue
	 */
	public String getCellValue() {
		return cellValue;
	}

	/**
	 * @param cellValue the cellValue to set
	 */
	public void setCellValue(String cellValue) {
		this.cellValue = cellValue;
	}
	
	public CellReference getCellReference() {
		return new CellReference(this.row, this.col);
	}

}
