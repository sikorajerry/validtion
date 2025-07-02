package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.io.data.excel.ExcelSheetUtils;
import com.monitorjbl.xlsx.StreamingReader;
import com.monitorjbl.xlsx.exceptions.OpenException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.ss.format.CellFormat;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.IOUtils;
import org.estat.sdmxsource.extension.datavalidation.Pair;
import org.estat.sdmxsource.util.excel.*;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.constants.SDMX_ERROR_CODE;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.exception.SdmxInternalServerException;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.base.OccurrenceBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.FiniteOccurrenceBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.UnboundedOccurrenceBeanImpl;
import org.sdmxsource.sdmx.util.beans.ConceptRefUtil;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataFormatException;
import org.sdmxsource.util.ObjectUtil;

import java.io.*;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public final class ExcelUtils {

	public enum READING_DIRECTION {
		LEFT_TO_RIGHT, TOP_TO_BOTTOM, ANY
	}

	private static final Logger logger = LogManager.getLogger(ExcelUtils.class);

	public static final String PARAMETER_SHEET_NAME = "Parameter";

	public static final String PARAMETER_MAP_SHEET_NAME = "Parameter_mapping";

	public static final String VAL_SHEET_NAME = "VAL";

	/** the position where the actual data start */
	public static final String DATA_START = "DataStart";
	/** the position where the actual data end */
	public static final String DATA_END = "DataEnd";
	/** skip one or several rows */
	public static final String SKIP_ROWS = "SkipRows";
	/** skip one or several columns */
	public static final String SKIP_COLUMNS = "SkipColumns";
	/** skip observation with certain value */
	public static final String SKIP_OBSERVATION_WITH_VALUE = "SkipObservationWithValue";
	/** skip observations with empty or wrong dimension or concept values when value is set to true. Default value is set to true */
	public static final String SKIP_INCOMPLETE_KEY = "SkipIncompleteKeys";
	/** character that separates multiple concepts defined in the same cell */
	public static final String CONCEPT_SEPARATOR = "ConceptSeparator";
	/** specify the name of the sheet containing a mapping between the couple text/dimension and the SDMX value*/
	public static final String TRANSCODING_SHEET = "TranscodingSheet";

	public static final String TRANSCODING_SHEET_NAME = "Trans";
	/** if set at an observation cell converter treats the observation as missing */
	public static final String MISSING_OBS_CHARACTER = "MissingObservationCharacter";
	/** the maximum number of empty cells allowed in the current row before start reading the next row.
	 * If not present the default is 1000. */
	public static final String MAX_NO_EMPTY_COLUMNS = "MaxNumOfEmptyColums";
	public static final String MAX_EMPTY_COLUMNS = "MaxEmptyColumns";

	/** the number of columns containing data */
	public static final String NUMBER_COLUMNS = "NumColumns";
	/** the number of columns containing data */
	public static final String NUMBER_COLUMS = "NumColums";

	public static final String ELEMENT = "Element";

	public static final String EXTERNAL = "ExternalParameterFile";
	/** The maximum row allowed to be empty before finishing reading data */
	public static final String MAX_EMPTY_ROWS = "MaxEmptyRows";
	/** If present and all the mandatory dimensions can be resolved when parsing the empty observation cell,
	 ** the Converter will create an observation with the observation value
	 ** equals with the value specified in the DefaultValue parameter */
	public static final String DEFAULT_VALUE = "DefaultValue";
	/** When the actual value of cells is needed for the output
	 ** we need to set this parameter on the parameter sheet with the value actualValue */
	public static final String FORMAT_VALUES = "formatValues";
	/** The parameter value should be an integer between 0 and 15.
	 ** If it is 0 the numbers will be rounded without decimals.
	 ** If the parameter value is greater than 15 then the value will be ignored and 15 will be used*/
	public static final String NUMBER_ROUNDING_PRECISION = "RoundingPrecision";

	public static final String DEFAULT_DECIMAL_PATTERN = "#.######";
	/** After an observation value is rounded according to parameter Rounding Precision,
	 ** the value is checked again and if the number of digits that remain
	 ** are more from the maxLength attribute of the DSD, then the value is rounded again. */
	public static final String ROUND_TO_FIT = "RoundToFit";
	/**
	 * A new parameter sheet in the parameter file, the value expected in this parameter is the name of another file which is “pre-processing sheet”.
	 * <hr>
	 * The pre-processing sheet will be codes that can be matched to DSD
	 * and, it will be placed to the position that can be matched to the input file too.
	 * <p>
	 * Elements not affected that should always be read from the input file/sheet :
	 * obs_value / the measure,
	 * obs_level attributes.
	 * </p>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1286">SDMXCONV-1286</a>*/
	public static final String CODES_FROM_FILE = "CodesFromExternalFile";

	public static final DecimalFormat DEFAULT_DECIMAL_FORMAT = (DecimalFormat) NumberFormat
			.getInstance(new Locale("en", "US"));

	public static final String OCCURRENCES = "Occurrences";

	/** character that separates multiple values of the same concept defined in the same cell */
	public static final String SUBFIELD_SEPARATOR = "SubfieldSeparator";

	public static final String ERROR_IF_EMPTY = "ErrorIfEmpty";

	static {
		DEFAULT_DECIMAL_FORMAT.applyPattern(DEFAULT_DECIMAL_PATTERN);
	}

	/**
	 * Handles exceptions by either throwing them or adding them to an exception handler.
	 * If the exception handler is null, an InvalidExcelParamsException is thrown.
	 * Otherwise, the exception is added to the exception handler.
	 * This method is designed to handle exceptions in a flexible way, allowing for different behaviors depending on the context.
	 *
	 * @param message The error message to be used in the exception.
	 * @param exceptionHandler The handler to which the exception should be added. If this is null, the exception will be thrown.
	 * @param position The position in the data where the error occurred.
	 * @throws InvalidExcelParamsException if the exception handler is null.
	 */
	public static void handeExceptions(String message, ExceptionHandler exceptionHandler, ErrorPosition position) throws InvalidExcelParamsException {
		if (exceptionHandler == null) {
			throw new InvalidExcelParamsException(message);
		} else {
			// SDMXCONV-740: This code handles the specific case of a data format exception related to parameter reading.
			exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.PARAMETERS_READER_ERROR, message, position.toString(), position, message));
		}
	}

	/**
	 * Handles exceptions silently by adding them to the handler without throwing an exception if the handler
	 * doesn't exist. This method is primarily used in getCell methods and reports errors in the
	 * reading process.
	 *
	 * @param message The error message to be handled.
	 * @param exceptionHandler The handler to which the exception is added.
	 * @param position The position where the error occurred.
	 */
	public static void handleExceptionSilently(String message, ExceptionHandler exceptionHandler, ErrorPosition position) {
		if (exceptionHandler != null) {
			// Handle the exception with a specific format (SDMXCONV-740)
			exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.WORKBOOK_READER_ERROR, message, position.toString(), position, message));
		}
	}

	/**
	 * This method reads the names of the parameter sheets from an Excel file.
	 * It supports both .xlsx/.xlsm and .xls formats.
	 *
	 * @param excelFile The Excel file to read from.
	 * @param inflateRatio The ratio used for inflating the .xls files.
	 * @return A list of parameter sheet names.
	 * @throws IOException If an I/O error occurs reading from the file or a malformed or unmappable byte sequence is read.
	 * @throws InvalidFormatException If the file format is not valid.
	 */
	public static List<String> readExcelParameterSheetNames(File excelFile, Double inflateRatio) throws IOException, InvalidFormatException {
		List<String> result;
		InputStream excelInputStream = null;
		BufferedInputStream bis = null;
		try {
			String extension = FilenameUtils.getExtension(excelFile.getName());
			excelInputStream = Files.newInputStream(excelFile.toPath());
			bis = new BufferedInputStream(excelInputStream);
			// Determine the file format to decide the processing method
			if (FileMagic.valueOf(bis).equals(FileMagic.OOXML)) {
				// For .xlsx and .xlsm formats
				result = readExcelParameterSheetNamesXlsx(bis);
			} else {
				// For .xls format
				result = readExcelParameterSheetNames(bis, inflateRatio);
			}
		} finally {
			// Ensure the streams are closed, even if an error occurs
			IOUtils.closeQuietly(excelInputStream);
			IOUtils.closeQuietly(bis);
		}
		return result;
	}


	/**
	 * Reads the sheet names of the Excel file and returns a list with those
	 * starting with Parameter. This method is called from the interface, so we need
	 * to check file type again
	 *
	 * @param parameterFile File contains the parameters
	 * @return a List of Names of sheets containing parameters configurations
	 * @throws IOException Input/Output Exception
	 * @throws InvalidFormatException Exception during opening and reading Excel
	 */
	public static List<String> readExcelParameterSheetNames(final InputStream parameterFile, Double inflateRatio) throws IOException, InvalidFormatException {
		List<String> excelParameterNames = new ArrayList<>();
		BufferedInputStream bis = null;
		Workbook workbook = null;
		try {
			if (inflateRatio!= null)
				ZipSecureFile.setMinInflateRatio(inflateRatio);
			bis = new BufferedInputStream(parameterFile);
			// Create Workbook instance
			if (FileMagic.valueOf(bis).equals(FileMagic.OOXML)) {
				// Xlsx and xlsm
				workbook = StreamingReader.builder().rowCacheSize(5000) // number of rows to keep in memory (defaults to
						// 10)
						// .bufferSize(4096) // buffer size to use when reading InputStream to file
						// (defaults to 1024)
						.open(bis); // InputStream or File for XLSX file (required)
			} else {
				// Xls
				workbook = WorkbookFactory.create(bis);
			}
			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				// Get first/desired sheet from the workbook
				Sheet sheet = workbook.getSheetAt(i);
				if (sheet.getSheetName().startsWith(PARAMETER_SHEET_NAME)
						&& !sheet.getSheetName().startsWith(PARAMETER_MAP_SHEET_NAME)) {
					excelParameterNames.add(sheet.getSheetName());
				}
			}
		} finally {
			// SDMXCONV-833
			if (workbook != null) {
				workbook.close();
			}
			IOUtils.closeQuietly(parameterFile);
			IOUtils.closeQuietly(bis);
		}
		return excelParameterNames;
	}

	/**
	 * Alternative method when parsing Xlsx Reads the sheet names of the Excel file
	 * and returns a list with those starting with Parameter.
	 *
	 * @param parameterFile The file containing the parameter sheet
	 * @return a List of Names of sheets containing parameters configurations
	 * @throws IOException Input/Output exception
	 */
	public static List<String> readExcelParameterSheetNamesXlsx(final InputStream parameterFile) throws IOException {
		List<String> excelParameterNames = new ArrayList<>();
        try (Workbook workbook = StreamingReader.builder().rowCacheSize(5000)
				// number of rows to keep in memory (defaults to 10)
                // .bufferSize(4096) buffer size to use when reading InputStream to file
                // (defaults to 1024)
                .open(parameterFile)) {
            // Create Workbook instance
            // number of rows to keep in memory (defaults to 10)
            // .bufferSize(4096) buffer size to use when reading InputStream to file
            // (defaults to 1024)
            // InputStream or File for XLSX file (required)
            for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                // Get first/desired sheet from the workbook
                Sheet sheet = workbook.getSheetAt(i);
                if (sheet.getSheetName().startsWith(PARAMETER_SHEET_NAME)
                        && !sheet.getSheetName().startsWith(PARAMETER_MAP_SHEET_NAME)) {
                    excelParameterNames.add(sheet.getSheetName());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // SDMXCONV-833
            IOUtils.closeQuietly(parameterFile);
        }
		return excelParameterNames;
	}

	/**
	 * Given the mapping file stream between data sheets and parameter sheets a map
	 * which for each data sheet the corresponding parameter sheet name that applies
	 * to the data sheet.
	 *
	 * @param mappingInputStream Stream of input file
	 * @return Map<String, String> - for each data sheet the corresponding parameter
	 *         sheet name is provided
	 */
	public static LinkedHashMap<String, ArrayList<String>> getParametersMap(final InputStream mappingInputStream)
			throws IOException {
		LinkedHashMap<String, ArrayList<String>> mappingParameters = new LinkedHashMap<>();
		try {
			String line;
			try (BufferedReader brdo = new BufferedReader(new InputStreamReader(mappingInputStream))) {
				while ((line = brdo.readLine()) != null) {
					String[] info = line.split("=", -1);
					String dataSheetName = info[0].trim();
					String parameterName = info[1].trim();
					// As value return an array with the list of values for this datasheet
					if (!dataSheetName.isEmpty()) {
						if (mappingParameters.containsKey(dataSheetName)) {
							mappingParameters.get(dataSheetName).add(parameterName);
						} else {
							ArrayList<String> arr = new ArrayList<>();
							arr.add(parameterName);
							mappingParameters.put(dataSheetName, arr);
						}
					}
				}
			}
		} finally {
			IOUtils.closeQuietly(mappingInputStream);
		}
		return mappingParameters;
	}

	/**
	 * Get a list of the names of sheets that are mapped only those will be
	 * processed
	 *
	 * @param mappingParams The mapping of parameters sheets.
	 * @return name of sheets with data
	 */
	public static List<String> allParamSheetNamesFromMapping(Map<String, ArrayList<String>> mappingParams) {
		List<String> paramSheetNames = new ArrayList<>();
		if (mappingParams == null)
			return null;
		for (String dataSheets : mappingParams.keySet()) {
			ArrayList<String> paramSheets = mappingParams.get(dataSheets);
			for (String paramSheetName : paramSheets) {
				paramSheetNames.add(paramSheetName);
			}
		}
		if (paramSheetNames != null && !paramSheetNames.isEmpty()) {
			// Remove duplicates
            Set<String> hs = new HashSet<>(paramSheetNames);
			paramSheetNames.clear();
			paramSheetNames.addAll(hs);
		}
		return paramSheetNames;
	}

	/**
	 * <p>Parse excel Parameter Sheet.</p>
	 * <p>Headers are required to be able to parse this Sheet correctly.</p>
	 * <p>After Parsed we set Excel Configuration Object.</p>
	 *
	 * @implNote Alternative implementation for Xlsx streaming parsing
	 */
	public static ExcelConfiguration readExcelParametersXlsx(Sheet sheet, ExceptionHandler exceptionHandler) throws InvalidExcelParamsException {
		String sheetName = sheet.getSheetName();
		// For each row an ExcelParameter is built
		ExcelConfiguration excelParameter = new ExcelConfiguration();
		excelParameter.setParameterName(sheet.getSheetName());
		int rowPosition = 0;
		//The column for occurrences
		int columnOfOccurrences = 0;
		// Iterate through each rows one by one
		Iterator<Row> rowIterator = sheet.iterator();
		List<Integer> skipRowsArray = new ArrayList<>();
		List<Integer> skipColumnsArray = new ArrayList<>();
		boolean hasOccurrences = false;
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			// For each row, iterate through all the columns
			Iterator<Cell> cellIterator = row.cellIterator();
			// first row should contain the headers and maybe the DataStart
			if (rowPosition == 0) {
				Cell cellFirstRow = getNextCell(cellIterator);
				if (!ELEMENT.equalsIgnoreCase(getNextParameterCellValue(cellFirstRow))) {
					String msg = "Parameter Sheet " + sheetName + " not formatted as expected: "
							+ "'Element' not found where expected.";
					handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(cellFirstRow, sheetName)));
				}
				cellFirstRow = getNextCell(cellIterator);
				if (!"Type".equalsIgnoreCase(getNextParameterCellValue(cellFirstRow))) {
					String msg = "Parameter Sheet " + sheetName + " not formatted as expected. "
							+ "'Type' not found where expected.";
					handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(cellFirstRow, sheetName)));
				}
				cellFirstRow = getNextCell(cellIterator);
				if (!"PosType".equalsIgnoreCase(getNextParameterCellValue(cellFirstRow))) {
					String msg = "Parameter Sheet " + sheetName + " not formatted as expected. "
							+ "'PosType' not found where expected.";
					handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(cellFirstRow, sheetName)));
				}
				cellFirstRow = getNextCell(cellIterator);
				if (!"Position".equalsIgnoreCase(getNextParameterCellValue(cellFirstRow))) {
					String msg = "Parameter Sheet " + sheetName + " not formatted as expected. "
							+ "'Position' not found where expected.";
					handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(cellFirstRow, sheetName)));
				}
				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					if (DATA_START.equalsIgnoreCase(getParameterCellValue(cell))) {
						cell = cellIterator.next();
						excelParameter.setDataStart(getParameterCellValue(cell));
					} else if (OCCURRENCES.equalsIgnoreCase(getParameterCellValue(cell))) {
						//if there is a column in the parameters sheet that has header "Occurrences".
						hasOccurrences = true;
						//Set the index of the column we found the "occurrences" for a component.
						columnOfOccurrences = cell.getColumnIndex();
					}
				}
			} else {
				if (ExcelSheetUtils.isNotEmptyRow(row)) {
					ExcelDimAttrConfig excelParameterElement = new ExcelDimAttrConfig();
					Cell cell = getNextCell(cellIterator);
					excelParameterElement.setId(getNextParameterCellValue(cell));
					cell = getNextCell(cellIterator);
					String type = getNextParameterCellValue(cell);

					if (ObjectUtil.validString(type)) {
						type = type.trim().toUpperCase();
						if (!ExcelElementType.ATT.getValue().equals(type) && !ExcelElementType.DIM.getValue()
								.equals(type) && !ExcelElementType.MES.getValue().equals(type)) {
							handeExceptions(type + " is not a valid Type Parameter", exceptionHandler,
									new ErrorPosition(getCellReferenceAsString(cell, sheetName)));
						}
						excelParameterElement.setType(ExcelElementType.valueOf(type));
					}
					cell = getNextCell(cellIterator);
					String positionType = getNextParameterCellValue(cell);
					if (ObjectUtil.validString(positionType)) {
						positionType = positionType.trim().toUpperCase();
						if (!ExcelPositionType.isValidPositionType(positionType)) {
							handeExceptions(positionType + " is not a valid Position Type", exceptionHandler,
									new ErrorPosition(getCellReferenceAsString(cell, sheetName)));
						}
						excelParameterElement.setPositionType(ExcelPositionType.valueOf(positionType));
						if (excelParameterElement.getPositionType() == ExcelPositionType.FIX) {
							excelParameterElement.setPosition(getCellReferenceAsString(cell, sheetName));
						}
					}
					// Parse Position Type
					if (ObjectUtil.validObject(excelParameterElement.getPositionType())){
						switch (excelParameterElement.getPositionType()) {
							case FIX: {
								cell = getNextCell(cellIterator);
								excelParameterElement.setPosition(getCellReferenceAsString(cell, sheetName));
								excelParameterElement.setFixValue(getNextParameterCellValue(cell));
								break;
							}
							case MIXED:
								excelParameterElement.setSubpositions(
										parseMixedPosition(cellIterator, exceptionHandler, sheetName));

								break;
							case SKIP:
								break;
							case CELL:
							case CELL_EXT:
							case ROW:
							case ROW_EXT:
							case COLUMN:
							case COLUMN_EXT:
							case OBS_LEVEL: {
								cell = getNextCell(cellIterator);
								String cellValuePosition = getNextParameterCellValue(cell);
								/* If the value contains "/" then we have multiple values inside a Cell such as
								 * B12/1 we split the position from the inside cell position. */
								if (ObjectUtil.validString(cellValuePosition)) {
									cellValuePosition = cellValuePosition.trim().toUpperCase();
									if (cellValuePosition.contains("/")) {
										String[] strArray = cellValuePosition.split("/");
										excelParameterElement.setPosition(strArray[0]);
										excelParameterElement.setPositionInsideCell(
												Integer.parseInt(strArray[1].trim()));
									} else {
										excelParameterElement.setPosition(cellValuePosition);
									}
								}
								break;
							}
						}
					}
					//If there is an 'Occurrences' Column parse the values, for min-max.
					if (hasOccurrences) {
						Entry<Integer, OccurrenceBean> occurrences = parseOccurrences(cellIterator, columnOfOccurrences, exceptionHandler, sheet);
						if (ObjectUtil.validObject(occurrences) && ObjectUtil.validObject(occurrences.getKey(), occurrences.getValue())) {
							excelParameterElement.setMinOccurrences(occurrences.getKey());
							excelParameterElement.setMaxOccurrences(occurrences.getValue());
						}
					}
					while (cellIterator.hasNext()) {
						Cell currentCell = cellIterator.next();
						String cellValue = getParameterCellValue(currentCell);
						if (ObjectUtil.validString(cellValue)) {
							if (DATA_START.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setDataStart(getNextParameterCellValue(currentCell));
							}
							if (DATA_END.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setDataEnd(getNextParameterCellValue(currentCell));
							}
							if (SKIP_ROWS.equalsIgnoreCase(cellValue)) {
								try {
									currentCell = cellIterator.next();
									String cellSkipRowsValue = getNextParameterCellValue(currentCell);
									if (cellSkipRowsValue.contains(",")) {
										skipRowsArray = getArrayIntFromString(cellSkipRowsValue);
									} else {
										skipRowsArray.add((Integer.parseInt(cellSkipRowsValue.trim())) - 1);
									}
								} catch (Exception ex) {
									String msg = "Value for SkipRows parameter is not valid.";
									handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheetName)));
								}
							}
							if (SKIP_COLUMNS.equalsIgnoreCase(cellValue)) {
								try {
									currentCell = cellIterator.next();
									String cellSkipColumnsValue = getNextParameterCellValue(currentCell);
									if (cellSkipColumnsValue.contains(",")) {
										skipColumnsArray = getArrayIntFromString(cellSkipColumnsValue);
									} else {
										skipColumnsArray.add((Integer.parseInt(cellSkipColumnsValue.trim())) - 1);
									}
								} catch (Exception ex) {
									String msg = "Value for SkipColumns parameter is not valid.";
									handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheetName)));
								}
							}
							if (SKIP_OBSERVATION_WITH_VALUE.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setSkipObservationWithValue(getNextParameterCellValue(currentCell));
							}
							if (SKIP_INCOMPLETE_KEY.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setSkipIncompleteKey(Boolean.parseBoolean(getNextParameterCellValue(currentCell)));
							}
							if (CONCEPT_SEPARATOR.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setConceptSeparator(getNextParameterCellValue(currentCell));
							}
							if (TRANSCODING_SHEET.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setTranscodingSheet(getNextParameterCellValue(currentCell));
							}
							if (MISSING_OBS_CHARACTER.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setMissingObsCharacter(getNextParameterCellValue(currentCell));
							}
							if (NUMBER_COLUMNS.equalsIgnoreCase(cellValue) || NUMBER_COLUMS.equalsIgnoreCase(cellValue)) {
								try {
									currentCell = cellIterator.next();
									excelParameter.setNumberOfColumns((int) Double.parseDouble(getNextParameterCellValue(currentCell)));
								} catch (Exception ex) {
									String msg = "Value for NumColumns parameter is not a valid Integer";
									handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheetName)));
								}
							}
							if (MAX_NO_EMPTY_COLUMNS.equalsIgnoreCase(cellValue)
									|| MAX_EMPTY_COLUMNS.equalsIgnoreCase(cellValue)) {
								try {
									currentCell = cellIterator.next();
									excelParameter.setNumberOfEmptyColumns((int) Double.parseDouble(getNextParameterCellValue(currentCell).trim()));
								} catch (Exception ex) {
									String msg = "Value for MaxNumOfEmptyColums parameter is not a valid Integer";
									handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheetName)));
								}
							}
							if (MAX_EMPTY_ROWS.equalsIgnoreCase(cellValue)) {
								try {
									currentCell = cellIterator.next();
									String cellVal = getNextParameterCellValue(currentCell);
									excelParameter.setMaxEmptyRows((int) Double.parseDouble(cellVal));
								} catch (Exception ex) {
									String msg = "Value for MaxEmptyRows parameter is not a valid Integer.";
									handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheetName)));
								}
							}
							if (DEFAULT_VALUE.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								String defaultValue = getNextParameterCellValue(currentCell);
								if (defaultValue == null || defaultValue.isEmpty()) {
									defaultValue = " ";
								}
								excelParameter.setDefaultValue(defaultValue);
							}
							if (NUMBER_ROUNDING_PRECISION.equalsIgnoreCase(cellValue)) {
								try {
									currentCell = cellIterator.next();
									String cellVal = getNextParameterCellValue(currentCell);
									excelParameter.setDecimalFormat(buildExcelNumberFormat((int) Double.parseDouble(cellVal)));
								} catch (Exception ex) {
									String msg = "Value for RoundingPrecision parameter is not a valid.";
									handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheetName)));
								}
							}
							if (ROUND_TO_FIT.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								excelParameter.setRoundToFit(Boolean.parseBoolean(getNextParameterCellValue(currentCell)));
							}
							if (FORMAT_VALUES.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								String cellVal = getNextParameterCellValue(currentCell);
								if (!cellVal.equalsIgnoreCase("actualValue") && !cellVal.equalsIgnoreCase("customFormat")
										&& !cellVal.equalsIgnoreCase("asDisplayed")) {
									String msg = "The value given for FormatValues parameter is not valid! Default Value will be used."
											+ " Please give one of the following "
											+ "'actualValue', 'customFormat', 'asDisplayed'!";
									handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheetName)));
									excelParameter.setFormatValues(FormatValues.forDescription("customFormat"));
								} else {
									excelParameter.setFormatValues(FormatValues.forDescription(cellVal));
								}
							}
							//SDMXCONV-1286
							if (CODES_FROM_FILE.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								String codesFromFile = getNextParameterCellValue(currentCell);
								excelParameter.setCodesFromFile(codesFromFile);
							}
							//SDMXCONV-1514
							if (SUBFIELD_SEPARATOR.equalsIgnoreCase(cellValue)) {
								currentCell = cellIterator.next();
								String subfieldSeparator = getNextParameterCellValue(currentCell);
								excelParameter.setSubfieldSeparator(subfieldSeparator);
							}
/*							if(ERROR_IF_EMPTY.equalsIgnoreCase(cellValue)){
								currentCell = cellIterator.next();
								String errorIfEmpty = getNextParameterCellValue(currentCell);
								excelParameter.setErrorIfEmpty(Boolean.parseBoolean(errorIfEmpty));
							}*/
						}
					}
					//SDMXCONV-1151
					if (ObjectUtil.validObject(excelParameterElement.getType()) && ObjectUtil.validString(excelParameterElement.getId())) {
						excelParameter.getParameterElements().add(excelParameterElement);
					}
				}
			}
			rowPosition++;
		}
		excelParameter.setSkipRows(skipRowsArray);
		excelParameter.setSkipColumns(skipColumnsArray);
		return excelParameter;
	}

	/**
	 * <p>If occurrences exist set the configuration.</p>
	 *
	 * @param cellIterator     The cell iterator to find the current cell
	 * @param columnIndex      Column of the cell of occurrences
	 * @param exceptionHandler Handling Exceptions with Struval way
	 * @param sheet            Current sheet
	 * @return a pair of Integer and OccurrenceBean representing minOccurs and maxOccurs
	 * @throws InvalidExcelParamsException Exception if the values of minOccurs or maxOccurs are not Integers or "unbounded"
	 */
	private static Entry<Integer, OccurrenceBean> parseOccurrences(Iterator<Cell> cellIterator,
																   int columnIndex,
																   ExceptionHandler exceptionHandler,
																   Sheet sheet) throws InvalidExcelParamsException {
			int minOccurrences = 0;
			OccurrenceBean maxOccurrences = null;
			//Find the cell of the occurrences based on column index
			while (cellIterator.hasNext()) {
				Cell currentCell = cellIterator.next();
				if(ObjectUtil.validObject(currentCell) && currentCell.getColumnIndex() <= columnIndex) {
						String occurrences = getParameterCellValue(currentCell);
						if (ObjectUtil.validString(occurrences)) {
							String[] occurrencesArr = StringUtils.split(occurrences, "-");
							try {
								if (ObjectUtil.validObject(occurrencesArr[0]))
									minOccurrences = Integer.parseInt(occurrencesArr[0]);
								if (ObjectUtil.validObject(occurrencesArr[1])) {
									if (("unbounded").equalsIgnoreCase(occurrencesArr[1])) {
										maxOccurrences = UnboundedOccurrenceBeanImpl.getInstance();
									} else {
										maxOccurrences = new FiniteOccurrenceBeanImpl(Integer.parseInt(occurrencesArr[1]));
									}
								}
							} catch (Exception exception) {
								handeExceptions("Occurrences values cannot be parsed.", exceptionHandler, new ErrorPosition(getCellReferenceAsString(currentCell, sheet.getSheetName())));
							}
						return Pair.of(minOccurrences, maxOccurrences);
					}
					if(currentCell.getColumnIndex() == columnIndex) {
						break;
					}
				} else {
					return null;
				}
			}
        return null;
    }

	/**
	 * <p>When MIXED is used as PosType not all the combinations are allowed.
	 * For example <MIXED COLUMNS 5 ROW 20 means that if COLUMN 5 is found empty ROW 20 will be used. </p>
	 *
	 * @param cellIterator     Iterate left to right to the cells of Excel file
	 * @param exceptionHandler Report exceptions to handler for struval
	 * @param sheetName        Name of the sheet we are currently reading
	 * @return LinkedHashMap<ExcelPositionType, LinkedHashMap < String, Integer>> Map of subpositions
	 * @throws InvalidExcelParamsException If the file format is not valid.
	 */
	private static LinkedHashMap<ExcelPositionType, LinkedHashMap<String, Integer>> parseMixedPosition(Iterator<Cell> cellIterator,
																									   ExceptionHandler exceptionHandler,
																									   String sheetName) throws InvalidExcelParamsException {
		// Read the subpositions of mixed ROW/COLUMN/CELL
		LinkedHashMap<ExcelPositionType, LinkedHashMap<String, Integer>> subpositions = new LinkedHashMap<>();
		LinkedHashMap<String, Integer> subpositionsInsideCell1 = new LinkedHashMap<>();
		Cell cell = getNextCell(cellIterator);
		ExcelPositionType posType = ExcelPositionType.valueOf(getNextParameterCellValue(cell));
		cell = getNextCell(cellIterator);
		String cellValuePosition = getNextParameterCellValue(cell);
		/*
		 * If the value contains "/" then we have multiple values inside a Cell such as
		 * B12/1 we split the position from the inside cell position.
		 */
		subpositionsInsideCell1.clear();
		if (cellValuePosition.contains("/")) {
			String[] strArray1 = cellValuePosition.split("/");
			subpositionsInsideCell1.put(strArray1[0], Integer.parseInt(strArray1[1].trim()));
		} else {
			subpositionsInsideCell1.put(cellValuePosition, 0);
		}
		subpositions.put(posType, subpositionsInsideCell1);
		//SDMXCONV-1160 Mixed type should have two types of position throw error otherwise
		cell = getNextCell(cellIterator);
		String secondTypeOfMixed = getNextParameterCellValue(cell);
		cell = getNextCell(cellIterator);
		String secondValueOfMixed = getNextParameterCellValue(cell);
		if(ObjectUtil.validString(secondTypeOfMixed, secondValueOfMixed)) {
			posType = ExcelPositionType.valueOf(secondTypeOfMixed);
			cellValuePosition = secondValueOfMixed;
		} else {
			String msg = "Invalid value of MIXED Position Type.";
			handeExceptions(msg, exceptionHandler, new ErrorPosition(getCellReferenceAsString(cell, sheetName)));
		}
		/*
		 * If the value contains "/" then we have multiple values inside a Cell such as
		 * B12/1 we split the position from the inside cell position.
		 */
		LinkedHashMap<String, Integer> subpositionsInsideCell2 = new LinkedHashMap<>();
		subpositionsInsideCell2.clear();
		if (cellValuePosition != null && cellValuePosition.contains("/")) {
			String[] strArray2 = cellValuePosition.split("/");
			subpositionsInsideCell2.put(strArray2[0], Integer.parseInt(strArray2[1].trim()));
		} else {
			subpositionsInsideCell2.put(cellValuePosition, 0);
		}
		subpositions.put(posType, subpositionsInsideCell2);
		return subpositions;
	}

	private static Workbook setWorkbook(InputStream is, Double inflateRatio) {
		Workbook workbook;
		//SDMXCONV-874
		if (inflateRatio != null) ZipSecureFile.setMinInflateRatio(inflateRatio);
		try(BufferedInputStream bis = new BufferedInputStream(is)) {
			// Create Workbook instance
			if (FileMagic.valueOf(bis).equals(FileMagic.OOXML)) {
				logger.info("FileMagic Type of file " + FileMagic.OOXML + " streaming reader will be used.");
				// Xlsx and xlsm
				workbook = StreamingReader.builder()
						.rowCacheSize(5000) // number of rows to keep in memory (defaults to 10)
						//.bufferSize(4096) //buffer size to use when reading InputStream to file(defaults to 1024)
						//.sstCacheSize(4096) //experimental - size of SST cache
						.open(bis); // InputStream or File for XLSX file(required)
			} else {
				logger.info("FileMagic Type of file " + FileMagic.valueOf(bis) + " old reader will be used.");
				// Xls
				workbook = WorkbookFactory.create(bis);
			}
		} catch (IOException | EncryptedDocumentException e) {
			throw new SdmxException(e, SDMX_ERROR_CODE.SEMANTIC_ERROR, ExceptionCode.WORKBOOK_READER_ERROR, "ExcelDataReaderEngine could not open/read workbook.");
		} catch (OpenException ex) {
			//SDMXCONV-874
			throw new SdmxInternalServerException("ExcelDataReaderEngine could not open/read workbook." + ex.getCause());
		}
		return workbook;
	}

	public static LinkedHashMap<String, ExcelSheetUtils> readCodes(InputStream is, List<ExcelConfiguration> configurations, Double inflateRatio, ExceptionHandler exceptionHandler) throws InvalidExcelParamsException, IOException {
		Workbook workbook = setWorkbook(is, inflateRatio);
		LinkedHashMap<String, ExcelSheetUtils> codes = null;
		if(ObjectUtil.validCollection(configurations)) {
			try {
				codes = readCodesSheets(workbook, configurations, exceptionHandler);
			} finally {
				workbook.close();
			}
		}
		return codes;
	}

	/**
	 * <u>Method that reads the sheet of codes.</u>
	 * <p>For every sheet found we set ExcelSheetUtil object,
	 * which holds the rows of the sheets.</p>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1286">SDMXCONV-1286</a>
	 * @param workbook Workbook where the parameter sheets is in
	 * @param configurations All the parameters sheets Object
	 * @param exceptionHandler handler to catch nor errors
	 * @throws InvalidExcelParamsException misconfiguration errors
	 */
	private static LinkedHashMap<String, ExcelSheetUtils> readCodesSheets(Workbook workbook, List<ExcelConfiguration> configurations, ExceptionHandler exceptionHandler) throws InvalidExcelParamsException {
		LinkedHashMap<String, ExcelSheetUtils> codesSheetsUtilList = new LinkedHashMap<>();
		for(ExcelConfiguration configuration : configurations) {
			String sheetName = configuration.getCodesFromFile();
			codesSheetsUtilList.put(sheetName, ExcelUtils.getCodesFromSheet(workbook, sheetName, exceptionHandler));
		}
		return codesSheetsUtilList;
	}

	/**
	 * Method that sets sheet and the list of rows for the codes from codesFromFile
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1286">SDMXCONV-1286</a>
	 * @param workbook Workbook of the Excel input file
	 * @param codesSheetName The name of the sheet containing the codes
	 * @param exceptionHandler handler of exception
	 * @return LinkedHashMap A map where the name of the sheet is the key and the list of values is the value.
	 * @throws InvalidExcelParamsException misconfiguration type of errors
	 */
	public static ExcelSheetUtils getCodesFromSheet(Workbook workbook, String codesSheetName, ExceptionHandler exceptionHandler) throws InvalidExcelParamsException {
		ExcelSheetUtils excelSheetUtils = null;
		if (codesSheetName != null && !codesSheetName.isEmpty()) {
			Sheet codesSheet;
			try {
				codesSheet = workbook.getSheet(codesSheetName);
			} catch(Exception ex) {
				codesSheet = null;
			}
			if (codesSheet == null || codesSheet.getLastRowNum() == 0) {
				return null;
			}
			try {
				Sheet currentSheet = workbook.getSheet(codesSheetName);
				excelSheetUtils = new ExcelSheetUtils(currentSheet);
				List<Row> codeSheetRows = excelSheetUtils.getRows(currentSheet);
				if(ObjectUtil.validCollection(codeSheetRows)) {
					excelSheetUtils.setRows(codeSheetRows);
				} else {
					throw new InvalidExcelParamsException("The sheet "+ codesSheetName + " is empty");
				}
			} catch (InvalidExcelParamsException e) {
				String message = "Error while reading the codes Sheet." + e.getMessage();
				handeExceptions(message, exceptionHandler, new ErrorPosition(codesSheetName));
			}
		}
		return excelSheetUtils;
	}

	/**
	 * Method to set the transcoding rules if a transcoding sheet exists.
	 * This method attempts to read a transcoding sheet from the provided workbook and
	 * generate a map of transcoding values. If the sheet does not exist or an error occurs
	 * during reading, the method will return null.
	 *
	 * @param workbook The workbook to read the transcoding sheet from.
	 * @param transcodingSheet The name of the transcoding sheet in the workbook.
	 * @param exceptionHandler The exception handler to use for handling any exceptions that occur.
	 * @return A map of transcoding values read from the sheet, or null if the sheet does not exist or an error occurs.
	 * @throws InvalidExcelParamsException If the parameters provided to the Excel reader are invalid.
	 */
	public static LinkedHashMap<String, List<ExcelTranscodingValue>> getTranscodingForDataSheet(Workbook workbook,
																								String transcodingSheet, ExceptionHandler exceptionHandler) throws InvalidExcelParamsException {
		Sheet transSheet = null;
		// Check if the transcodingSheet is not null and not empty
		if (transcodingSheet != null && !transcodingSheet.isEmpty()) {
			try {
				// Attempt to get the sheet from the workbook
				transSheet = workbook.getSheet(transcodingSheet);
			} catch(Exception ex) {
			}
			// If the sheet is null or empty, return null
			if (transSheet == null || transSheet.getLastRowNum() == 0) {
				return null;
			}
			LinkedHashMap<String, List<ExcelTranscodingValue>> transMap = null;
			try {
				// Attempt to read the structure set map from the sheet
				transMap = ExcelUtils.readStructureSetMap(transSheet, exceptionHandler);
			} catch (InvalidExcelParamsException e) {
				// Handle the exception and log the error message
				String message = "Error while reading the transcoding Sheet." + e.getMessage();
				handeExceptions(message, exceptionHandler, new ErrorPosition(transSheet.getSheetName()));
			}
			// If the transcoding map is not null and not empty, return it
			if (transMap != null && !transMap.isEmpty()) {
				return transMap;
			}
		}
		// If no transcoding map was found or an error occurred, return null
		return null;
	}

	/**
	 * This method finds the code that the 'value' will be transcoded to
	 * @param cref
	 * @param value
	 * @param transcoding
	 * @return
	 */
	public static Entry<String, Boolean> getValue_fromTranscoding(final String cref, final String value, LinkedHashMap<String, List<ExcelTranscodingValue>> transcoding) {
		AbstractMap.Entry<String, Boolean> valueFormulaAware = new AbstractMap.SimpleEntry<>(value, false);
		String returnvalue;
		final LinkedHashMap<String, List<ExcelTranscodingValue>> structureSetMap = transcoding;
		final List<ExcelTranscodingValue> rule = structureSetMap.get(cref);
		boolean find = false;
		// check if the value belongs in any rule
		for (ExcelTranscodingValue found : rule) {
			if (found.getText().equals(value)) {
				returnvalue = found.getValue();
				valueFormulaAware = new AbstractMap.SimpleEntry<>(returnvalue, found.isFormula());
				find = true;
			}
		}
		if (!find) {
			valueFormulaAware = new AbstractMap.SimpleEntry<>(value, false);
		}
		return valueFormulaAware;
	}

	/**
	 * Returns the String value of the value contained in the given Cell.
	 *
	 * @param cell The current cell
	 * @return the String value of the value contained by the Cell
	 */
	public static String getParameterCellValue(Cell cell) {
		if (cell == null) {
			return null;
		}

		CellType cellType = (cell.getCellTypeEnum() == CellType.FORMULA) ? cell.getCachedFormulaResultTypeEnum()
				: cell.getCellTypeEnum();
		switch (cellType) {
		// In stream processing excel if a formula cell is found returns the value
		// inside "", so we need to do some trimming
		// Although we shouldn't have formula type at this point. Has todo something
		// with the cache
		case FORMULA:
			String value;
			try {
				value = StringUtils.strip(String.valueOf(cell.getStringCellValue()).trim(), "\"");
			} catch (Exception e1) {
				value = "";
			}
			return value;
		case BLANK:
			return "";

		case BOOLEAN:
			return String.valueOf(cell.getBooleanCellValue());

		case NUMERIC:
			String retValue;
			retValue = getString(cell);
			return retValue.replaceAll(",", "");

		case STRING:
			return cell.getStringCellValue();

		case ERROR:
			// SDMXCONV-963
			return cell.getCellFormula();

		default:
			return "";
		}
	}

	/**
	 * This method is used to convert the cell content into a string.
	 * It handles different cell types and formats.
	 *
	 * @param cell The cell from which the content is to be extracted.
	 * @return The cell content as a string.
	 */
	private static String getString(Cell cell) {
		String retValue; // The return value

		// Check if the cell has a date format
		if (DateUtil.isCellDateFormatted(cell)) {
			try {
				// Try to get the date value from the cell and format it
				Date d = cell.getDateCellValue();
				SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
				retValue = fmt.format(d);
			} catch (Exception e) {
				// If an exception occurs, set the cell type to STRING and get the value
				cell.setCellType(CellType.STRING);
				retValue = cell.getRichStringCellValue().toString();
			}
		} else {
			// Get the data format index of the cell
			short formatIndex = cell.getCellStyle().getDataFormat();

			// Check if the format index is greater than 164, which indicates a custom format
			if (formatIndex > 164) {
				CellFormat cellFormat = CellFormat.getInstance(cell.getCellStyle().getDataFormatString());
				retValue = cellFormat.apply(cell).text;
			} else {
				// If the format index is not greater than 164, format the cell value as a decimal
				retValue = DEFAULT_DECIMAL_FORMAT.format(cell.getNumericCellValue());
			}
		}

		return retValue; // Return the cell content as a string
	}


	/**
	 * Returns the String <b>value</b> contained in the given Cell and a <b>flag</b>
	 * that indicates if the value returned is a formula that cannot be computed.
	 * <p>
	 * This flag is used to know if we need to evaluate the formula again in the
	 * data input file. Common use case scenario is a Reference formula to a cell
	 * that refers to another sheet and we need to compute it later.
	 * </p>
	 *
	 * @param cell The current cell
	 * @return String, Boolean
	 */
	public static Entry<String, Boolean> getParameterCellValueWithFormula(Cell cell) {
		AbstractMap.Entry<String, Boolean> valueFormulaAware;
		if (cell == null) {
			return null;
		}

		CellType cellType = (cell.getCellTypeEnum() == CellType.FORMULA) ? cell.getCachedFormulaResultTypeEnum()
				: cell.getCellTypeEnum();

		switch (cellType) {
		// In stream processing excel if a formula cell is found returns the value
		// inside "", so we need to do some trimming
		// Although we shouldn't have formula type at this point.
		// Has to do something with the cache
		case FORMULA:
			String value;
			try {
				value = StringUtils.strip(String.valueOf(cell.getStringCellValue()).trim(), "\"");
			} catch (Exception e1) {
				// e1.printStackTrace();
				value = "";
			}
			valueFormulaAware = new AbstractMap.SimpleEntry<>(value, false);
			return valueFormulaAware;
            case BOOLEAN:
			valueFormulaAware = new AbstractMap.SimpleEntry<>(String.valueOf(cell.getBooleanCellValue()),
                    false);
			return valueFormulaAware;
		case NUMERIC:
			String retValue = getString(cell);
			valueFormulaAware = new AbstractMap.SimpleEntry<>(retValue.replaceAll(",", ""), false);
			return valueFormulaAware;
		case STRING:
			valueFormulaAware = new AbstractMap.SimpleEntry<>(cell.getStringCellValue(), false);
			return valueFormulaAware;
		case ERROR:
			// SDMXCONV-963
			String formula = cell.getCellFormula();
			valueFormulaAware = new AbstractMap.SimpleEntry<>(cell.getCellFormula(), true);
			if (!"".equals(formula)) {
				String toRemove = StringUtils.substringBetween(formula, "[", "]");
				String result = StringUtils.remove(formula, "[" + toRemove + "]");
				valueFormulaAware = new AbstractMap.SimpleEntry<>(result, true);
			}
			return valueFormulaAware;
        case BLANK:
        default:
			valueFormulaAware = new AbstractMap.SimpleEntry<>("", false);
			return valueFormulaAware;
		}
	}

	/**
	 * This method is used to get a map of parameters from a given file.
	 * It reads the file and calls the getParametersMap method with an InputStream of the file.
	 * It ensures that the InputStream is closed after use, even if an exception is thrown.
	 *
	 * @see #getParametersMap(InputStream) for details on how the map is generated.
	 *
	 * @param sheetMappingFile The file from which to read the parameters.
	 * @return A map where the keys are parameter names and the values are lists of associated values.
	 * @throws IOException If an I/O error occurs reading from the file or a malformed or unmappable byte sequence is read.
	 */
	public static Map<String, ArrayList<String>> getParametersMap(File sheetMappingFile) throws IOException {
		InputStream inpuStream = null;
		Map<String, ArrayList<String>> result;
		try {
			inpuStream = Files.newInputStream(sheetMappingFile.toPath());
			result = getParametersMap(inpuStream);
		} finally {
			IOUtils.closeQuietly(inpuStream);
		}
		return result;
	}


	/**
	 * Returns extension from the filename
	 *
	 * @param filename
	 * @return String file extension
	 */
	public static String getExcelExtension(String filename) {
		String ext;
		ext = FilenameUtils.getExtension(filename);
		return ext;
	}

	/**
	 * Moves to next cell in the iterator and gets the value. When it starts a row
	 * the iterator is with cell at position -1. Be aware that this method modifies
	 * the iterator by moving the cell position with one to the right.
	 *
	 * @return the String value of the value contained by the next Cell
	 */
	public static String getNextParameterCellValue(Cell cell) {
		if (ObjectUtil.validObject(cell)) {
			return getParameterCellValue(cell);
		}
		return null;
	}

	public static Cell getNextCell(Iterator<Cell> cellIterator) {
		if (cellIterator.hasNext()) {
            return cellIterator.next();
		}
		return null;
	}

	/**
	 * Get the cell as parameter and return the cell position as standard Excel
	 * reference format (e.g. NiceData!A1)
	 *
	 * @param cell The current cell
	 * @param sheetName The current sheet name
	 * @return String
	 */
	public static String getCellReferenceAsString(Cell cell, String sheetName) {
		String cellReferenceStr = null;
		if(ObjectUtil.validObject(cell)) {
			CellReference celRef = new CellReference(cell);
			cellReferenceStr = sheetName + "!" + celRef.formatAsString(false);
		}
		return cellReferenceStr;
	}

	/**
	 * Get the cell as parameter and return the cell position as standard Excel
	 * reference format (e.g. NiceData!A1)
	 *
	 * @param cell
	 * @return
	 */
	public static String getCellReferenceAsString(String cell, String sheetName) {
		String cellReferenceStr;
		CellReference celRef = new CellReference(cell);
		cellReferenceStr = sheetName + "!" + celRef.formatAsString(false);
		return cellReferenceStr;
	}

	/**
	 * returns true if the provided line is a parameter line
	 *
	 * @param line
	 * @return
	 */
	public static boolean isParameterElement(String line) {
		if ("".equalsIgnoreCase(line.trim())) {
			return false;
		}

		String[] info = line.split("\\s+", -1);

		// if line contains nothing or only empty spaces skip line - false
        return !ELEMENT.equalsIgnoreCase(info[0]) && !DATA_START.equalsIgnoreCase(info[0])
                && !DATA_END.equalsIgnoreCase(info[0]) && !SKIP_ROWS.equalsIgnoreCase(info[0])
                && !SKIP_OBSERVATION_WITH_VALUE.equalsIgnoreCase(info[0]) && !MAX_EMPTY_ROWS.equalsIgnoreCase(info[0])
                && !NUMBER_COLUMNS.equalsIgnoreCase(info[0]) && !MAX_NO_EMPTY_COLUMNS.equalsIgnoreCase(info[0])
                && !CONCEPT_SEPARATOR.equalsIgnoreCase(info[0]) && !TRANSCODING_SHEET.equalsIgnoreCase(info[0])
                && !MISSING_OBS_CHARACTER.equalsIgnoreCase(info[0]) && !SKIP_INCOMPLETE_KEY.equalsIgnoreCase(info[0])
                && !FORMAT_VALUES.equalsIgnoreCase(info[0]) && !NUMBER_COLUMS.equalsIgnoreCase(info[0])
                && !MAX_EMPTY_COLUMNS.equalsIgnoreCase(info[0]) && !SUBFIELD_SEPARATOR.equalsIgnoreCase(info[0]);
    }

	/**
	 * builds a decimal format for the provided rounding precision
	 *
	 * @param roundingPrecision the precision
	 * @return
	 */
	private static DecimalFormat buildExcelNumberFormat(int roundingPrecision) {
		DecimalFormat result = (DecimalFormat) NumberFormat.getInstance(new Locale("en", "US"));
		if (roundingPrecision == 0) {
			result.applyPattern("#");
		} else if (roundingPrecision > 0) {
			String formatPattern = "#.";
			for (int i = 1; i <= roundingPrecision; i++) {
				formatPattern += "#";
				if (i > 15) {
					break;
				}
			}
			result.applyPattern(formatPattern);
		}
		return result;
	}

	/**
	 * Returns the non-hidden sheets
	 *
	 * @param workbook
	 * @return
	 */
	public static List<String> getVisibleSheetNames(Workbook workbook) {
		List<String> result = new ArrayList<>();

		for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
			Sheet sheet = workbook.getSheetAt(i);
			if (!workbook.isSheetHidden(i)) {
				result.add(sheet.getSheetName());
			}
		}
		return result;
	}

	public static List<String> getSheetsWithValues(Workbook workbook) {
		List<String> result = new ArrayList<>();
		for (String sheetName : getVisibleSheetNames(workbook)) {
			if (!sheetName.startsWith(VAL_SHEET_NAME) && !sheetName.startsWith(PARAMETER_SHEET_NAME)
					&& !sheetName.startsWith(PARAMETER_MAP_SHEET_NAME)
					&& !sheetName.startsWith(TRANSCODING_SHEET_NAME)) {
				result.add(sheetName);
			}
		}
		return result;
	}

	public static int getColumnNumber(String columnAsLetterOrInteger) {
		int result = -1;
		if (columnAsLetterOrInteger.matches("[0-9]*")) {
			result = Integer.parseInt(columnAsLetterOrInteger) - 1;
		} else {
			result = CellReference.convertColStringToIndex(columnAsLetterOrInteger);
		}
		return result;
	}

	/**
	 * This method converts a comma-separated string into a list of integers.
	 * The input string is expected to contain integers. Each integer in the resulting list is reduced by one.
	 * This is because the original use case for this method was to convert 1-indexed values to 0-indexed values.
	 *
	 * @param cellValues A string of comma-separated integers.
	 * @return A list of integers derived from the input string, each reduced by one.
	 */
	private static List<Integer> getArrayIntFromString(String cellValues) {
		// Split the input string by comma
		String[] strArray = cellValues.split(",");

		// Initialize an empty list to store the converted integers
		List<Integer> intArray = new ArrayList<>();

		// Iterate over the split string array
		for (String s : strArray) {
			// Convert each string to an integer, reduce it by one, and add it to the list
			intArray.add((Integer.parseInt(s.trim())) - 1);
		}

		// Return the list of converted integers
		return intArray;
	}


	/**
	 * Shortcut method reads the structure set from input.
	 *
	 * @param sheet				Sheet of transcoding
	 * @param exceptionHandler	Handler of struval for exceptions
	 * @return LinkedHashMap<String, List<ExcelTranscodingValue>>
	 * @throws InvalidExcelParamsException
	 */
	public static LinkedHashMap<String, List<ExcelTranscodingValue>> readStructureSetMap(Sheet sheet, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException {
		LinkedHashMap<String, List<ExcelTranscodingValue>> result = readStructureSet(sheet, exceptionHandler);
		return result;
	}

	public static LinkedHashMap<String, List<ExcelTranscodingValue>> readStructureSet(Sheet sheet, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException {
		LinkedHashMap<String, List<ExcelTranscodingValue>> transcodingMap = new LinkedHashMap<>();
		Iterator<Row> rowIterator = sheet.iterator();
		String text;
		String dimension;
		String value;
		boolean isFormula;

		// first make the map
		int rowPosition = 0;
		while (rowIterator.hasNext()) {
			ExcelTranscodingValue trancoding = null;
			text = "";
			dimension = "";
			value = "";
			ErrorPosition position = null;
			Row row = rowIterator.next();
			// For each row, iterate through all the columns
			Iterator<Cell> cellIterator = row.cellIterator();
			if (cellIterator.hasNext()) {
				Cell cell0 = row.getCell(0);
				if (cell0 != null) {
					position = new ErrorPosition(getCellReferenceAsString(cell0, sheet.getSheetName()));
					text = getParameterCellValue(cell0).toString();
				}
				Cell cell1 = row.getCell(1);
				if (cell1 != null) {
					position = new ErrorPosition(getCellReferenceAsString(cell1, sheet.getSheetName()));
					dimension = getParameterCellValue(cell1).toString();
				}
				Cell cell2 = row.getCell(2);
				if (cell2 != null) {
					position = new ErrorPosition(getCellReferenceAsString(cell2, sheet.getSheetName()));
					Entry<String, Boolean> valueMap = getParameterCellValueWithFormula(cell2);
					value = valueMap.getKey();
					isFormula = valueMap.getValue();
					if (!dimension.isEmpty() & !text.isEmpty() & !"".equals(value)) {
						trancoding = new ExcelTranscodingValue(text, value, isFormula);
					}
				}

				if (rowPosition == 0) {
					if (!"Text".equalsIgnoreCase(text)) {
						String msg = "Transcoding Sheet " + sheet.getSheetName()
								+ " not formatted as expected. 'Text' not found where expected.";
						handeExceptions(msg, exceptionHandler, position);
					}
					if (!"Dimension".equalsIgnoreCase(dimension)) {
						String msg = "Transcoding Sheet " + sheet.getSheetName()
								+ " not formatted as expected. 'Dimension' not found where expected.";
						handeExceptions(msg, exceptionHandler, position);
					}
					if (!"Value".equalsIgnoreCase(value)) {
						String msg = "Transcoding Sheet " + sheet.getSheetName()
								+ " not formatted as expected. 'Value' not found where expected.";
						handeExceptions(msg, exceptionHandler, position);
					}
				} else {
					List<ExcelTranscodingValue> transcodingLs = new ArrayList<>();
					if (trancoding != null) {
						if (transcodingMap.containsKey(dimension)) {
							transcodingMap.get(dimension).add(trancoding);
						} else {
							transcodingLs.add(trancoding);
							transcodingMap.put(dimension, transcodingLs);
						}
					}
				}
			}
			rowPosition++;
		}
		return transcodingMap;
	}

	/**
	 * This method reads Excel configuration from a given workbook.
	 * It iterates through all the sheets in the workbook and adds the configuration to a list if the sheet name starts with PARAMETER_SHEET_NAME but not with PARAMETER_MAP_SHEET_NAME.
	 *
	 * @param workbook The workbook from which the Excel configuration is to be read.
	 * @param exceptionHandler The handler for exceptions that may occur during the reading process.
	 * @return A list of ExcelConfiguration objects read from the workbook.
	 * @throws InvalidExcelParamsException If the parameters in the Excel file are invalid.
	 */
	public static List<ExcelConfiguration> readExcelConfigFromWorkbook(Workbook workbook, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException {
		// Initialize the result list
		List<ExcelConfiguration> result = new ArrayList<>();

		// Iterate through all sheets in the workbook
		for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
			// Get the current sheet from the workbook
			Sheet sheet = workbook.getSheetAt(i);

			// Check if the sheet name starts with PARAMETER_SHEET_NAME but not with PARAMETER_MAP_SHEET_NAME
			if (sheet.getSheetName().startsWith(PARAMETER_SHEET_NAME)
					&& !sheet.getSheetName().startsWith(PARAMETER_MAP_SHEET_NAME)) {
				// If the condition is met, read the parameters from the sheet and add it to the result list
				result.add(readExcelParametersXlsx(sheet, exceptionHandler));
			}
		}

		// Return the list of ExcelConfiguration objects
		return result;
	}

	/**
	 * Reads Excel configuration from a given file.
	 *
	 * @param parameterFile The file containing the Excel configuration.
	 * @param exceptionHandler The handler for exceptions that may occur during the reading process.
	 * @return A list of ExcelConfiguration objects representing the configuration in the file.
	 * @throws IOException If there is an error reading the file.
	 * @throws InvalidExcelParamsException If the Excel parameters in the file are invalid.
	 * @throws InvalidFormatException If the file format is not supported.
	 */
	public static List<ExcelConfiguration> readExcelConfig(File parameterFile, ExceptionHandler exceptionHandler)
			throws IOException, InvalidExcelParamsException, InvalidFormatException {
		// Get the file extension to determine if it's an external file or not.
		String fileExtension = FilenameUtils.getExtension(parameterFile.getName());
		boolean isExternal = !fileExtension.startsWith("xl");
        // If the file extension starts with "xl", it's not an external file.
        InputStream paramStream = null;
		List<ExcelConfiguration> result;
		try {
			// Open the file for reading.
			paramStream = Files.newInputStream(parameterFile.toPath());
			// Read the Excel configuration from the file.
			result = readExcelConfig(paramStream, isExternal, exceptionHandler);
		} finally {
			// Ensure the input stream is closed, even if an error occurs.
			IOUtils.closeQuietly(paramStream);
		}
		// Return the read configuration.
		return result;
	}

	/**
	 * Reads the data from the provided InputStream. The data is interpreted differently
	 * depending on whether it is an external parameter file or parameter sheets.
	 * Returns a list of ExcelConfiguration objects, each containing information on how to process a data sheet.
	 * This method is primarily used for tests.
	 *
	 * @param parameterFile The InputStream from which to read the Excel configuration data.
	 * @param isExternal A boolean flag indicating whether the parameter file is external.
	 * @param exceptionHandler An ExceptionHandler to handle any exceptions that may occur during the reading process.
	 * @return A list of ExcelConfiguration objects.
	 * @throws IOException If an I/O error occurs reading from the file or a malformed or unmappable byte sequence is read.
	 * @throws InvalidExcelParamsException If the parameters in the Excel file are invalid.
	 * @throws InvalidFormatException If the format of the Excel file is invalid.
	 */
	public static List<ExcelConfiguration> readExcelConfig(final InputStream parameterFile, boolean isExternal, ExceptionHandler exceptionHandler)
			throws IOException, InvalidExcelParamsException, InvalidFormatException {
		List<ExcelConfiguration> excelParameters = new ArrayList<ExcelConfiguration>();
		try {
			if (isExternal) {
				ExcelConfiguration excelParameter = readExcelConfigFromText(parameterFile, exceptionHandler);
				excelParameters.add(excelParameter);
			} else {
				// The list of ExcelConfiguration objects will be populated during the reading process.
				excelParameters = readExcelConfigFromXlsx(parameterFile, exceptionHandler);
			}
		} finally {
			// Ensure the InputStream is closed quietly to avoid potential resource leaks.
			IOUtils.closeQuietly(parameterFile);
		}
		return excelParameters;
	}


	/**
	 * reads the parameters stored into the Excel workbook
	 *
	 * @param parameterFile
	 * @return List<ExcelConfiguration>
	 * @throws Exception
	 */
	public static List<ExcelConfiguration> readExcelConfigFromXlsx(final InputStream parameterFile, ExceptionHandler exceptionHandler)
			throws IOException, InvalidExcelParamsException, InvalidFormatException {
		List<ExcelConfiguration> excelParameters;
		BufferedInputStream bis = null;
		Workbook workbook = null;
		try {
			bis = new BufferedInputStream(parameterFile);
			// Create Workbook instance
			// Decide if we will stream process the file based on its format
			if (FileMagic.valueOf(bis).equals(FileMagic.OOXML)) {
				// Xlsx and xlsm
				workbook = StreamingReader.builder().rowCacheSize(5000) // number of rows to keep in memory (defaults to
																		// 10)
						// .bufferSize(4096) // buffer size to use when reading InputStream to file
						// (defaults to 1024)
						.open(bis); // InputStream or File for XLSX file (required)
				excelParameters = readExcelConfigFromWorkbookXlsx(workbook, exceptionHandler);
			} else {
				// Xls
				workbook = WorkbookFactory.create(bis);
				excelParameters = readExcelConfigFromWorkbook(workbook, exceptionHandler);
			}
		} finally {
			// SDMXCONV-833
			if (workbook != null) {
				workbook.close();
			}
			IOUtils.closeQuietly(parameterFile);
			IOUtils.closeQuietly(bis);
		}
		return excelParameters;
	}

	/**
	 * This method reads the Excel configuration from a given workbook.
	 * It iterates through each sheet in the workbook and if the sheet's name starts with PARAMETER_SHEET_NAME
	 * but does not start with PARAMETER_MAP_SHEET_NAME, it reads the Excel parameters from the sheet.
	 *
	 * @param workbook The workbook from which to read the Excel configuration.
	 * @param exceptionHandler The exception handler to use if an exception occurs during the reading process.
	 * @return A list of ExcelConfiguration objects that have been read from the workbook.
	 * @throws InvalidExcelParamsException If the parameters in the Excel file are invalid.
	 */
	public static List<ExcelConfiguration> readExcelConfigFromWorkbookXlsx(Workbook workbook, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException {
		List<ExcelConfiguration> result = new ArrayList<>();
		for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
			// Get the sheet from the workbook at the current index
			Sheet sheet = workbook.getSheetAt(i);

			// If the sheet's name starts with PARAMETER_SHEET_NAME but does not start with PARAMETER_MAP_SHEET_NAME,
			// read the Excel parameters from the sheet and add it to the result list
			if (sheet.getSheetName().startsWith(PARAMETER_SHEET_NAME)
					&& !sheet.getSheetName().startsWith(PARAMETER_MAP_SHEET_NAME)) {
				result.add(readExcelParametersXlsx(sheet, exceptionHandler));
			}
		}
		return result;
	}


	/**
	 * Method that read from the given input stream the parameters for processing an
	 * Excel file.
	 *
	 * @param parameterFile
	 * @param  exceptionHandler
	 * @return ExcelConfiguration
	 * @throws Exception
	 */
	public static ExcelConfiguration readExcelConfigFromText(final InputStream parameterFile, ExceptionHandler exceptionHandler)
			throws IOException, InvalidExcelParamsException {
		ExcelConfiguration excelParameter = new ExcelConfiguration();
		excelParameter.setParameterName(EXTERNAL);
		List<Integer> skipRowsArray = new ArrayList<>();
		List<Integer> skipColumnsArray = new ArrayList<>();
		try {
			String line = null;
			BufferedReader brdo = new BufferedReader(new InputStreamReader(parameterFile));
			while ((line = brdo.readLine()) != null) {
				String[] info = line.split("\\s+", -1);
				ExcelDimAttrConfig excelParameterElement = new ExcelDimAttrConfig();
				excelParameter.setSkipIncompleteKey(false); //Nadia's request 11/09/2019
				if (isParameterElement(line)) {
					excelParameterElement.setId(info[0].trim());

					String type = info[1].trim();
					if (!ExcelElementType.ATT.getValue().equals(type)
							&& !ExcelElementType.DIM.getValue().equals(type)) {
						String msg = type + " is not a valid Type Parameter";
						handeExceptions(msg, exceptionHandler, new ErrorPosition());
					}
					excelParameterElement.setType(ExcelElementType.valueOf(type));

					String positionType = info[2].trim();
					if (!ExcelPositionType.isValidPositionType(positionType)) {
						String msg = excelParameterElement.getPositionType() + " is not a valid Position Parameter!";
						handeExceptions(msg, exceptionHandler,  new ErrorPosition("TXT file"));
					}

					excelParameterElement.setPositionType(ExcelPositionType.valueOf(positionType));

					if (ExcelPositionType.MIXED.equals(excelParameterElement.getPositionType())) {
						// Read the subpositions of mixed ROW/COLUMN/CELL
						LinkedHashMap<ExcelPositionType, LinkedHashMap<String, Integer>> subpositions = new LinkedHashMap<ExcelPositionType, LinkedHashMap<String, Integer>>();
						LinkedHashMap<String, Integer> subpositionsInsideCell1 = new LinkedHashMap<String, Integer>();

						ExcelPositionType posType = ExcelPositionType.valueOf(info[3].trim());
						String cellValuePosition = info[4].trim();
						/*
						 * If the value contains "/" then we have multiple values inside a Cell such as
						 * B12/1 we split the position from the inside cell position.
						 */
						subpositionsInsideCell1.clear();
						if (cellValuePosition.contains("/")) {
							String[] strArray1 = cellValuePosition.split("/");
							subpositionsInsideCell1.put(strArray1[0], Integer.parseInt(strArray1[1].trim()));
						} else {
							subpositionsInsideCell1.put(cellValuePosition, 0);
						}
						subpositions.put(posType, subpositionsInsideCell1);
						//
						posType = ExcelPositionType.valueOf(info[5].trim());
						cellValuePosition = info[6].trim();

						/*
						 * If the value contains "/" then we have multiple values inside a Cell such as
						 * B12/1 we split the position from the inside cell position.
						 */
						LinkedHashMap<String, Integer> subpositionsInsideCell2 = new LinkedHashMap<String, Integer>();
						subpositionsInsideCell2.clear();
						if (cellValuePosition.contains("/")) {
							String[] strArray2 = cellValuePosition.split("/");
							subpositionsInsideCell2.put(strArray2[0], Integer.parseInt(strArray2[1].trim()));
						} else {
							subpositionsInsideCell2.put(cellValuePosition, 0);
						}
						subpositions.put(posType, subpositionsInsideCell2);
						excelParameterElement.setSubpositions(subpositions);

					} else if (ExcelPositionType.FIX.equals(excelParameterElement.getPositionType())) {
						String fixedValue = "";
						fixedValue += info[3];
						excelParameterElement.setFixValue(fixedValue);
					} else if (ExcelPositionType.SKIP.equals(excelParameterElement.getPositionType())) {
						// just SKIP
					} else {
						String cellValuePosition = info[3].trim().toUpperCase();
						/*
						 * If the value contains "/" then we have multiple values inside a Cell such as
						 * B12/1 we split the position from the inside cell position.
						 */
						if (cellValuePosition.contains("/")) {
							String[] strArray = cellValuePosition.split("/");
							excelParameterElement.setPosition(strArray[0]);
							excelParameterElement.setPositionInsideCell(Integer.parseInt(strArray[1].trim()));
						} else {
							excelParameterElement.setPosition(cellValuePosition);
						}
					}
				}
				// trying to find the DataStart, NumColumns and MaxNumberOfEmptyColumns
				for (int i = 0; i < info.length; i++) {
					if (DATA_START.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setDataStart(info[i + 1].trim());
						break;
					}

					if (DATA_END.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setDataEnd(info[i + 1].trim());
						break;
					}

					if (SKIP_ROWS.equalsIgnoreCase(info[i].trim())) {
						String cellSkipRowsValue = info[i + 1].trim();
						if (cellSkipRowsValue.contains(",")) {
							skipRowsArray = getArrayIntFromString(cellSkipRowsValue);
						} else {
							skipRowsArray.add((Integer.parseInt(cellSkipRowsValue.trim())) - 1);
						}
						break;
					}

					if (SKIP_COLUMNS.equalsIgnoreCase(info[i].trim())) {
						String cellSkipColumnsValue = info[i + 1].trim();
						if (cellSkipColumnsValue.contains(",")) {
							skipColumnsArray = getArrayIntFromString(cellSkipColumnsValue);
						} else {
							skipColumnsArray.add((Integer.parseInt(cellSkipColumnsValue.trim())) - 1);
						}
						break;
					}

					if (SKIP_OBSERVATION_WITH_VALUE.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setSkipObservationWithValue(info[i + 1].trim());
						break;
					}

					if (SKIP_INCOMPLETE_KEY.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setSkipIncompleteKey(Boolean.parseBoolean(info[i + 1].trim()));
						break;
					}

					if (CONCEPT_SEPARATOR.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setConceptSeparator(info[i + 1].trim());
						break;
					}
					if (SUBFIELD_SEPARATOR.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setSubfieldSeparator(info[i + 1].trim());
						break;
					}
					if (TRANSCODING_SHEET.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setTranscodingSheet(info[i + 1].trim());
						break;
					}
					if (MISSING_OBS_CHARACTER.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setMissingObsCharacter(info[i + 1].trim());
						break;
					}

					if (NUMBER_COLUMNS.equalsIgnoreCase(info[i].trim())
							|| NUMBER_COLUMS.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setNumberOfColumns(Integer.parseInt(info[i + 1].trim()));
						break;
					}

					if (MAX_NO_EMPTY_COLUMNS.equalsIgnoreCase(info[i].trim())
							|| MAX_EMPTY_COLUMNS.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setNumberOfEmptyColumns(Integer.parseInt(info[i + 1].trim()));
						break;
					}

					if (MAX_EMPTY_ROWS.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setMaxEmptyRows(Integer.parseInt(info[i + 1].trim()));
						break;
					}
					if (DEFAULT_VALUE.equalsIgnoreCase(info[i].trim())) {
						String defaultValue = info[i + 1].trim();
						if (defaultValue == null || defaultValue.isEmpty()) {
							defaultValue = " ";
						}
						excelParameter.setDefaultValue(defaultValue);
						break;
					}
					if (NUMBER_ROUNDING_PRECISION.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setDecimalFormat(buildExcelNumberFormat(Integer.parseInt(info[i + 1].trim())));
						break;
					}
					if (ROUND_TO_FIT.equalsIgnoreCase(info[i].trim())) {
						excelParameter.setRoundToFit(Boolean.parseBoolean(info[i + 1].trim()));
					}

					if (FORMAT_VALUES.equalsIgnoreCase(info[i].trim())) {
						String cellVal = info[i + 1].trim();
						if (!cellVal.equalsIgnoreCase("actualValue") && !cellVal.equalsIgnoreCase("customFormat")
								&& !cellVal.equalsIgnoreCase("asDisplayed")) {
							logger.warn(
									"The value given for FormatValues parameter is not valid! Default Value will be used."
											+ " Please give one of the following "
											+ "-'actualValue', -'customFormat', -'asDisplayed'!");
							excelParameter.setFormatValues(FormatValues.forDescription("customFormat"));
						} else {
							excelParameter.setFormatValues(FormatValues.forDescription(cellVal));
						}
					}
				}
				if (excelParameterElement != null && excelParameterElement.getId() != null) {
					excelParameter.getParameterElements().add(excelParameterElement);
				}
			}
			excelParameter.setSkipRows(skipRowsArray);
			excelParameter.setSkipColumns(skipColumnsArray);
		} finally {
			IOUtils.closeQuietly(parameterFile);
		}
		return excelParameter;
	}

	/**
	 * @see #readExcelDataSheetNames(InputStream, Double)
	 * @param excelFile
	 * @return
	 * @throws IOException
	 */
	public static List<String> readExcelDataSheetNames(File excelFile, Double inflateRatio) throws IOException, InvalidFormatException {
		List<String> result;
		InputStream inputStream = null;
		try {
			inputStream = Files.newInputStream(excelFile.toPath());
			String extension = FilenameUtils.getExtension(excelFile.getName());
			result = readExcelDataSheetNames(inputStream, inflateRatio);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
		return result;
	}

	/**
	 * Reads the sheet names of the Excel file and returns a list with those not
	 * starting with Parameter or Val or Trans, meaning those are the data sheets.
	 *
	 * @param parameterFile
	 * @return a List of Names of sheets containing parameters configurations
	 * @throws Exception
	 */
	public static List<String> readExcelDataSheetNames(final InputStream parameterFile, Double inflateRatio) throws IOException {
		List<String> excelDataSheetNames = new ArrayList<>();
		BufferedInputStream bis = null;
		Workbook workbook = null;
		try {
			if (inflateRatio != null)
				ZipSecureFile.setMinInflateRatio(inflateRatio);

			bis = new BufferedInputStream(parameterFile);
			// Create Workbook instance
			// Decide if we will stream process the file based on its format
			if (FileMagic.valueOf(bis).equals(FileMagic.OOXML)) {
				// Xlsx and xlsm
				workbook = StreamingReader.builder().rowCacheSize(5000) // number of rows to keep in memory (defaults to
						// 10)
						// .bufferSize(4096) // buffer size to use when reading InputStream to file
						// (defaults to 1024)
						.open(bis); // InputStream or File for XLSX file (required)
			} else {
				// Xls
				workbook = WorkbookFactory.create(bis);
			}

			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				// Get first/desired sheet from the workbook
				Sheet sheet = workbook.getSheetAt(i);
				/*
				 * SDMXCONV-811 if (workbook.isSheetHidden(i)) { continue; }
				 */
				if (!sheet.getSheetName().startsWith(PARAMETER_SHEET_NAME)
						&& !sheet.getSheetName().startsWith(VAL_SHEET_NAME)
						&& !sheet.getSheetName().startsWith(TRANSCODING_SHEET_NAME)) {
					excelDataSheetNames.add(sheet.getSheetName());
				}
			}
		} finally {
			// SDMXCONV-833
			if (workbook != null) {
				workbook.close();
			}
			IOUtils.closeQuietly(parameterFile);
			IOUtils.closeQuietly(bis);
		}
		return excelDataSheetNames;
	}

	/**
	 * This method checks if the provided TreeMap has valid values.
	 * It first checks if the map itself is valid, then it iterates over the values to check their validity.
	 * The method uses ObjectUtil.validString to determine the validity of the values.
	 * If any value in the map is valid, the method returns true. If no valid values are found, it returns false.
	 *
	 * @param values TreeMap with Integer keys and String values to be validated.
	 * @return boolean indicating whether the map contains at least one valid value.
	 */
	public static boolean checkIfMapValidValues(TreeMap<Integer, String> values) {
		// Check if the map is valid
		if(!checkIfMapValid(values))
			return false;

		// Iterate over the values in the map
		for(String value : values.values()) {
			// If a valid value is found, return true
			if(ObjectUtil.validString(value)) {
				return true;
			}
		}
		// If no valid values are found, return false
		return false;
	}

	public static boolean checkIfMapValid(TreeMap<Integer, String> values) {
		if(!ObjectUtil.validMap(values))
			return false;
        return ObjectUtil.validCollection(values.values());
    }

	/**
	 * Checks if the concept reference has mapped rules.
	 *
	 * @param cref
	 * @param transcoding
	 * @return true if there are rules for ths concept reference
	 */
	public static boolean hasRules(final String cref, LinkedHashMap<String, List<ExcelTranscodingValue>> transcoding) {
		boolean hasRule = false;
		if (transcoding == null) {
			// there are no transcoding rules
			return hasRule;
		} else {
			if (transcoding.containsKey(cref)) {
				hasRule = true;
			}
			return hasRule;
		}
	}

	/**
	 * This method is used to compare a given dataSheetName with the keys in a mappingParameters map.
	 * If the dataSheetName exists as a key, the parameterName is added to the corresponding ArrayList.
	 * If the dataSheetName does not exist as a key, a new ArrayList is created, the parameterName is added to it,
	 * and this new ArrayList is added to the map with the dataSheetName as the key.
	 *
	 * @param mappingParameters A LinkedHashMap containing dataSheetNames as keys and an ArrayList of parameterNames as values.
	 * @param dataSheetName     The name of the data sheet to be compared with the keys in the map.
	 * @param parameterName     The name of the parameter to be added to the ArrayList corresponding to the dataSheetName key.
	 */
	public static void compareDataSheetName(LinkedHashMap<String, ArrayList<String>> mappingParameters, String dataSheetName, String parameterName) {
	// Check if dataSheetName is not an empty string
		if (!"".equals(dataSheetName)) {
			// Check if mappingParameters contains dataSheetName as a key
			if (mappingParameters.containsKey(dataSheetName)) {
				// If it does, add parameterName to the ArrayList corresponding to this key
				mappingParameters.get(dataSheetName).add(parameterName);
			} else {
				// If it does not, create a new ArrayList, add parameterName to it
				ArrayList<String> arr = new ArrayList<>();
				arr.add(parameterName);
				// Add this new ArrayList to mappingParameters with dataSheetName as the key
				mappingParameters.put(dataSheetName, arr);
			}
		}
	}

	/**
	 * This method retrieves a list of concept IDs from a given DataStructureBean.
	 * The method of retrieval depends on the version of the SDMX schema used.
	 *
	 * @param dsd The DataStructureBean from which to extract concept IDs.
	 * @param structureVersion The version of the SDMX schema used.
	 * @return A list of concept IDs as strings.
	 */
	public static List<String> getConceptsAsString(DataStructureBean dsd, SDMX_SCHEMA structureVersion) {
		// Initialize an empty list to store the concept IDs
		List<String> concepts = new ArrayList<>();

		// Check if the DataStructureBean is valid
		if(ObjectUtil.validObject(dsd)) {
			// Check if the components of the DataStructureBean are valid
			if(ObjectUtil.validCollection(dsd.getComponents())) {
				// Iterate over each component in the DataStructureBean
				for(ComponentBean component : dsd.getComponents()) {
					String id = null;
					// If the SDMX schema version is 2, use a specific method to retrieve the concept ID
					if(structureVersion == SDMX_SCHEMA.VERSION_TWO) {
						// Exclude the time dimension and primary measure from the concept ID retrieval
						if(component != dsd.getTimeDimension() && component != dsd.getPrimaryMeasure()) {
							id = ConceptRefUtil.getConceptId(component.getConceptRef());
						} else if(component == dsd.getTimeDimension()) {
							// If the component is the time dimension, get its ID directly
							id = dsd.getTimeDimension().getId();
						} else if(component == dsd.getPrimaryMeasure()) {
							// If the component is the primary measure, get its ID directly
							id = dsd.getPrimaryMeasure().getId();
						}
					} else {
						// If the SDMX schema version is not 2, get the component ID directly
						id = component.getId();
					}
					// Add the retrieved concept ID to the list
					concepts.add(id);
				}
			}
		}
		// Return the list of concept IDs
		return concepts;
	}


}
