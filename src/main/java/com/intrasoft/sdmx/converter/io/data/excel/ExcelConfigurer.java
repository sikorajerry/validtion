package com.intrasoft.sdmx.converter.io.data.excel;

import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.monitorjbl.xlsx.StreamingReader;
import com.monitorjbl.xlsx.exceptions.MissingSheetException;
import com.monitorjbl.xlsx.exceptions.OpenException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.util.IOUtils;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.sdmxsource.util.excel.ExcelTranscodingValue;
import org.estat.sdmxsource.util.excel.InvalidExcelParamsException;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.constants.SDMX_ERROR_CODE;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.exception.SdmxInternalServerException;
import org.sdmxsource.sdmx.api.exception.SdmxSyntaxException;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataFormatException;
import org.sdmxsource.util.ObjectUtil;

import java.io.*;
import java.util.*;

/**
 * Class that will set the configuration objects from file.
 */
public class ExcelConfigurer {

	private static Logger logger = LogManager.getLogger(ExcelConfigurer.class);

	private InputStream is;

	private volatile Workbook workbook;

	private volatile ExceptionHandler exceptionHandler;

	private volatile ExcelInputConfigImpl excelInputConfig;

	/** Transcoding rules read from trans sheet */
	private volatile LinkedHashMap<String, List<ExcelTranscodingValue>> transcoding;

	private volatile  LinkedHashMap<String, ArrayList<String>> excelParameterMultipleMap;

	private volatile Map<String, Integer> allDataSheetNames;

	public volatile LinkedHashMap<String, ArrayList<String>> mappingParamsSheets;

	private volatile boolean mappingInsideExcel;

	private volatile boolean transcodingInsideExcel;

	public LinkedHashMap<String, ArrayList<String>> getMappingParamsSheets() {
		return mappingParamsSheets;
	}

	public Map<String, Integer> getAllDataSheetNames() {
		return allDataSheetNames;
	}

	public LinkedHashMap<String, ArrayList<String>> getExcelParameterMultipleMap() {
		return excelParameterMultipleMap;
	}

	public LinkedHashMap<String, List<ExcelTranscodingValue>> getTranscoding() {
		return this.transcoding;
	}

	public void setTranscoding(LinkedHashMap<String, List<ExcelTranscodingValue>> trans) {
		this.transcoding = trans;
	}

	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	public void setExcelInputConfig(ExcelInputConfigImpl excelInputConfig) {
		this.excelInputConfig = excelInputConfig;
	}
	
	public LinkedHashMap<String, LinkedHashMap<String, List<ExcelTranscodingValue>>> getExcelInputConfigTranscodingMapSheets() {
		return this.excelInputConfig.getTranscodingMapSheets();
	}

	public ExcelConfigurer(Workbook workbook, ExceptionHandler exceptionHandler, ExcelInputConfigImpl excelInputConfig) {
		this.workbook = workbook;
		this.exceptionHandler = exceptionHandler;
		this.excelInputConfig = excelInputConfig;
	}

	public ExcelConfigurer(InputStream is, ExcelInputConfigImpl excelInputConfig, ExceptionHandler exceptionHandler) {
		this.is = is;
		this.excelInputConfig = excelInputConfig;
		this.exceptionHandler = exceptionHandler;
		this.workbook = setWorkbook(is);
	}

	public ExcelConfigurer(ExcelInputConfigImpl excelInputConfig) {
		this.excelInputConfig = excelInputConfig;
	}

	public ExcelConfigurer() {

	}

	private Workbook setWorkbook(InputStream is) {
		Workbook workbook;
		//SDMXCONV-874
		if (excelInputConfig.getInflateRatio() != null) ZipSecureFile.setMinInflateRatio(excelInputConfig.getInflateRatio());
		try(BufferedInputStream bis = new BufferedInputStream(is);) {
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

	/**
	 * Main method that will configure the objects needed to start reading an excel file.
	 * @param excelDataLocation
	 */
	public void run(ReadableDataLocation excelDataLocation) throws IOException {
		this.allDataSheetNames = findAllDataSheetNames();
		try(InputStream is = excelDataLocation.getInputStream()) {
			setMapping(is);
		}
		try(InputStream is = excelDataLocation.getInputStream()) {
			this.excelParameterMultipleMap = setMapBetweenMapParam(is);
		}
		//If parameter sheet never found throw an error
		if (excelInputConfig.getConfigurations() == null) {
			String msg = "EXCEL Parameters sheet missing, cannot read the data.";
			this.exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.PARAMETERS_READER_ERROR,
					msg,
					"The Dataset requires an embedded or external Parameters sheet to process the data.",
					new ErrorPosition(),
					msg));
		} else {
			//SDMXCONV-1057
			try {
				if(excelInputConfig.isTranscodingInsideExcel())
					transcodingSheetsRead(excelInputConfig.getConfigurations(), workbook, this.exceptionHandler);
			} catch (InvalidExcelParamsException ex) {
				String msg = ex.getMessage();
				this.exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.PARAMETERS_READER_ERROR,
						msg,
						"",
						new ErrorPosition(),
						msg));
			}
		}
	}

	private void setMapping(InputStream inputStream) {
		//if mapping is inside excel then read it and set the mapping object
		if (excelInputConfig!=null && excelInputConfig.hasMappingInsideExcel()) {
			try {
				excelInputConfig.setDataSheetWithParamSheetsMapping(readMappingSheets(inputStream, this.exceptionHandler));
			} catch (IOException e) {
				exceptionHandler.handleException(new SdmxInternalServerException("I/O Error while trying to read the mapping sheet inside input file."));
			} catch (InvalidExcelParamsException ex) {
				String msg = ex.getMessage();
				this.exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.PARAMETERS_READER_ERROR,
						msg,
						"",
						new ErrorPosition(),
						msg));
			}
		}
	}

	/**
	 * Compute the list of names of parameter Sheets and
	 * set the configuration in Excel Input Config.
	 */
	private LinkedHashMap<String, ArrayList<String>> setMapBetweenMapParam(InputStream is){
		/* Map between data sheets and excel parameters sheets */
		LinkedHashMap<String, ArrayList<String>> excelParameterMultipleMap = excelInputConfig.getDataSheetWithParamSheetsMapping();
		//If parameters is inside excel then read it and set the configurations object
		if (excelInputConfig.hasConfigInsideExcel()) {
			List<String> paramSheetNames = ExcelUtils.allParamSheetNamesFromMapping(excelParameterMultipleMap);
			try {
				excelInputConfig.setConfiguration(readExcelParameters(is, paramSheetNames, this.exceptionHandler));
				if(this.excelInputConfig.hasConfigInsideExcel()) {
					if(ObjectUtil.validCollection(excelInputConfig.getConfigurations())) {
						LinkedHashMap<String, ExcelSheetUtils> codeSheetUtils = new LinkedHashMap<String, ExcelSheetUtils>();
						for (int i = 0; i < excelInputConfig.getConfigurations().size(); i++) {
							if (ObjectUtil.validString(excelInputConfig.getConfigurations().get(i).getCodesFromFile()))
								codeSheetUtils.put(excelInputConfig.getConfigurations().get(i).getCodesFromFile(), ExcelUtils.getCodesFromSheet(workbook, excelInputConfig.getConfigurations().get(i).getCodesFromFile(), this.exceptionHandler));
						}
						//SDMXCONV-1286, after the configurations are set we set the codes sheet values
						excelInputConfig.setCodesSheetUtils(codeSheetUtils);
					}
				}
			} catch (EncryptedDocumentException | InvalidFormatException | IOException e) {
				exceptionHandler.handleException(new SdmxInternalServerException("I/O Error while trying to read the Parameters sheets inside input file."));
			} catch (InvalidExcelParamsException ex) {
				String msg = ex.getMessage();
				this.exceptionHandler.handleException(new SdmxDataFormatException(ExceptionCode.PARAMETERS_READER_ERROR,
						msg,
						"",
						new ErrorPosition(),
						msg));
			}
		}
		return excelParameterMultipleMap;
	}

	public List<ExcelConfiguration> readExcelParameters(InputStream inputStream, List<String> paramSheetNames, ExceptionHandler exceptionHandler)
			throws EncryptedDocumentException, InvalidFormatException, IOException, InvalidExcelParamsException {
		List<ExcelConfiguration> result = new ArrayList<>();
		BufferedInputStream bis = null;
		Workbook workbook = null;
		try {
			if (ObjectUtil.validObject(excelInputConfig) && excelInputConfig.getInflateRatio() != null)
				ZipSecureFile.setMinInflateRatio(excelInputConfig.getInflateRatio());
			bis = new BufferedInputStream(inputStream);
			// Create Workbook instance
			if (FileMagic.valueOf(bis).equals(FileMagic.OOXML)) {
				// Xlsx and xlsm
				workbook = StreamingReader.builder().rowCacheSize(5000) // number of rows to keep in memory (defaults to 10)
						// .bufferSize(4096) // buffer size to use when reading InputStream to file
						// (defaults to 1024)
						.open(bis); // InputStream or File for XLSX file (required)
			} else {
				// Xls
				workbook = WorkbookFactory.create(bis);
				this.workbook = workbook;
			}
			result = readExcelParametersXlsx(workbook, paramSheetNames, exceptionHandler);

		} catch (OpenException oe) {
			ExcelUtils.handeExceptions(oe.getCause().toString(), exceptionHandler, new ErrorPosition(workbook.toString()));
		} finally {
			// SDMXCONV-833
			if (workbook != null) {
				workbook.close();
			}
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(bis);
		}

		return result;
	}

	/**
	 * <p>Read the parameters sheets of a workbook. <br>
	 * <p>Read the transcoding sheets of a workbook, added for SDMXCONV-963.
	 * Firstly, this method will be called for the external parameters file.
	 * <p>We save every name of transconding sheets we find into transcodingMapSheets.
	 * Even if the sheet doesn't exist in that file.
	 * </p>
	 * <p>Second time this is called for the inside parameters sheets and
	 * searches for transcoding sheets from internal sheets but also searches
	 * for transcoding sheets that wasn't there in the external file.
	 * <hr><b>Priorities</b> added for SDMXCONV-980
	 * <ul>
	 * <li>Parameters external + transcoding external</li>
	 * <li>Parameters external + transcoding internal</li>
	 * <li>Parameters Internal + transcoding internal</li>
	 * </ul>
	 * @see #readExcelParameters(InputStream,List,ExceptionHandler)
	 * @param workbook
	 * @param paramSheetNames
	 * @param exceptionHandler
	 * @return
	 * @throws InvalidExcelParamsException
	 */
	private List<ExcelConfiguration> readExcelParametersXlsx(Workbook workbook, List<String> paramSheetNames, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException {
		List<ExcelConfiguration> result = new ArrayList<>();
		int count = 0;
		if (paramSheetNames != null && !paramSheetNames.isEmpty() && paramSheetNames.size() != 0) {
			for(String sheetName : paramSheetNames){
				try {
					Sheet sheet = workbook.getSheet(sheetName);
					result.add(ExcelUtils.readExcelParametersXlsx(sheet, exceptionHandler));
					count = count + 1; // We count the parameter sheets we found if none was there then return null
				} catch (MissingSheetException ex) {
					String message = "Error while fetching the parameter Sheet " + sheetName+ ", could not be found. ";
					ExcelUtils.handeExceptions(message, exceptionHandler, new ErrorPosition(sheetName));
				}
			}
		} else {
			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				// Get first/desired sheet from the workbook
				Sheet sheet = workbook.getSheetAt(i);
				if(sheet.getSheetName().startsWith(ExcelUtils.PARAMETER_SHEET_NAME)) {
					//SDMXCONV-1065
					result.add(ExcelUtils.readExcelParametersXlsx(sheet, exceptionHandler));
					count = count + 1; // We count the parameter sheets we found if none was there then return null
				}
			}
		}
		//SDMXCONV-980
		transcodingSheetsRead(result, workbook, exceptionHandler);
		if (count == 0) {
			return null;
		} else {
			return result;
		}
	}

	/**
	 * Get the transcoding for the sheet we are currently reading.
	 * <p>Firstly we check if the transcoding sheet name from the parameters exist inside input data file.
	 * if exists then we check if there is another one with the same name inside the ExcelUtils#transcodingMapSheets
	 * and if exists then we don't overwrite it because external has a higher priority.
	 * Else we add it inside the transcodingMapSheets.</p>
	 * <p>And then we check if a transcoding sheet with this name exists in the map and we set it inside ExcelUtils.</p>
	 *
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-963">SDMXCONV-963</a>
	 */
	public void setTranscoding(String transcodingSheetName) {
		try {
			LinkedHashMap<String, List<ExcelTranscodingValue>> transcoding;
			if (allDataSheetNames.containsKey(transcodingSheetName)) {
				transcoding = ExcelUtils.getTranscodingForDataSheet(workbook, transcodingSheetName, this.exceptionHandler);
				if (transcoding != null && !transcoding.isEmpty())
					addTranscodingMapSheets(transcodingSheetName, transcoding);
			}
			if (this.excelInputConfig.getTranscodingMapSheets()!= null && !this.excelInputConfig.getTranscodingMapSheets().isEmpty()) {
				if (this.excelInputConfig.getTranscodingMapSheets().containsKey(transcodingSheetName)) {
					setTranscoding(this.excelInputConfig.getTranscodingMapSheets().get(transcodingSheetName));
				}
			}
		} catch (InvalidExcelParamsException e) {
			if (this.exceptionHandler != null)
				exceptionHandler.handleException(new SdmxSyntaxException(ExceptionCode.PARAMETERS_READER_ERROR, e.getMessage()));
		}
	}

	private void transcodingSheetsRead(List<ExcelConfiguration> configs, Workbook workbook, ExceptionHandler exceptionHandler) throws InvalidExcelParamsException {
		// From parameters sheets we get the transcoding sheets names.
		// SDMXCONV-963, SDMXCONV-980
		for (ExcelConfiguration config : configs) {
			String transName = config.getTranscodingSheet();
			if (ObjectUtil.validString(transName) && !ObjectUtil.validMap(this.excelInputConfig.getTranscodingMapSheets())) {
				// We only add the sheet names at first.
				addTranscodingMapSheets(transName, null);
			}
		}
		// For every name in the transcodingMapSheets from whatever file
		// we read transcoding sheet and create the object to be used.
		if(this.excelInputConfig.getTranscodingMapSheets()!=null) {
			for (String transcodingSheetName : this.excelInputConfig.getTranscodingMapSheets().keySet()) {
				// read the transcoding sheet from workbook
				LinkedHashMap<String, List<ExcelTranscodingValue>> trans = null;
				try {
					trans = ExcelUtils.getTranscodingForDataSheet(workbook, transcodingSheetName, exceptionHandler);
				} catch (InvalidExcelParamsException e) {
					String message = "Error while fetching the transcoding Sheet:" + transcodingSheetName+ ". "+ e.getMessage();
					ExcelUtils.handeExceptions(message, exceptionHandler, new ErrorPosition(transcodingSheetName));
				}
				if (trans != null && (this.excelInputConfig.getTranscodingMapSheets().get(transcodingSheetName) == null)) {
					this.excelInputConfig.getTranscodingMapSheets().put(transcodingSheetName, trans);
				}
			}
			this.excelInputConfig.setTranscodingMapSheets(this.excelInputConfig.getTranscodingMapSheets());
		}
	}

	private void addTranscodingMapSheets(String sheetName, LinkedHashMap<String, List<ExcelTranscodingValue>> transcoding) {
		if (this.excelInputConfig.getTranscodingMapSheets() != null) {
			// SDMXCONV-963
			// if a transcoding sheet with the same name is already parsed
			// then we don't parse it again
			// it means that we have external sheet with the same name and it has a priority
			if (!this.excelInputConfig.getTranscodingMapSheets().containsKey(sheetName)) {
				this.excelInputConfig.getTranscodingMapSheets().put(sheetName, transcoding);
			}
		} else {
			this.excelInputConfig.setTranscodingMapSheets(new LinkedHashMap<>());
			this.excelInputConfig.getTranscodingMapSheets().put(sheetName, transcoding);
		}
	}


	/**
	 * Reads the mapping sheet either from external parameter Excel File or inside
	 * the input Excel File
	 *
	 * @param configFileIs
	 * @throws IOException
	 * @throws InvalidExcelParamsException
	 */
	public LinkedHashMap<String, ArrayList<String>> readMappingSheets(InputStream configFileIs, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException, IOException {
		LinkedHashMap<String, ArrayList<String>> mappingParamsSheets = null;
		// If there is a parameters File
		if (configFileIs != null) {
			mappingParamsSheets = getParametersMapSheet(configFileIs, exceptionHandler);
		}
		return mappingParamsSheets;
	}

	/**
	 * @param parameterFile stream
	 * @return parameter map sheet
	 * @throws IOException
	 */
	private LinkedHashMap<String, ArrayList<String>> getParametersMapSheet(InputStream parameterFile, ExceptionHandler exceptionHandler)
			throws IOException, InvalidExcelParamsException {
		// BufferedInputStream bis = null;
		LinkedHashMap<String, ArrayList<String>> result = null;
		Workbook workbook = null;
		BufferedInputStream bis = null;
		try {
			if (ObjectUtil.validObject(excelInputConfig) && excelInputConfig.getInflateRatio() != null)
				ZipSecureFile.setMinInflateRatio(excelInputConfig.getInflateRatio());
			bis = new BufferedInputStream(parameterFile);
			// Create Workbook instance
			if (FileMagic.valueOf(bis).equals(FileMagic.OOXML)) {
				// Xlsx and xlsm
				workbook = StreamingReader.builder().rowCacheSize(5000) // number of rows to keep in memory (defaults to 10)
						.open(bis); // InputStream or File for XLSX file (required)
				result = getParametersMapSheet(workbook, exceptionHandler);
			} else {
				// Xls
				workbook = WorkbookFactory.create(bis);
				result = getParametersMapSheet(workbook, exceptionHandler);
			}
		} catch (OpenException oe) {
			ExcelUtils.handeExceptions(oe.getCause().toString(), exceptionHandler, new ErrorPosition(workbook.toString()));
		} finally {
			// SDMXCONV-833
			if (workbook != null) {
				workbook.close();
			}
			IOUtils.closeQuietly(parameterFile);
			IOUtils.closeQuietly(bis);
		}
		return result;
	}

	/**
	 * Extracts parameter mappings from a specific sheet in the given workbook.
	 *
	 * @param workbook The Excel workbook to process
	 * @param exceptionHandler Handler for managing exceptions during processing
	 * @return A LinkedHashMap containing parameter mappings
	 * @throws InvalidExcelParamsException If the Excel parameters are invalid
	 */
	private LinkedHashMap<String, ArrayList<String>> getParametersMapSheet(Workbook workbook, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException {
		LinkedHashMap<String, ArrayList<String>> result = new LinkedHashMap<>();
		for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
			// Get desired sheet from the workbook
			Sheet sheet = workbook.getSheetAt(i);

			if (sheet.getSheetName().startsWith(ExcelUtils.PARAMETER_MAP_SHEET_NAME)) {
				result = getParametersMap(sheet, exceptionHandler);
			}
		}
		return result;
	}

	/**
	 * Takes as input an Excel File and the names of the parameters sheets we expect
	 * from the mapping reads the sheets and set all the parameters found
	 *
	 * @param excelFile
	 * @param paramSheetNames
	 * @return List<ExcelConfiguration> - all the Parameters found
	 * @throws EncryptedDocumentException
	 * @throws InvalidFormatException
	 * @throws IOException
	 * @throws InvalidExcelParamsException
	 */
	public final List<ExcelConfiguration> readExcelParameters(File excelFile, List<String> paramSheetNames, ExceptionHandler exceptionHandler)
			throws EncryptedDocumentException, InvalidFormatException, IOException, InvalidExcelParamsException {
		List<ExcelConfiguration> result;
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(excelFile);
			result = readExcelParameters(inputStream, paramSheetNames, exceptionHandler);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
		return result;
	}

	/**
	 * Given the mapping sheet create a mapping of parameter sheets and data sheets
	 * for each data sheet the corresponding parameter sheet name that applies to
	 * the data sheet
	 *
	 * @param mappingInputSheet Sheet inside excel with Mapping
	 * @return Map<String, String> - for each data sheet the corresponding parameter
	 *         sheet name is provided
	 */
	private LinkedHashMap<String, ArrayList<String>> getParametersMap(final Sheet mappingInputSheet, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException {
		LinkedHashMap<String, ArrayList<String>> multipleValuesMap = new LinkedHashMap<String, ArrayList<String>>();
		// Iterate through each rows one by one
		String sheetName = mappingInputSheet.getSheetName();
		Iterator<Row> rowIterator = mappingInputSheet.iterator();
		String dataSheetName = null;
		String parameterName = null;
		int rowPosition = 0;

		while (rowIterator.hasNext()) {
			dataSheetName = "";
			parameterName = "";
			Row row = rowIterator.next();
			// For each row, iterate through all the columns
			Iterator<Cell> cellIterator = row.cellIterator();
			// first row should contain the headers and maybe the DataStart
			if (rowPosition == 0) {
				Cell cellFirstRow = ExcelUtils.getNextCell(cellIterator);
				if (!"Data sheet".equalsIgnoreCase(ExcelUtils.getNextParameterCellValue(cellFirstRow))) {
					String msg = "Parameter map Sheet " + mappingInputSheet.getSheetName()
							+ " not formatted as expected. " + "'Data sheet' not found where expected.";
					ExcelUtils.handeExceptions(msg, exceptionHandler, new ErrorPosition(ExcelUtils.getCellReferenceAsString(cellFirstRow, mappingInputSheet.getSheetName())));
				}
				cellFirstRow = ExcelUtils.getNextCell(cellIterator);
				if (!"Parameter sheet".equalsIgnoreCase(ExcelUtils.getNextParameterCellValue(cellFirstRow))) {
					String msg = "Parameter map Sheet " + mappingInputSheet.getSheetName()
							+ " not formatted as expected. " + "'Parameter sheet' not found where expected.";
					ExcelUtils.handeExceptions(msg, exceptionHandler, new ErrorPosition(ExcelUtils.getCellReferenceAsString(cellFirstRow, mappingInputSheet.getSheetName())));
				}
			} else {
				if (cellIterator.hasNext()) {
					Cell cell0 = row.getCell(0);
					if (cell0 != null) {
						if (cell0.getCellType() == CellType.STRING) {
							dataSheetName = cell0.getStringCellValue().trim();
						}
					}
					Cell cell1 = row.getCell(1);
					if (cell1 != null) {
						if (cell1.getCellType() == CellType.STRING) {
							parameterName = cell1.getStringCellValue().trim();
						}
					}
				}
				// As value return an array with the list of values for this datasheet
				ExcelUtils.compareDataSheetName(multipleValuesMap, dataSheetName, parameterName);
			}
			rowPosition++;
		}
		return multipleValuesMap;
	}


	private LinkedHashMap<String, ArrayList<String>> getParametersMapSheet(File sheetMappingFile, ExceptionHandler exceptionHandler)
			throws IOException, InvalidExcelParamsException {
		LinkedHashMap<String, ArrayList<String>> result = new LinkedHashMap<>();
		try (InputStream inputStream = new FileInputStream(sheetMappingFile)) {
			result = getParametersMapSheet(inputStream, exceptionHandler);
		}
		return result;
	}

	/**
	 * Reads the mapping sheet either from external parameter Excel File or inside
	 * the input Excel File
	 *
	 * @param configFile
	 * @param inputFile
	 * @throws IOException
	 * @throws InvalidExcelParamsException
	 */
	public LinkedHashMap<String, ArrayList<String>> readMappingSheets(File configFile, File inputFile, ExceptionHandler exceptionHandler)
			throws InvalidExcelParamsException, IOException {
		this.mappingParamsSheets = null;
		// If there is a parameters File
		if (configFile != null) {
			this.mappingParamsSheets = getParametersMapSheet(configFile, exceptionHandler);
		}
		// and we found a mapping sheet
		if (this.mappingParamsSheets != null && !this.mappingParamsSheets.isEmpty() && this.mappingParamsSheets.size() != 0) {
			this.mappingInsideExcel = false;
			// If there was no mapping inside the external file check inside input file
		} else {
			this.mappingParamsSheets = getParametersMapSheet(inputFile, exceptionHandler);
			this.mappingInsideExcel = true;
		}
		return this.mappingParamsSheets;
	}

	/**
	 * Find the names of all the Sheets in the workbook.
	 *
	 * @return List<String>
	 */
	private Map<String, Integer> findAllDataSheetNames() {
		Map<String, Integer> sheetNames = new HashMap<>();
		for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
			sheetNames.put(workbook.getSheetName(i), i);
		}
		return sheetNames;
	}

}
