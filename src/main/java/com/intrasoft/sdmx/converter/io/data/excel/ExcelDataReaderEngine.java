package com.intrasoft.sdmx.converter.io.data.excel;

import com.intrasoft.sdmx.converter.services.ExcelUtils;
import com.intrasoft.sdmx.converter.services.ExcelUtils.READING_DIRECTION;
import com.intrasoft.sdmx.converter.structures.DataStructureScanner;
import com.monitorjbl.xlsx.StreamingReader;
import com.monitorjbl.xlsx.exceptions.OpenException;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.ss.format.CellFormat;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellReference;
import org.estat.sdmxsource.extension.model.ErrorReporter;
import org.estat.sdmxsource.util.excel.*;
import org.sdmxsource.sdmx.api.constants.*;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.exception.SdmxInternalServerException;
import org.sdmxsource.sdmx.api.exception.SdmxNoResultsException;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.base.RepresentationBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextFormatBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
import org.sdmxsource.sdmx.api.model.beans.registry.ProvisionAgreementBean;
import org.sdmxsource.sdmx.api.model.data.ComplexNodeValue;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.AnnotationType;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.RecordReaderCounter;
import org.sdmxsource.sdmx.dataparser.engine.reader.ThreadLocalOutputReporter;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.AnnotationBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.ComplexNodeValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyValueImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.KeyableImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.data.ObservationImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetHeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.base.AnnotationMutableBeanImpl;
import org.sdmxsource.sdmx.util.beans.reference.MaintainableRefBeanImpl;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.sdmx.validation.exceptions.TypeOfException;
import org.sdmxsource.util.ObjectUtil;
import org.sdmxsource.util.io.ByteCountHolder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import static com.intrasoft.sdmx.converter.io.data.excel.ExcelCellUtils.*;
import static com.intrasoft.sdmx.converter.services.ExcelUtils.checkIfMapValidValues;
import static com.intrasoft.sdmx.converter.services.ExcelUtils.getConceptsAsString;

/**
 * <h2>Reader Engine that complies with DataReaderEngine interface.</h2>
 * <p>For xlsx, xlsm files using the plugin
 * <a href="https://github.com/monitorjbl/excel-streaming-reader">https://github.com/monitorjbl/excel-streaming-reader</a> Using older version 1.2.0 for Java 8.</p>
 * <p>For older xls files using the apache poi, which is significantly slower.</p>
 * <p>Main Implementation difference: we read the rows of the excel, in a streaming way.</p>
 *
 * @author tkaraisku
 */
public class ExcelDataReaderEngine implements DataValidationErrorHolder, DataReaderEngine, RecordReaderCounter, ErrorReporter {

	private static final String EXTERNAL_SHEET_CODE_SUFFIX = "_EXT";
	private static final Logger logger = LogManager.getLogger(ExcelDataReaderEngine.class);
	/**
	 * the input stream for the Excel reader
	 */
	private final ReadableDataLocation excelDataLocation;
	/**
	 * the mapping between sheet names and the configuration
	 * to be used for that sheet
	 */
	private final ExcelInputConfigImpl excelInputConfig;
	private final DataflowBean dataflow;
	private final List<AnnotationBeanImpl> annotations = new ArrayList<>();
	private final Map<String, TreeMap<Integer, String>> attributesOnDatasetNode = new HashMap<>();
	protected String currentTransSheet;
	protected List<Row> transRows = null;
	/**
	 * A list that holds all the cells that a null component
	 * (dimension, attribute) detected per sheet.
	 */
	LinkedHashMap<String, HashSet<String>> cellWithNullComponentsPerSheet = new LinkedHashMap<>();
	/**
	 * List in which we store the columns we found empty.
	 * We clear this for every sheet.
	 */
	List<Integer> emptyColumnsLs = new ArrayList<>();
	int consecutiveEmptyRows;
	private String primaryMeasure;
	/**
	 * the data structure scanner/parser
	 */
	private DataStructureScanner dataStructureScanner;
	private SdmxBeanRetrievalManager beanRetrieval;
	private DataStructureBean dataStructure;
	/**
	 * The exception handler
	 */
	private ExceptionHandler exceptionHandler;
	/**
	 * the Excel workbook
	 */
	private volatile Workbook workbook;
	/**
	 * the name of the current sheet being processed
	 */
	private volatile String currentSheetName;
	/**
	 * Previous parameter sheet name
	 *
	 * @see "SDMXCONV-900"
	 */
	private String previousParameterSheetName = "";
	/**
	 * Previous Transcoding sheet name
	 *
	 * @see "SDMXCONV-900"
	 */
	private String previousTransSheetName = "";
	/**
	 * variable to determine if streaming is necessary (xlsx, xlsm) or not (xls)
	 */
	private boolean isXlsx;
	/**
	 * the coordinates of the next potential observation
	 */
	private CellReference nextPotentialObsPointer;
	/**
	 * the values of the current observation
	 */
	private Observation currentObservation;
	/**
	 * the keys for the current series
	 */
	private Keyable currentKeyable = null;
	/**
	 * the parser for the Excel configuration
	 */
	private ExcelInputConfigParser configParser;
	/**
	 * the names of the Excel sheets containing values
	 * (these sheets will be processed)
	 */
	private Queue<String> dataSheetNames;
	/**
	 * this flag will only allow one single call to moveNextDataset for this
	 * reader
	 */
	private boolean datasetConsumed = false;
	/**
	 * count the observation that are ignored
	 */
	private int ignoredObsCount;
	/**
	 * count the observations that are processed
	 */
	private volatile int obsCount;
	/**
	 * count the series that are processed
	 */
	private volatile int seriesCount;
	/**
	 * count the dataset that are processed
	 */
	private volatile int datasetCount;
	/**
	 * count of attributes with position OBS_LEVEL
	 */
	private int attributesAtObsLevelCount;
	/**
	 * Number of configuration sheets per sheet
	 */
	private LinkedHashMap<String, Integer> numberOfConfigs;
	/**
	 * From the current sheet we create the list of all rows that it contains
	 * this approach is necessary to achieve both streaming (xlsx,xlsm) and non-streaming (xls) reading of the Excel files
	 */
	private volatile List<Row> rows = null;
	private Map<String, Integer> allDataSheetNames = null;
	private AnnotationBeanImpl inputFormatAnn = null;
	/**
	 * Start and end positions that are determined
	 * by parameter sheet attributes
	 * DataStart, DataEnd
	 */
	private int dataStartLine;
	private int dataStartColumn;
	private int dataEndColumn = 0;
	private int dataEndLine = 0;
	private int maxNumberOfEmptyColumns = -1;
	private int maxNumberOfEmptyRows = -1;
	//It is used on SneakPeekNextKeyable and indicates cell with same Keyable
	private CellReference cellWithSameKeyable;
	private DatasetHeaderBean currentDatasetHeader;
	private ExcelSheetUtils excelSheetUtils;
	/**
	 * All the existent Excel parameters there can be many ExcelConfiguration
	 * e.g. one per sheet
	 */
	private List<ExcelConfiguration> allExcelParameters;
	/**
	 * Counter for empty observations found
	 */
	private int foundEmpty;
	private int numConsecutiveEmptyCols = 0;
	private CellReference dataStartCell;
	private DimensionBean dimensionAtObservation;
	private boolean isTimeSeries;
	private ExcelConfigurer excelConfigurer;
	private LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();
	private Integer maximumErrorOccurrencesNumber;
	private int order;
	private int errorLimit;
	private Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
	/**
	 * All components of the dsd
	 */
	private List<String> concepts;

	private CountingInputStream countingStream;

	/**
	 * <h2>Main constructor for excel reader engine.</h2>
	 *
	 * @param dataLocation     Location of the data
	 * @param dataStructure    Structure Object
	 * @param dataflow         Dataflow structure
	 * @param excelInputConfig Configuration Object
	 */
	public ExcelDataReaderEngine(ReadableDataLocation dataLocation,
								 DataStructureBean dataStructure,
								 DataflowBean dataflow,
								 ExcelInputConfig excelInputConfig) {
		this.excelDataLocation = dataLocation;
		this.excelInputConfig = (ExcelInputConfigImpl) excelInputConfig;
		this.dataStructureScanner = new DataStructureScanner(dataStructure);
		this.dataStructure = dataStructure;
		this.dataflow = dataflow;
		if(ObjectUtil.validObject(dataStructure.getPrimaryMeasure())) {
			this.primaryMeasure = dataStructure.getPrimaryMeasure().getId();
		} else {
			this.primaryMeasure = PrimaryMeasureBean.FIXED_ID;
		}
		this.excelSheetUtils = new ExcelSheetUtils();
		this.exceptionHandler = new FirstFailureExceptionHandler();
	}

	/**
	 * Reader for Excel with multiple sheet configurations
	 *
	 * @param dataLocation     Location of the data
	 * @param beanRetrieval    Retrieval of structures
	 * @param dataStructure    Structure Object
	 * @param dataflow         Dataflow Object
	 * @param excelInputConfig Configuration of Excel
	 * @param exceptionHandler Handler for exception
	 */
	public ExcelDataReaderEngine(ReadableDataLocation dataLocation,
								 SdmxBeanRetrievalManager beanRetrieval,
								 DataStructureBean dataStructure,
								 DataflowBean dataflow,
								 ExcelInputConfig excelInputConfig,
								 ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
		this.beanRetrieval = beanRetrieval;
		this.dataStructure = dataStructure;
		this.dataflow = dataflow;
		if (beanRetrieval == null && dataStructure == null) {
			throw new IllegalArgumentException(
					"ExcelDataReaderEngine expects either a SdmxBeanRetrievalManager or a DataStructureBean to be able to interpret the structures");
		} else if (dataStructure != null) {
			this.dataStructure = dataStructure;
		} else if (dataflow != null && (dataflow.isExternalReference() == null || !dataflow.isExternalReference().isTrue())) {
			this.dataStructure = beanRetrieval.getMaintainableBean(DataStructureBean.class, dataflow.getDataStructureRef());
		} else {
			Set<DataStructureBean> maintainableBeans = beanRetrieval.getMaintainableBeans(DataStructureBean.class, new MaintainableRefBeanImpl(), false, true);
			if (!maintainableBeans.isEmpty()) {
				this.dataStructure = maintainableBeans.iterator().next();
			} else {
				throw new IllegalArgumentException(
						"ExcelDataReaderEngine expects DataStructureBean to be able to interpret the structures");
			}
			setCurrentDsd(this.dataStructure);
		}
		this.excelDataLocation = dataLocation;
		this.excelInputConfig = (ExcelInputConfigImpl) excelInputConfig;
		if (dataflow != null) {
			MaintainableRefBean currentFlow = dataflow.getDataStructureRef().getMaintainableReference();
			setCurrentDsd(currentFlow);
		}
		//SDMXCONV-967
		if (this.dataStructure != null) {
			this.dataStructureScanner = new DataStructureScanner(this.dataStructure);
		}
		this.excelSheetUtils = new ExcelSheetUtils();
		if (this.dataStructure != null) {
			if(ObjectUtil.validObject(this.dataStructure.getPrimaryMeasure())) {
				this.primaryMeasure = this.dataStructure.getPrimaryMeasure().getId();
			} else {
				this.primaryMeasure = PrimaryMeasureBean.FIXED_ID;
			}
		}
	}

	@Override
	public Object2ObjectLinkedOpenHashMap<String, ErrorPosition> getErrorPositions() {
		return errorPositions;
	}

	@Override
	public void setErrorPositions(Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions) {
		this.errorPositions = errorPositions;
	}

	public int getDataStartLine() {
		return dataStartLine;
	}

	public void setDataStartLine(int dataStartLine) {
		this.dataStartLine = dataStartLine;
	}

	public int getDataStartColumn() {
		return dataStartColumn;
	}

	public void setDataStartColumn(int dataStartColumn) {
		this.dataStartColumn = dataStartColumn;
	}

	public void setDataEndColumn(int dataEndColumn) {
		this.dataEndColumn = dataEndColumn;
	}

	public void setDataEndLine(int dataEndLine) {
		this.dataEndLine = dataEndLine;
	}

	public int getFoundEmpty() {
		return foundEmpty;
	}

	/**
	 * This is a getter of all the existent Excel parameters
	 *
	 * @return List<ExcelParameter> - all the existent Excel parameters
	 */
	public List<ExcelConfiguration> getAllExcelParameters() {
		return allExcelParameters;
	}

	/**
	 * This is a setter for the all the existent Excel parameters.
	 *
	 * @param allExcelParameters Parameters list
	 */
	public void setAllExcelParameters(List<ExcelConfiguration> allExcelParameters) {
		this.allExcelParameters = allExcelParameters;
	}

	/**
	 * Implements RecordReaderCounter interface
	 */
	@Override
	public int getIngoredObsCount() {
		return ignoredObsCount;
	}

	/**
	 * Implements RecordReaderCounter interface
	 */
	@Override
	public int getObsCount() {
		return obsCount;
	}
	@Override
	public int getSeriesCount() {
		return this.seriesCount;
	}
	@Override
	public int getDatasetCount() {
		return this.datasetCount;
	}
	private void setCurrentDsd(MaintainableRefBean dsdRef) {
		if (beanRetrieval != null) {
			DataStructureBean dsd = beanRetrieval.getMaintainableBean(DataStructureBean.class, dsdRef);
			if (dsd == null) {
				throw new SdmxNoResultsException(ExceptionCode.DATASET_REF_DATASTRUCTURE_NOT_RESOLVED, dsdRef);
			}
			setCurrentDsd(dsd);
		}
	}

	protected void setCurrentDsd(DataStructureBean currentDsd) {
		this.dataStructure = currentDsd;
	}

	@Override
	public HeaderBean getHeader() {
		return excelInputConfig.getHeader();
	}

	@Override
	public DataReaderEngine createCopy() {
		return null;
	}

	@Override
	public ProvisionAgreementBean getProvisionAgreement() {
		return null;
	}

	@Override
	public DataflowBean getDataFlow() {
		return dataflow;
	}

	@Override
	public DataStructureBean getDataStructure() {
		return dataStructure;
	}

	@Override
	public int getDatasetPosition() {
		return 0;
	}

	@Override
	public int getKeyablePosition() {
		return 0;
	}

	@Override
	public int getObsPosition() {
		return 0;
	}

	@Override
	public DatasetHeaderBean getCurrentDatasetHeaderBean() {
		return currentDatasetHeader;
	}

	@Override
	public List<KeyValue> getDatasetAttributes() {
		List<KeyValue> returnList = new ArrayList<>();
		for (Map.Entry<String, TreeMap<Integer, String>> entry : attributesOnDatasetNode.entrySet()) {
			detectComplexComponent(entry.getKey(), returnList, entry.getValue());
		}
		return returnList;
	}

	@Override
	public Keyable getCurrentKey() {
		return currentKeyable;
	}

	@Override
	public Observation getCurrentObservation() {
		return currentObservation;
	}

	/**
	 * <h2>Move the cell pointer horizontally</h2>
	 * <p>Method that moves the cell pointer horizontally first and then vertically
	 * and returns row-column index of the next cell. If end is reached return
	 * -100 in both indexes for row and column.
	 * </p>
	 * <p>We get the next cell from excel almost always. In case of an empty row,
	 * we skip current line and get the next one. In case of an empty row we skip
	 * current cell and get next one too.
	 * </p>
	 * <ul>
	 * 		<li>(functionality of SkipColumns Parameter is implemented here) </li>
	 * 		<li>(functionality of SkipRows Parameter is implemented here) </li>
	 * 		<li>(functionality of DataStart Parameter is implemented here) </li>
	 * 		<li>(functionality of DataEnd Parameter is implemented here) </li>
	 * 		<li>(functionality of NumColumns Parameter is implemented here) </li>
	 * 		<li>(functionality of MaxEmptyRows Parameter is implemented here) </li>
	 * 		<li>(functionality of MaxNumOfEmptyColumns Parameter is implemented here)</li>
	 * </ul>
	 **/
	private int[] moveNextCellHorizontal(CellReference cellPointer) {
		int[] indexes = new int[2];
		// In the first position of the array we get the row index
		indexes[0] = -1;
		// In the second position of the array we get the column index
		indexes[1] = -1;
		consecutiveEmptyRows = 0;
		// The number of columns that will be processed. This is used if DataEnd is not Present
		List<Integer> skipColumns = configParser.getSkipColumns();
		int numberOfColumns = configParser.getNumberOfColumns();
		if (numberOfColumns > 0) {
			//Count the number of columns that have data only and not observation level attributes and not flagged to be skipped
			numberOfColumns = numberOfColumns + (attributesAtObsLevelCount * numberOfColumns) + (skipColumns.isEmpty() ? 0 : skipColumns.size());
		}
		// Skip rows which exist in SKIP_ROWS
		List<Integer> skipRows = configParser.getSkipRows();
		//Initial values for rowIndex, colIndex
		int rowIndex = cellPointer.getRow();
		int colIndex = cellPointer.getCol();
		//If the column we currently are must be skipped we move one cell (including OBS_LEVEL attributes CELLs)
		colIndex = colIndex + (attributesAtObsLevelCount + 1);
		while (!skipColumns.isEmpty() && skipColumns.contains(colIndex)) {
			colIndex = colIndex + 1;
		}
		//Check what value has the cell we are processing right now
		CurrentCellAttributes cellAttributes = new CurrentCellAttributes(
				getCellValue(rows, rowIndex, colIndex, configParser.getDecimalFormat(), configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported()),
				rowIndex,
				colIndex,
				-1,
				-1);
		HashSet<Integer> emptyRowsFound = new HashSet<>();
		while (!returnCell(cellAttributes, skipRows, skipColumns)) {
			//Check if we reached far away from the rows we have in the sheet
			if (cellAttributes.getRow() < rows.size()) {
				// SkipRows Parameter implement here
				cellAttributes = processCellToSkipRows(cellAttributes, skipRows);
				// if after skip rows we reached the end then exit the loop to continue
				if (cellAttributes.getIndexs()[0] == -100) break;
				// If the row we are processing is Empty move to next row
				if (ExcelSheetUtils.isEmptyRow(rows.get(cellAttributes.getRow()), configParser.getDecimalFormat(),
						configParser.getFormatValues(),
						excelInputConfig.isFormulaErrorsReported())) {
					// Show one error per empty line SDMXCONV-1176
					// add the line we want to throw the error in a list and add the exception outside the loop
					if (emptyRowDisplayError(cellAttributes.getRow(), skipRows, dataEndLine, dataEndColumn, configParser.isSkipIncompleteKey())) {
						emptyRowsFound.add(cellAttributes.getRow() + 1);
					}
					// If maxNumberOfEmptyRow is set to 0 then
					// we don't count the ignored cells we exit immediately
					if (maxNumberOfEmptyRows == 0 && dataEndLine <= 0) {
						cellAttributes.setIndexs(-100, -100);
						return cellAttributes.getIndexs();
					}
					cellAttributes.setCol(cellAttributes.getCol() + (attributesAtObsLevelCount + 1));
					// Process current cell to find if is the next position to be read,
					// or we need to moveForward in the next row to find a valid cell
					cellAttributes = processCellAttributesCol(cellAttributes, numberOfColumns, true);
					// if after processCell we reached the end then exit the loop to continue
					if (cellAttributes.getIndexs()[0] == -100) break;
				}
				// To move next column we must check, if this column is out of limit
				// If column is empty, and we didn't reach the end get next cell
				cellAttributes = processCellAttributesCol(cellAttributes, numberOfColumns, false);
				// if after processCell we reached the end then exit the loop to continue
				if (cellAttributes.getIndexs()[0] == -100) break;
			} else {
				/* If for some reason we have moved far away from the end of the
				 * data that sheet contains return -100 indexes to stop immediately */
				cellAttributes.setIndexs(-100, -100);
				break;
			}
			// SDMXCONV-975
			// If the current column is an empty column then we skip it internally
			// without returning a value and without giving a chance to moveNextObservation to go to next cell.
			// List emptyColumnsLs is populated during returnCell
			if (emptyColumnsLs.contains(cellAttributes.getCol())) {
				numConsecutiveEmptyCols = numConsecutiveEmptyCols + 1;
				//move to next column
				cellAttributes.setCol(cellAttributes.getCol() + (attributesAtObsLevelCount + 1));
				cellAttributes.setIndexs(cellAttributes.getRow(), cellAttributes.getCol());
				//skip current line and go to next
			} else {
				numConsecutiveEmptyCols = 0;
			}
		}
		for (int emptyRow : emptyRowsFound) {
			String errorMsg = "Empty row " + emptyRow + " in sheet " + this.currentSheetName +
					" found in the data, that is not included in the skipRows." +
					" Delete the row if it is not needed, or include it in the SkipRows.";
			if (this.exceptionHandler != null) {
				DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, errorMsg)
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition(this.currentSheetName+"!"+emptyRow))
						.typeOfException(TypeOfException.SdmxDataFormatException)
						.args(errorMsg)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		}
		return cellAttributes.getIndexs();
	}

	/**
	 * <h1>Throw errors for empty rows.</h1>
	 * If a row is empty check if:
	 * <ul>
	 *     <li>is not included in the skipRows list, </li>
	 *     <li>data area is defined with DataStart DataEnd parameters, </li>
	 *     <li>skipIncompleteKey is set to false</li>
	 * </ul>
	 *
	 * @param row               The empty row
	 * @param skipRows          List of rows to skip defined in the parameter sheet
	 * @param dataEndLine       The line number where we stop reading
	 * @param dataEndColumn     The column number where we stop reading.
	 * @param skipIncompleteKey Parameter defined in the parameter sheet
	 * @return boolean - Show the error or not
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1176">SDMXCONV-1176</a>
	 */
	private boolean emptyRowDisplayError(int row, List<Integer> skipRows, int dataEndLine, int dataEndColumn, boolean skipIncompleteKey) {
		if (ObjectUtil.validCollection(skipRows) && skipRows.contains(row)) {
			return false;
		}
		if (skipIncompleteKey) {
			return false;
		}
		return ObjectUtil.validString(configParser.getDataEndCell()) && dataEndLine > 0 && dataEndColumn > 0;
	}

	/**
	 * <h2>Method that checks if the last cell of the row is reached.</h2>
	 * If it is reached we change row. If the row is empty we increase the counter
	 * for empty rows, else we clean it.
	 * <p>Everytime we move one step we check if the end is reached with
	 * {@link #stopParsingRow(int, int, int, int) stopParsingRow} method.</p>
	 * <p>When end is reached we return the indexes [-100, -100] for row and column.</p>
	 *
	 * @param cellAttributes  Cell Attributes
	 * @param numberOfColumns Number of Columns
	 * @param isEmptyRow      If a row is empty
	 * @return CurrentCellAttributes
	 */
	private CurrentCellAttributes processCellAttributesCol(CurrentCellAttributes cellAttributes, int numberOfColumns, boolean isEmptyRow) {
		boolean stopCol = stopParsingCol(cellAttributes.getCol(), dataEndColumn, maxNumberOfEmptyColumns, numConsecutiveEmptyCols, numberOfColumns);
		if (!stopCol) {
			// Next cell was found
			cellAttributes.setIndexs(cellAttributes.getRow(), cellAttributes.getCol());
		} else {
			// If column is out of reach we move to next row
			if (isEmptyRow) consecutiveEmptyRows++;
			// setting the new row
			cellAttributes.setRow(cellAttributes.getRow() + 1);
			numConsecutiveEmptyCols = 0;
			// To move next row we must check if this row is out of
			// the limit every time
			if (stopParsingRow(cellAttributes.getRow(), dataEndLine, maxNumberOfEmptyRows, consecutiveEmptyRows)) {
				cellAttributes.setIndexs(-100, -100);
			} else {
				cellAttributes.setCol(dataStartColumn);
				cellAttributes.setIndexs(cellAttributes.getRow(), dataStartColumn);
				consecutiveEmptyRows = 0;
			}
			//SDMXCONV-1569 we check the bytes only when we go in next row, to avoid a performance overhead
			ByteCountHolder.setByteCount(countingStream.getByteCount());
		}
		return cellAttributes;
	}

	/**
	 * <h2>Method that checks the rows to be skipped.</h2>
	 * Method that process the row and checks if must be skipped,
	 * according to the parameter skipRows (that contains a list of row numbers).
	 * <p>Everytime we change row we must check if the end is reached.</p>
	 * <p>When end is reached we return the indexes [-100, -100] for row and column.</p>
	 *
	 * @param cellAttributes Cell attributes
	 * @param skipRows       List of rows that we will skip
	 * @return CurrentCellAttributes
	 */
	private CurrentCellAttributes processCellToSkipRows(CurrentCellAttributes cellAttributes, List<Integer> skipRows) {
		if (skipRows != null && skipRows.contains(cellAttributes.getRow())) {
			// move to next row
			cellAttributes.setRow(cellAttributes.getRow() + 1);
			// clean up the counter for empty columns
			numConsecutiveEmptyCols = 0;
			// To move next row we must check
			// if this row is out of the limit every time
			if (stopParsingRow(cellAttributes.getRow(), dataEndLine, maxNumberOfEmptyRows, consecutiveEmptyRows)) {
				cellAttributes.setIndexs(-100, -100);
			} else {
				cellAttributes.setCol(dataStartColumn);
				cellAttributes.setIndexs(cellAttributes.getRow(), cellAttributes.getCol());
				consecutiveEmptyRows = 0;
			}
		}
		return cellAttributes;
	}

	/**
	 * <h2>Determine if one cell is valid.</h2>
	 * Method that determines if the cell position is valid and should be
	 * returned or not. If it is not returned the pointer will move one
	 * position.
	 *
	 * @param cellAttributes Cell attributes
	 * @param skipRows       List of rows that we will skip
	 * @return true or false
	 */
	private boolean returnCell(CurrentCellAttributes cellAttributes, List<Integer> skipRows, List<Integer> skipColumns) {
		boolean isCellValid = true;
		if (cellAttributes.getIndexs()[1] == -100) {
			numConsecutiveEmptyCols = 0;
			isCellValid = true;
			return isCellValid;
		}
		if (cellAttributes.getIndexs()[0] == -1) {
			isCellValid = false;
			return isCellValid;
		}
		if (cellAttributes.getRow() >= rows.size()) {
			isCellValid = true;
			return isCellValid;
		}
		if (ExcelSheetUtils.isEmptyRow(rows.get(cellAttributes.getRow()), configParser.getDecimalFormat(),
				configParser.getFormatValues(),
				excelInputConfig.isFormulaErrorsReported())) {
			// If maxNumberOfEmptyRow is set to 0 then
			// we don't count the ignored cells we exit immediately
			if (configParser.getMaxEmptyRows() == 0 && dataEndLine <= 0) {
				return false;
			}
			// Count the cell as null
			numConsecutiveEmptyCols = numConsecutiveEmptyCols + 1;
			isCellValid = false;
			return isCellValid;
		}
		if (skipRows != null && skipRows.contains(cellAttributes.getRow())) {
			isCellValid = false;
			return isCellValid;
		}
		if (skipColumns != null && skipColumns.contains(cellAttributes.getCol())) {
			isCellValid = false;
			return isCellValid;
		}

		//SDMXCONV-1278
		// don't return empty column unless there is default observation value
		// that means that the user want to create observations for every empty cell.
		if (excelSheetUtils.isEmptyColumn(
				emptyColumnsLs, rows, configParser,
				cellAttributes.getCol(),
				excelInputConfig.isFormulaErrorsReported())) {
			// If there was no dataEnd and there was number of columns even if the column is empty we want to continue processing.
			//SDMXCONV-1432
			if (this.dataEndColumn <= 0 && configParser.getNumberOfColumns() > 0) {
				int dataEndColumn = (dataStartColumn + configParser.getNumberOfColumns()) - 1;
				// Is column in the range of data
				if (cellAttributes.getCol() <= dataEndColumn && cellAttributes.getCol() >= dataStartColumn && ObjectUtil.validString(configParser.getDefaultObsValue())) {
					return true;
				}
			}
			emptyColumnsLs.add(cellAttributes.getCol());
			isCellValid = false;
		}

		return isCellValid;
	}

	/**
	 * <h2>Current row should be the last row or not.</h2>
	 * Determine if we should stop the reading, or move to next row.
	 *
	 * @param dataRow              Number of the row
	 * @param dataEndLine          Number of the line to end
	 * @param maxNumberOfEmptyRows How many empty rows will be parsed
	 * @param foundEmptyRows       How many empty rows were found
	 * @return true if the last row is reached
	 */
	private boolean stopParsingRow(int dataRow, int dataEndLine, int maxNumberOfEmptyRows, int foundEmptyRows) {
		boolean stop = false;
		if (dataEndLine > 0) {
			return dataRow > dataEndLine;
		}
		if (dataRow >= rows.size()) {
			return true;
		}
		if (maxNumberOfEmptyRows > 0) {
			return foundEmptyRows != 0 && foundEmptyRows >= maxNumberOfEmptyRows;
		}
		if (dataRow < 0 && dataRow != -100) {
			String errorMsg = "We reached the end of Rows in the DataSheet: " + currentSheetName
					+ ". There is a misconfiguration error inside Parameter sheet: '" + configParser.getParameterName() + "'.";
			if (this.exceptionHandler != null) {
				DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.WORKBOOK_READER_ERROR, errorMsg)
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition())
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(errorMsg)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		}
		return stop;
	}

	/**
	 * <h2>Current Column should be the last column or not.</h2>
	 * Determine if we should change row, or move to next column.
	 *
	 * @param dataCol              Number of current column
	 * @param dataEndColumn        Number of the column that is the end
	 * @param maxNumberOfEmptyCols Number of empty columns that will be parsed
	 * @param foundEmptyCols       Number of the empty columns found
	 * @param numberOfColumns      Number of columns that we will read
	 * @return true if the last column is reached
	 */
	private boolean stopParsingCol(int dataCol, int dataEndColumn,
								   int maxNumberOfEmptyCols, int foundEmptyCols,
								   int numberOfColumns) {
		boolean stop = false;
		if (dataEndColumn > 0) {
			return dataCol > dataEndColumn;
		}
		if (numberOfColumns > 0) {
			return dataCol > (dataStartColumn + numberOfColumns) - 1;
		}
		if (maxNumberOfEmptyCols > -1) {
			return foundEmptyCols >= maxNumberOfEmptyCols;
		}
		//If we get here is a disaster back up plan! Because no other end was found.
		//And we read until the end of all files!
		if (dataCol > 16384 || (dataCol < 0 && dataCol != -100)) {
			String errorMsg = "We reached the end of Columns in the DataSheet: " + currentSheetName
					+ ". There is a misconfiguration error inside Parameter sheet: '" + configParser.getParameterName() + "'.";
			DataValidationError dataValidationError = new DataValidationError
					.DataValidationErrorBuilder(ExceptionCode.CONFIGURATION_READER_ERROR, errorMsg)
					.errorDisplayable(true)
					.errorPosition(new ErrorPosition())
					.typeOfException(TypeOfException.SdmxSyntaxException)
					.args(errorMsg)
					.order(++this.order)
					.build();
			addError(ValidationEngineType.READER, dataValidationError);
		}
		return stop;
	}

	@Override
	public boolean moveNextObservation() {
		CellReference tempObsCoordinates = nextPotentialObsPointer;
		if (sneakPeekNextKeyable()) {
			tempObsCoordinates = cellWithSameKeyable;
		}
		boolean result = false;
		// if an observation has been found
		if (tempObsCoordinates != null) {
			// Hold the previous row for moveNextObservation before calling the moveNextCell
			int previousRow = tempObsCoordinates.getRow();
			// we check the series to which the new observation belongs
			Keyable newKeyable = detectSeriesValuesRelativeTo(tempObsCoordinates);
			// found observation is on the same keyable as the previous one?
			// so we can say that the new observation
			// is still inside the same series as the previous
			if (newKeyable != null && newKeyable.equals(currentKeyable)) {
				currentObservation = buildSdmxObservation(tempObsCoordinates, currentKeyable);
				ErrorPosition error = new ErrorPosition(ExcelUtils.getCellReferenceAsString(tempObsCoordinates.formatAsString(), currentSheetName), this.primaryMeasure);
				this.errorPositions.put(this.primaryMeasure, error);
				result = true;
				// move the pointer to the next potential observation
				// so that the search for the next obs starts from there
				int[] rowCol = moveNextCellHorizontal(tempObsCoordinates);
				if (rowCol[0] < -1 || rowCol[1] < -1) {
					nextPotentialObsPointer = null;
					//return false;
				} else {
					nextPotentialObsPointer = new CellReference(rowCol[0], rowCol[1]);
				}
				if (currentObservation == null) {
					return moveNextObservation();
				}
			}
			/* If the newKeyable = null we don't want to change keyable
			 * and return false, we want to move one position and search
			 * for another observation for this keyable. */
			if (newKeyable == null) {
				// move the pointer to the next potential observation
				// so that the search for the next obs starts from there
				int[] rowCol = moveNextCellHorizontal(tempObsCoordinates);
				if (rowCol[0] < -1 || rowCol[1] < -1) {
					nextPotentialObsPointer = null;
					//return false;
				} else {
					nextPotentialObsPointer = new CellReference(rowCol[0], rowCol[1]);
				}
				//SDMXCONV-953, SDMXCONV-952
				// If last column in a row is reached
				// then we return false and move to next row
				if (previousRow != rowCol[0]) {
					return false;
				} else {
					return moveNextObservation();
				}
			}
		}
		return result;
	}

	/**
	 * Method used to predict if we have an observation
	 * in the given cell. The method is a copy of moveNextObservation,
	 * without changing the global variables (currentObservation, nextPotentialObsPointer).
	 *
	 * @param seriesPointer - the cell we want to check
	 * @return boolean - whether there is a next observation or not
	 */
	public boolean sneakPeekNextObservation(CellReference seriesPointer) {
		boolean result = false;
		String obsValue = sneakSdmxObservation(seriesPointer);
		if (obsValue != null && !"".equals(obsValue)) {
			result = true;
			return result;
		}
		return result;
    }

	/**
	 * <b>Predict if in the cell observation and keyable exists.</b>
	 * <p>Method used to predict if we have a keyable with observation in the given cell,
	 * which series are equal with current keyable's. The method is a copy of moveNextKeyable,
	 * without changing the global variables (currentObservation, nextPotentialObsPointer).
	 * </p>
	 *
	 * @return boolean - whether there is a next observation or not
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1007">SDMXCONV-1007</a>
	 */
	public boolean sneakPeekNextKeyable() {
		// check if the next potential series has a different set of values than the previous
		CellReference seriesPointer = nextPotentialObsPointer;
		Keyable keyableFromNewPosition = null;
		if (seriesPointer != null) {
			keyableFromNewPosition = detectSeriesValuesRelativeTo(seriesPointer);
			while (keyableFromNewPosition == null //Cell's keyable check for null and skip it
					|| !sneakPeekNextObservation(seriesPointer)) { //Cell's observation is null then the keyable should be skipped too
				int[] rowCol = moveNextCellHorizontal(seriesPointer);
				if (rowCol[0] < -1 || rowCol[1] < -1) {
					return false;
				} else {
					seriesPointer = new CellReference(rowCol[0], rowCol[1]);
				}
				keyableFromNewPosition = detectSeriesValuesRelativeTo(seriesPointer);
			}
		}
		if (keyableFromNewPosition != null && keyableFromNewPosition.equals(currentKeyable)) {
			cellWithSameKeyable = seriesPointer;
			return true;
		} else
			return false;
	}

	@Override
	public boolean moveNextKeyable() {
		// Check if the next potential series has a different set of values than the previous
		CellReference seriesPointer = nextPotentialObsPointer;
		Keyable keyableFromNewPosition = null;
		if (seriesPointer != null) {
			keyableFromNewPosition = detectSeriesValuesRelativeTo(seriesPointer);
			while (keyableFromNewPosition == null //Cell's keyable check for null and skip it
					|| !sneakPeekNextObservation(seriesPointer)) { //Cell's observation is null then the keyable should be skipped too
				int[] rowCol = moveNextCellHorizontal(seriesPointer);
				if (rowCol[0] < -1 || rowCol[1] < -1) {
					seriesPointer = null;
					break;
				} else {
					seriesPointer = new CellReference(rowCol[0], rowCol[1]);
				}
				keyableFromNewPosition = detectSeriesValuesRelativeTo(seriesPointer);
			}
		}
		boolean seriesLocatedSuccessfully = seriesPointer != null;
		// we found the next series in the current sheet
		if (seriesLocatedSuccessfully) {
			nextPotentialObsPointer = seriesPointer;
			currentKeyable = keyableFromNewPosition;
			ErrorPosition errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(seriesPointer.formatAsString(), currentSheetName), this.primaryMeasure);
			this.errorPositions.put(this.primaryMeasure, errorPosition);
		} else {
			// check if there are any other sheets with data
			String dataSheet;
			if (!dataSheetNames.isEmpty()) {
				dataSheet = dataSheetNames.poll();
				configureReaderForSheet(dataSheet);
				seriesLocatedSuccessfully = moveNextKeyable();
			}
		}
		if(seriesLocatedSuccessfully) {
			this.seriesCount = this.seriesCount + 1;
		}
		return seriesLocatedSuccessfully;
	}

	@Override
	public boolean moveNextDataset() {
		boolean hasDataset = !datasetConsumed;
		if (hasDataset) {
			datasetConsumed = true;
			if (!dataSheetNames.isEmpty()) {
				String dataSheet;
				dataSheet = dataSheetNames.poll();
				configureReaderForSheet(dataSheet);
				for (String dataSetAttr : dataStructureScanner.getDatasetLevelAttributes()) {
					TreeMap<Integer, String> attributes = detectComponentValue(null, dataSetAttr);
					attributesOnDatasetNode.put(dataSetAttr, attributes);
				}
				DatasetStructureReferenceBean datasetStructureReferenceBean = new DatasetStructureReferenceBeanImpl(
						null,
						getDataStructure().asReference(),
						null,
						null,
						dataStructureScanner.getDimensionAtObservation());
				//SDMXCONV-880
				if (getHeader() != null && getHeader().getAction() != null && getHeader().getAction().getAction() != null) {
					this.currentDatasetHeader = new DatasetHeaderBeanImpl(getDataStructure().getId(), DATASET_ACTION.getAction(getHeader().getAction().getAction()), datasetStructureReferenceBean);
				} else {
					this.currentDatasetHeader = new DatasetHeaderBeanImpl(getDataStructure().getId(), DATASET_ACTION.INFORMATION, datasetStructureReferenceBean);
				}
			} else {
				String errorMsg = "No sheets with values was found in the excel file!";
				DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.CONFIGURATION_READER_ERROR, errorMsg)
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition())
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(errorMsg)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		}
		//SDMXCONV-1057
		if (configParser == null) {
			return false;
		}

		return hasDataset;
	}

	private void configureReaderForSheet(String sheetName) {
		currentSheetName = sheetName;
		//SDMXCONV-890
		int sheetIx = -1;
		for (Entry<String, Integer> entry : allDataSheetNames.entrySet()) {
			if (entry.getKey().equalsIgnoreCase(sheetName)) {
				sheetIx = entry.getValue();
			}
		}
		emptyColumnsLs.clear();
		//SDMXCONV-890
		if (sheetIx < 0 && this.exceptionHandler != null) {
			String errorMsg = "The sheet '" + sheetName + "' doesn't exist inside the workbook of the excel file. "
					+ " Check the mapping sheet and try again.";
			DataValidationError dataValidationError = new DataValidationError
					.DataValidationErrorBuilder(ExceptionCode.CONFIGURATION_READER_ERROR, errorMsg)
					.errorDisplayable(true)
					.errorPosition(new ErrorPosition(sheetName))
					.typeOfException(TypeOfException.SdmxSyntaxException)
					.args(errorMsg)
					.order(++this.order)
					.build();
			addError(ValidationEngineType.READER, dataValidationError);
			return;
		}
		/* the Excel sheet that is being processed */
		Sheet currentSheet = workbook.getSheetAt(sheetIx);
		currentSheetName = currentSheet.getSheetName();
		cellWithNullComponentsPerSheet.put(currentSheetName, new HashSet<>());
		// Fetch rows for this sheet, we read it one time at the beginning
		// The alternative implementation for streaming xlsx and xls
		// relies on this list of rows which are stored in a List Object and then processed
		excelSheetUtils = new ExcelSheetUtils(currentSheet);
		rows = excelSheetUtils.getRows(currentSheet);
		try {
			Integer indexConfig = numberOfConfigs.get(sheetName);
			if (indexConfig != null) {
				configParser = new ExcelInputConfigParser(excelInputConfig.getParametersListForDataSheet(sheetName).get(indexConfig - 1), this.exceptionHandler, this.concepts);
				numberOfConfigs.put(sheetName, indexConfig - 1);
				//SDMXCONV-900
				/*	If the parameter sheet is the same with the previous one
					then we don't set the various configuration values again
					We only need to set the pointer to the initial dataStart cell */
				if (previousParameterSheetName.equals(configParser.getParameterName())) {
					// Set the starting point for parsing the series
					nextPotentialObsPointer = dataStartCell;
					return;
				}
			} else {
				configParser = new ExcelInputConfigParser(excelInputConfig.getParametersListForDataSheet(sheetName).get(0), this.exceptionHandler, this.concepts);
			}
			attributesAtObsLevelCount = configParser.computeNumOfAttributesObsLevel(configParser.getAttributesAtObsLevel(), configParser.getDimensionsConfig());
			maxNumberOfEmptyColumns = (configParser.getNumberOfEmptyColumns() != -1) ? configParser.getNumberOfEmptyColumns() : 10;
			maxNumberOfEmptyRows = configParser.getMaxEmptyRows();
		} catch (InvalidExcelParamsException e) {
			if (!dataSheetNames.isEmpty()) {
				String dataSheet;
				dataSheet = dataSheetNames.poll();
				configureReaderForSheet(dataSheet);
			}
			if (this.exceptionHandler != null) {
				DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, e.getMessage())
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition())
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(e.getMessage())
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		} catch (InvalidExcelParamsExceptionFatal e) {
			configParser = null;
		}
		if (configParser != null) {
			validateParametersStartEnd();
			// Set the starting point for parsing the series
			nextPotentialObsPointer = dataStartCell;
			// SDMXCONV-900
			// If there is a transcoding sheet we need to read it
			// SDMXCONV-1270
			// We set the transcoding every time because previous quicker method wasn't working for parallel requests
			if (configParser.getTranscodingSheet() != null/* && !previousTransSheetName.equals(configParser.getTranscodingSheet())*/) {
				currentTransSheet = null;
				transRows = null;
				if (ObjectUtil.validObject(excelConfigurer))
					excelConfigurer.setTranscoding(configParser.getTranscodingSheet());
			}
			previousParameterSheetName = configParser.getParameterName();
			previousTransSheetName = configParser.getTranscodingSheet();
			validateDimensionsConfig(this.dataStructure.getDimensionList().getDimensions(), sheetName);
		}
	}

	/**
	 * Method that checks if DataStart/DataEnd exists or have valid values.
	 * <p>
	 * If valid variables then dataStartLine/dataStartColumn
	 * and dataEndLine/dataEndColumn are set. Also we check if there are parameters
	 * in the configParser that tell us the end. If there are not we throw fatal error.
	 * </p>
	 */
	private void validateParametersStartEnd() {
		if (configParser == null) return;
		if (configParser.getDataStartCell() != null && !"".equals(configParser.getDataStartCell())) {
			try {
				dataStartCell = new CellReference(configParser.getDataStartCell());
				// Set the starting and point from config
				if (dataStartCell.getRow() >= 0 && dataStartCell.getCol() >= 0) {
					setDataStartLine(dataStartCell.getRow());
					setDataStartColumn(dataStartCell.getCol());
				} else {
					String msg = "DataStart parameter value" + configParser.getDataStartCell() + " is not a valid cell. Inside parameter sheet '" + configParser.getParameterName() + "'.";
					if (this.exceptionHandler != null) {
						DataValidationError dataValidationError = new DataValidationError
								.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, msg)
								.errorDisplayable(true)
								.errorPosition(new ErrorPosition(configParser.getParameterName() + "!" + configParser.getDataStartCell()))
								.typeOfException(TypeOfException.SdmxSyntaxException)
								.args(msg)
								.order(++this.order)
								.build();
						addError(ValidationEngineType.READER, dataValidationError);
					}
				}
			} catch (IllegalArgumentException ex) { //this exception is thrown if CellReference cannot be found from DataStart
				String msg = "DataStart parameter value is not a valid cell. Inside parameter sheet '" + configParser.getParameterName() + "'.";
				if (this.exceptionHandler != null) {
					DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, msg)
							.errorDisplayable(true)
							.errorPosition(new ErrorPosition(configParser.getParameterName() + "!" + configParser.getDataStartCell()))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(msg)
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
				}
			}
		} else {
			String msg = "DataStart doesn't exist to Parameter Sheet " + configParser.getParameterName();
			if (this.exceptionHandler != null) {
				DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, msg)
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition())
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(msg)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		}
		if (ObjectUtil.validString(configParser.getDataEndCell())) {
			try {
				CellReference dataEndCell = new CellReference(configParser.getDataEndCell());
				if (dataEndCell.getRow() >= 0 && dataEndCell.getCol() >= 0) {
					setDataEndLine(dataEndCell.getRow());
					setDataEndColumn(dataEndCell.getCol());
				} else {
					String msg = "DataEnd parameter value " + configParser.getDataEndCell() + " is not a valid cell. Inside parameter sheet '" + configParser.getParameterName() + "'.";
					if (this.exceptionHandler != null) {
						DataValidationError dataValidationError = new DataValidationError
								.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, msg)
								.errorDisplayable(true)
								.errorPosition(new ErrorPosition(configParser.getParameterName() + "!" + configParser.getDataEndCell()))
								.typeOfException(TypeOfException.SdmxSyntaxException)
								.args(msg)
								.order(++this.order)
								.build();
						addError(ValidationEngineType.READER, dataValidationError);
					}
				}
			} catch (IllegalArgumentException ex) { //this exception is thrown if CellReference cannot be found from DataEnd
				String msg = "DataEnd parameter value is not a valid cell. Inside parameter sheet '" + configParser.getParameterName() + "'.";
				if (this.exceptionHandler != null) {
					DataValidationError dataValidationError = new DataValidationError
							.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, msg)
							.errorDisplayable(true)
							.errorPosition(new ErrorPosition(configParser.getParameterName() + "!" + configParser.getDataEndCell()))
							.typeOfException(TypeOfException.SdmxSyntaxException)
							.args(msg)
							.order(++this.order)
							.build();
					addError(ValidationEngineType.READER, dataValidationError);
				}
			}
		} else {
			//SDMXCONV-1569 reset dataEnd to avoid previous configs linger on
			setDataEndLine(-1);
			setDataEndColumn(-1);
		}
	}

	/**
	 * In this method we initialise workbook, current sheet, excel parameters.
	 * <p>Everything to be able to start reading.
	 * This method is called at start up,
	 * before moveNextDataset.</p>
	 */
	@Override
	public void reset() {
		this.ignoredObsCount = 0;
		this.obsCount = 0;
		this.seriesCount = 0;
		this.datasetCount = 0;
		this.datasetConsumed = false;
		this.currentTransSheet = null;
		this.transRows = null;
		this.workbook = setWorkbook();
		//SDMXCONV-816, SDMXCONV-867
		if (ThreadLocalOutputReporter.getWriteAnnotations() != null && ThreadLocalOutputReporter.getWriteAnnotations().get()) {
			AnnotationMutableBeanImpl annotation = new AnnotationMutableBeanImpl();
			annotation.setTitle(isXlsx ? "XLSX" : "XLS");
			annotation.setType(AnnotationType.ANN_INPUT_FORMAT.name());
			this.inputFormatAnn = new AnnotationBeanImpl(annotation, null);
		}
		this.excelConfigurer = new ExcelConfigurer(this.workbook, this.exceptionHandler, this.excelInputConfig);
		try {
			this.excelConfigurer.run(excelDataLocation);
		} catch (IOException e) {
			this.exceptionHandler.handleException(new SdmxInternalServerException("I/O Error while trying to read the mapping sheet inside input file."));
		}
		this.allDataSheetNames = excelConfigurer.getAllDataSheetNames();
		this.numberOfConfigs = excelInputConfig.numberOfConfigurationsPerSheet();
		this.dataSheetNames = new LinkedList<>(excelConfigurer.getExcelParameterMultipleMap().keySet());
		//if no sheets from the mapping take all sheets that are not hidden
		if (dataSheetNames.isEmpty()) dataSheetNames = new LinkedList<>(ExcelUtils.getSheetsWithValues(workbook));
		this.cellWithNullComponentsPerSheet.clear();
		this.numConsecutiveEmptyCols = 0;
		this.dimensionAtObservation = getDimensionAtObservation();
		this.isTimeSeries = this.dataStructure.getTimeDimension() != null && dimensionAtObservation.equals(this.dataStructure.getTimeDimension());
		this.concepts = getConceptsAsString(this.dataStructure, this.excelInputConfig.getStructureSchemaVersion());
	}


	@Override
	public void copyToOutputStream(OutputStream outputStream) {
		throw new UnsupportedOperationException("the copy to output stream operation is not supported yet");
	}

	@Override
	public void close() {
		//Count the Ignored Cells last
		this.ignoredObsCount = countIgnoredCells(cellWithNullComponentsPerSheet);
		if (excelDataLocation != null) excelDataLocation.close();
		try {
			if (workbook != null) workbook.close();
			if (this.countingStream != null) this.countingStream.close();
		} catch (IOException e) {
			exceptionHandler.handleException(new SdmxInternalServerException("I/O Error while trying to close excel input file."));
		}
	}

	/**
	 * Creates the keyable given the observation cell reference.
	 * <p>Firstly fetches the Dimensions, <br>
	 * then the Group Level Attributes, <br>
	 * then the Series Level Attributes, <br>
	 * then the Dimension At Observation. </p>
	 * <ul><li>(functionality of skipIncompleteKey is implemented here)</li></ul>
	 *
	 * @param obsCoordinates - the cell of the observation we want to find the keyable.
	 * @return Keyable
	 */
	Keyable detectSeriesValuesRelativeTo(CellReference obsCoordinates) {
		LinkedHashMap<String, String> dimensionsMissing = new LinkedHashMap<>();
		boolean skipIncompleteKey = configParser.isSkipIncompleteKey();
		int nullCounter = 0;
		List<KeyValue> keys = new ArrayList<>();
		for (DimensionBean dimension : this.dataStructure.getDimensionList().getDimensions()) {
			if (!dimension.getId().equals(dataStructureScanner.getDimensionAtObservation())) {
				TreeMap<Integer, String> values = detectComponentValue(obsCoordinates, dimension.getId());
				if (!configParser.getSkipElements().contains(dimension.getId())) {
					if(!checkIfMapValidValues(values)) {
						cellWithNullComponentsPerSheet.get(currentSheetName).add(obsCoordinates.formatAsString());
						nullCounter = nullCounter + 1;
						dimensionsMissing.put(obsCoordinates.formatAsString(), dimension.getId());
					} else {
						for (Entry value : values.entrySet()) {
							if (value == null || "".equals(value.getValue())) {
								cellWithNullComponentsPerSheet.get(currentSheetName).add(obsCoordinates.formatAsString());
								nullCounter = nullCounter + 1;
								dimensionsMissing.put(obsCoordinates.formatAsString(), dimension.getId());
							} else {
								detectComplexComponent(dimension.getId(), keys, values);
							}
						}
					}
				}
			}
		}
		List<KeyValue> attributes = new ArrayList<>();
		for (String attr : dataStructureScanner.getGroupLevelAttributes()) {
			TreeMap<Integer, String> values = detectComponentValue(obsCoordinates, attr);
			if (!configParser.getSkipElements().contains(attr)) {
				if (!checkIfMapValidValues(values)) {
					// SDMXCONV-720 if there is an empty value in an
					// attribute we want to write it as empty
					if (hasConfigThisDimension(attr)) {
						attributes.add(new KeyValueImpl("", attr));
					}
				} else {
					detectComplexComponent(attr, attributes, values);
				}
			}
		}
		for (String attr : dataStructureScanner.getSeriesLevelAttributes()) {
			TreeMap<Integer, String> values = detectComponentValue(obsCoordinates, attr);
			if (!configParser.getSkipElements().contains(attr)) {
				if (!checkIfMapValidValues(values)) {
					// SDMXCONV-720 if there is an empty value in an
					// attribute we want to write it as empty
					//attributes.add(new KeyValueImpl("", attr));
				} else {
					detectComplexComponent(attr, attributes, values);
				}
			}
		}
		TreeMap<Integer, String> obsTimeValues = detectComponentValue(obsCoordinates, dataStructureScanner.getDimensionAtObservation());
		//Assume time is only one
		String obsTime = null;
		if(checkIfMapValidValues(obsTimeValues))
			obsTime = obsTimeValues.firstEntry().getValue();

		if (!configParser.getSkipElements().contains(dataStructureScanner.getDimensionAtObservation())) {
			if (obsTime == null || obsTime.isEmpty()) {
				nullCounter = nullCounter + 1;
				cellWithNullComponentsPerSheet.get(currentSheetName).add(obsCoordinates.formatAsString());
				dimensionsMissing.put(obsCoordinates.formatAsString(), obsTime);
			}
		}
		if (nullCounter != 0) {
			String obsValue = getCellValue(rows, obsCoordinates, configParser.getDecimalFormat(), configParser.getFormatValues(),
					excelInputConfig.isFormulaErrorsReported());
			//If observation value is null then we don't
			//count the observation in ignored observations sum
			if ((obsValue == null || obsValue.isEmpty()))
				/*&& !compareCellValue(getCell(rows, obsCoordinates), configParser.getDecimalFormat(), configParser.getSkipObservationWithValue()) */ {
				HashSet<String> cellsToIgnore = cellWithNullComponentsPerSheet.get(currentSheetName);
				if (cellsToIgnore != null) {
					cellWithNullComponentsPerSheet.get(currentSheetName).remove(obsCoordinates.formatAsString());
				}
			}
			// Skip Observation with empty dimension
			if (skipIncompleteKey) {
				return null;
			} else {
				if (sneakPeekNextObservation(obsCoordinates)) {
					String errorMsg = "The Value of dimension " + dimensionsMissing.get(obsCoordinates.formatAsString())
							+ " is missing, set parameter 'SkipIncompleteKeys'= true to skip the Observation in the cell: "
							+ obsCoordinates.formatAsString() + ".";
					//Check if is conversion otherwise struval is going to catch it
					if (exceptionHandler instanceof FirstFailureExceptionHandler) {
						DataValidationError dataValidationError = new DataValidationError
								.DataValidationErrorBuilder(ExceptionCode.DATASET_MISSING_VALUE_FOR_DIMENSION, errorMsg)
								.errorDisplayable(true)
								.errorPosition(new ErrorPosition(obsCoordinates.formatAsString()))
								.typeOfException(TypeOfException.SdmxDataFormatException)
								.args(errorMsg)
								.order(++this.order)
								.build();
						addError(ValidationEngineType.READER, dataValidationError);
					}
				}
			}
		}

        return getKeyable(obsTime, keys, attributes);
	}

	private KeyableImpl getKeyable(String obsTime, List<KeyValue> keys, List<KeyValue> attributes) {
		KeyableImpl keyable;
		// With XS Measure support, we might have a TimeDimension but the Keyable will need to have a cross-sectional object
		if (isTimeSeries) {
			// Time Series means that the dimension at observation is the Time Dimension and there is a Time Dimension
			if (obsTime != null) {
				// time format has been commented for the reasons mentioned above
				// see SDMXSOURCE-13
				TIME_FORMAT timeFormat = TIME_FORMAT.MONTH;
				keyable = new KeyableImpl(this.dataflow, this.dataStructure, keys, attributes, timeFormat);
			} else {
				// What do we do here throw an error ?
				// Because we have time series but the Time Dimension is missing
				keyable = new KeyableImpl(this.dataflow, this.dataStructure, keys, attributes, (TIME_FORMAT) null);
			}
		} else {
			if (obsTime != null) {
				TIME_FORMAT timeFormat = TIME_FORMAT.MONTH;
				keyable = new KeyableImpl(this.dataflow, this.dataStructure, keys, attributes, timeFormat, dimensionAtObservation.getId(), obsTime);
			} else {
				keyable = new KeyableImpl(this.dataflow, this.dataStructure, keys, attributes, null, dimensionAtObservation.getId(), "");
			}
		}
		return keyable;
	}

	/**
	 * <p>Create the keyValues based to list of component values found.
	 * Whether is complex or not.</p>
	 *
	 * @param componentId     The ID/name of component
	 * @param keyValues       KeyValues to add the new ones
	 * @param componentValues The values found for this component
	 */
	private void detectComplexComponent(String componentId, List<KeyValue> keyValues, TreeMap<Integer, String> componentValues) {
		if(ObjectUtil.validMap(componentValues)) {
			if(dataStructure.isComponentComplex(componentId)) {
				KeyValue complexValue = new KeyValueImpl((String) null, componentId);
				for (Entry<Integer, String> attributeValue : componentValues.entrySet()) {
					//SDMXCONV-1095 && SDMXCONV-1066
					String value = attributeValue.getValue();
					if (detectIfTextTypeIsNumber(componentId)) {
						value = StringUtils.trim(attributeValue.getValue());
					}
					ComplexNodeValue complexNodeValue = new ComplexNodeValueImpl(value, null);
					complexValue.getComplexNodeValues().add(complexNodeValue);
				}
				keyValues.add(complexValue);
			} else {
				//SDMXCONV-1095 && SDMXCONV-1066
				String value = componentValues.firstEntry().getValue();
				if (detectIfTextTypeIsNumber(componentId)) {
					value = StringUtils.trim(componentValues.firstEntry().getValue());
				}
				KeyValue keyValue = new KeyValueImpl(value, componentId);
				keyValues.add(keyValue);
			}
		}
	}

	private DimensionBean getDimensionAtObservation() {
		// when XS measures are used then we use the Measure Dimension
		List<DimensionBean> listOfDimensionBeans = this.dataStructure.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
		if (this.dataStructure.getTimeDimension() != null) {
			return this.dataStructure.getTimeDimension();
		} else {
			if (!listOfDimensionBeans.isEmpty()) {
				return listOfDimensionBeans.get(0);
			} else {
				List<DimensionBean> dimensions = this.dataStructure.getDimensionList().getDimensions();
				return dimensions.get(dimensions.size() - 1);
			}
		}
	}

	/**
	 * Method that counts the cells from the emptyCellsFromSheets global
	 * variable to find out how many are the ignored observations.
	 *
	 * @param emptyCellsFromSheets Empty cells
	 * @return Int Number of ignored observations
	 */
	private int countIgnoredCells(LinkedHashMap<String, HashSet<String>> emptyCellsFromSheets) {
		int counter = 0;
		// loop over the set using an entry set
		for (Entry<String, HashSet<String>> sheet : emptyCellsFromSheets.entrySet()) {
			HashSet<String> values = sheet.getValue();
			counter = counter + values.size();
		}
		return counter;
	}

	/**
	 * <b>Detects the values for a component.</b>
	 * <p>Detects the values of the specified component based on the observation coordinates.
	 * The values for a component could be multiple since SDMX 3.0 hence the TreeMap.
	 * </p>
	 *
	 * @param obsCoordinates The coordinates of the observation
	 * @param component      The component ( dimension / attribute) name
	 * @return TreeMap<Integer, String> Order and Value as String since SDMX 3.0
	 */
	TreeMap<Integer, String> detectComponentValue(CellReference obsCoordinates, String component) {
		TreeMap<Integer, String> dimensionValues = null;
		if (configParser == null) return null;
		List<ExcelDimAttrConfig> dimAttrConfigs = configParser.getConfigForDimension(component);
		if (ObjectUtil.validCollection(dimAttrConfigs)) {
			for (ExcelDimAttrConfig dimAttrConfig : dimAttrConfigs) {
				String dimensionValue = null;
				List<Row> rowsForCodes = defineFromWhereToRead(excelInputConfig.getCodesSheetUtils(), dimAttrConfig, component);
				if (dimAttrConfig != null) {
					try {
						// This component doesn't have skip as position Type
						if (!configParser.getSkipElements().contains(component)) {
							if (obsCoordinates != null) {
								int dataStartColumn = obsCoordinates.getCol();
								int dataStartLine = obsCoordinates.getRow() + 1;
								dimensionValue = getElementValue(rowsForCodes, dimAttrConfig, component, dataStartColumn, dataStartLine, configParser.getConceptSeparator(), configParser.getDecimalFormat());
							} else {// for DataSet attributes
								if (dimAttrConfig.getPositionType() == ExcelPositionType.FIX) {
									dimensionValue = getElementValue(rowsForCodes, dimAttrConfig, component, dataStartColumn, dataStartLine, configParser.getConceptSeparator(), configParser.getDecimalFormat());
								} else {
									//SDMXCONV-881
									if (dimAttrConfig.getPositionType() == ExcelPositionType.CELL || dimAttrConfig.getPositionType() == ExcelPositionType.CELL_EXT) {
										CellReference cellReference = new CellReference(dimAttrConfig.getPosition());
										dimensionValues = detectComponentValue(cellReference, component);
									} else if (dimAttrConfig.getPositionType() == ExcelPositionType.COLUMN || dimAttrConfig.getPositionType() == ExcelPositionType.COLUMN_EXT) {
										String columnLetter = excelSheetUtils.getColumnStringFromStringNumber(dimAttrConfig.getPosition());
										String cell = columnLetter + (dataStartLine + 1);
										CellReference cellReference = new CellReference(cell);
										dimensionValues = detectComponentValue(cellReference, component);
									} else if (dimAttrConfig.getPositionType() == ExcelPositionType.ROW || dimAttrConfig.getPositionType() == ExcelPositionType.ROW_EXT) {
										String row = dimAttrConfig.getPosition();
										int rowNum = Integer.parseInt(row) - 1;
										CellReference cellReference = new CellReference(rowNum, dataStartColumn);
										dimensionValues = detectComponentValue(cellReference, component);
									}
								}
							}
						}
					} catch (ExcelReaderException e) {
						String msg = e.getMessage();
						ErrorPosition errorPosition = this.errorPositions.get(component);
						DataValidationError dataValidationError = new DataValidationError
								.DataValidationErrorBuilder(ExceptionCode.CONFIGURATION_READER_ERROR, msg)
								.errorDisplayable(true)
								.errorPosition(new ErrorPosition(errorPosition.getCurrentCell(), component))
								.typeOfException(TypeOfException.SdmxDataFormatException)
								.args(msg)
								.order(++this.order)
								.build();
						addError(ValidationEngineType.READER, dataValidationError);
					}
				} else {
					boolean skipIncompleteKey = configParser.isSkipIncompleteKey();
					if (!skipIncompleteKey) {
						//SDMXCONV-948
						AttributeBean attribute = this.dataStructure.getAttribute(component);
						boolean throwException = true;
						if (attribute != null) {
							if (!attribute.isMandatory()) {
								throwException = false;
							}
						}
						String msg = "The " + component + " is not configured in the parameters sheet " + currentSheetName + ".";
						//while converting
						if (throwException && exceptionHandler instanceof FirstFailureExceptionHandler) {
							ErrorPosition errorPosition = this.errorPositions.get(component);
							DataValidationError dataValidationError = new DataValidationError
									.DataValidationErrorBuilder(ExceptionCode.CONFIGURATION_READER_ERROR, msg)
									.errorDisplayable(true)
									.errorPosition(new ErrorPosition(errorPosition.getCurrentCell(), component))
									.typeOfException(TypeOfException.SdmxDataFormatException)
									.args(msg)
									.order(++this.order)
									.build();
							addError(ValidationEngineType.READER, dataValidationError);
						}
					}
				}
				if (ObjectUtil.validString(dimensionValue)) {
					if(this.dataStructure.isComponentComplex(component) && ObjectUtil.validString(configParser.getSubfieldSeparator())) {
						if(dimensionValue.contains(configParser.getSubfieldSeparator())) {
							dimensionValues = splitValuesWithSeparator(dimensionValue,  configParser.getSubfieldSeparator());
							//Do not add the value again.
							dimensionValue = null;
						}
					}
					if (!ObjectUtil.validMap(dimensionValues)) {
						dimensionValues = new TreeMap<>();
					}
					if(ObjectUtil.validString(dimensionValue) && ObjectUtil.validObject(dimAttrConfig))
						dimensionValues.put(dimAttrConfig.getOrder(), dimensionValue);
				}
			}
		}
		return dimensionValues;
	}

	/**
	 * <b>Split the value found in a cell with the separator.</b>
	 * <p>If the component is complex we split, else the value added as-is.</p>
	 * <ul><li>(functionality of SubfieldSeparator is implemented here)</li></ul>
	 *
	 * @param componentValue 	The Value of the component found in a cell.
	 * @param separator      	The separator of the values
	 * @return A treemap with the values and the order.
	 */
	private TreeMap<Integer, String> splitValuesWithSeparator(String componentValue, String separator) {
		String[] arrayValues = componentValue.split(separator);
		TreeMap<Integer, String> values = new TreeMap<>();
		for(int i = 0; i < arrayValues.length; i++) {
			values.put(i, arrayValues[i]);
		}
		return values;
	}


	/**
	 * <u>Method tha determines from which sheet we are going to read component values.</u>
	 * <p>Priorities:</p>
	 * <ul>
	 *     <li>CodesFromFile contains a sheet for codes and the component is primary measure read from data file.</li>
	 *     <li>CodesFromFile contains a sheet for codes and the component's value position is obs_level/mixed read from data file.</li>
	 *     <li>CodesFromFile contains a sheet for codes and the component is not the above two <u>read from Codes file.</u></li>
	 *     <li>CodesFromFile does not contain a sheet for codes <u>read from data file</u></li>
	 * </ul>
	 *
	 * @param codeSheetUtils The Object that holds the configuration for every code sheet
	 * @param dimAttrConfig  The configuration for this particular component as defined in the parameter sheet
	 * @param component      The name of the component
	 * @return List of rows from where we will read the component value
	 */
	private List<Row> defineFromWhereToRead(LinkedHashMap<String, ExcelSheetUtils> codeSheetUtils, ExcelDimAttrConfig dimAttrConfig, String component) {
		if (!ObjectUtil.validString(configParser.getCodesFromFile())) {
			return this.rows;
		}
		//This means the component is not declared in the parameter sheet.
		if (!ObjectUtil.validObject(dimAttrConfig)) {
			return this.rows;
		}
		if (primaryMeasure.equals(component)) {
			return this.rows;
		}
		if (dimAttrConfig.getPositionType() == ExcelPositionType.OBS_LEVEL) {
			return this.rows;
		}
		if (dimAttrConfig.getPositionType() == ExcelPositionType.MIXED) {
			return this.rows;
		}
		if (ObjectUtil.validObject(codeSheetUtils)) {
			if (ObjectUtil.validObject(dimAttrConfig.getPositionType()) && dimAttrConfig.getPositionType().name().toUpperCase().endsWith(EXTERNAL_SHEET_CODE_SUFFIX)) {
				ExcelSheetUtils sheetUtils = codeSheetUtils.get(configParser.getCodesFromFile());
				if (ObjectUtil.validObject(sheetUtils)) {
					return sheetUtils.getRows();
				} else
					return this.rows;
			} else {
				return this.rows;
			}
		}

		String msg = "Error while trying to read the sheet '" + configParser.getCodesFromFile() + "'.";
		if (this.exceptionHandler != null) {
			DataValidationError dataValidationError = new DataValidationError
					.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, msg)
					.errorDisplayable(true)
					.errorPosition(new ErrorPosition(configParser.getCodesFromFile(), component))
					.typeOfException(TypeOfException.SdmxDataFormatException)
					.args(msg)
					.order(++this.order)
					.build();
			addError(ValidationEngineType.READER, dataValidationError);
		}
		//Everything else has failed, we search in the default sheet for component values
		return this.rows;
	}

	/**
	 * Check if the dimension given
	 * has whatever configuration in the parameter sheet.
	 *
	 * @param component The dimension ID
	 * @return boolean If it has configuration inside the parameters sheet or not.
	 */
	private boolean hasConfigThisDimension(String component) {
		boolean has;
		List<ExcelDimAttrConfig> dimAttrConfig = configParser.getConfigForDimension(component);
		has = ObjectUtil.validCollection(dimAttrConfig);
		return has;
	}

	/**
	 * <p>
	 * Evaluates the value for a component with the help of the parameter Sheet.
	 * Alternative method because of the streaming reading of an xlsx Gets the
	 * value of the element provided: DIMENSION or ATTRIBUTE with the given coordinates.
	 * </p>
	 * <ul>
	 * 		<li>(functionality of ConceptSeparator Parameter is implemented here)</li>
	 * </ul>
	 *
	 * @param rows             List<Row>
	 * @param parameterElement Components Configuration
	 * @param conceptRef       Concept Reference
	 * @param dataColumn       The column of the data
	 * @param dataRow          The row of the data
	 * @param separator        The Concept separator
	 * @param decimalFormat    The decimal format
	 * @return the value inside the sheet of the element provided:
	 * DIMENSION or ATTRIBUTE
	 * @throws ExcelReaderException The exception when a position is not found, or cannot be read.
	 */
	private String getElementValue(List<Row> rows, ExcelDimAttrConfig parameterElement, String conceptRef,
								   int dataColumn, int dataRow, String separator, DecimalFormat decimalFormat) throws ExcelReaderException {
		String elementValue = null;
		Cell cell;
		String sheetName = currentSheetName;
		ErrorPosition errorPosition = null;
		switch (parameterElement.getPositionType()) {
			case CELL:
			case CELL_EXT:
				cell = getCell(rows, parameterElement.getPosition(), this.exceptionHandler);
				errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
				// If there is a Concept Separator inside Cell such as M\N\23
				if (separator != null) {
					elementValue = getCellValue(cell, parameterElement.getPositionInsideCell(), separator, decimalFormat, configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				} else {
					elementValue = getCellValue(cell, decimalFormat, configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				}
				break;
			case COLUMN:
			case COLUMN_EXT:
				String columnLetter = excelSheetUtils.getColumnStringFromStringNumber(parameterElement.getPosition());
				cell = getCell(rows, columnLetter + dataRow, this.exceptionHandler);
				errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
				// If there is a Concept Separator inside Cell such as M\N\23
				if (separator != null) {
					elementValue = getCellValue(cell, parameterElement.getPositionInsideCell(), separator, decimalFormat, configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				} else {
					elementValue = getCellValue(cell, decimalFormat, configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				}
				break;
			case ROW:
			case ROW_EXT:
				cell = getCell(rows, CellReference.convertNumToColString(dataColumn) + parameterElement.getPosition(), this.exceptionHandler);
				errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
				// If there is a Concept Separator inside Cell such as M\N\23
				if (separator != null) {
					elementValue = getCellValue(cell, parameterElement.getPositionInsideCell(), separator, decimalFormat, configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				} else {
					elementValue = getCellValue(cell, decimalFormat, configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				}
				break;

			case MIXED:
				for (Entry<ExcelPositionType, LinkedHashMap<String, Integer>> entry : parameterElement.getSubpositions()
						.entrySet()) {
					ExcelPositionType mixedPosType = entry.getKey();
					switch (mixedPosType) {
						case CELL:
							Map<String, Integer> cellSubpositionLetter = entry.getValue();
							for (String key : cellSubpositionLetter.keySet()) {
								cell = getCell(rows, key, this.exceptionHandler);
								errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
								// If there is a Concept Separator inside Cell such as M\N\23
								if (separator != null) {
									elementValue = getCellValue(cell, cellSubpositionLetter.get(key), separator, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								} else {
									elementValue = getCellValue(cell, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								}
							}
							break;
						case COLUMN:
							Map<String, Integer> columnSupPosLetter = parameterElement.getSubpositions()
									.get(ExcelPositionType.COLUMN);
							for (String key : columnSupPosLetter.keySet()) {
								String columnSubpositionLetter = excelSheetUtils.getColumnStringFromStringNumber(key);
								cell = getCell(rows, columnSubpositionLetter + dataRow, this.exceptionHandler);
								errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
								// If there is a Concept Separator inside Cell such as M\N\23
								if (separator != null) {
									elementValue = getCellValue(cell, columnSupPosLetter.get(key), separator, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								} else {
									elementValue = getCellValue(cell, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								}
							}
							break;
						case ROW:
							Map<String, Integer> rowSupPos = parameterElement.getSubpositions().get(ExcelPositionType.ROW);
							for (String key : rowSupPos.keySet()) {
								int rowSubposition = Integer.parseInt(key);
								cell = getCell(rows, CellReference.convertNumToColString(dataColumn) + rowSubposition, this.exceptionHandler);
								errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
								// If there is a Concept Separator inside Cell such as M\N\23
								if (separator != null) {
									elementValue = getCellValue(cell, rowSupPos.get(key), separator, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								} else {
									elementValue = getCellValue(cell, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								}
							}
							break;
						case OBS_LEVEL:
							Map<String, Integer> colObsLevel = parameterElement.getSubpositions()
									.get(ExcelPositionType.OBS_LEVEL);
							for (Map.Entry<String,Integer> colEntry : colObsLevel.entrySet()) {
								int intDataColumn = dataColumn + Integer.parseInt(colEntry.getKey());
								cell = getCell(rows, CellReference.convertNumToColString(intDataColumn) + dataRow, this.exceptionHandler);
								errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
								// If there is a Concept Separator inside Cell such as M\N\23
								if (separator != null) {
									elementValue = getCellValue(cell, colObsLevel.get(colEntry.getKey()), separator, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								} else {
									elementValue = getCellValue(cell, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
								}
							}
							break;
						case FIX:
							Map<String, Integer> colFix = parameterElement.getSubpositions().get(ExcelPositionType.FIX);
							errorPosition = new ErrorPosition(parameterElement.getPosition());
							for (String key : colFix.keySet()) {
								elementValue = key;
							}
							break;
						case SKIP:
							// just skip :)
							break;
						default:
							break;
					}
					if (elementValue != null && !elementValue.isEmpty()) {
						break;
					}
				}
				break;
			case OBS_LEVEL:
				int intDataColumn = dataColumn + Integer.parseInt(parameterElement.getPosition());
				cell = getCell(rows, CellReference.convertNumToColString(intDataColumn) + dataRow, this.exceptionHandler);
				errorPosition = new ErrorPosition(ExcelUtils.getCellReferenceAsString(cell, sheetName), conceptRef);
				// If there is a Concept Separator inside Cell such as M\N\23
				if (separator != null) {
					elementValue = getCellValue(cell, parameterElement.getPositionInsideCell(), separator, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				} else {
					elementValue = getCellValue(cell, decimalFormat, this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
				}
				break;
			case FIX:
				elementValue = parameterElement.getFixValue();
				errorPosition = new ErrorPosition(parameterElement.getPosition());
				break;
			case SKIP:
				// just skip :)
				break;
			default:
				throw new ExcelReaderException("The " + parameterElement.getId() + " was not found in sheet "
						+ " at the expected position or it has an invalid position type");
		}
		if (parameterElement.getPositionType().getPositionTypeName().endsWith("_EXT")) {
			sheetName = configParser.getCodesFromFile();
		}
		this.errorPositions.put(conceptRef, errorPosition);
		if (elementValue != null) {
			elementValue = elementValue.trim();
		}
		if (ExcelUtils.hasRules(conceptRef, this.excelConfigurer.getTranscoding())) {
			Entry<String, Boolean> transcoded = ExcelUtils.getValue_fromTranscoding(conceptRef, elementValue, this.excelConfigurer.getTranscoding());
			//SDMXCONV-963
			return getValueFromTranscodingFormula(transcoded, decimalFormat);
		} else {
			return elementValue;
		}
	}

	/**
	 * <p>Builds an ObservationImpl (sdmx api object) based on the provided
	 * observation pointer and the series key.
	 * </p>
	 * <ul>
	 * 		<li>(functionality of DefaultValue Parameter is implemented here)</li>
	 * 		<li>(functionality of MissingObservationCharacter is implemented here)</li>
	 * 		<li>(functionality of SkipObservationWithValue is implemented here)</li>
	 * </ul>
	 *
	 * @param obsPointer - CellReference of the position we are currently are
	 * @param seriesKey  - Keyable the keyable of the position we are
	 * @return Observation
	 */
	private Observation buildSdmxObservation(CellReference obsPointer, Keyable seriesKey) {
		ErrorPosition error = new ErrorPosition(ExcelUtils.getCellReferenceAsString(obsPointer.formatAsString(), currentSheetName), primaryMeasure);
		this.errorPositions.put(primaryMeasure, error);    //SDMXCONV-1396
		//SDMXCONV-816, SDMXCONV-867
		if (Boolean.TRUE.equals(ThreadLocalOutputReporter.getWriteAnnotations().get())) {
			AnnotationMutableBeanImpl annotation = new AnnotationMutableBeanImpl();
			annotation.setTitle(this.errorPositions.get(this.primaryMeasure).toString());
			annotation.setType(AnnotationType.ANN_OBS_COORDINATES.name());
			AnnotationBeanImpl obsCoordinatesAnn = new AnnotationBeanImpl(annotation, null);
			//clear the list before adding the annotations for every annotation
			annotations.clear();
			annotations.add(obsCoordinatesAnn);
			annotations.add(inputFormatAnn);
		}
		String obsDefaultValue = configParser.getDefaultObsValue();
		// Boolean to know if we have a Default Observation Value or not
		boolean obsDefaultValueParamExists = obsDefaultValue != null && !obsDefaultValue.isEmpty();
        String missingObsCharacter = configParser.getMissingObsCharacter();
		// Boolean to know if we have a Missing Observation Character Value or not
		boolean missingObsCharacterExists = missingObsCharacter != null && !missingObsCharacter.isEmpty();
        String skipObservationWithValue = configParser.getSkipObservationWithValue();
		// Boolean to know if we have a Skip Observation With Value or not
		boolean skipObservationWithValueExists = skipObservationWithValue != null && !skipObservationWithValue.isEmpty();
        String obsValue = getCellValue(rows, obsPointer, configParser.getDecimalFormat(), this.configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported());
		//SDMXCONV-1269 in case the value contains only spaces
		obsValue = StringUtils.trim(obsValue);
		// if obsValue is empty and DEFAULT_VALUE exists in parameters sheet
		// create a new observation having the value
		// taken from DEFAULT_VALUE parameter
		if ((obsValue == null || obsValue.length() == 0) && obsDefaultValueParamExists) {
			obsValue = obsDefaultValue != null ? obsDefaultValue : "";
		}
		// if obsValue exists and equals missingObsCharacter and DEFAULT_VALUE
		// exists in parameters sheet create a new observation having the value
		// taken from DEFAULT_VALUE parameter and treat this obs as missing
		if (missingObsCharacterExists) {
			if ((obsValue != null && obsValue.length() != 0) && obsValue.equalsIgnoreCase(missingObsCharacter)) {
				// If there is no default value,
				// skip the value of the missing Observation
				if (obsDefaultValueParamExists) {
					obsValue = obsDefaultValue != null ? obsDefaultValue : "";
				} else {
					foundEmpty++;
				}
			}
		}
		// If the ObsValue equals with skipObservationWithValue
		// then skip the observation and increase the ignored obs counter
		if (skipObservationWithValueExists) {
			if (compareCellValue(getCell(rows, obsPointer), configParser.getDecimalFormat(), skipObservationWithValue)) {
				cellWithNullComponentsPerSheet.get(currentSheetName).add(obsPointer.formatAsString());
				obsValue = null;
			}
		}
		String obsTime = null;
		TreeMap<Integer, String> obsTimeValues = detectComponentValue(obsPointer, dataStructureScanner.getDimensionAtObservation());
		if(checkIfMapValidValues(obsTimeValues))
			obsTime = obsTimeValues.firstEntry().getValue();
		List<KeyValue> attributes = new ArrayList<>();
		for (String obsLevelAttrName : dataStructureScanner.getObservationLevelAttributes()) {
			TreeMap<Integer, String> obsAttrValues = detectComponentValue(obsPointer, obsLevelAttrName);
			if (!checkIfMapValidValues(obsAttrValues)) {
				if (!configParser.getSkipElements().contains(obsLevelAttrName)) {
					/* SDMXCONV-720 if there is an empty value in
					 * an attribute we want to write it as empty
					 * This was commented out because of SDMXCONV-934,
					 * and it was implemented after SDMXCONV-720, SDMXCONV-635, SDMXCONV-776
					 * if(hasConfigThisDimension(obsLevelAttrName)) {
					 * 		attributes.add(new KeyValueImpl(obsAttrValue, obsLevelAttrName));
					 * }
					 */
				}
			} else {
				detectComplexComponent(obsLevelAttrName, attributes, obsAttrValues);
			}
		}
		List<KeyValue> measures = new ArrayList<>();
		//If the SDMX 3.0 structure file has measures
		if(ObjectUtil.validCollection(this.dataStructure.getMeasures())) {
			for (MeasureBean measure : this.dataStructure.getMeasures()) {
				TreeMap<Integer, String> measureValues = detectComponentValue(obsPointer, measure.getId());
				//Measures Can be complex
				if (ObjectUtil.validMap(measureValues)) {
					detectComplexComponent(measure.getId(), measures, measureValues);
				}
			}
		}
		//SDMXCONV-901
		if (ExcelUtils.hasRules(primaryMeasure, this.excelConfigurer.getTranscoding())) {
			obsValue = ExcelUtils.getValue_fromTranscoding(primaryMeasure, obsValue, this.excelConfigurer.getTranscoding()).getKey();
		}
		//SDMXCONV-1095 && SDMXCONV-1066
		if (detectIfTextTypeIsNumber(primaryMeasure)) {
			obsValue = StringUtils.trim(obsValue);
			BigInteger maxLength = detectMaxLength(primaryMeasure);
			obsValue = roundToFit(obsValue, maxLength, configParser.isRoundToFit());
		}

		Observation observation;
		if (obsValue == null || "".equals(obsValue)) {
			foundEmpty++;
			observation = null;
		} else {
			this.obsCount = this.obsCount + 1;
			observation = buildObservation(seriesKey, attributes, measures, obsValue, obsTime);
		}
		return observation;
	}


	/**
	 * <strong>This method is used to build an Observation object based on the provided parameters.</strong>
	 * The Observation object is built differently depending on whether the data structure has complex components
	 * and the version of the structure schema.
	 *
	 * @param keyable       The Keyable object used to build the Observation.
	 * @param attributes    The list of KeyValue objects representing the attributes of the Observation.
	 * @param measures      The list of KeyValue objects representing the measures of the Observation.
	 * @param obsValue      The value of the Observation.
	 * @param obsTime       The time of the Observation.
	 * @return              The built Observation object.
	 */
	private Observation buildObservation(Keyable keyable, List<KeyValue> attributes, List<KeyValue> measures, String obsValue, String obsTime) {
		Observation observation;
		AnnotationBeanImpl[] annotationsArr = annotations.toArray(new AnnotationBeanImpl[annotations.size()]);
		if (getDataStructure().hasComplexComponents() && getDataStructure().isCompatible(SDMX_SCHEMA.VERSION_THREE)) {
			obsValueAsMeasure(obsValue, measures);
			observation = new ObservationImpl(keyable, obsTime, computeObsValue(obsValue), attributes, measures, null, annotationsArr);
		} else if (!getDataStructure().hasComplexComponents() && this.excelInputConfig.getStructureSchemaVersion() == SDMX_SCHEMA.VERSION_THREE) {
			obsValueAsMeasure(obsValue, measures);
			observation = new ObservationImpl(keyable, obsTime, computeObsValue(obsValue), attributes, measures, null, annotationsArr);
		} else {
			//SDMXCONV-890
			if (obsTime != null && !obsTime.isEmpty()) {
				observation = new ObservationImpl(keyable, obsTime, computeObsValue(obsValue), attributes, annotationsArr);
			} else {//SDMXCONV-1016
				observation = new ObservationImpl(keyable, computeObsValue(obsValue), attributes, annotationsArr);
			}
		}
		return observation;
	}

	/**
	 * <strong>This method is used to compute observation value based on the given original observation value.</strong>
	 * <p>It checks if the origin observation value is valid and not null.
	 * If the origin observation value equals to "null" and the default observation value parameter is also "null",
	 * it will set the observation value to an empty string.
	 * It implements the special case of parameter DefaultValue=null.
	 * </p>
	 *
	 * @param originObsValue The original observation value that needs to be computed.
	 * @return The computed observation value. If the original observation value is "null" and the default observation value is also "null", it returns an empty string.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1537">SDMXCONV-1537</a>
	 */
	private String computeObsValue(String originObsValue) {
		String obsValue = originObsValue;
		if (ObjectUtil.validString(originObsValue)) {
			if (originObsValue.equals("null") && "null".equals(configParser.getDefaultObsValue())) {
				obsValue = "";
			}
		}
		return obsValue;
	}

	/**
	 * <b>Add in measures OBS_VALUE if needed.</b>
	 * <p>Fallback support for SDMX 3.0 DSDs.</p>
	 * @param obsValue Observation Value
	 * @param measures List of measures.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1537">SDMXCONV-1537</a>
	 */
	private void obsValueAsMeasure(String obsValue, List<KeyValue> measures) {
		if(ObjectUtil.validString(obsValue)) {
			KeyValue primary;
			if(obsValue.equals("null") && "null".equals(configParser.getDefaultObsValue())) {
				primary = new KeyValueImpl("", PrimaryMeasureBean.FIXED_ID);
			} else {
				primary = new KeyValueImpl(obsValue, PrimaryMeasureBean.FIXED_ID);
			}
			if (measures == null) {
				measures = new ArrayList<>();
			}
			measures.add(primary);
		}
	}

	/**
	 * <h2>Rounding Mechanism</h2>
	 * <p>Rounding based on the digits of maxLength of the dsd.
	 * only applied to observation value.</p>
	 * <p>(functionality of DefaultValue Parameter is implemented here)</p>
	 *
	 * @param obsValue   The Observation value
	 * @param maxLength  The max Length of the value of the concept
	 * @param roundToFit If the rounding mechanism will be used
	 * @return String The Value of the observation after the rounding.
	 */
	private String roundToFit(String obsValue, BigInteger maxLength, boolean roundToFit) {
		try {
			if (maxLength != null && roundToFit) {
				BigInteger strLength = BigInteger.valueOf(obsValue.length());
				if (strLength.compareTo(maxLength) == 1) {
					String concatObsValue = obsValue.substring(0, maxLength.intValue());
					BigDecimal bdConcatObsValue = new BigDecimal(concatObsValue);
					BigDecimal bdObsValue = new BigDecimal(obsValue);
					bdObsValue = bdObsValue.setScale(bdConcatObsValue.scale(), BigDecimal.ROUND_HALF_UP);
					obsValue = bdObsValue.toString();
				}
			}
		} catch (NumberFormatException exception) {
			//Do nothing we will return the initial value
		}
		return obsValue;
	}

	/**
	 * <p>Builds an ObservationImpl (sdmx api object) based on the provided
	 * observation pointer and the series key.
	 * </p>
	 * <ul>
	 * 		<li>(functionality of DefaultValue Parameter is implemented here)</li>
	 * 		<li>(functionality of MissingObservationCharacter is implemented here)</li>
	 * 		<li>(functionality of SkipObservationWithValue is implemented here)</li>
	 * </ul>
	 *
	 * @param obsPointer CellReference of the position we are currently are
	 * @return String The observation value
	 */
	private String sneakSdmxObservation(CellReference obsPointer) {
		String obsDefaultValue = configParser.getDefaultObsValue();
		// Boolean to know if we have a Default Observation Value or not
		boolean obsDefaultValueParamExists = obsDefaultValue != null && !obsDefaultValue.isEmpty();
        String missingObsCharacter = configParser.getMissingObsCharacter();
		// Boolean to know if we have a Missing Observation Character Value or not
		boolean missingObsCharacterExists = missingObsCharacter != null && !missingObsCharacter.isEmpty();
        String skipObservationWithValue = configParser.getSkipObservationWithValue();
		// Boolean to know if we have a Skip Observation With Value or not
		boolean skipObservationWithValueExists = skipObservationWithValue != null && !skipObservationWithValue.isEmpty();
        String obsValue = getCellValue(rows, obsPointer, configParser.getDecimalFormat(), this.configParser.getFormatValues(),
				excelInputConfig.isFormulaErrorsReported());
		// if obsValue is empty and DEFAULT_VALUE exists in parameters sheet
		// create a new observation having the value
		// taken from DEFAULT_VALUE parameter
		if ((obsValue == null || obsValue.length() == 0) && obsDefaultValueParamExists) {
			obsValue = obsDefaultValue != null ? obsDefaultValue : "";
		}
		// if obsValue exists and equals missingObsCharacter and DEFAULT_VALUE
		// exists in parameters sheet create a new observation having the value
		// taken from DEFAULT_VALUE parameter and treat this obs as missing
		if (missingObsCharacterExists) {
			if (obsValue != null && obsValue.length() != 0 && obsValue.equalsIgnoreCase(missingObsCharacter)) {
				// If there is no default value,
				// skip the value of the missing Observation
				if (obsDefaultValueParamExists) {
					obsValue = obsDefaultValue != null ? obsDefaultValue : "";
				} else {
					foundEmpty++;
				}
			}
		}
		// If the ObsValue equals with skipObservationWithValue
		// then skip the observation and increase the ignored obs counter
		if (skipObservationWithValueExists) {
			if (compareCellValue(getCell(rows, obsPointer), configParser.getDecimalFormat(), skipObservationWithValue)) {
				cellWithNullComponentsPerSheet.get(currentSheetName).add(obsPointer.formatAsString());
				obsValue = null;
			}
		}
		String observation;
		if (obsValue == null || "".equals(obsValue)) {
			foundEmpty++;
			observation = null;
		} else {
			observation = obsValue;
		}
		return observation;
	}

	/**
	 * Method that compares the value found in the current Cell we are reading
	 * with the value that we got from the parameter sheet
	 * skipObservationWithValue.
	 *
	 * @param cell                 The current cell
	 * @param decimalFormat        The Format of decimals to be used
	 * @param observationWithValue The observation value to compare with.
	 * @return If this observation value needs to be skipped
	 */
	private boolean compareCellValue(Cell cell, DecimalFormat decimalFormat, String observationWithValue) {
		boolean equalsWith = false;
		// if cell empty
		if (cell == null) {
			return false;
		}
		CellType cellType = (cell.getCellTypeEnum() == CellType.FORMULA) ? cell.getCachedFormulaResultTypeEnum()
				: cell.getCellTypeEnum();

		String value;
		if (cellType == CellType.FORMULA) {
			try {
				value = StringUtils.strip(String.valueOf(cell.getStringCellValue()).trim(), "\"");
			} catch (Exception e1) {
				value = "";
			}
			if (value.equalsIgnoreCase(observationWithValue)) {
				return true;
			}
		}
		if (cellType == CellType.BLANK) {
			// SDMXCONV-780 If observation=="null" skip empty values
			return ("null").equals(observationWithValue);
		}
		if (cellType == CellType.BOOLEAN) {
			value = String.valueOf(cell.getBooleanCellValue());
			if (value.equalsIgnoreCase(observationWithValue)) {
				return true;
			}
			// Check formatted values if equal
			CellFormat cellFormat = CellFormat.getInstance(cell.getCellStyle().getDataFormatString());
			value = cellFormat.apply(cell).text;
			if (value.equalsIgnoreCase(observationWithValue)) {
				return true;
			}
		}

		if (cellType == CellType.NUMERIC) {
			String retValue;
			retValue = decimalFormat.format(cell.getNumericCellValue());
			// Check numeric value if equal return true
			if (retValue.equalsIgnoreCase(observationWithValue)) {
				return true;
			}
			// Check formatted values if equal
			CellFormat cellFormat = CellFormat.getInstance(cell.getCellStyle().getDataFormatString());
			retValue = cellFormat.apply(cell).text;
			if (retValue.equalsIgnoreCase(observationWithValue)) {
				return true;
			}
			try {// Parse the string value as double
				if (observationWithValue != null && !"null".equals(observationWithValue)) {
					Double obsValue = Double.parseDouble(observationWithValue);
					if (cell.getNumericCellValue() == obsValue) {
						return true;
					}
				}
			} catch (Exception e) {
				logger.warn("SkipObservationWithValue {} cannot be parsed as double.", observationWithValue);
			}
		}
		if (cellType == CellType.STRING) {
			value = cell.getStringCellValue();
			if (value.equalsIgnoreCase(observationWithValue)) {
				return true;
			}
			// Check formatted values if equal
			CellFormat cellFormat = CellFormat.getInstance(cell.getCellStyle().getDataFormatString());
			value = cellFormat.apply(cell).text;
			if (value.equalsIgnoreCase(observationWithValue)) {
				return true;
			}
		}
		return equalsWith;
	}

	/**
	 * Get the transcoding value given the transcoding rule.
	 * <p>If a formula was detected during {@link ExcelUtils#getParameterCellValueWithFormula(Cell)}
	 * which could not be evaluated, we assume it is a referral to a sheet/cell inside the input file
	 * and not in the file where the transcoding sheet exists.
	 * </p>
	 * <p>We try to find the referenced cell from the formula text.
	 * </p>
	 *
	 * @param transcoded
	 * @param decimalFormat
	 * @return
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-963">SDMXCONV-963</a>
	 */
	private String getValueFromTranscodingFormula(Entry<String, Boolean> transcoded, DecimalFormat decimalFormat) {
		//if transcoded.getValue() is true means we have a formula
		if (transcoded.getValue()) {
			String value;
			CellReference cellRef = new CellReference(transcoded.getKey());
            if (currentTransSheet == null || !currentTransSheet.equalsIgnoreCase(cellRef.getSheetName())) {
                currentTransSheet = cellRef.getSheetName();
                ExcelSheetUtils excelSheetUtils = new ExcelSheetUtils();
                transRows = excelSheetUtils.getRows(workbook.getSheet(cellRef.getSheetName()));
            }
            value = getCellValue(transRows, cellRef, decimalFormat, this.configParser.getFormatValues(),
                    excelInputConfig.isFormulaErrorsReported());
            return value;
		} else {
			return transcoded.getKey();
		}
	}

	/**
	 * Method necessary to open the workbook from an Excel file we detect what
	 * excel type the file is and either stream it or use the old apache poi to
	 * read it. Workbook and streams needs closing.
	 *
	 * @return Workbook
	 */
	private Workbook setWorkbook() {
		BufferedInputStream bis;
		Workbook workbook;
		//SDMXCONV-874
		if (excelInputConfig.getInflateRatio() != null)
			ZipSecureFile.setMinInflateRatio(excelInputConfig.getInflateRatio());
		try {
			this.countingStream = new CountingInputStream(new BufferedInputStream(excelDataLocation.getInputStream()));
			// Create Workbook instance
			if (FileMagic.valueOf(this.countingStream).equals(FileMagic.OOXML)) {
				logger.info("FileMagic Type of file " + FileMagic.OOXML + " streaming reader will be used.");
				isXlsx = true;
				// Xlsx and xlsm
				workbook = StreamingReader.builder()
						.rowCacheSize(5000)
						.bufferSize(4096) //buffer size to use when reading InputStream to file(defaults to 1024)
						//.sstCacheSize(4096) //experimental - size of SST cache
						.open(this.countingStream); // InputStream or File for XLSX file(required)
			} else {
				logger.info("FileMagic Type of file " + FileMagic.valueOf(this.countingStream) + " old reader will be used.");
				isXlsx = false;
				// Xls
				workbook = WorkbookFactory.create(this.countingStream);
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
	 * <h2>Checks is a component has type number</h2>
	 * <p>Search in the datastructures for the component,
	 * check if there is a text format in the dsd,
	 * and then check if it has one of the number types.</p>
	 *
	 * @param dimension id of the component
	 * @return boolean
	 */
	private boolean detectIfTextTypeIsNumber(String dimension) {
		boolean isNumber = false;
		ComponentBean component = dataStructure.getComponent(dimension);
		RepresentationBean representationBean = null;
		if (component != null) {
			representationBean = component.getRepresentation();
		}
		TextFormatBean textFormat = null;
		if (representationBean != null) {
			textFormat = representationBean.getTextFormat();
		}
		if (textFormat != null && (textFormat.getTextType() == TEXT_TYPE.BIG_INTEGER
				|| textFormat.getTextType() == TEXT_TYPE.INTEGER
				|| textFormat.getTextType() == TEXT_TYPE.LONG
				|| textFormat.getTextType() == TEXT_TYPE.SHORT
				|| textFormat.getTextType() == TEXT_TYPE.DECIMAL
				|| textFormat.getTextType() == TEXT_TYPE.FLOAT
				|| textFormat.getTextType() == TEXT_TYPE.DOUBLE)) {

			isNumber = true;
		}
		return isNumber;
	}

	/**
	 * <h2>Return maxLength, if component has maxLength defined in the DSD.</h2>
	 *
	 * @param dimension
	 * @return BigInteger
	 */
	private BigInteger detectMaxLength(String dimension) {
		BigInteger maxLength = null;
		ComponentBean component = dataStructure.getComponent(dimension);
		RepresentationBean representationBean = null;
		if (component != null) {
			representationBean = component.getRepresentation();
		}
		TextFormatBean textFormat = null;
		if (representationBean != null) {
			textFormat = representationBean.getTextFormat();
		}
		if (textFormat != null) {
			maxLength = textFormat.getMaxLength();
		}
		return maxLength;
	}

	/**
	 * Check if all dimensions exist in the parameter sheet.
	 * We don't check if the configuration is valid, only if it exists.
	 * This check will be performed only once after setting the parser.
	 *
	 * @param dimensions List<DimensionBean>
	 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-944">SDMXCONV-944</a>
	 */
	public void validateDimensionsConfig(List<DimensionBean> dimensions, String sheetName) {
		List<String> unConfiguredDimensions = new ArrayList<>();
		for (DimensionBean dimension : dimensions) {
			if (!this.configParser.getDimensionsConfig().containsKey(dimension.getId())) {
				unConfiguredDimensions.add(dimension.getId());
			}
		}
		if (unConfiguredDimensions != null && !unConfiguredDimensions.isEmpty()) {
			String msg = "Configuration for Dimensions: " + Arrays.toString(unConfiguredDimensions.toArray()) + " is missing, for data sheet " + sheetName + ".";
			if (this.exceptionHandler != null) {
				DataValidationError dataValidationError = new DataValidationError
						.DataValidationErrorBuilder(ExceptionCode.PARAMETERS_READER_ERROR, msg)
						.errorDisplayable(true)
						.errorPosition(new ErrorPosition())
						.typeOfException(TypeOfException.SdmxSyntaxException)
						.args(msg)
						.order(++this.order)
						.build();
				addError(ValidationEngineType.READER, dataValidationError);
			}
		}
	}

	/**
	 * Old method to read next step.
	 *
	 * @param startCell
	 * @return CellReference
	 */
	@Deprecated
	CellReference computeNextObsCoordinatesFromTopToBottom(CellReference startCell) {
		CellReference cellPointer = new CellReference(startCell.getRow(), startCell.getCol());
		int consecutiveEmptyCols = 0;
		int rowIndex = -1;
		while ((rowIndex = seekFirstNonEmptyObsVertically(rows, cellPointer, configParser.getMaxEmptyRows())) == -1
				&& consecutiveEmptyCols <= configParser.getMaxEmptyCols()) {
			cellPointer = new CellReference(getDataStartLine(), cellPointer.getCol() + configParser.getAttributesAtObsLevelCount() + 1);
			consecutiveEmptyCols += configParser.getAttributesAtObsLevelCount() + 1;
		}
		CellReference result = null;
		if (consecutiveEmptyCols < configParser.getMaxEmptyCols()) {
			result = new CellReference(rowIndex, cellPointer.getCol());
		}
		return result;
	}

	/**
	 * Returns the index of the first non-empty cell on the specified row
	 * (starting from position colNumber) or -1 if only empty values have been
	 * found
	 *
	 * @param startCell - the cell where the search begins (inclusive)
	 * @return the column index where the first non-empty value was found or -1
	 * if no values have been found before reaching the
	 * maxAllowedEmptyCols threshold
	 * @deprecated
	 */
	@Deprecated
	public int seekFirstNonEmptyObsHorizontally(Sheet sheet, CellReference startCell, int step, int maxEmptyColumns) {
		int colIndex = startCell.getCol();
		int emptyCols = 0;
		while (emptyCols < maxEmptyColumns && isBlankCell(sheet, startCell.getRow(), colIndex)) {
			colIndex += step;
			emptyCols += step;
		}
		return emptyCols < maxEmptyColumns ? colIndex : -1;
	}

	@Deprecated
	public int seekFirstNonEmptyObsHorizontally(List<Row> rows, CellReference startCell, int step,
												int maxEmptyColumns) {
		int colIndex = startCell.getCol();
		int emptyCols = 0;
		while (emptyCols < maxEmptyColumns && isBlankCell(rows, startCell.getRow(), colIndex)
				&& colIndex <= dataEndColumn) {
			colIndex += step;
			emptyCols += step;
		}
		if (colIndex > dataEndColumn) {
			return -1;
		}
		if (emptyCols > maxEmptyColumns) {
			return -1;
		}
		return colIndex;
	}

	/**
	 * Returns the row index where the first non-empty blank cell is found or -1
	 * when the number of empty rows read is higher than this value -1 will be
	 * returned
	 *
	 * @param startCell - the cell where the search begins (inclusive)
	 * @return the index of the row where the first non-empty value is found or
	 * -1 in case only empty values are found
	 */
	@Deprecated
	public int seekFirstNonEmptyObsVertically(Sheet sheet, CellReference startCell, int maxEmptyRows) {
		int rowIndex = startCell.getRow();
		int emptyRows = 0;
		while (emptyRows < maxEmptyRows && isBlankCell(sheet, rowIndex, startCell.getCol())) {
			rowIndex++;
			emptyRows++;
		}
		return emptyRows < maxEmptyRows ? rowIndex : -1;
	}

	@Deprecated
	public int seekFirstNonEmptyObsVertically(List<Row> rows, CellReference startCell, int maxEmptyRows) {
		int rowIndex = startCell.getRow();
		int emptyRows = 0;
		while (emptyRows < maxEmptyRows && isBlankCell(rows, rowIndex, startCell.getCol()) && rowIndex <= dataEndLine) {
			rowIndex++;
			emptyRows++;
		}
		if (rowIndex > dataEndLine) {
			return -1;
		}
		if (emptyRows >= maxEmptyRows) {
			return -1;
		}
		return rowIndex;
	}

	/**
	 * @param startCell        Start cell
	 * @param readingDirection the reading direction for the current sheet
	 * @return CellReference
	 */
	@Deprecated
	CellReference computeNextObservationCoordinates(CellReference startCell,
													READING_DIRECTION readingDirection) {
		CellReference nextObsCoordinates;
		if (READING_DIRECTION.LEFT_TO_RIGHT.equals(readingDirection)
				|| READING_DIRECTION.ANY.equals(readingDirection)) {
			nextObsCoordinates = computeNextObsCoordinatesFromLeftToRight(startCell);
		} else {// top to bottom
			nextObsCoordinates = computeNextObsCoordinatesFromTopToBottom(startCell);
		}
		return nextObsCoordinates;
	}

	@Deprecated
	public CellReference nextPotentialObservation(CellReference lastObservationCoordinates, READING_DIRECTION readingDirection) {
		CellReference result;
		if (READING_DIRECTION.LEFT_TO_RIGHT.equals(readingDirection)
				|| READING_DIRECTION.ANY.equals(readingDirection)) {
			int attributesAtObsLevelCount = configParser.getAttributesAtObsLevelCount();
			CellReference nextCellReference = new CellReference(lastObservationCoordinates.getRow(),
					lastObservationCoordinates.getCol() + attributesAtObsLevelCount + 1);
			result = computeNextObsCoordinatesFromLeftToRight(nextCellReference);
		} else {// top to bottom
			result = new CellReference(lastObservationCoordinates.getRow() + 1, lastObservationCoordinates.getCol());
		}
		return result;
	}

	/**
	 * <b>Compute the next valid reading position.</b>
	 *
	 * @param startCell The starting Cell reference
	 * @return CellReference The next valid position.
	 */
	@Deprecated
	CellReference computeNextObsCoordinatesFromLeftToRight(CellReference startCell) {
		CellReference cellPointer = startCell;
		int consecutiveEmptyRows = 0;
		int colIndexOfFirstNonEmptyCell = -1;
		int attributesAtObsLevelCount = configParser.getAttributesAtObsLevelCount() + 1;

		// If the number of columns of data expected is not specified then take
		// 1000, so we stop when we reach dataEnd
		int maxNumberOfEmptyColumns = (configParser.getNumberOfEmptyColumns() != -1)
				? configParser.getNumberOfEmptyColumns() : 1000;
		int maxNumberOfEmptyRows = (configParser.getMaxEmptyRows() != -1) ? configParser.getMaxEmptyRows() : 1000;

		// If there is a DataEnd Line value or parameter DataEnd exists
		// then ignore maxEmptyRows value and don't stop
		boolean stop = false;
		while (!stop) {
			if (ExcelSheetUtils.isEmptyRow(ExcelSheetUtils.getRow(rows, cellPointer.getRow()), configParser.getDecimalFormat(), configParser.getFormatValues(), excelInputConfig.isFormulaErrorsReported())) {
				consecutiveEmptyRows++;
				cellPointer = new CellReference(cellPointer.getRow() + 1, getDataStartColumn());
			} else {
				colIndexOfFirstNonEmptyCell = seekFirstNonEmptyObsHorizontally(rows, cellPointer,
						attributesAtObsLevelCount, maxNumberOfEmptyColumns);
				if (colIndexOfFirstNonEmptyCell == -1) {
					consecutiveEmptyRows++;
					cellPointer = new CellReference(cellPointer.getRow() + 1, getDataStartColumn());
                } else {
					cellPointer = new CellReference(cellPointer.getRow(), colIndexOfFirstNonEmptyCell);
                }
                break;
            }
			if (dataEndLine > 0) {
				stop = stopParsingRow(cellPointer.getRow(), dataEndLine, maxNumberOfEmptyRows, consecutiveEmptyRows); // SDMXCONV-715
			} else {
				stop = (consecutiveEmptyRows > maxNumberOfEmptyRows);
			}
		}
		return cellPointer;
	}

	// Added for struval, when the reader is used for validation and exceptions cannot be thrown as is
	@Override
	public ExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}

	@Override
	public void setExceptionHandler(ExceptionHandler exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	@Override
	public LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> getErrorsByEngine() {
		return this.errorsByEngine;
	}

	@Override
	public void setErrorsByEngine(LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine) {
		this.errorsByEngine = errorsByEngine;
	}

	@Override
	public Integer getMaximumErrorOccurrencesNumber() {
		return this.maximumErrorOccurrencesNumber;
	}

	@Override
	public void setMaximumErrorOccurrencesNumber(Integer maximumErrorOccurrencesNumber) {
		this.maximumErrorOccurrencesNumber = maximumErrorOccurrencesNumber;
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	@Override
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public int getErrorLimit() {
		return this.errorLimit;
	}

	@Override
	public void setErrorLimit(int errorLimit) {
		this.errorLimit = errorLimit;
	}
}
