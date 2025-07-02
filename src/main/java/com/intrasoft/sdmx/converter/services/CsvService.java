package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidColumnMappingException;
import com.intrasoft.sdmx.converter.services.exceptions.WriteMappingException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.util.IOUtils;
import org.estat.sdmxsource.util.csv.*;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.Files;
import java.util.*;

@Service
public class CsvService {

	private static final Logger logger = LogManager.getLogger(CsvService.class);

	/**
	 * Method that parses a mapping.xml file and returns a map of dimensions-values
	 * 
	 * @param mappingIs InputStream
	 * @param csvLevels	level
	 * @return Map<String, String[]> result
	 * @throws InvalidColumnMappingException is thrown when the mapping is configured not correctly
	 */
	public Map<String, CsvInColumnMapping> buildInputDimensionLevelMapping(final InputStream mappingIs, int csvLevels)
			throws InvalidColumnMappingException {
		Map<String, CsvInColumnMapping> result;
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(mappingIs);
			// normalize text representation
			doc.getDocumentElement().normalize();
			NodeList listOfConcepts = doc.getDocumentElement().getElementsByTagName("Concept");
			result = buildInputDimensionLevelMapping(listOfConcepts);
		} catch (ParserConfigurationException e) {
			throw new InvalidColumnMappingException("Invalid configuration mapping file: " + e.getMessage(), e);
		} catch (SAXException e) {
			throw new InvalidColumnMappingException("Invalid parsing the mapping file: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new InvalidColumnMappingException("Invalid mapping file: " + e.getMessage(), e);
		}
		return result;
	}

	public Map<String, CsvInColumnMapping> buildInputDimensionLevelMapping(final NodeList conceptsNodeList) {
		Map<String, CsvInColumnMapping> result = new LinkedHashMap<>();
		for (int temp = 0; temp < conceptsNodeList.getLength(); temp++) {
			Node firstConceptsNode = conceptsNodeList.item(temp);
			if (firstConceptsNode.getNodeType() == Node.ELEMENT_NODE) {
				Element conceptElement = (Element) firstConceptsNode;
				String name = conceptElement.getAttribute("name");
				result.put(name, buildInputDimensionLevelMapping(conceptElement));
			}
		}
		return result;
	}

	public CsvInColumnMapping buildInputDimensionLevelMapping(Element conceptElement) {
		CsvInColumnMapping result = new CsvInColumnMapping();
		String columnFixedValue = conceptElement.getAttribute("value");
		String isFixed = conceptElement.getAttribute("fixed");
		String level = conceptElement.getAttribute("level");
		String name = conceptElement.getAttribute("name");
		List<CsvColumn> columns = new ArrayList<>();
		result.setFixed(Boolean.parseBoolean(isFixed));
		if (result.isFixed()) {
			if (columnFixedValue == null) {
				result.setFixedValue("");
			} else {
				result.setFixedValue(columnFixedValue);
			}
			result.setColumns(null);
		} else {
			if (columnFixedValue.contains("+")) {
				String[] crefParts = columnFixedValue.split("\\+");// "+" is special regex character
				for (int j = 0; j < crefParts.length; j++) {
					columns.add(new CsvColumn(Integer.valueOf(crefParts[j])));
				}
			} else if (ObjectUtil.validString(columnFixedValue)) {
				columns.add(new CsvColumn(Integer.valueOf(columnFixedValue), "Column " +columnFixedValue, name));
			}
			result.setColumns(columns);
		}
		if (level == null || level.isEmpty()) {
			result.setLevel(1);
		} else {
			result.setLevel(Integer.valueOf(level));
		}
		return result;
	}

	/**
	 * <strong>Read input Mapping file</strong>
	 *
	 * @param mappingFile File
	 * @param csvLevels   levels
	 * @return Map<String, CsvInColumnMapping>
	 * @throws InvalidColumnMappingException
	 */
	public Map<String, CsvInColumnMapping> buildInputDimensionLevelMapping(final File mappingFile, int csvLevels)
			throws InvalidColumnMappingException {
		Map<String, CsvInColumnMapping> result;
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(mappingFile);
			result = buildInputDimensionLevelMapping(inputStream, csvLevels);
		} catch (FileNotFoundException e) {
			throw new InvalidColumnMappingException("The mapping file could not be found: ", e);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
		return result;
	}

	/**
	 * <strong>Read file from Path and build the mapping from file</strong>
	 *
	 * @param filePath
	 * @param csvLevels
	 * @return Map<String, CsvInColumnMapping>
	 * @throws InvalidColumnMappingException
	 */
	public Map<String, CsvInColumnMapping> buildInputDimensionLevelMapping(String filePath, int csvLevels)
			throws InvalidColumnMappingException {
		return buildInputDimensionLevelMapping(new File(filePath), csvLevels);
	}

	/**
	 * Method that parses a mapping.xml file and returns a map of dimensions-values
	 * 
	 * @param mappingIs mapping Stream
	 * @return Map<String, String[]> result
	 * @throws InvalidColumnMappingException
	 */
	public MultiLevelCsvOutColMapping buildOutputDimensionLevelMapping(final InputStream mappingIs)
			throws InvalidColumnMappingException {
		MultiLevelCsvOutColMapping map = new MultiLevelCsvOutColMapping();
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(mappingIs);
			// normalize text representation
			doc.getDocumentElement().normalize();
			NodeList listOfConcepts = doc.getDocumentElement().getElementsByTagName("Concept");
			for (int temp = 0; temp < listOfConcepts.getLength(); temp++) {
				Node firstConceptsNode = listOfConcepts.item(temp);
				if (firstConceptsNode.getNodeType() == Node.ELEMENT_NODE) {
					Element firstConceptElement = (Element) firstConceptsNode;
					final String name = firstConceptElement.getAttribute("name");
					final String value = firstConceptElement.getAttribute("value");
					final String isFixed = firstConceptElement.getAttribute("fixed");
					final String level = firstConceptElement.getAttribute("level");
					Boolean fixedComponentValue = Boolean.valueOf(isFixed);
					if (!fixedComponentValue) {
						Integer levelAsInteger = 0;
						if (level != null && !level.isEmpty()) {
							levelAsInteger = Integer.valueOf(level);
						}
						map.addMapping(levelAsInteger, Integer.valueOf(value), name);
					} else {
						logger.warn("fixed values do not count for ouput mapping !");
					}
				}
			}
		} catch (ParserConfigurationException e) {
			throw new InvalidColumnMappingException("Invalid configuration mapping file: " + e.getMessage(), e);
		} catch (SAXException e) {
			throw new InvalidColumnMappingException("Invalid parsing the mapping file: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new InvalidColumnMappingException("Invalid mapping file: " + e.getMessage(), e);
		}
		return map;
	}

	/**
	 * <strong>Read Output mapping from file.</strong>
	 *
	 * @param mappingFile File
	 * @return MultiLevelCsvOutColMapping
	 * @throws InvalidColumnMappingException
	 */
	public MultiLevelCsvOutColMapping buildOutputDimensionLevelMapping(final File mappingFile)
			throws InvalidColumnMappingException {
		MultiLevelCsvOutColMapping result;
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(mappingFile);
			result = buildOutputDimensionLevelMapping(inputStream);
		} catch (FileNotFoundException e) {
			throw new InvalidColumnMappingException("The mapping file could not be found: ", e);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
		return result;
	}

	void checkColumnMappingFile(File columnMappingFile, List<String> expectedComponents, int maxLevels) throws InvalidColumnMappingException {
		checkColumnMappingFile(columnMappingFile, expectedComponents, maxLevels, false);
	}
	/**
	 * <strong>Validation of the mapping file</strong>
	 * <p>Checks the provided column mapping file for errors this method has been
	 * copied and adapted form the MappingDialog (old converter)
	 * </p>
	 * 
	 * @param columnMappingFile
	 * @param expectedComponents
	 * @param maxLevels
	 * @throws InvalidColumnMappingException
	 */
	void checkColumnMappingFile(File columnMappingFile,
								List<String> expectedComponents,
								int maxLevels,
								boolean isStructureVersionThree) throws InvalidColumnMappingException {
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(columnMappingFile);
			doc.getDocumentElement().normalize();
			NodeList listOfConcepts = doc.getDocumentElement().getElementsByTagName("Concept");
			if (listOfConcepts == null || listOfConcepts.getLength() == 0) {
				throw new InvalidColumnMappingException("No content found in the column mapping file");
			}
			// check if the dimensions/attributes/measures belong to the expected components
			for (int mapping = 0; mapping < listOfConcepts.getLength(); mapping++) {
				Node firstConceptsNode = listOfConcepts.item(mapping);
				Element firstConceptElement = (Element) firstConceptsNode;
				String nameAttribute = firstConceptElement.getAttribute("name");
				// check the dimension values
				if (nameAttribute != null) {
					if(isStructureVersionThree) {
						boolean isFound = false;
						for(String name: expectedComponents) {
							if(nameAttribute.equals(name) || (nameAttribute.startsWith(name) && (StringUtils.isNumeric(nameAttribute.substring(name.length()))))) {
								isFound = true;
								break;
							}
						}
						
						if (!isFound) {
							throw new InvalidColumnMappingException(
									"Invalid mapping file:" + nameAttribute + " does not belong to the selected structure");
						}
					} else {
						if (!expectedComponents.contains(nameAttribute)) {
							throw new InvalidColumnMappingException(
									"Invalid mapping file:" + nameAttribute + " does not belong to the selected structure");
						}
					}
				} else {
					throw new InvalidColumnMappingException("Invalid mapping file: name attribute missing or empty");
				}

				// check the level values
				String levelAttribute = firstConceptElement.getAttribute("level");
				if (levelAttribute != null) {
					int levelAsInt;
					if (levelAttribute.isEmpty()) {
						levelAsInt = 1;
					} else {
						levelAsInt = Integer.parseInt(levelAttribute);
					}
					if (levelAsInt > maxLevels) {
						throw new InvalidColumnMappingException("The mapping for " + nameAttribute + " has a level of "
								+ levelAsInt + " higher than expected (" + maxLevels + ")");
					}
				} else {
					throw new InvalidColumnMappingException("Invalid mapping file: level attribute missing or empty");
				}
			}
		} catch (NumberFormatException nfe) {
			throw new InvalidColumnMappingException(
					"Error reading the levels in the column mapping file: One of the levels is not a number");
		} catch (IOException ioExc) {
			throw new InvalidColumnMappingException("Error reading the column mapping file from disk", ioExc);
		} catch (ParserConfigurationException ioExc) {
			throw new InvalidColumnMappingException("Error configuration the column mapping file from disk", ioExc);
		} catch (SAXException ioExc) {
			throw new InvalidColumnMappingException("Error parsing the column mapping file from disk", ioExc);
		}
	}

	/**
	 * <strong>Validation of the mapping file and build it.</strong>
	 * <p>Checks the validity of the given mapping file against the accepted dimensions
	 * and in accordance with the levels provided
	 * </p>
	 *
	 * @param mappingFile       the mapping file
	 * @param acceptedDimsAttrs the list of accepted dimensions and attributes
	 * @param csvLevels         the number of csv levels
	 * @return Map<String, CsvInColumnMapping>
	 * @throws InvalidColumnMappingException if the mapping file is not valid
	 */
	public Map<String, CsvInColumnMapping> checkFileAndBuildInputMapping(File mappingFile,
																		 List<String> acceptedDimsAttrs,
																		 int csvLevels,
																		 boolean isStructureVersionThree) throws InvalidColumnMappingException {
		checkColumnMappingFile(mappingFile, acceptedDimsAttrs, csvLevels, isStructureVersionThree);
		return buildInputDimensionLevelMapping(mappingFile, csvLevels);
	}
	public Map<String, CsvInColumnMapping> checkFileAndBuildInputMapping(String mappingFileName,
			List<String> acceptedDimsAttrs, int csvLevels, boolean isStructureVersionThree) throws InvalidColumnMappingException {
		return checkFileAndBuildInputMapping(new File(mappingFileName), acceptedDimsAttrs, csvLevels, isStructureVersionThree);
	}

	/**
	 * <strong>Validate and build output mapping from file.</strong>
	 * <p>checks the validity of the given mapping file against the accepted dimensions
	 * and in accordance with the levels provided</p>
	 * 
	 * @param mappingFile the mapping file
	 * @param acceptedDimsAttrs the list of accepted dimensions and attributes
	 * @param csvLevels the number of csv levels
	 * @return MultiLevelCsvOutColMapping
	 * @throws InvalidColumnMappingException if the mapping file is not valid
	 */
	public MultiLevelCsvOutColMapping checkFileAndBuildOutputMapping(File mappingFile, List<String> acceptedDimsAttrs,
			int csvLevels, boolean isStructureVersionThree) throws InvalidColumnMappingException {
		checkColumnMappingFile(mappingFile, acceptedDimsAttrs, csvLevels, isStructureVersionThree);
		return buildOutputDimensionLevelMapping(mappingFile);
	}

	public MultiLevelCsvOutColMapping checkFileAndBuildOutputMapping(String mappingFileName,
			List<String> acceptedDimsAttrs, int csvLevels) throws InvalidColumnMappingException {
		return checkFileAndBuildOutputMapping(new File(mappingFileName), acceptedDimsAttrs, csvLevels, false);
	}

	/**
	 * <strong>Initialize mapping from header row.</strong>
	 * <p>Initializes the input column mapping by matching the name of the column with
	 * the name of the dimension/attribute (if possible). In case a matching cannot
	 * be done, an empty list of columns is mapped.</p>
	 *
	 * @param componentsToMap the list of components to be mapped (dimensions, attributes, measures)
	 * @param columns the list of columns.
	 *
	 * @return the mapping
	 */
	public Map<String, CsvInColumnMapping> initInputColumnMappingWithCsvHeaders(List<String> componentsToMap, List<CsvColumn> columns) {
		return initInputColumnMappingWithCsvHeaders(componentsToMap, columns, false);
	}

	public Map<String, CsvInColumnMapping> initInputColumnMappingWithCsvHeaders(List<String> componentsToMap,
			List<CsvColumn> columns, boolean isStructureVersionThree) {
		logger.debug("initializing column mapping for components {}", componentsToMap);
		Map<String, CsvInColumnMapping> result = new LinkedHashMap<>(); // linked hash map to preserve the order
		List<String> columnHeaders = extractConceptFromHeaderRow(columns);
		for (int i = 0; i < componentsToMap.size(); i++) {
			String component = componentsToMap.get(i);
			CsvInColumnMapping mapping = new CsvInColumnMapping();
			mapping.setFixed(false);
			mapping.setLevel(NumberUtils.INTEGER_ONE);
			List<CsvColumn> columnsToBeMapped = new ArrayList<>();

			if(isStructureVersionThree) {
				for(String name: columnHeaders) {
					if(name.equals(component) || (name.startsWith(component) && (StringUtils.isNumeric(name.substring(component.length()))))) {
						int index = columnHeaders.indexOf(name)+1;
						columnsToBeMapped.add(new CsvColumn(index, "Column "+ index, name));
					}
				}
			} else {
				if (columnHeaders != null && columnHeaders.contains(component)) {
					int index = columnHeaders.indexOf(component)+1;
					columnsToBeMapped.add(new CsvColumn(index, "Column "+ index, component));
				}
			}
			mapping.setColumns(columnsToBeMapped);
			result.put(component, mapping);
		}
		return result;
	}

	public Map<String, CsvInColumnMapping> initInputColumnMapping(List<String> componentsToMap, List<CsvColumn> columns, boolean useHeader, boolean isSdmx30) {
		logger.debug("initializing column mapping for components {}", componentsToMap);
		Map<String, CsvInColumnMapping> result; // linked hash map to preserve the order
		if(useHeader) {
			if(isSdmx30) {
				result = initInputColumnMappingWithCsvHeaders(extractConceptFromHeaderRow(columns), columns, true);
			} else {
				result = initInputColumnMappingWithCsvHeaders(componentsToMap, columns);
			}
		} else {
			result = initInputColumnMappingWithoutCsvHeaders(componentsToMap);
		}
		return result;
	}

	/**
	 * creates a default mapping for the given components like: dimension 1 ->
	 * column 1 dimension 2 -> column 2 ...
	 * 
	 * @return
	 */
	public Map<String, CsvInColumnMapping> initInputColumnMappingWithoutCsvHeaders(List<String> componentsToMap) {
		Map<String, CsvInColumnMapping> result = new LinkedHashMap<>();

		for (String component : componentsToMap) {
			CsvInColumnMapping mapping = new CsvInColumnMapping();
			mapping.setFixed(false);
			mapping.setLevel(NumberUtils.INTEGER_ONE);
			int index = componentsToMap.indexOf(component)+1;
			CsvColumn column = new CsvColumn(index, "Column "+ index, component);
			mapping.setColumns(Arrays.asList(column));
			result.put(component, mapping);
		}
		return result;
	}

	/**
	 * extracts the column headers from the list of columns
	 * 
	 * @param columnHeaders
	 * @return
	 */
	public List<String> extractHeaders(List<CsvColumn> columnHeaders) {
		List<String> result = new ArrayList<>();
		if (columnHeaders != null && !columnHeaders.isEmpty()) {
			for (CsvColumn column : columnHeaders) {
				result.add(column.getHeader());
			}
		}
		return result;
	}

	/**
	 * extracts the column headers from the list of columns
	 *
	 * @param columnHeaders
	 * @return
	 */
	public List<String> extractConceptFromHeaderRow(List<CsvColumn> columnHeaders) {
		List<String> result = new ArrayList<>();
		if (columnHeaders != null && !columnHeaders.isEmpty()) {
			for (CsvColumn column : columnHeaders) {
				result.add(column.getConcept());
			}
		}
		return result;
	}

	/**
	 * initializes the output column mapping by scanning the data structure and
	 * populating an array of CsvOutColumnMapping objects with level 1 and the
	 * structure elements as components
	 *
	 * @return
	 */
	public MultiLevelCsvOutColMapping initMultiLevelOutputColumnMapping(List<String> components) {
		MultiLevelCsvOutColMapping multiLevelCsvOutColMapping = new MultiLevelCsvOutColMapping();
		logger.debug("initializing input column mapping for components {} ", components);
		for (int i = 0; i < components.size(); i++) {
			multiLevelCsvOutColMapping.addMapping(SingleLevelCsvOutColMapping.SINGLE_LEVEL, i+1, components.get(i));
		}
		return multiLevelCsvOutColMapping;
	}

	/**
	 * initializes the output column mapping by scanning the data structure and
	 * populating an array of CsvOutColumnMapping objects with level 1 and the
	 * structure elements as components
	 *
	 * @return List<CsvOutColumnMapping>
	 */
	public static List<CsvOutColumnMapping> initOutputColumnMapping(List<String> components) {
		logger.debug("initializing input column mapping for components {} ", components);
		List<CsvOutColumnMapping> mappings = new ArrayList<>();
		int i = 1;
		for (String component : components) {
			CsvOutColumnMapping mapping = new CsvOutColumnMapping();
			mapping.setComponent(component);
			mapping.setLevel(NumberUtils.INTEGER_ONE);
			mapping.setIndex(i);
			mappings.add(mapping);
			i++;
		}
		return mappings;
	}

	/**
	 * <p>Open and close the input CSV file, to read the first line.
	 * Count the delimiters.</p>
	 * <p>Count the delimiters to find out if the columns are above the limit of CSV reader.
	 * This will be used for SDMXCONV-1247</p>
	 * @param csvFile Input File csv
	 * @param delimiter The delimiter
	 * @return long number of delimiters found in the first line
	 */
	public static Long readFirstLine(File csvFile, String delimiter) {
		long numberOfDelimiters = 0;
		try(InputStream inputStream = Files.newInputStream(csvFile.toPath())) {
			numberOfDelimiters = readFirstLine(inputStream, delimiter);
		} catch (IOException ioException) {
			throw new RuntimeException("Cannot find csv file " + ioException.getMessage(), ioException);
		}

		return numberOfDelimiters;
	}

	/**
	 * @see #readFirstLine(File csvFile, String delimiter)
	 */
	public static Long readFirstLine(InputStream inputStream, String delimiter) {
		long numberOfDelimiters = 0;
		try(BufferedReader in = new BufferedReader(new InputStreamReader(inputStream))) {
			String first = in.readLine();
			if(ObjectUtil.validString(first) && delimiter!=null && delimiter.length()>0) {
				numberOfDelimiters = first.chars().filter(c -> c == delimiter.charAt(0)).count();
			}
		} catch (IOException ioException) {
			throw new RuntimeException("Cannot find csv file " + ioException.getMessage(), ioException);
		}

		return numberOfDelimiters;
	}

	/**
	 * reads the first row from the given CSV file and returns a list of CsvColumns
	 * with the same size as the first row. If the inputHasHeaders is true then the
	 * returned list will have those values as headers otherwise (inputHasHeaders =
	 * false) then the returned list will have default header names (e.g. Column 1,
	 * Column 2 ... )
	 * 
	 * @param csvFile the file
	 * @return a list of column names
	 */
	public List<CsvColumn> readColumnHeaders(File csvFile, String delimiter, boolean inputHasHeaders) {
		List<CsvColumn> result;
		FileInputStream csvInputStream = null;
		try {
			csvInputStream = new FileInputStream(csvFile);
			result = readColumnHeaders(csvInputStream, delimiter, inputHasHeaders);
		} catch (FileNotFoundException fnfException) {
			throw new RuntimeException("Cannot find csv file " + fnfException.getMessage(), fnfException);
		} finally {
			IOUtils.closeQuietly(csvInputStream);
		}
		return result;
	}

	/**
	 * reads the header for the given csv input
	 * 
	 * @param csvInputStream the csv input stream
	 * @param fieldDelimiter the delimiter
	 * @return
	 */
	public List<CsvColumn> readColumnHeaders(InputStream csvInputStream, String fieldDelimiter, boolean inputHasHeaders) {
		List<CsvColumn> result = new ArrayList<>();
		CsvParserSettings parserSettings = new CsvParserSettings();
		parserSettings.getFormat().setDelimiter(fieldDelimiter);
		parserSettings.setDelimiterDetectionEnabled(true);// Set tab as delimiter
		parserSettings.setLineSeparatorDetectionEnabled(true); // Automatically detect line separators
		CsvParser parser = new CsvParser(parserSettings);
		parser.beginParsing(csvInputStream, "UTF-8");
			String[] columnNamesAsArray = parser.parseNext();
			if(columnNamesAsArray==null)
				throw new RuntimeException("Could not read input file, check if the csv/flr file is empty!");
			for (int index = 1; index <= columnNamesAsArray.length; index++) {
				result.add(inputHasHeaders ? new CsvColumn(index, "Column "+ index,  columnNamesAsArray[index-1]) : new CsvColumn(index));
			}
		return result;
	}

	/**
	 * returns the indexes of the columns used in the provided input mapping.
	 * ATTENTION: the indexes are zero-based
	 *
	 * @param mappings the input column mapping
	 * @return a list of column indexes
	 */
	public List<Integer> getIndexesOfColumnsInMapping(Map<String, CsvInColumnMapping> mappings) {
		List<Integer> result = new ArrayList<>();
		for (Map.Entry<String, CsvInColumnMapping> entry : mappings.entrySet()) {
			if (!entry.getValue().isFixed()) {
				List<CsvColumn> columns = entry.getValue().getColumns();
				for (CsvColumn column : columns) {
					result.add(column.getIndex());
				}
			}
		}
		return result;
	}

	/**
	 * Returns the column headers of the columns used in the provided input mapping.
	 * 
	 * @param mappings the input column mapping
	 * @return a list of column headers
	 */
	public List<String> getHeadersOfColumnsInMapping(Map<String, CsvInColumnMapping> mappings) {
		List<String> result = new ArrayList<>();
		for (Map.Entry<String, CsvInColumnMapping> entry : mappings.entrySet()) {
			if (!entry.getValue().isFixed()) {
				List<CsvColumn> columns = entry.getValue().getColumns();
				for (CsvColumn column : columns) {
					result.add(column.getHeader());
				}
			}
		}
		return result;
	}

	/**
	 * returns the column headers of the columns used in the provided input mapping.
	 * 
	 * @param mappings the input column mapping
	 * @return a list of column headers
	 */
	public List<CsvColumn> getColumnsInMapping(Map<String, CsvInColumnMapping> mappings) {
		List<CsvColumn> result = new ArrayList<>();
		for (Map.Entry<String, CsvInColumnMapping> entry : mappings.entrySet()) {
			if (!entry.getValue().isFixed()) {
				List<CsvColumn> columns = entry.getValue().getColumns();
				for (CsvColumn column : columns) {
					int index = column.getIndex();
					CsvColumn csvColumn = new CsvColumn(index, "Column "+ index, entry.getKey());
					result.add(csvColumn);
				}
			}
		}
		return result;
	}

	/**
	 * returns a list of components (dimensions, attributes, measures) used in the
	 * provided output mappings
	 * 
	 * @param mappings the output mappings
	 * @return a list of components
	 */
	public List<String> getComponentsInMapping(List<CsvOutColumnMapping> mappings) {
		List<String> result = new ArrayList<>();
		for (CsvOutColumnMapping mapping : mappings) {
			result.add(mapping.getComponent());
		}
		return result;
	}

	/**
	 * Method that saves Csv mapping to a xml File.
	 * The output of this method is the xml File.
	 * @param mappings
	 * @param output
	 * @throws WriteMappingException
	 */
	public void saveInMapping(Map<String, CsvInColumnMapping> mappings, File output) throws WriteMappingException {
		try {
			FileOutputStream outputStream = new FileOutputStream(output);
			saveInMapping(mappings, outputStream);
		} catch (FileNotFoundException e) {
			throw new WriteMappingException("Mapping file not found!", e);
		}
	}

	public void saveInMapping(Map<String, CsvInColumnMapping> mappings, OutputStream outputFile) throws WriteMappingException {
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document xmlDocument = docBuilder.newDocument();
			xmlDocument.setXmlStandalone(false);
			xmlDocument.setXmlVersion("1.0");
			//Create the root element for xml
			Element root = xmlDocument.createElement("Mapping");
			xmlDocument.appendChild(root);
			for (Map.Entry<String, CsvInColumnMapping> entry : mappings.entrySet()) {
				//Every mapping value is a concept tag in xml
				Element concept = xmlDocument.createElement("Concept");
				root.appendChild(concept);
				//Name Attribute in the concept tag
				concept.setAttribute("name", entry.getKey());
				if (!entry.getValue().isFixed()) {
					List<CsvColumn> columns = entry.getValue().getColumns();
					String colsMap = "";
					int cntr = 0;
					for (CsvColumn column : columns) {
						cntr++;
						//SDMXCONV-1011
							colsMap += column.getIndex();

						if (cntr != columns.size()) {
							colsMap += "+";
						}
					}
					concept.setAttribute("value", colsMap);
				} else {
					concept.setAttribute("value", entry.getValue().getFixedValue());
				}
				concept.setAttribute("fixed", Boolean.toString(entry.getValue().isFixed()));
				concept.setAttribute("level", entry.getValue().getLevel().toString());
			}
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			DOMSource source = new DOMSource(xmlDocument);
			StreamResult result = new StreamResult(outputFile);
			transformer.transform(source, result);
		} catch (Exception ex) {
			throw new WriteMappingException("Error while writing the mapping File!", ex);
		}
	}
	
	

	public void saveOutMapping(MultiLevelCsvOutColMapping mappings, File output, int levels) throws WriteMappingException {
		try {
			FileOutputStream outputStream = new FileOutputStream(output);
			saveOutMapping(mappings, outputStream, levels);
		} catch (FileNotFoundException e) {
			throw new WriteMappingException("Mapping file not found!", e);
		}
	}

	/**
	 * Method that saves Csv mapping to a xml File.
	 * The output of this method is the xml File.
	 * @param mappings Map<String, CsvInColumnMapping> mappings. The object that holds the mapping info.
	 * @param outputFile File output. The File in which we will create the XML DOM.
	 * @param levels For single level this is always one.
	 */
	public void saveOutMapping(MultiLevelCsvOutColMapping mappings, OutputStream outputFile, int levels) throws WriteMappingException {		
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document xmlDocument = docBuilder.newDocument();
			xmlDocument.setXmlStandalone(false);
			xmlDocument.setXmlVersion("1.0");
			Element root = xmlDocument.createElement("Mapping");
			xmlDocument.appendChild(root);
			Map<Integer, TreeMap<Integer, String>> dimensionsMappedOnLevel = mappings.getDimensionsMappedOnLevel();
			boolean indexOneFound = false;
			//Search if index one was found, if not means that sae input was called somewhere outside the edit mapping page
			//and the index doesn't need to be increase by one.
			for (int level=1; level <= levels; level++) {
				Map<Integer, String> mappingsPerLevel = dimensionsMappedOnLevel.get(level);
				if (mappingsPerLevel != null) {
					for (Integer key : mappingsPerLevel.keySet()) {
						int colIndex = key+1;
						if(colIndex==1)
							indexOneFound = true;
					}
				}
			}
			for (int level=1; level <= levels; level++) {
				Map<Integer, String> mappingsPerLevel = dimensionsMappedOnLevel.get(level);
				if (mappingsPerLevel != null) {
					for(Integer key : mappingsPerLevel.keySet()) {
						int colIndex=0;
						if(indexOneFound) {
							colIndex = key + 1;
						} else {
							colIndex = key;
						}
						String mappingPerLevel = mappingsPerLevel.get(key);
						Element concept = xmlDocument.createElement("Concept");
						root.appendChild(concept);
						concept.setAttribute("name", mappingPerLevel);
						concept.setAttribute("value", Integer.toString(colIndex));
						concept.setAttribute("level",  Integer.toString(level));
						concept.setAttribute("fixed",  "false");
					}
				}
			}
			// Write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			DOMSource source = new DOMSource(xmlDocument);
			StreamResult result = new StreamResult(outputFile);
			transformer.transform(source, result);
		} catch (Exception ex) {
			throw new WriteMappingException("Error while writing the mapping File!", ex);
		}
	}

	public void saveOutMapping(List<CsvOutColumnMapping> mappings, OutputStream outputFile, int levels) throws WriteMappingException {		
		try {		
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document xmlDocument = docBuilder.newDocument();
			xmlDocument.setXmlStandalone(false);
			xmlDocument.setXmlVersion("1.0");
			Element root = xmlDocument.createElement("Mapping");
			xmlDocument.appendChild(root);
			int value = 1;
			for(CsvOutColumnMapping mapping: mappings) {	
				Element concept = xmlDocument.createElement("Concept");
				root.appendChild(concept);
				concept.setAttribute("name", mapping.getComponent());
				concept.setAttribute("value", Integer.toString(value));
				concept.setAttribute("level",  Integer.toString(mapping.getLevel()));
				concept.setAttribute("fixed",  "false");
				//Index of the column
				value = value +1;
			}

			// Write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			DOMSource source = new DOMSource(xmlDocument);
			StreamResult result = new StreamResult(outputFile);
			transformer.transform(source, result);
		} catch (Exception ex) {
			throw new WriteMappingException("Error while writing the mapping File!", ex);
		}
	}

	/**
	 * Convert Output Mapping to Input Mapping.
	 * <p>We get output mapping and read it and try to make from its values the input mapping.</p>
	 * <p>This is used for validation output of multilevel in web application.</p>
	 *
	 * @param outMapping The mapping of output
	 * @return Map<String, CsvInColumnMapping> The mapping to be used in input.
	 */
	public static Map<String, CsvInColumnMapping> fromOutputMappingToInput(MultiLevelCsvOutColMapping outMapping) {
		Map<String, CsvInColumnMapping> inputMappings = new LinkedHashMap<>();
		if(ObjectUtil.validObject(outMapping) && ObjectUtil.validMap(outMapping.getDimensionsMappedOnLevel())) {
			for (Map.Entry<Integer, TreeMap<Integer, String>> entry : outMapping.getDimensionsMappedOnLevel().entrySet()) {
				Integer level = entry.getKey();
				if(ObjectUtil.validObject(entry.getValue())) {
					for(Map.Entry<Integer, String> val : entry.getValue().entrySet()) {
						String concept = val.getValue();
						Integer numOfColumn = val.getKey();
						if(ObjectUtil.validString(concept) && ObjectUtil.validObject(numOfColumn)) {
							CsvInColumnMapping csvInColumnMapping = new CsvInColumnMapping();
							csvInColumnMapping.setLevel(level);
							CsvColumn csvColumn = new CsvColumn(numOfColumn, "Column "+numOfColumn, concept);
							csvInColumnMapping.setColumns(Collections.singletonList(csvColumn));
							inputMappings.put(concept, csvInColumnMapping);
						}
					}
				}
			}
		}
		return inputMappings;
	}

	/**
	 * Convert Output Mapping to Input Mapping.
	 * <p>We get output mapping and read it and try to make from its values the input mapping.</p>
	 * <p>This is used for validation output of multilevel in web application.</p>
	 *
	 * @param outMapping The mapping of output
	 * @return Map<String, CsvInColumnMapping> The mapping to be used in input.
	 */
	public static Map<String, CsvInColumnMapping> fromOutputMappingComplexToInput(List<CsvOutColumnMapping> outMapping) {
		Map<String, CsvInColumnMapping> inputMappings = new LinkedHashMap<>();
		if(ObjectUtil.validObject(outMapping) && ObjectUtil.validCollection(outMapping)) {
			for(CsvOutColumnMapping mapping : outMapping) {
				if(ObjectUtil.validObject(mapping)) {
					CsvInColumnMapping csvInColumnMapping = new CsvInColumnMapping();
					csvInColumnMapping.setLevel(mapping.getLevel());
					CsvColumn csvColumn = new CsvColumn(mapping.getIndex(), "Column " + mapping.getIndex(), mapping.getComponent());
					csvColumn.setMaxOccurrences(mapping.getMaxOccurences());
					csvColumn.setMinOccurrences(mapping.getMinOcurrences());
					csvInColumnMapping.setColumns(Collections.singletonList(csvColumn));
					inputMappings.put(mapping.getComponent(), csvInColumnMapping);
				}
			}
		}
		return inputMappings;
	}
}
