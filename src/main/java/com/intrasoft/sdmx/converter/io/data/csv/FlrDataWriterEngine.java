package com.intrasoft.sdmx.converter.io.data.csv;

import com.intrasoft.sdmx.converter.config.FlrOutputConfig;
import com.intrasoft.sdmx.converter.io.data.TranscodingEngine;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthWriter;
import com.univocity.parsers.fixed.FixedWidthWriterSettings;
import org.apache.commons.lang3.StringUtils;
import org.estat.sdmxsource.util.csv.FixedWidth;
import org.estat.sdmxsource.util.csv.FlrInColumnMapping;
import org.sdmxsource.sdmx.api.constants.BASE_DATA_FORMAT;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.data.ComplexNodeValue;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.SdmxCsvDataWriterEngine;
import org.sdmxsource.util.ObjectUtil;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <h3>Writer that creates an Flr file for output.</h3>
 * FLR files are fixed-width csv's and <i>Univocity</i> FixedWidthWriter is used.
 * <p>Main difference from CSV is that there is no delimiter,
 * and a padding character is needed to fill in the extra chars in a value
 * that is smaller than the width.
 * </p>
 *
 * @see <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-775">SDMXCONV-775</a>
 * @since 2020-01-28
 */
public class FlrDataWriterEngine extends SdmxCsvDataWriterEngine {
	private FlrOutputConfig configurations;
	private FixedWidthWriter writer;
	private DataStructureBean dsd;
	private LinkedHashMap<String, FlrInColumnMapping> sortedMapping;
	private TranscodingEngine transcoding = null;

	public FlrDataWriterEngine(SDMX_SCHEMA schemaVersion, BASE_DATA_FORMAT dataFormat, OutputStream out) {
		this(schemaVersion, dataFormat, out, new FlrOutputConfig());
		throw new RuntimeException("No configurations found for the FLR writer, a mapping must be present to proceed to writing.");
	}

	public FlrDataWriterEngine(SDMX_SCHEMA schemaVersion, BASE_DATA_FORMAT dataFormat, OutputStream out, FlrOutputConfig configurations) {
		super(schemaVersion, dataFormat, out);
		super.setSchemaVersion(schemaVersion);
		super.setOut(out);
		this.configurations = configurations;
	}

	@Override
	public void startDataset(DataflowBean dataflowBean,
							 DataStructureBean dsd,
							 DatasetHeaderBean header,
							 AnnotationBean... annotations) {

		super.setCurrentPosition(POSITION.DATASET);
		this.dsd = dsd;
		super.setDataflowBean(dataflowBean);
		super.setPrimaryMeasureConcept(getComponentId(dsd.getPrimaryMeasure()));
		super.setTimeConcept(getComponentId(dsd.getTimeDimension()));
		super.setDimensionAtObservation(header.getDataStructureReference().getDimensionAtObservation());
		this.transcoding = new TranscodingEngine(configurations.getTranscoding());
		this.sortedMapping = this.configurations.getMapping();
		// Clear the components maps
		getGroupsMap().clear();
		getValuesGroupAttributeMap().clear();
		getValuesDataSetAttributeMap().clear();
		getValuesSeriesAttributeMap().clear();
		getValuesAnnotationsMap().clear();
		getValuesObsAttributeMap().clear();
		getPositionsConsceptMap().clear();
		getValuesConsceptMap().clear();

		FixedWidthWriterSettings settings = null;
		//Set the widths for every row
		FixedWidthFields fixedWidthFields;
		if (this.configurations.getMapping() != null && !this.configurations.getMapping().isEmpty() && !this.configurations.checkIfAutoExists(this.configurations.getMapping())) {
			setPositionsConsceptMap(populateCompomentToPosition(this.configurations.getMapping()));
			fixedWidthFields = setFieldLengths(this.configurations.getMapping());
		} else {
			populateDimensionPositionsMap(this.dsd, getPositionsConsceptMap());
			fixedWidthFields = setFieldLengthsFromCounting(this.configurations.getLengthsCounting(), getPositionsConsceptMap());
		}
		settings = new FixedWidthWriterSettings(fixedWidthFields);
		settings.getFormat().setLineSeparator("\n");
		settings.getFormat().setPadding(this.configurations.getPadding().charAt(0));
		settings.getMaxCharsPerColumn();
		//SDMXCONV-1080, do not trim spaces
		settings.setIgnoreLeadingWhitespaces(false);
		settings.setIgnoreTrailingWhitespaces(false);
        this.writer = new FixedWidthWriter(new OutputStreamWriter(getOut(), StandardCharsets.UTF_8), settings);
        getMappingFromFixedWidthFields(fixedWidthFields);
		populateGroupsList(this.dsd, getGroupsDimensionList());
		//Initialize row arrays
		arrayRow = new String[getPositionsConsceptMap().size()];
	}

	/**
	 * We calculate the mapping of Flr Output after reading the input,
	 * and eliminate "Auto" fields from mapping with real values.
	 * <p>Finally, we store it in the FlrOutputConfig.</p>
	 *
	 * @param fixedWidthFields
	 */
	private void getMappingFromFixedWidthFields(FixedWidthFields fixedWidthFields) {
		LinkedHashMap<String, FlrInColumnMapping> mapping = new LinkedHashMap<>();
		int index = 1;
		for(int i=0; i<fixedWidthFields.getFieldNames().length; i++){
			FixedWidth fixWidth = new FixedWidth();
			String fieldName = fixedWidthFields.getFieldNames()[i].toString();
			int fieldLength = fixedWidthFields.getFieldLengths()[i];
			String fixedValue = isFixedValue(fieldName);
			FlrInColumnMapping map = findMapping(fieldName, mapping);
			if(fixedValue == null) {
				fixWidth.setConceptName(fieldName);
				fixWidth.setStart(index);
				index = index + (fieldLength-1);
				fixWidth.setEnd(index);
				map.getPositions().add(fixWidth);
				map.setFixed(false);
				map.setLevel(1);
				index = index +1;
			} else {
				map.setFixed(true);
				map.setLevel(1);
				map.setFixedValue(fixedValue);
				map.setPositions(null);
			}
			mapping.put(fieldName, map);
		}
		configurations.setFinalMapping(mapping);
	}

	/**
	 * Read structure and populate a map for positions and columns with:
	 * <ul>
	 <li>For the first column, the dataflow column, always is the term DATAFLOW.</li>
	 <li>For a dimension column, is the dimension's ID or both ID and localised name.</li>
	 <li>For the measure column, always is the term OBS_VALUE.</li>
	 <li>For an attribute column, is the attribute's ID or both ID and localised name.</li>
	 <li>For any custom column, is any custom but unique term</li>
	 <li>For any column from header XML (optional)</li>
	 <li>For annotations internal SDMX_CSV (optional)</li>
	 * </ul>
	 * @param dsd DataStructureBean dsd
	 * @param pos LinkedHashMap with columns and positions written
	 */
	@Override
	protected void populateDimensionPositionsMap(DataStructureBean dsd, LinkedHashMap<String,Integer> pos) {
		//Last position indexer
		int indx;
		//Time Dimension
		DimensionBean time = dsd.getTimeDimension();
		//Dimensions listed from dsd
		List<DimensionBean> dims = dsd.getDimensions();
		for(DimensionBean dim:dims) {
			if(!dim.equals(time)) {
				indx = pos.size() - 1;
				if( dim!=null && dim.getId()!=null ) {
					pos.put(dim.getId(), indx + 1);
				}
			}
		}
		//Time dimension
		if(time!=null) {
			indx = pos.size() - 1;
			pos.put(time.getId(), indx + 1);
		}
		indx = pos.size() - 1;
		//Primary Measure
		// FIXME this should not be used in SDMX CSV 2.0.0 (removed from .NET CSV 2.0 writer)
		PrimaryMeasureBean primaryMeasure = dsd.getPrimaryMeasure();
		if (primaryMeasure != null) {
			pos.put(primaryMeasure.getId(), indx + 1);
		}
		List<MeasureBean> measures = dsd.getMeasures();
		if(ObjectUtil.validCollection(measures)) {
			for (MeasureBean measure : measures) {
				indx = pos.size() - 1;
				if (measure != null && measure.getId() != null && !pos.containsKey(measure.getId())) {
					pos.put(measure.getId(), indx + 1);
				}
			}
		}
		List<AttributeBean> attrs = dsd.getAttributes();
		//Attributes Listed from dsd
		for(AttributeBean attr:attrs) {
			indx = pos.size() - 1;
			if( attr!=null && attr.getId()!=null ) {
				pos.put(attr.getId(), indx + 1);
			}
		}
		//Add position for annotations for internal Sdmx Csv
		addAnnotationsPositions(indx, pos);
		this.header.addAll(pos.keySet());
		this.positionsConsceptMap = pos;
	}

	/**
	 * This method is used to find a mapping for a given field name in the provided LinkedHashMap.
	 * If the field name is complex (contains a space), it retrieves the mapping associated with the substring before the space.
	 * If no mapping is found for the field name, a new FlrInColumnMapping object is created.
	 *
	 * @param fieldName The name of the field for which the mapping is to be found.
	 * @param mapping The LinkedHashMap containing the mappings.
	 * @return Found FlrInColumnMapping object or a new one if no mapping was found.
	 */
	private FlrInColumnMapping findMapping(String fieldName, LinkedHashMap<String, FlrInColumnMapping> mapping) {
		FlrInColumnMapping map;
		// Check if the field name is complex (contains a space)
		if(ObjectUtil.validString(fieldName) && fieldName.contains(" ")) {
			// Get the substring before the space
			fieldName = StringUtils.substringBefore(fieldName, " ");
			// Check if the mapping contains the field name
			if(mapping.containsKey(fieldName)) {
				// Retrieve the mapping
				map = mapping.get(fieldName);
			} else {
				// Create a new mapping if none was found
				map = new FlrInColumnMapping();
			}
		} else {
			// Create a new mapping if the field name is not complex
			map = new FlrInColumnMapping();
		}
		return map;
	}

	/**
	 * This method checks if a given componentId has a fixed value in the mapping.
	 *
	 * <p>The method first checks if the sortedMapping is not null and not empty.
	 * If it is, the method returns null. If it's not, it checks if the sortedMapping contains the provided componentId.</p>
	 * <p>If the componentId is found, it retrieves the corresponding FlrInColumnMapping object and checks if it has a fixed value.
	 * If it does, it returns this fixed value. If it doesn't, it returns null.</p>
	 * <p>This method is primarily used when creating the final map, allowing us to exclude any entries that have an "AUTO" value.</p>
	 *
	 * @param componentId The id of the component to check for a fixed value.
	 * @return The fixed value of the componentId if it exists and is fixed, null otherwise.
	 */
	private String isFixedValue(String componentId){
		String fixed = null;
		if(this.sortedMapping!=null && !this.sortedMapping.isEmpty()){
			if(this.sortedMapping.containsKey(componentId)) {
				// Retrieve the FlrInColumnMapping object for the componentId
				FlrInColumnMapping map = this.sortedMapping.get(componentId);
				// If the FlrInColumnMapping object is not null and has a fixed value, retrieve the fixed value
				if(map!=null && map.isFixed()) {
					fixed = map.getFixedValue();
				}
			}
		}
		return fixed;
	}

	/**
	 * Transcoding added for components values.
	 *
	 * @param id    Component Id
	 * @param value Component value without the transcoding
	 * @see SdmxCsvDataWriterEngine#writeGroupKeyValue(String, String)
	 */
	@Override
	public void writeGroupKeyValue(String id, String value) {
		super.writeGroupKeyValue(id, transcoding.getValueFromTranscoding(id, value));
	}

	/**
	 * Transcoding added for components values.
	 *
	 * @param id    Component Id
	 * @param value Component value without the transcoding
	 * @see SdmxCsvDataWriterEngine#writeSeriesKeyValue(String, String)
	 */
	@Override
	public void writeSeriesKeyValue(String id, String value) {
		super.writeSeriesKeyValue(id, transcoding.getValueFromTranscoding(id, value));
	}

	/**
	 * Transcoding added for components values.
	 *
	 * @param id    Component Id
	 * @param value Component value without the transcoding
	 * @see SdmxCsvDataWriterEngine#writeAttributeValue(String, String)
	 */
	@Override
	public void writeAttributeValue(String id, String value) {
		super.writeAttributeValue(id, transcoding.getValueFromTranscoding(id, value));
	}

	/**
	 * Transcoding added for components values.
	 *
	 * @param obsConceptValue Concept Id
	 * @param obsValue        Component value without the transcoding
	 * @param annotations
	 * @see SdmxCsvDataWriterEngine#writeObservation(String, String, AnnotationBean... annotations)
	 */
	@Override
	public void writeObservation(String obsConceptValue, String obsValue, AnnotationBean... annotations) {
		if (getDimensionAtObservation() == null) {
			writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID, transcoding.getValueFromTranscoding(DimensionBean.TIME_DIMENSION_FIXED_ID, obsConceptValue), obsValue, annotations);
		} else {
			writeObservation(getDimensionAtObservation(), transcoding.getValueFromTranscoding(getDimensionAtObservation(), obsConceptValue), obsValue, annotations);
		}
	}

	@Override
	protected void writeRow() {
		if (ObjectUtil.validArray(arrayFinalRow)) {
			arrayFinalRow = addFixedValuesToRow(arrayFinalRow);
			writer.writeRow(arrayFinalRow);
			arrayFinalRow = null;
		}
	}

	@Override
	protected void addDatasetAttributes() {
		for (String dataset : getValuesDataSetAttributeMap().keySet()) {
			getValuesConsceptMap().put(dataset, transcoding.getValueFromTranscoding(dataset, getValuesDataSetAttributeMap().get(dataset)));
			if(ObjectUtil.validObject(positionsConsceptMap.get(dataset)))
				arrayRow[positionsConsceptMap.get(dataset)] = valuesDataSetAttributeMap.get(dataset);
		}
	}

	@Override
	protected void addObservation(String observationConceptId, String obsConceptValue) {
		getValuesConsceptMap().put(observationConceptId, transcoding.getValueFromTranscoding(observationConceptId, obsConceptValue));
		if(ObjectUtil.validObject(positionsConsceptMap.get(observationConceptId)))
			arrayRow[positionsConsceptMap.get(observationConceptId)] = obsConceptValue;
	}

	@Override
	public void writeObservation(String observationConceptId, String obsConceptValue, String obsValue, AnnotationBean... annotations) {
		clearComponents();
		writeRow();
		//add dataset attributes
		addDatasetAttributes();
		setCurrentPosition(POSITION.OBSERVATION);
		//add observation in the row
		addObservation(observationConceptId, obsConceptValue);
		//add primary measure
		addPrimaryMeasure(obsValue);
		//add group in the row
		populateGroup();
		arrayRow = new String[positionsConsceptMap.size()];
		//SDMXCONV-1295
		refreshComponentsValues(valuesConsceptMap);
		arrayFinalRow = arrayRow.clone();
	}

	/**
	 * <strong>Refresh Values from the map we keep all values for the row to be written.</strong>
	 * <br>
	 * Re-enter all the values. No need to call clearComponentsPosition from inside refreshComponentsValues
	 * @param valuesMap The map with values for a concept id.
	 * <p><small>Note: This does not include SDMX Headers Mapping and that is the only reason we dont inherit from SdmxCsvDataWriterEngine.</small></p>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1014">SDMXCONV-1014</a>
	 */
	@Override
	protected void refreshComponentsValues(LinkedHashMap<String, String> valuesMap) {
		if(ObjectUtil.validMap(positionsConsceptMap)) {
			for (Map.Entry<String, String> entry : valuesMap.entrySet()) {
				Integer position = positionsConsceptMap.get(entry.getKey());
				String value = transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
				if (ObjectUtil.validObject(position)) {
					arrayRow[position] = value;
				}
			}
		}
	}

	/**
	 * Method that takes the row that is about to be written and adds the fixed values.
	 * Is the only way to add fixed values for the attributes that don't exist at all in the input data.
	 * @param row
	 * @return
	 */
	private String[] addFixedValuesToRow(String[] row){
		if(row!=null && row.length>0 && sortedMapping!=null) {
			//Get fixed values from the map
			for (Map.Entry entry : sortedMapping.entrySet()) {
				FlrInColumnMapping map = (FlrInColumnMapping) entry.getValue();
				if (map != null && map.isFixed() && map.getFixedValue() != null) {
					String dim = (String) entry.getKey();
					//Find the position of the fixed value from the positions map
					if (getPositionsConsceptMap().containsKey(dim)) {
						int position = getPositionsConsceptMap().get(dim);
						//replace whatever value was there
						row[position] = map.getFixedValue();
					}
				}
			}
		}
		return row;
	}

	/**
	 * Method that retrieves the fixed value for a component, if a fixed value exists in the mapping.
	 * This method is used when we need to override the default value of a component with a fixed value.
	 *
	 * @param sortedMapping A sorted map of component keys to their mappings.
	 * @param key The key of the component for which we want to retrieve the fixed value.
	 * @return The fixed value for the component, if it exists. Null otherwise.
	 */
	private String getFixedValuesIfExist(LinkedHashMap<String, FlrInColumnMapping> sortedMapping, String key) {
		String value = null;

		// Check if the component key exists in the sorted mapping and if it has a fixed value.
		// This is necessary to ensure we don't attempt to retrieve a fixed value for a component that doesn't have one.
		if(sortedMapping!=null && !sortedMapping.isEmpty() && sortedMapping.containsKey(key)){
			FlrInColumnMapping mapping = sortedMapping.get(key);
			if(mapping.isFixed()) {
				value = mapping.getFixedValue();
			}
		}

		return value;
	}

	/**
	 * Writes complex attribute value based on the provided KeyValue object.
	 * If the KeyValue object or its complex node values are invalid, it throws an IllegalArgumentException.
	 * If a valid mapping with suffix is found, it writes the complex attribute as a complex concept.
	 * Otherwise, it writes complex nodes with separator.
	 *
	 * @param keyValue The KeyValue object containing the complex attribute value to write.
	 * @throws IllegalArgumentException if the keyValue or keyValue.getComplexNodeValues() is null or empty.
	 */
	@Override
	public void writeComplexAttributeValue(KeyValue keyValue) {
		if(ObjectUtil.validObject(keyValue) && ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
			if(ObjectUtil.validCollection(findMappingWithSuffix(keyValue))) {
				writeComplexAsConcept(keyValue);
			} else {
				writeComplexNodesWithSeparator(keyValue);
			}
		} else {
			throw new IllegalArgumentException("Error while writing complex attributes: keyValue.getComplexNodeValues() cannot be null or empty.");
		}
	}

	/**
	 * <strong>This method is used to find mappings that have a blank space in the concept id.</strong>
	 * <p>
	 *     If such a key is found this concept is complex with multiple values
	 *     e.g. MEASURE_1 has two values MEASURE_1 1 and MEASURE_1 2.
	 * </p>
	 * @param keyValue The KeyValue object which contains the concept to be searched for in the keys of the positionsConceptMap.
	 * @return A list of integers representing the values of the keys in the positionsConsceptMap that start with the concept from the keyValue parameter.
	 *         Returns null if no such keys are found.
	 */
	private List<Integer> findMappingWithSuffix(KeyValue keyValue) {
		List<Integer> complexMap = null;
		for (Map.Entry<String, Integer> key : this.positionsConsceptMap.entrySet()) {
			// Check if the key starts with the concept from the keyValue parameter
			if (key.getKey().startsWith(keyValue.getConcept()+" ")) {
				// Initialize the list if it hasn't been initialized yet
				if(!ObjectUtil.validObject(complexMap)) {
					complexMap = new ArrayList<>();
				}
				// Add the value of the key to the list
				complexMap.add(key.getValue());
			}
		}
		// Return the list of values or null if no keys were found that start with the concept from the keyValue parameter
		return complexMap;
	}

	/**
	 * <strong>This method writes complex node values as a concept for a given key-value pair.</strong>
	 * <p>
	 * It iterates over complex node values and creates a complex concept by appending an index to the concept of the key-value
	 * e.g. MEASURE_1 has two values MEASURE_1 1 and MEASURE_1 2.
	 * If the complex concept is present in the positions concept map, it fetches the value from the transcoding
	 * and writes the attribute value at the current position.
	 * </p>
	 * @param keyValue The key-value pair for which complex node values are to be written as a concept.
	 */
	private void writeComplexAsConcept(KeyValue keyValue) {
		// Initialize index for appending to complex concept
		int index = 1;
		for(ComplexNodeValue node: keyValue.getComplexNodeValues()) {
			// Create complex concept by appending index to the concept of the key-value
			String complexConcept = keyValue.getConcept() + " " + index;
			// Check if the complex concept is present in the positions concept map
			if (getPositionsConsceptMap().containsKey(complexConcept)) {
				// Fetch the value from the transcoding for the concept and the code of the node
				String value = transcoding.getValueFromTranscoding(keyValue.getConcept(), node.getCode());
				// Write the attribute value at the current position
				super.writeAttributeValue(getCurrentPosition(), complexConcept, value);
			}
			index++;
		}
	}

	/**
	 * This method handles the complex node values of the given KeyValue.
	 * <p>The complex nodes are treated as one field with concept separator between the values.</p>
	 *
	 * @param keyValue The KeyValue object to be processed.
	 */
	private void writeComplexNodesWithSeparator(KeyValue keyValue) {
		StringBuilder sb = new StringBuilder();
		int countNodes = 0;
		for(ComplexNodeValue node: keyValue.getComplexNodeValues()) {
			if(node.getCode() != null) {
				if(countNodes>0) {
					sb.append(this.configurations.getSubFieldSeparationChar());
				}
				sb.append(transcoding.getValueFromTranscoding(keyValue.getConcept(), node.getCode()));
				countNodes++;
			} else if(ObjectUtil.validCollection(node.getTexts())) {
				for(TextTypeWrapper text : node.getTexts()) {
					if(countNodes>0)
						sb.append(this.configurations.getSubFieldSeparationChar());
					sb.append(text.getLocale()!=null ? text.getLocale()+":"+text.getValue() : text.getValue());
					countNodes++;
				}
			}
		}
		super.writeAttributeValue(getCurrentPosition(), keyValue.getConcept(), sb.toString());
	}

	@Override
	public void writeMeasureValue(String id, String value) {
		valuesConsceptMap.put(id, value);
		writeAttributeValue(id, value);
	}

	@Override
	public void writeComplexMeasureValue(KeyValue keyValue) {
		writeComplexAttributeValue(keyValue);
	}

	@Override
	public void writeObservation(String obsConceptValue, AnnotationBean... annotations) {
		writeObservation(obsConceptValue, null, annotations);
	}

	@Override
	public void close(FooterMessage... footer) {
		setMapping();
		if (ObjectUtil.validArray(arrayFinalRow)) {
			arrayFinalRow = addFixedValuesToRow(arrayFinalRow);
			if (arrayFinalRow.length >= 0) {
				if (writer != null)
					writer.writeRow(arrayFinalRow);
			}
			arrayFinalRow = null;
		}
		if (writer != null)
			writer.close();
	}

	private void setMapping() {
		LinkedHashMap<String, FlrInColumnMapping> currentMap = configurations.getFinalMapping();
		LinkedHashMap<String, FlrInColumnMapping> newMap = new LinkedHashMap<>();
		if(ObjectUtil.validMap(currentMap)) {
			for (Map.Entry<String, FlrInColumnMapping> map : currentMap.entrySet()) {
				String fieldName;
				FlrInColumnMapping fieldMap = null;
				if (ObjectUtil.validObject(map) && ObjectUtil.validString(map.getKey())) {
					//Contains blank space means that it is complex
					if (map.getKey().contains(" ")) {
						// Get the substring before the space
						fieldName = StringUtils.substringBefore(map.getKey(), " ");
						if (!newMap.containsKey(fieldName)) {
							List<FlrInColumnMapping> currentSubset = currentMap.entrySet()
									.stream()
									.filter(e -> e.getKey().startsWith(fieldName + " "))
									.map(Map.Entry::getValue)
									.collect(Collectors.toList());
							List<FixedWidth> fixedWidths = new ArrayList<>();
							for (FlrInColumnMapping subset : currentSubset) {
								fixedWidths.addAll(subset.getPositions());
							}
							fieldMap = map.getValue();
							fieldMap.setPositions(fixedWidths);
						}
					} else {
						fieldName = map.getKey();
						fieldMap = map.getValue();
					}
					if (ObjectUtil.validString(fieldName) && ObjectUtil.validObject(fieldMap)) {
						newMap.put(fieldName, fieldMap);
					}
				}
			}
		}
		configurations.setFinalMapping(newMap);
	}

	/**
	 * This method is used to find all the positions as integers for all the components that exist in the mapping file.
	 * It populates a LinkedHashMap with the component as the key and its position as the value.
	 *
	 * @param sortedMapping A LinkedHashMap containing the sorted mapping of components.
	 * @return A LinkedHashMap where the key is the component and the value is its position.
	 */
	private LinkedHashMap<String, Integer> populateCompomentToPosition(LinkedHashMap<String, FlrInColumnMapping> sortedMapping) {
		LinkedHashMap<String, Integer> compomentToPosition = new LinkedHashMap<>();
		int i = 0;
		for (Map.Entry entry : sortedMapping.entrySet()) {
			String dimension = (String) entry.getKey();
			FlrInColumnMapping map = (FlrInColumnMapping) entry.getValue();
			//If there is a fixed value or some Positions then added to populate Map else is considered not existent
			if(map.getFixedValue()!=null && !"".equals(map.getFixedValue()) || ObjectUtil.validCollection(map.getPositions())) {
				// If the map has only one position, add it to the compomentToPosition map.
				if(map.getPositions().size() <= 1) {
					compomentToPosition.put(dimension, i);
					i++;
				} else {
					// If the map has multiple positions, add each one to the compomentToPosition map.
					int indx = 1;
					for(FixedWidth ignored : map.getPositions()) {
						// For complex components (SDMX 3.0), add the component ID followed by a blank space and count.
						compomentToPosition.put(dimension + " " + indx, i);
						indx++;
						i++;
					}
				}
			}
		}
		return compomentToPosition;
	}

	/**
	 * This method is used to set field lengths from a given counting map.
	 * It also updates the positions concept map for the given fields.
	 *
	 * @param lengthsCounting A map containing the field lengths obtained from counting.
	 * @param positionsConsceptMap A map containing the concept positions of the fields.
	 * @return A FixedWidthFields object with updated field lengths and positions.
	 */
	private FixedWidthFields setFieldLengthsFromCounting(Map<String, Integer> lengthsCounting, LinkedHashMap<String, Integer> positionsConsceptMap) {
		FixedWidthFields widthFields = new FixedWidthFields();
		LinkedHashMap<String, Integer> positions = new LinkedHashMap<>();
		int i = 0;
		for (Map.Entry entry : positionsConsceptMap.entrySet()) {
			String component = (String) entry.getKey();
			String fixedValue = getFixedValuesIfExist(this.sortedMapping, component);
			Integer length = null;
			if(fixedValue!=null) {
				length = fixedValue.length();
			} else {
				length = lengthsCounting.get(component);
			}
			if (length != null && length > 0) {
				positions.put(component, i);
				widthFields.addField(component, length);
				i++;
			}
		}
		setPositionsConsceptMap(positions);
		return widthFields;
	}

	/**
	 * Sets the field lengths of Univocity writer from the provided mapping.
	 * Univocity requires setting lengths for the fields that we are going to write.
	 * This method sorts the mapping and sets the field lengths accordingly.
	 *
	 * @param mapping a map containing the column mappings
	 * @return a FixedWidthFields object with the field lengths set
	 */
	private FixedWidthFields setFieldLengths(Map<String, FlrInColumnMapping> mapping) {
		//Sort the mapping
		FixedWidthFields widthFields = new FixedWidthFields();
		for (Map.Entry<String, FlrInColumnMapping> entry : mapping.entrySet()) {
			String attribute = entry.getKey();
			FlrInColumnMapping columnMapping = entry.getValue();
			int length = 0;
			// Case fixed = false and fixedValue = "AUTO"
			if (columnMapping.getFixedValue()!= null && !columnMapping.isFixed()
					&& "AUTO".equalsIgnoreCase(columnMapping.getFixedValue())) {
				if (this.configurations.getLengthsCounting().containsKey(attribute)) {
					length = this.configurations.getLengthsCounting().get(attribute);
				} else {
					length = 0;
				}
			// Case fixed = false and fixedValue contains positions
			} else if (!columnMapping.isFixed() && !"AUTO".equalsIgnoreCase(columnMapping.getFixedValue())) {
				int indx = 1;
				for (FixedWidth fixedPos : columnMapping.getPositions()) {
					length = (fixedPos.getEnd() + 1) - fixedPos.getStart();
					//This means that the component is Complex, so it has more than one positions
					if(columnMapping.getPositions().size() > 1) {
						if (length > 0) {
							widthFields.addField(attribute + " " + indx, length);
						}
						indx++;
					}
				}
			// In every other case we get the fixedValue's length even if it is zero, we eliminate zeros afterward
			} else {
				if (columnMapping.getFixedValue() != null) {
					length = columnMapping.getFixedValue().length();
				}
			}
			if(columnMapping.getPositions().size() <= 1) {
				if (length > 0) {
					widthFields.addField(attribute, length);
				}
			}
		}
		return widthFields;
	}

	public FlrOutputConfig getConfigurations() {
		return configurations;
	}

	public void setConfigurations(FlrOutputConfig configurations) {
		this.configurations = configurations;
	}
}
