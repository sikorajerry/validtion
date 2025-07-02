package org.estat.sdmxsource.engine.reader;

import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.PrimaryMeasureBean;
import org.sdmxsource.sdmx.api.model.data.ComplexNodeValue;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.util.ObjectUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <h3>Reader that counts the max length of all the values per component.</h3>
 * Class that is used only in case we need to write in FLR format
 * and no mapping is provided by the user.
 * To write in fixed-Length format such as Flr
 * we must set the character length of every column.
 * <p>We read all the input and store the max length of all values for a component (dimensions, attributes etc.)</p>
 */
public class DataCountingLengthReaderEngine {
	private DataReaderEngine dataReaderEngine;
	private HeaderBean header;
	private List<DatasetHeaderBean> datasetHeaderBeans = new ArrayList<>();
	private DataStructureBean dataStructureBean;
	private DataflowBean dataflowBean;
	private Map<String, Integer> lengthPerComponent = new HashMap<>();

	public DataCountingLengthReaderEngine(DataReaderEngine dataReaderEngine) {
		this.dataReaderEngine = dataReaderEngine;
	}

	/**
	 * This method reads the data and calculates the maximum lengths for the values.
	 * It is important for understanding the structure and size of the data.
	 *
	 * @return A map where the keys are the data components and the values are their maximum lengths.
	 */
	public Map<String, Integer> countLengths() {
		try {
			dataReaderEngine.reset();
			header = dataReaderEngine.getHeader();
			while (dataReaderEngine.moveNextDataset()) {
				DatasetHeaderBean datasetHeader = dataReaderEngine.getCurrentDatasetHeaderBean();
				datasetHeaderBeans.add(datasetHeader);
				String dimensionAtObs = datasetHeader.getDataStructureReference().getDimensionAtObservation();
				dataStructureBean = dataReaderEngine.getDataStructure();
				// Check if the data structure has complex components (SDMX 3.0)
				boolean isSdmx30 = dataStructureBean.hasComplexComponents();
				dataflowBean = dataReaderEngine.getDataFlow();
				// Iterate over the attributes of the dataset and find their maximum lengths
				for (KeyValue kv : dataReaderEngine.getDatasetAttributes()) {
					findMaxOverKeyvalue(kv, isSdmx30);
				}
				while (dataReaderEngine.moveNextKeyable()) {
					Keyable currentKey = dataReaderEngine.getCurrentKey();
					// Iterate over the dimensions of the current keyable and find their maximum lengths
					for (KeyValue kv : currentKey.getKey()) {
						findMaxOverKeyvalue(kv, isSdmx30);
					}
					// Iterate over the attributes of the current keyable and find their maximum lengths
					for (KeyValue kv : currentKey.getAttributes()) {
						findMaxOverKeyvalue(kv, isSdmx30);
					}
					// Iterate over the observations in the data reader engine
					while (dataReaderEngine.moveNextObservation()) {
						Observation observation = dataReaderEngine.getCurrentObservation();
						if(!isSdmx30) {
							if (observation.getCrossSectionalValue() != null) {
								findMaxOverSimpleKeyvalue(dimensionAtObs, observation.getCrossSectionalValue().getCode());
							} else {
								findMaxOverSimpleKeyvalue(dataStructureBean.getTimeDimension().getId(), observation.getObsTime());
								findMaxOverSimpleKeyvalue(dataStructureBean.getPrimaryMeasure().getId(), observation.getObservationValue());
							}
						} else {
							if (dimensionAtObs.equalsIgnoreCase(DimensionBean.TIME_DIMENSION_FIXED_ID)) {
								//If we have primary concept time then we write observation with obs time
								findMaxOverSimpleKeyvalue(dataStructureBean.getTimeDimension().getId(), observation.getObsTime());
							} else {
								//If the primary concept is something else then try to write observation with that
								String primaryConceptValue = currentKey.getKeyValue(dimensionAtObs);
								findMaxOverSimpleKeyvalue(dimensionAtObs, primaryConceptValue);
							}
						}

						if (ObjectUtil.validCollection(observation.getMeasures())) {
							for (KeyValue measure : observation.getMeasures()) {
								findMaxOverKeyvalue(measure, isSdmx30);
							}
						}  else if (ObjectUtil.validCollection(dataReaderEngine.getDataStructure().getMeasures())
								&& ObjectUtil.validObject(dataReaderEngine.getDataStructure().getMeasure(PrimaryMeasureBean.FIXED_ID))) {
							/* 	In case we are having SDMX 3.0 but the file we read
								is in previous version the OBS_VALUE
								is not read as measure but as a primary measure. See also SDMXCONV-1523 */
							if (ObjectUtil.validString(observation.getObservationValue()))
								findMaxOverSimpleKeyvalue(PrimaryMeasureBean.FIXED_ID, observation.getObservationValue());
						}

						for (KeyValue kv : observation.getAttributes()) {
							findMaxOverKeyvalue(kv, isSdmx30);
						}
					}
				}
			}
		} finally {
/*			if(dataReaderEngine!=null)
				dataReaderEngine.close();*/
		}
		return lengthPerComponent;
	}

	private void findMaxOverKeyvalue(KeyValue keyValue, boolean isSdmx30) {
		if(isSdmx30 && dataStructureBean.isComponentComplex(keyValue.getConcept())) {
			findMaxOverComplexKeyvalue(keyValue);
		} else {
			findMaxOverSimpleKeyvalue(keyValue.getConcept(), keyValue.getCode());
		}
	}


	/**
	 * This method is used to find the maximum length of a complex key-value pair.
	 * A complex key-value pair is defined as a key-value pair where the value is a string
	 * composed of multiple components separated by a '#'.
	 *
	 * @param keyValue The complex key-value pair to be evaluated.
	 */
	private void findMaxOverComplexKeyvalue(KeyValue keyValue) {
		// Check if the keyValue object is valid
		if(ObjectUtil.validObject(keyValue)) {
			// Retrieve the current maximum length for the given concept
			Integer currentLength =  lengthPerComponent.get(keyValue.getConcept());
			if(currentLength == null) {
				currentLength = 0;
			}
			Integer complexLength = 0;
			int i = 0;
			// Check if the complex node values collection is valid
			if(ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
				// Iterate over each complex node value
				for(ComplexNodeValue complex : keyValue.getComplexNodeValues()) {
					// Check if it's not the last element in the collection
					if (keyValue.getComplexNodeValues().size()-1 != i) {
						// If the complex node value's code is valid, add its length to the complexLength
						if (ObjectUtil.validString(complex.getCode())) {
							complexLength = complexLength + complex.getCode().length() + 1;
						}
					} else {
						// If it's the last element, add its length to the complexLength without the separator
						if (ObjectUtil.validString(complex.getCode())) {
							complexLength = complexLength + complex.getCode().length();
						}
					}
					i++;
				}
			}
			// Find the maximum between the current maximum length and the complexLength
			currentLength = findMax(complexLength, currentLength);
			// Update the maximum length for the given concept
			lengthPerComponent.put(keyValue.getConcept(), currentLength);
		}
	}

	private void findMaxOverSimpleKeyvalue(String concept, String value) {
		if(value!=null && !value.isEmpty()) {
			Integer currentLength =  lengthPerComponent.get(concept);
			if(currentLength == null) {
				currentLength = 0;
			}
			currentLength = findMax(value.length(), currentLength);
			lengthPerComponent.put(concept, currentLength);
		}
	}

	/**
	 * Method that finds max length
	 * @param currentLength current component's character length
	 * @param maxLength the max found till now
	 * @return int the maximum length
	 */
	private int findMax(int currentLength, int maxLength) {
		if(currentLength> maxLength) {
			maxLength = currentLength;
		}
		return maxLength;
	}
}
