package com.intrasoft.sdmx.converter.tranformation;

import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.util.ObjectUtil;

/**
 * <b>Writes the keyable when the output is SDMX 2.0 Cross Sectional</b>
 * Covers the following cases:
 * <ul>
 *     <li>Time Series</li>
 *     <li>No Time at all but no Cross measures</li>
 *     <li>No Time and the dsd has Cross Sectional measures</li>
 * </ul>
 */
public class CrossSectionalWriterStrategy implements IReadWriteStrategy {

	boolean hasTimeDimension = false;

	/**
	 * <b>Write keyable that doesn't contain time.</b>
	 *
	 * @param dataReaderEngine Reader that holds the current key that needs writing
	 * @param dataWriterEngine Writer that will write the readers key
	 * @param currentKey       The key we want to write
	 * @param dimensionAtObs   Dimension At Observation
	 * @implNote Writers may change to write All Dimensions as Dimension at Observation, if this is requested.
	 */
	public static void write21NonTimeseriesKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs) {
		dataWriterEngine.startSeries();
		for (KeyValue keyValue : currentKey.getKey()) {
			if (ObjectUtil.validString(keyValue.getCode()))
				dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
			else
				throw new SdmxSemmanticException("Empty value for component " + keyValue.getConcept());
		}
		for (KeyValue keyValue : currentKey.getAttributes()) {
			dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
		}
		while (dataReaderEngine.moveNextObservation()) {
			Observation observation = dataReaderEngine.getCurrentObservation();
			AnnotationBean[] annotations = null;
			if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
				annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
			}
			dataWriterEngine.writeObservation(dimensionAtObs, observation.getSeriesKey().getKeyValue(dimensionAtObs), observation.getObservationValue(), annotations);
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	@Override
	public String computeDimensionAtObservation(DataReaderEngine dataReaderEngine) {
		hasTimeDimension = dataReaderEngine.getDataStructure().getTimeDimension() != null;
		String dimensionAtObservation = null;
		//If there is a measure dimension we found our dimension at observation
		if (dataReaderEngine.getDataStructure().getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION) != null &&
				dataReaderEngine.getDataStructure().getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION).size() > 0) {
			DimensionBean measure = dataReaderEngine.getDataStructure().getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION).get(0);
			dimensionAtObservation = measure.getId();
			return dimensionAtObservation;
		}
		return IReadWriteStrategy.super.computeDimensionAtObservation(dataReaderEngine);
	}

	@Override
	public void readWrite(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, SDMX_SCHEMA sdmxSchema, String dimensionAtObservation) {
		Keyable currentKey = dataReaderEngine.getCurrentKey();
		if (!currentKey.isSeries() || currentKey.getGroupName() != null) {
			dataWriterEngine.startGroup(currentKey.getGroupName());
			for (KeyValue keyValue : currentKey.getKey()) {
				dataWriterEngine.writeGroupKeyValue(keyValue.getConcept(), keyValue.getCode());
			}
			for (KeyValue keyValue : currentKey.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		} else {
			if (!hasTimeDimension) {
				if (dataReaderEngine.getDataStructure() instanceof CrossSectionalDataStructureBean) {
					writeCrossSectionalKeyables(dataReaderEngine, dataWriterEngine, currentKey, dimensionAtObservation);
				} else {
					write21NonTimeseriesKeyables(dataReaderEngine, dataWriterEngine, currentKey, dimensionAtObservation);
				}
			} else {
				writeTimeseriesKeyables(dataReaderEngine, dataWriterEngine, currentKey, dimensionAtObservation);
			}
		}
	}

	/**
	 * Write key when we write Cross Sectional and we have time.
	 *
	 * @param dataReaderEngine Reader that holds the current key that needs writing
	 * @param dataWriterEngine Writer that will write the readers key
	 * @param currentKey       The key we want to write
	 * @param dimensionAtObs   Dimension At Observation
	 */
	private void writeTimeseriesKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs) {
		dataWriterEngine.startSeries();
		for (KeyValue keyValue : currentKey.getKey()) {
			if (!keyValue.getConcept().equalsIgnoreCase(dimensionAtObs)) {
				dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
		for (KeyValue keyValue : currentKey.getAttributes()) {
			dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
		}
		while (dataReaderEngine.moveNextObservation()) {
			Observation observation = dataReaderEngine.getCurrentObservation();
			AnnotationBean[] annotations = null;
			if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
				annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
			}
			String crossSectionalValue = currentKey.getKeyValue(dimensionAtObs);
			dataWriterEngine.writeObservation(dimensionAtObs, crossSectionalValue, observation.getObservationValue(), annotations);

			//write the TIME_PERIOD this is a HACK for CrossSectionDataWriterEngine write after the writeObservation
			if (!observation.isCrossSection() && ObjectUtil.validString(observation.getObsTime())) {
				dataWriterEngine.writeSeriesKeyValue(DimensionBean.TIME_DIMENSION_FIXED_ID, observation.getObsTime());
			}
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	/**
	 * <b>Write Cross Key with no time dimension</b>
	 * <p>In case of Cross we write the series concepts inside move Observation and the primary measure is not OBS_VALUE but code of cross.</p>
	 *
	 * @param dataReaderEngine Reader that we read the input
	 * @param dataWriterEngine Writer here will always be Cross Sectional
	 * @param currentKey       The key that we have read and we want to write
	 * @param dimensionAtObs   The dimension at Observation, this needs to be computed prior the writing of this key
	 */
	private void writeCrossSectionalKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs) {
		dataWriterEngine.startSeries();
		while (dataReaderEngine.moveNextObservation()) {
			Observation observation = dataReaderEngine.getCurrentObservation();
			AnnotationBean[] annotations = null;
			if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
				annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
			}
			for (KeyValue keyValue : currentKey.getKey()) {
				dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
			}
			for (KeyValue keyValue : currentKey.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
			dataWriterEngine.writeObservation(dimensionAtObs, observation.getCrossSectionalValue().getCode(), observation.getObservationValue(), annotations);
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}
}
