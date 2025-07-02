package com.intrasoft.sdmx.converter.tranformation;

import org.sdmxsource.sdmx.api.constants.DIMENSION_AT_OBSERVATION;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
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
 * <b>Writes the keyable when the output is SDMX 2.1</b>
 * Covers the following cases:
 * <ul>
 *     <li>Time Series</li>
 *     <li>No Time at all</li>
 *     <li>No Time and dimension at Observation is AllDimensions</li>
 * </ul>
 */
public class TwoPointOneWriterStrategy implements IReadWriteStrategy {

	boolean hasTimeDimension = false;

	/**
	 * <b>Write keyable that doesn't contain time.</b>
	 *
	 * @param dataReaderEngine Reader that holds the current key that needs writing
	 * @param dataWriterEngine Writer at this
	 * @param currentKey       The key we want to write
	 * @implNote Writers may change to write All Dimensions as Dimension at Observation, if this is requested.
	 */
	public void write21NonTimeseriesKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey) {
		dataWriterEngine.startSeries();
		String dimensionAtObservation = IReadWriteStrategy.computeDimensionAtObservation(currentKey.getCrossSectionConcept(), dataReaderEngine.getDataStructure());
		//SDMXCONV-892
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
			String obsValue = computeObservationValue(observation);
			AnnotationBean[] annotations = null;
			if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
				annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
			}
			dataWriterEngine.writeObservation(dimensionAtObservation, observation.getCrossSectionalValue().getCode(), obsValue, annotations);
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	@Override
	public String computeDimensionAtObservation(DataReaderEngine dataReaderEngine) {
		hasTimeDimension = dataReaderEngine.getDataStructure().getTimeDimension() != null;
		return IReadWriteStrategy.super.computeDimensionAtObservation(dataReaderEngine);
	}

	@Override
	public void readWrite(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, SDMX_SCHEMA sdmxSchema, String dimensionAtObservation) {
		Keyable currentKey = dataReaderEngine.getCurrentKey();
		if (currentKey.isTimeSeries() && currentKey.getGroupName() == null) {
			//Write a time series
			writeTimeseriesKeyables(dataReaderEngine, dataWriterEngine, currentKey, dimensionAtObservation);
		} else if (!currentKey.isTimeSeries() && currentKey.getGroupName() == null) {
			//Write without time
			if (!(dataReaderEngine.getDataStructure() instanceof CrossSectionalDataStructureBean)
					&& ObjectUtil.validString(currentKey.getCrossSectionConcept())
					&& currentKey.getCrossSectionConcept().equals(DIMENSION_AT_OBSERVATION.ALL.getVal())) {
				writeFlat(dataReaderEngine, dataWriterEngine, currentKey);
			} else {
				write21NonTimeseriesKeyables(dataReaderEngine, dataWriterEngine, currentKey);
			}
		}
		if (!currentKey.isSeries() || currentKey.getGroupName() != null) {
			//Write if we have group
			dataWriterEngine.startGroup(currentKey.getGroupName());
			for (KeyValue keyValue : currentKey.getKey()) {
				dataWriterEngine.writeGroupKeyValue(keyValue.getConcept(), keyValue.getCode());
			}
			for (KeyValue keyValue : currentKey.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	/**
	 * <b>Common case of writing a time series</b>
	 *
	 * @param dataReaderEngine Reader that holds the current key that needs writing
	 * @param dataWriterEngine Writer that will write the readers key
	 * @param currentKey       The key we want to write
	 * @param dimensionAtObs   Dimension At Observation
	 */
	private void writeTimeseriesKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs) {
		boolean isTimeSeries = currentKey.isTimeSeries();
		boolean isTimeDimension = dimensionAtObs.equalsIgnoreCase(DimensionBean.TIME_DIMENSION_FIXED_ID);
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
			String obsValue = computeObservationValue(observation);
			AnnotationBean[] annotations = null;

			if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
				annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
			}
			if (isTimeSeries && isTimeDimension) {
				dataWriterEngine.writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID, observation.getObsTime(), obsValue, annotations);
			} else {
				String crossSectionalValue = currentKey.getKeyValue(dimensionAtObs);
				dataWriterEngine.writeObservation(dimensionAtObs, crossSectionalValue, obsValue, annotations);
			}
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}
}
