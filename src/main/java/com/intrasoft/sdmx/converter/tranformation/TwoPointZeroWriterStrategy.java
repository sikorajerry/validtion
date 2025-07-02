package com.intrasoft.sdmx.converter.tranformation;

import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;

/**
 * <b>Writes the keyable when the output is SDMX 2.0 except Cross Sectional.</b>
 * Covers the following cases:
 * <ul>
 *     <li>Time Series</li>
 * </ul>
 *
 * @implNote 2.0 Writers always will have time
 */
public class TwoPointZeroWriterStrategy implements IReadWriteStrategy {
	boolean hasTimeDimension = false;

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
	 * @param dataWriterEngine Writer at this could be only 2.0 except Cross
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
			dataWriterEngine.writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID, observation.getObsTime(), observation.getObservationValue(), annotations);
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}
}
