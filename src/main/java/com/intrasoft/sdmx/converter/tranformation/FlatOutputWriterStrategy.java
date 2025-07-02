/*******************************************************************************
 * Copyright (c) 2013 Metadata Technology Ltd.
 *
 * All rights reserved. This program and the accompanying materials are made
 * available under the terms of the GNU Lesser General Public License v 3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This file is part of the SDMX Component Library.
 *
 * The SDMX Component Library is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * The SDMX Component Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with The SDMX Component Library If not, see
 * http://www.gnu.org/licenses/lgpl.
 *
 * Contributors:
 * Metadata Technology - initial API and implementation
 ******************************************************************************/

package com.intrasoft.sdmx.converter.tranformation;

import org.sdmxsource.sdmx.api.constants.DIMENSION_AT_OBSERVATION;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.PrimaryMeasureBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.util.ObjectUtil;

public class FlatOutputWriterStrategy implements IReadWriteStrategy {

	/**
	 * <b>Write keyable that doesn't contain time.</b>
	 *
	 * @param dataReaderEngine Reader that holds the current key that needs writing
	 * @param dataWriterEngine Writer that will write the readers key
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
		// If it is not TimeSeries and there is a time dimension we need to write the time dimension as a series key value
		if (currentKey.getDataStructure().getTimeDimension() != null) {
			dataWriterEngine.writeSeriesKeyValue(DimensionBean.TIME_DIMENSION_FIXED_ID, currentKey.getObsTime());
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
	public void readWrite(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, SDMX_SCHEMA sdmxSchema, String dimensionAtObservation) {
		DataStructureBean structure = dataReaderEngine.getDataStructure();
		if(ObjectUtil.validObject(dataReaderEngine) && ObjectUtil.validObject(dataReaderEngine))
		if(!structure.hasComplexComponents() && sdmxSchema != SDMX_SCHEMA.VERSION_THREE) {
			//Neither has complex component nor SMDX 3.0 DSD
			writePreviousVersion(dataReaderEngine, dataWriterEngine, dimensionAtObservation);
		} else if(structure.isCompatible(SDMX_SCHEMA.VERSION_TWO_POINT_ONE)) {
			//When the file is not Version Three and the schema is compatible with 3.0
			writeCompatibleVersion(dataReaderEngine, dataWriterEngine, dimensionAtObservation);
		} else {
			//The dsd has complex components and the file can only be SDMX 3.0
			writeThreeVersion(dataReaderEngine, dataWriterEngine, dimensionAtObservation);
		}
	}

	private void writeCompatibleVersion(DataReaderEngine dataReaderEngine,
										DataWriterEngine dataWriterEngine,
										String dimensionAtObservation) {
		Keyable currentKey = dataReaderEngine.getCurrentKey();
		if(currentKey.isSeries() && currentKey.getGroupName() == null) {
			dataWriterEngine.startSeries();
			writeSeriesKeys(currentKey, dataWriterEngine, dimensionAtObservation);
			while (dataReaderEngine.moveNextObservation()) {
				Observation observation = dataReaderEngine.getCurrentObservation();
				String observationValue = computeObservationValue(observation);
				AnnotationBean[] annotations = null;
				if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
					annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
				}
				if (dimensionAtObservation.equalsIgnoreCase(DimensionBean.TIME_DIMENSION_FIXED_ID)) {
					//We have time series
					dataWriterEngine.writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID, observation.getObsTime(), observationValue, annotations);
				} else {
					// we have non-time series
					String crossSectionalValue = currentKey.getKeyValue(dimensionAtObservation);
					// TODO: fix for flr TC_JIRA.SDMXCONV-1295_SDMX_3.0
					if(!ObjectUtil.validString(crossSectionalValue) && ObjectUtil.validString(currentKey.getCrossSectionConcept())) {
						crossSectionalValue = observation.getCrossSectionalValue().getCode();
					}
					dataWriterEngine.writeObservation(dimensionAtObservation, crossSectionalValue, observationValue, annotations);
				}
				//Write non-complex observation measures
				if(ObjectUtil.validCollection(observation.getMeasures())) {
					for(KeyValue measure : observation.getMeasures()) {
						if(!ObjectUtil.validCollection(measure.getComplexNodeValues())
								&& !PrimaryMeasureBean.FIXED_ID.equals(measure.getConcept())) {
							dataWriterEngine.writeMeasureValue(measure.getConcept(), measure.getCode());
						}
					}
				}
				for (KeyValue keyValue : observation.getAttributes()) {
					dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
				}
			}
		} else {
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
	 * <b>This method writes when dsd compatible with 3.0</b>
	 * <p>Primary Measure needs to be hacked.</p>
	 * @param dataReaderEngine
	 * @param dataWriterEngine
	 * @param dimensionAtObservation
	 */
	private void writeThreeVersion(DataReaderEngine dataReaderEngine,
								   DataWriterEngine dataWriterEngine,
								   String dimensionAtObservation) {
		Keyable currentKey = dataReaderEngine.getCurrentKey();
		if(currentKey.isSeries() && currentKey.getGroupName() == null) {
			dataWriterEngine.startSeries();
			writeSeriesKeys(currentKey, dataWriterEngine, dimensionAtObservation);
			while (dataReaderEngine.moveNextObservation()) {
				Observation observation = dataReaderEngine.getCurrentObservation();
				AnnotationBean[] annotations = null;
				if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
					annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
				}
				if(dimensionAtObservation.equalsIgnoreCase(DimensionBean.TIME_DIMENSION_FIXED_ID)) {
					//If we have primary concept time then we write observation with obs time
					dataWriterEngine.writeObservation(observation.getObsTime(), annotations);
				} else {
					//If the primary concept is something else then try to write observation with that
					String primaryConceptValue = currentKey.getKeyValue(dimensionAtObservation);
					dataWriterEngine.writeObservation(primaryConceptValue, annotations);
				}
				
				//Write non-complex observation measures first
				if(ObjectUtil.validCollection(observation.getMeasures())) {
					for (KeyValue measure : observation.getMeasures()) {
						if (!ObjectUtil.validCollection(measure.getComplexNodeValues())) {
							dataWriterEngine.writeMeasureValue(measure.getConcept(), measure.getCode());
						}
					}
				}
				//Write non-complex observation attributes first
				for (KeyValue keyValue : observation.getAttributes()) {
					if(!ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
						dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
					}
				}
				//Write complex observation measures then
				if(ObjectUtil.validCollection(observation.getMeasures())) {
					for (KeyValue measure : observation.getMeasures()) {
						if (ObjectUtil.validCollection(measure.getComplexNodeValues())) {
							dataWriterEngine.writeComplexMeasureValue(measure);
						}
					}
				}
				//Write complex observation attributes then
				for (KeyValue keyValue : observation.getAttributes()) {
					if(ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
						dataWriterEngine.writeComplexAttributeValue(keyValue);
					}
				}
			}
		} else {
			//Write if we have group
			dataWriterEngine.startGroup(currentKey.getGroupName());
			for (KeyValue keyValue : currentKey.getKey()) {
				if (!keyValue.getConcept().equalsIgnoreCase(dimensionAtObservation)) {
					if (ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
						dataWriterEngine.writeComplexAttributeValue(keyValue);
					} else {
						dataWriterEngine.writeGroupKeyValue(keyValue.getConcept(), keyValue.getCode());
					}
				}
			}
			for (KeyValue keyValue : currentKey.getAttributes()) {
				if(ObjectUtil.validCollection(keyValue.getComplexNodeValues())){
					dataWriterEngine.writeComplexAttributeValue(keyValue);
				} else {
					dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
				}
			}
		}
	}

	private void writeSeriesKeys(Keyable currentKey, DataWriterEngine dataWriterEngine, String dimensionAtObservation) {
		for (KeyValue keyValue : currentKey.getKey()) {
			if (!keyValue.getConcept().equalsIgnoreCase(dimensionAtObservation)) {
				if(ObjectUtil.validObject(keyValue) && ObjectUtil.validCollection(keyValue.getComplexNodeValues())){
					dataWriterEngine.writeComplexAttributeValue(keyValue);
				} else {
					dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
				}
			}
		}
		for (KeyValue keyValue : currentKey.getAttributes()) {
			if(ObjectUtil.validObject(keyValue) && ObjectUtil.validCollection(keyValue.getComplexNodeValues())){
				dataWriterEngine.writeComplexAttributeValue(keyValue);
			} else {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	/**
	 * This method writes when dsd compatible with previous versions in case 2.1, 2.0 etc
	 * @param dataReaderEngine
	 * @param dataWriterEngine
	 * @param dimensionAtObservation
	 */
	private void writePreviousVersion(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, String dimensionAtObservation) {
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
