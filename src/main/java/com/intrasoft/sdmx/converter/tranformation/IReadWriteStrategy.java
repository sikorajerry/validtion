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

import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.PrimaryMeasureBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.util.ObjectUtil;

import java.util.List;

public interface IReadWriteStrategy {

	default void reset(DataReaderEngine dataReaderEngine) {
		if (dataReaderEngine == null)
			throw new SdmxSemmanticException("Error during reading the input file. No suitable reader is identified");

		dataReaderEngine.reset();
	}

	default void writeHeader(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine) {
		HeaderBean header = dataReaderEngine.getHeader();
		dataWriterEngine.writeHeader(header);
	}

	default void writeDataset(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine) {
		//Reading and writing starts here
		DatasetHeaderBean datasetHeader = dataReaderEngine.getCurrentDatasetHeaderBean();
		dataWriterEngine.startDataset(dataReaderEngine.getDataFlow(), dataReaderEngine.getDataStructure(), datasetHeader);
		for (KeyValue kv : dataReaderEngine.getDatasetAttributes()) {
			if(ObjectUtil.validObject(kv) && ObjectUtil.validCollection(kv.getComplexNodeValues())){
				dataWriterEngine.writeComplexAttributeValue(kv);
			} else if (kv.getCode() != null && !kv.getCode().isEmpty()) {
				//B. Write DataSet Attributes
				dataWriterEngine.writeAttributeValue(kv.getConcept(), kv.getCode());
			}
		}
	}

	default String computeDimensionAtObservation(DataReaderEngine dataReaderEngine) {
		String dimensionAtObservation = null;
		DatasetHeaderBean datasetHeader = dataReaderEngine.getCurrentDatasetHeaderBean();
		if (ObjectUtil.validObject(datasetHeader)
				&& datasetHeader.getDataStructureReference().getDimensionAtObservation() != null
				&& ("TIME".equalsIgnoreCase(datasetHeader.getDataStructureReference().getDimensionAtObservation()) ||
						"TIME_PERIOD".equalsIgnoreCase(datasetHeader.getDataStructureReference().getDimensionAtObservation()))) {
			//TIME replaced with TIME_PERIOD
			dimensionAtObservation = DimensionBean.TIME_DIMENSION_FIXED_ID;
			DatasetStructureReferenceBeanImpl datasetStructureReference = new DatasetStructureReferenceBeanImpl(datasetHeader.getDataStructureReference().getId(),
					datasetHeader.getDataStructureReference().getStructureReference(),
					null,
					null,
					dimensionAtObservation);
			//Modify in case of TIME the datasetHeader given
			dataReaderEngine.getCurrentDatasetHeaderBean().modifyDataStructureReference(datasetStructureReference);
		} else {
			dimensionAtObservation = computeDimensionAtObservation(datasetHeader.getDataStructureReference().getDimensionAtObservation(), dataReaderEngine.getDataStructure());
		}

		return dimensionAtObservation;
	}

	/**
	 * <b>Case of flat File with AllDimensions</b>
	 *
	 * <p>When we have a flat file where dimension At observation is set to Alldimensions,
	 * we want a dimension to write the observations with we choose the last from the series,
	 * if there is no measure and no time. To achieve this we need to not write that dim
	 * while writing the series and write it during writeObservation.</p>
	 * <p>It contains the case of reading a flat file (file that the dimension at Observation is AllDimensions). </p>
	 * <p>When we read AllDimensions we write the last concept and make it dimension At observation.</p>
	 * <p>This is because the writers dont support writing without dimAtObs.</p>
	 *
	 * @param dataReaderEngine Reader that holds the current key that needs writing
	 * @param dataWriterEngine Writer that will write the readers key
	 * @param currentKey       The key we want to write
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-945">SDMXCONV-945</a>
	 */
	default void writeFlat(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey) {
		dataWriterEngine.startSeries();
		String dimensionAtObservation = IReadWriteStrategy.computeDimensionAtObservation(currentKey.getCrossSectionConcept(), dataReaderEngine.getDataStructure());
		//SDMXCONV-892
		for (KeyValue keyValue : currentKey.getKey()) {
			//If Concept is dimension At observation we don't write it here we will write it with the Observation
			if (!keyValue.getConcept().equalsIgnoreCase(dimensionAtObservation)) {
				dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
			}
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
			dataWriterEngine.writeObservation(dimensionAtObservation, observation.getSeriesKey().getKeyValue(dimensionAtObservation), obsValue, annotations);
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	/**
	 * In SDMX 3.0 files the OBS_VALUE is just a simple measure and can be found under measures of Observation
	 * @param observation The current Observation
	 * @return String observation Value
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1523">SDMXCONV-1523</a>
	 */
	default String computeObservationValue(Observation observation) {
		String obsValue = observation.getObservationValue();
		//In SDMX 3.0 files the OBS_VALUE is just a simple measure and can be found under measures of Observation
		if(!ObjectUtil.validObject(obsValue) && ObjectUtil.validObject(observation.getMeasure(PrimaryMeasureBean.FIXED_ID))) {
			obsValue = observation.getMeasure(PrimaryMeasureBean.FIXED_ID).getCode();
		}
		return obsValue;
	}

	/**
	 * <b>Compute the dimension at Observation</b>
	 * <p>This method was made for computing dimensionAtObservation == null
	 * or dimensionAtObservation == AllDimensions
	 * For AllDimensions case we use TimeDimension or the last Dimension Found.</p>
	 * @see 'SDMXCONV-841'
	 * @param dimensionAtObservation
	 * @param dataStructureBean
	 * @return dimensionAtObservation
	 */
	static String computeDimensionAtObservation(String dimensionAtObservation, DataStructureBean dataStructureBean) {
		// This algorithm probably exists also elsewhere
		// fail safe or when AllDimensions are set
		if (dimensionAtObservation == null || "AllDimensions".equalsIgnoreCase(dimensionAtObservation)) {
			if (dataStructureBean.getTimeDimension() != null) {
				dimensionAtObservation = dataStructureBean.getTimeDimension().getId();
			} else {
				List<DimensionBean> measureDimensions = dataStructureBean.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
				List<DimensionBean> normalDimensions = dataStructureBean.getDimensions(SDMX_STRUCTURE_TYPE.DIMENSION);
				if (measureDimensions.isEmpty()) {
					if (dataStructureBean instanceof CrossSectionalDataStructureBean) {
						CrossSectionalDataStructureBean crossDsd = (CrossSectionalDataStructureBean) dataStructureBean;
						List<ComponentBean> crossSectionalAttachObservation = crossDsd.getCrossSectionalAttachObservation(SDMX_STRUCTURE_TYPE.DIMENSION);
						if (crossSectionalAttachObservation.isEmpty()) {
							// impossible scenario
							dimensionAtObservation = normalDimensions.get(normalDimensions.size() - 1).getId();
						} else {
							dimensionAtObservation = crossSectionalAttachObservation.get(crossSectionalAttachObservation.size() - 1).getId();
						}
					} else {
						// get the last
						dimensionAtObservation = normalDimensions.get(normalDimensions.size() - 1).getId();
					}
				} else {
					dimensionAtObservation = measureDimensions.get(measureDimensions.size() - 1).getId();
				}
			}
		}
		return dimensionAtObservation;
	}

	void readWrite(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, SDMX_SCHEMA sdmxSchema, String dimensionAtObservation);
}
