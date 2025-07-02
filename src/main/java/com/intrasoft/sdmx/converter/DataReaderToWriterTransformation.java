/**
 * Copyright 2015 EUROSTAT
 * <p>
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter;

import com.intrasoft.sdmx.converter.io.data.Formats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.DIMENSION_AT_OBSERVATION;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.CrossSectionDataWriterEngine;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.util.ObjectUtil;
import java.util.List;

/**
 * Class that middles data from reader to writer.
 *
 * @author Mihaela Munteanu
 * @since 29.05.2017
 *
 */
@Deprecated
public class DataReaderToWriterTransformation {

	private static Logger logger = LogManager.getLogger(DataReaderToWriterTransformation.class);

	//SDMXCONV-1087, output file's format added in parameter list of copyToWriter method's signature
	public static void copyToWriter(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Formats outFormat) {
		dataReaderEngine.reset();
		HeaderBean header = dataReaderEngine.getHeader();
		dataWriterEngine.writeHeader(header);
		try {
			if (dataReaderEngine != null) {
				while (dataReaderEngine.moveNextDataset()) {
					DatasetHeaderBean datasetHeader = dataReaderEngine.getCurrentDatasetHeaderBean();
					boolean hasTimeDimension = dataReaderEngine.getDataStructure().getTimeDimension() != null;
					boolean isCrossSectionalDsd = dataReaderEngine.getDataStructure() instanceof CrossSectionalDataStructureBean;
					//SDMXCONV-1087
					//Checks what is needed to throw error
					if (!hasTimeDimension) {
						if (outFormat == Formats.COMPACT_SDMX || outFormat == Formats.GENERIC_SDMX || outFormat == Formats.UTILITY_SDMX) {
							throw new SdmxSemmanticException("The DSD is not SDMX v2.0 compatible, It does not have a Time Dimension which is needed for time series 2.0 formats Compact, Generic and Utility");
						}
					}

					//1. first compute dimension at Observation for all cases
					DatasetStructureReferenceBean datasetStructureReference = null;
					String dimensionAtObservation = null;
					if (dataWriterEngine instanceof CrossSectionDataWriterEngine &&
							dataReaderEngine.getDataStructure().getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION) != null &&
							dataReaderEngine.getDataStructure().getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION).size() > 0) {

						DimensionBean measure = ((CrossSectionalDataStructureBean) dataReaderEngine.getDataStructure()).getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION).get(0);
						dimensionAtObservation = measure.getId();

					} else if (datasetHeader != null && datasetHeader.getDataStructureReference().getDimensionAtObservation() != null &&
							("TIME".equalsIgnoreCase(datasetHeader.getDataStructureReference().getDimensionAtObservation()) ||
									"TIME_PERIOD".equalsIgnoreCase(datasetHeader.getDataStructureReference().getDimensionAtObservation()))) {
						//TIME replaced with TIME_PERIOD 
						dimensionAtObservation = DimensionBean.TIME_DIMENSION_FIXED_ID;
						datasetStructureReference = new DatasetStructureReferenceBeanImpl(datasetHeader.getDataStructureReference().getId(),
								datasetHeader.getDataStructureReference().getStructureReference(),
								null,
								null,
								dimensionAtObservation);
						//2. Modify in case of TIME the datasetHeader given
						datasetHeader = datasetHeader.modifyDataStructureReference(datasetStructureReference);
					} else {
						//CrossSectionalDataReader case is implemented separately with cache, this is for Structure Specific and Generic 2.1
						//dimensionAtObservation = datasetHeader.getDataStructureReference().getDimensionAtObservation();
						dimensionAtObservation = computeDimensionAtObservation(datasetHeader.getDataStructureReference().getDimensionAtObservation(), dataReaderEngine.getDataStructure());
					}

					//Reading and writing starts here
					//A. StartDataset
					dataWriterEngine.startDataset(dataReaderEngine.getDataFlow(), dataReaderEngine.getDataStructure(), datasetHeader);
					for (KeyValue kv : dataReaderEngine.getDatasetAttributes()) {
						if (kv.getCode() != null && !kv.getCode().isEmpty()) {
							//B. Write DataSet Attributes
							dataWriterEngine.writeAttributeValue(kv.getConcept(), kv.getCode());
						}
					}
					while (dataReaderEngine.moveNextKeyable()) {
						//C. Write Keyable this is the utter chaos
						Keyable currentKey = dataReaderEngine.getCurrentKey();

						if (!hasTimeDimension && isCrossSectionalDsd/* && currentKey.isSeries() && dataWriterEngine instanceof CrossSectionDataWriterEngine*/) {
								writeCrossSectionalKeyables(dataReaderEngine, dataWriterEngine, currentKey, dimensionAtObservation);
						} else {
							//TODO this needs to be re-written , it is not correct for the cross sectional case
							if ((currentKey.isTimeSeries() && currentKey.getGroupName() == null)) {
								//a time series
								writeTimeseriesKeyables(dataReaderEngine, dataWriterEngine, currentKey, dimensionAtObservation, datasetHeader);
							}
							if (!currentKey.isTimeSeries() && currentKey.getGroupName() == null) {
								//this is for an output structure specific 2.1 or generic 2.1 with no time dimension
								write21NonTimeseriesKeyables(dataReaderEngine, dataWriterEngine, currentKey, dimensionAtObservation, datasetHeader);
							}
							//D. Write group
							if (!currentKey.isSeries() || currentKey.getGroupName() != null) {
								dataWriterEngine.startGroup(currentKey.getGroupName());
								for (KeyValue keyValue : currentKey.getKey()) {
									dataWriterEngine.writeGroupKeyValue(keyValue.getConcept(), keyValue.getCode());
								}
								for (KeyValue keyValue : currentKey.getAttributes()) {
									dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
								}
							}
						}
					}//end move next keyable
				}//end move next dataset
			}
		} catch (Exception ex) {
			logger.debug("Error when transferring data from reader to writer", ex);
			throw ex;
		} finally {
			dataReaderEngine.close();
			dataWriterEngine.close();
		}
	}

	/**
	 * Compute the dimension at Observation
	 * this method was made for computing dimensionAtObservation == null
	 * or dimensionAtObservation == AllDimensions
	 * For AllDimensions case we use TimeDimension or the last Dimension Found
	 * @see 'SDMXCONV-841'
	 * @param dimensionAtObservation
	 * @param dataStructureBean
	 * @return dimensionAtObservation
	 */
	private static String computeDimensionAtObservation(String dimensionAtObservation, DataStructureBean dataStructureBean) {
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

	/**
	 * In Case of writing with buffered writer engine
	 * and not cross sectional, write Series access in the right order
	 * before nextObservation and not inside it.
	 * @see #writeCrossSectionalKeyables(DataReaderEngine, DataWriterEngine, Keyable, String)
	 * @param dataReaderEngine
	 * @param dataWriterEngine
	 * @param currentKey
	 * @param dimensionAtObs
	 */
	private static void writeKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs) {
		dataWriterEngine.startSeries();
		for (KeyValue keyValue : currentKey.getKey()) {
			dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
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
			dataWriterEngine.writeObservation(dimensionAtObs, observation.getCrossSectionalValue().getCode(), observation.getObservationValue(), annotations);
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	private static void writeTimeseriesKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs, DatasetHeaderBean datasetHeader) {
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
			// Is the datasetHeader here output -or- input ? Can we check if dimensionAtObs is TIME_PERIOD ?
			if (datasetHeader.isTimeSeries() && !(dataWriterEngine instanceof CrossSectionDataWriterEngine)) {
				dataWriterEngine.writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID, observation.getObsTime(), observation.getObservationValue(), annotations);
			} else { // we either have non-Cross Sectional writer or non-time series (is this possible here ?)
				String crossSectionalValue = currentKey.getKeyValue(dimensionAtObs);
/*				if ("TIME_PERIOD".equalsIgnoreCase(dimensionAtObs)) {
					dataWriterEngine.writeObservation(dimensionAtObs, observation.getObsTime(), observation.getObservationValue(), annotations);
				} else {*/
					dataWriterEngine.writeObservation(dimensionAtObs, crossSectionalValue, observation.getObservationValue(), annotations);
/*				}*/
				// we need to also write the TIME_PERIOD - HACK for CrossSectionDataWriterEngine write after the writeObservation FIXME
				if (!observation.isCrossSection() && ObjectUtil.validString(observation.getObsTime())) {
					dataWriterEngine.writeSeriesKeyValue(DimensionBean.TIME_DIMENSION_FIXED_ID, observation.getObsTime());
				}
			}
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	private static void writeCrossSectionalKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs, DatasetHeaderBean datasetHeader) {
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
			dataWriterEngine.writeSeriesKeyValue(dimensionAtObs, observation.getCrossSectionalValue().getCode());
			for (KeyValue keyValue : currentKey.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
			if (datasetHeader.isTimeSeries()) {
				dataWriterEngine.writeObservation(dimensionAtObs,
						observation.getObsTime(),
						observation.getObservationValue(),
						annotations);
			} else {
				// FIXME what if there is no time dimension
				dataWriterEngine.writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID,
						observation.getObsTime(),
						observation.getObservationValue(),
						annotations);
			}
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}

	private static void writeCrossSectionalKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs) {
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

	public static void write21NonTimeseriesKeyables(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, Keyable currentKey, String dimensionAtObs, DatasetHeaderBean datasetHeader) {
		dataWriterEngine.startSeries();
		/* SDMXCONV-945
		 * When we have a flat file where dimension At observation is set to Alldimensions,
		 * we want a dimension to write the observations with we choose the last from the series,
		 * if there is no measure and no time. To achieve this we need to not write that dim
		 * while writing the series and write it during writeObservation.
		 */
		boolean isFlat = false;
		String dimensionAtObservation = computeDimensionAtObservation(currentKey.getCrossSectionConcept(), dataReaderEngine.getDataStructure());
		if (currentKey.getCrossSectionConcept() != null && !currentKey.getCrossSectionConcept().isEmpty()) {
			if (currentKey.getCrossSectionConcept().equals(DIMENSION_AT_OBSERVATION.ALL.getVal())) {
				isFlat = true;
			}
		}
		//SDMXCONV-892
		for (KeyValue keyValue : currentKey.getKey()) {
			if (isFlat) {
				if (!keyValue.getConcept().equalsIgnoreCase(dimensionAtObservation)) {
					dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
				}
			} else {
				if(ObjectUtil.validString(keyValue.getCode()))
					dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
				else
					throw new SdmxSemmanticException("Empty value for component " + keyValue.getConcept());
			}
		}
		// If it is not TimeSeries and there is a time dimension we need to write the time dimension as a series key value
		if (!currentKey.isTimeSeries() && currentKey.getDataStructure().getTimeDimension() != null) {
			dataWriterEngine.writeSeriesKeyValue(DimensionBean.TIME_DIMENSION_FIXED_ID, currentKey.getObsTime());
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
			if (datasetHeader.isTimeSeries()) {
				dataWriterEngine.writeObservation(dimensionAtObs,
						observation.getObsTime(),
						observation.getObservationValue(),
						annotations);

			} else {
				if (isFlat) {
					dataWriterEngine.writeObservation(dimensionAtObservation, observation.getSeriesKey().getKeyValue(dimensionAtObservation), observation.getObservationValue(), annotations);
				} else {
					dataWriterEngine.writeObservation(observation.getCrossSectionalValue().getCode(), observation.getObservationValue(), annotations);
				}
			}
			for (KeyValue keyValue : observation.getAttributes()) {
				dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
			}
		}
	}
}