package com.intrasoft.sdmx.converter.tranformation;

import org.estat.sdmxsource.engine.decorators.ObservationCounterDecorator;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.PrimaryMeasureBean;
import org.sdmxsource.sdmx.api.model.data.ComplexNodeValue;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.util.ObjectUtil;

public class ThreePointZeroWriterStrategy implements IReadWriteStrategy {

	@Override
	public void writeDataset(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine) {
		DataflowBean dataflowBean = dataReaderEngine.getDataFlow();
		DataStructureBean dataStructureBean = dataReaderEngine.getDataStructure();

		//Reading and writing starts here
		DatasetHeaderBean datasetHeader = dataReaderEngine.getCurrentDatasetHeaderBean();
		//SDMXCONV-1486 set dataflow/datastructure because header is not read yet.
		if(ObjectUtil.validObject(datasetHeader)
				&& ObjectUtil.validObject(datasetHeader.getDataStructureReference())
					&& ObjectUtil.validObject(datasetHeader.getDataStructureReference().getStructureReference())) {
			if(datasetHeader.getDataStructureReference().getStructureReference().getMaintainableStructureType() == SDMX_STRUCTURE_TYPE.DATAFLOW) {
				if(!ObjectUtil.validObject(dataflowBean) && dataReaderEngine instanceof ObservationCounterDecorator) {
					ObservationCounterDecorator readerDecorator = (ObservationCounterDecorator) dataReaderEngine;
					dataflowBean = readerDecorator.getDataReaderEngine().getDataFlow();
				}
			}
			if(datasetHeader.getDataStructureReference().getStructureReference().getMaintainableStructureType() == SDMX_STRUCTURE_TYPE.DSD) {
				if(!ObjectUtil.validObject(dataflowBean) && dataReaderEngine instanceof ObservationCounterDecorator) {
					ObservationCounterDecorator readerDecorator = (ObservationCounterDecorator) dataReaderEngine;
					dataStructureBean = readerDecorator.getDataReaderEngine().getDataStructure();
				}
			}
		}
		dataWriterEngine.startDataset(dataflowBean, dataStructureBean, datasetHeader);
		for (KeyValue kv : dataReaderEngine.getDatasetAttributes()) {
			if(ObjectUtil.validObject(kv) && ObjectUtil.validCollection(kv.getComplexNodeValues())){
				dataWriterEngine.writeComplexAttributeValue(kv);
			} else if (kv.getCode() != null && !kv.getCode().isEmpty()) {
				//B. Write DataSet Attributes
				dataWriterEngine.writeAttributeValue(kv.getConcept(), kv.getCode());
			}
		}
	}


	@Override
	public void readWrite(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, SDMX_SCHEMA sdmxSchema, String dimensionAtObservation) {

		if (!dataReaderEngine.getDataStructure().isCompatible(SDMX_SCHEMA.VERSION_THREE))
			throw new SdmxSemmanticException("The DSD is not SDMX v3.0 compatible.");

		Keyable currentKey = dataReaderEngine.getCurrentKey();
		if (!currentKey.isSeries() && currentKey.getGroupName() != null) {
			boolean startGroup = false;
			for (KeyValue keyValue : currentKey.getAttributes()) {
				if (!ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
					if (ObjectUtil.validString(keyValue.getCode())) {
						startGroup = true;
						break;
					}
				} else {
					for (ComplexNodeValue complexNodeValue : keyValue.getComplexNodeValues()) {
						if (ObjectUtil.validString(complexNodeValue.getCode())) {
							startGroup = true;
							break;
						} else {
							if (ObjectUtil.validCollection(complexNodeValue.getTexts())) {
								for (TextTypeWrapper textTypeWrapper : complexNodeValue.getTexts()) {
									if (ObjectUtil.validString(textTypeWrapper.getValue())) {
										startGroup = true;
										break;
									}
								}
							}
						}
					}
				}
			}

			if (startGroup) {
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
					if (!ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
						dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
					}
				}

				for (KeyValue keyValue : currentKey.getAttributes()) {
					if (ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
						dataWriterEngine.writeComplexAttributeValue(keyValue);
					}
				}
			}
		} else if (currentKey.isSeries() && currentKey.getGroupName() == null) {
			dataWriterEngine.startSeries();
			for (KeyValue keyValue : currentKey.getKey()) {
				if (!keyValue.getConcept().equalsIgnoreCase(dimensionAtObservation)) {
					dataWriterEngine.writeSeriesKeyValue(keyValue.getConcept(), keyValue.getCode());
				}
			}
			for (KeyValue keyValue : currentKey.getAttributes()) {
				if (!ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
					dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
				}
			}
			for (KeyValue keyValue : currentKey.getAttributes()) {
				if (ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
					dataWriterEngine.writeComplexAttributeValue(keyValue);
				}
			}
			//Firstly write only the simple measures and attributes and then the complex ones.
			while (dataReaderEngine.moveNextObservation()) {
				Observation observation = dataReaderEngine.getCurrentObservation();
				AnnotationBean[] annotations = null;
				if (observation.getAnnotations() != null && !observation.getAnnotations().isEmpty()) {
					annotations = observation.getAnnotations().toArray(new AnnotationBean[]{});
				}
				//Write the observation
				if (dimensionAtObservation.equalsIgnoreCase(DimensionBean.TIME_DIMENSION_FIXED_ID)) {
					//If we have primary concept time then we write observation with obs time
					dataWriterEngine.writeObservation(observation.getObsTime(), annotations);
				} else {
					//If the primary concept is something else then try to write observation with that
					String primaryConceptValue = currentKey.getKeyValue(dimensionAtObservation);
					if(!ObjectUtil.validString(primaryConceptValue)) {
						if(ObjectUtil.validObject(observation.getCrossSectionalValue()))
							primaryConceptValue = observation.getCrossSectionalValue().getCode();
					}
					dataWriterEngine.writeObservation(primaryConceptValue, annotations);

				}
				if (ObjectUtil.validCollection(observation.getMeasures())) {
					//Write non-complex observation measures first
					for (KeyValue measure : observation.getMeasures()) {
						if (!ObjectUtil.validCollection(measure.getComplexNodeValues())) {
							if(ObjectUtil.validObject(measure) && ObjectUtil.validString(measure.getCode())) {
								dataWriterEngine.writeMeasureValue(measure.getConcept(), measure.getCode());
							}
						}
					}
				} else if (ObjectUtil.validCollection(dataReaderEngine.getDataStructure().getMeasures())
						&& ObjectUtil.validObject(dataReaderEngine.getDataStructure().getMeasure(PrimaryMeasureBean.FIXED_ID))) {
					/* 	In case we are writing SDMX 3.0 but the file we read
						is in previous version the OBS_VALUE
						is not read as measure but as a primary measure. See also SDMXCONV-1523 */
					if (ObjectUtil.validString(observation.getObservationValue()))
						dataWriterEngine.writeMeasureValue(PrimaryMeasureBean.FIXED_ID, observation.getObservationValue());
				}
				//Write non-complex observation attributes first
				for (KeyValue keyValue : observation.getAttributes()) {
					if (!ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
						dataWriterEngine.writeAttributeValue(keyValue.getConcept(), keyValue.getCode());
					}
				}
				if (ObjectUtil.validCollection(observation.getMeasures())) {
					//Write complex observation measures then
					for (KeyValue measure : observation.getMeasures()) {
						if (ObjectUtil.validCollection(measure.getComplexNodeValues())) {
							dataWriterEngine.writeComplexMeasureValue(measure);
						}
					}
				}
				//Write complex observation attributes then
				for (KeyValue keyValue : observation.getAttributes()) {
					if (ObjectUtil.validCollection(keyValue.getComplexNodeValues())) {
						dataWriterEngine.writeComplexAttributeValue(keyValue);
					}
				}
			}
		}
	}
}
