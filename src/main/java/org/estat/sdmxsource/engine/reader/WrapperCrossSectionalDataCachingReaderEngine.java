/**
 *
 * Copyright 2015 EUROSTAT
 *
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * 	https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package org.estat.sdmxsource.engine.reader;

import com.intrasoft.sdmx.converter.cache.ConverterCacheManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.ATTRIBUTE_ATTACHMENT_LEVEL;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;

import javax.cache.Cache;
import java.util.*;
import java.util.Map.Entry;

/**
 * Class the facilitates reading from a CrossSectionalDataReader engine and writes to the provided writer in two steps.
 * Takes all the data from the Reader (prepareCrossSectionalDataInCache(DataReaderEngine dataReaderEngine)), saves it in the cache in the non-CrossSectional form
 * and writes it using the Writer (writeCrossSectionalData(DataWriterEngine dataWriterEngine)). 
 * 
 * The dataReaderEngine for CrossSectional returns Time Series and Observations for each series. 
 * But is is not ordered because of the CrossSectional Measures. 
 * 
 * Data comes like below. It is grouped by time.  
 * 		<ns1:Series FREQ="A" DEMO="EMIGT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1995" OBS_VALUE="26666" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
		</ns1:Series>
		<ns1:Series FREQ="A" DEMO="IMMIT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1995" OBS_VALUE="25555" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
		</ns1:Series>
		<ns1:Series FREQ="A" DEMO="ADJT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1995" OBS_VALUE="1111" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
		</ns1:Series>
		<ns1:Series FREQ="A" DEMO="EMIGT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1994" OBS_VALUE="657482" TIME_FORMAT="P1Y"
				UNIT="PERS" UNIT_MULT="0" DECI="3"></ns1:Obs>
		</ns1:Series>
		<ns1:Series FREQ="A" DEMO="IMMIT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1994" OBS_VALUE="912038" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
		</ns1:Series>
		<ns1:Series FREQ="A" DEMO="ADJT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1994" OBS_VALUE="748498" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
		</ns1:Series>

 *  And should be like this, group by the same keyable (containing the Measure Dimension):
 *		<ns1:Series FREQ="A" DEMO="EMIGT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1995" OBS_VALUE="26666" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
			<ns1:Obs TIME="1994" OBS_VALUE="657482" TIME_FORMAT="P1Y"
				UNIT="PERS" UNIT_MULT="0" DECI="3"></ns1:Obs>
		</ns1:Series>
		<ns1:Series FREQ="A" DEMO="IMMIT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1995" OBS_VALUE="25555" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
			<ns1:Obs TIME="1994" OBS_VALUE="912038" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
		</ns1:Series>
		<ns1:Series FREQ="A" DEMO="ADJT" COUNTRY="FR" SEX="F">
			<ns1:Obs TIME="1995" OBS_VALUE="1111" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
			<ns1:Obs TIME="1994" OBS_VALUE="748498" TIME_FORMAT="P1Y"
				UNIT_MULT="0" UNIT="PERS" DECI="3"></ns1:Obs>
		</ns1:Series>  
 * @author Mihaela Munteanu
 * @since  7th of June 2017
 *
 */
public class WrapperCrossSectionalDataCachingReaderEngine{

	private static Logger logger = LogManager.getLogger(WrapperCrossSectionalDataCachingReaderEngine.class);
	
	HeaderBean header;
	List<DatasetHeaderBean> datasetHeaderBeans = new ArrayList<>();
	DataStructureBean dataStructureBean; 
	DataflowBean dataflowBean;
	
	Cache<LinkedHashMap<String, String>, LinkedHashMap<String,String>> xsCache;

	LinkedHashMap<String,String> startDataSetAtributesElementKey;
	
	HashMap<String,String> startDataSetAtributesForGroupKey;
	
	ConverterCacheManager converterCacheManager = ConverterCacheManager.INSTANCE;
	
	private String cacheName;
	
	private static final String NEXT = "Next";
	private static final String OBS_VALUE = "OBS_VALUE";
	private static final String GROUP = "Group";
	private static final String GROUP_NAME = "GroupName";
	private static final String DATASET = "Dataset";
	
	public WrapperCrossSectionalDataCachingReaderEngine() {
		cacheName = ConverterCacheManager.CROSS_SECTIONAL_CACHE + (new Date()).toString();
		converterCacheManager.createConverterCache(cacheName);
		xsCache = converterCacheManager.getConverterCache(cacheName);
	}

	boolean checkAttachmentLevel(String attachmentLevel, String conceptId) {
		boolean belongsToLevel = false;
		AttributeBean attributeBean = dataStructureBean.getAttribute(conceptId);
		switch(attachmentLevel) {
			case "Dataset":
				if(attributeBean!=null && ATTRIBUTE_ATTACHMENT_LEVEL.DATA_SET == attributeBean.getAttachmentLevel()) {
					belongsToLevel = true;
				}
				break;
			case "Group" :
				break;
			case "Series" :
				if(attributeBean!=null && (ATTRIBUTE_ATTACHMENT_LEVEL.DIMENSION_GROUP == attributeBean.getAttachmentLevel() || ATTRIBUTE_ATTACHMENT_LEVEL.GROUP == attributeBean.getAttachmentLevel())) {
					belongsToLevel = true;
				}
				break;
			case "Observation" :
				if(attributeBean!=null && (ATTRIBUTE_ATTACHMENT_LEVEL.OBSERVATION == attributeBean.getAttachmentLevel())) {
					belongsToLevel = true;
				}
				break;
			default:
				break;
		}
		return belongsToLevel;
	}

	public void prepareCrossSectionalDataInCacheFromReader(DataReaderEngine dataReaderEngine) {
		dataReaderEngine.reset();
		header = dataReaderEngine.getHeader();
		
		while(dataReaderEngine.moveNextDataset()) {
			boolean firstKeyable = true;
			boolean firstGroup = true;
			LinkedHashMap<String, String> previousSeriesKeyable = null;
			LinkedHashMap<String, String> previousGroupKeyable =null;
			
			DatasetHeaderBean datasetHeader = dataReaderEngine.getCurrentDatasetHeaderBean();
			datasetHeaderBeans.add(datasetHeader);
			String dimensionAtObs = datasetHeader.getDataStructureReference().getDimensionAtObservation();
			dataStructureBean = dataReaderEngine.getDataStructure();
			dataflowBean = dataReaderEngine.getDataFlow();

			LinkedHashMap<String, String> datasetAttributes = new LinkedHashMap<String, String>();
			for(KeyValue kv : dataReaderEngine.getDatasetAttributes()) {
				datasetAttributes.put(kv.getConcept(), kv.getCode());
				
			}
			startDataSetAtributesElementKey = datasetAttributes;
			startDataSetAtributesElementKey.put(DATASET, "true");
			LinkedHashMap<String, String> datasetGroupKey = new LinkedHashMap<>();
			datasetGroupKey.putAll(datasetAttributes);
			datasetGroupKey.put(GROUP, "true");
			startDataSetAtributesForGroupKey = datasetGroupKey;				

			while (dataReaderEngine.moveNextKeyable()) {
				Keyable currentKey = dataReaderEngine.getCurrentKey();

				if (!currentKey.isTimeSeries() && !DimensionBean.TIME_DIMENSION_FIXED_ID.equals(dimensionAtObs)) {
					while (dataReaderEngine.moveNextObservation()) {
						//TODO build different keyable (with measure and value) and write to cache
						Observation observation = dataReaderEngine.getCurrentObservation();

						LinkedHashMap<String, String> seriesKeyable = new LinkedHashMap<>();
						for (KeyValue keyValue : currentKey.getKey()) {
							seriesKeyable.put(keyValue.getConcept(), keyValue.getCode());
						} 
						seriesKeyable.put(dimensionAtObs, observation.getCrossSectionalValue().getCode());
						for (KeyValue keyValue : currentKey.getAttributes()) {
							seriesKeyable.put(keyValue.getConcept(), keyValue.getCode());
						}
						for (KeyValue keyValue : observation.getAttributes()) {
							if(checkAttachmentLevel("Series", keyValue.getConcept())) {
								seriesKeyable.put(keyValue.getConcept(), keyValue.getCode());
							}
						}
						
						if (firstKeyable) {
							converterCacheManager.putKeyAndValue(xsCache, datasetAttributes, seriesKeyable);
							logger.debug(datasetAttributes + " => " + seriesKeyable);
							firstKeyable = false;
						} else {
							LinkedHashMap<String, String> resultFromCache = converterCacheManager.getValueFromKey(xsCache, seriesKeyable);
							if (resultFromCache == null) {
								converterCacheManager.putKeyAndValue(xsCache, previousSeriesKeyable, seriesKeyable);
								logger.debug(previousSeriesKeyable + " => " + seriesKeyable);
								previousSeriesKeyable = new LinkedHashMap<>();//reset

							} else {
								//put it back as it might get erased from cache
								converterCacheManager.putKeyAndValue(xsCache, seriesKeyable, resultFromCache);
								logger.debug("Put back not to be erased: " + seriesKeyable + " => " + resultFromCache);
							}
						}
						
						previousSeriesKeyable = new LinkedHashMap<>();
						previousSeriesKeyable.putAll(seriesKeyable);
						previousSeriesKeyable.put(NEXT, "true");

						LinkedHashMap<String, String> observationCache = new LinkedHashMap<>();
						observationCache.put(DimensionBean.TIME_DIMENSION_FIXED_ID, observation.getObsTime());
						observationCache.put(OBS_VALUE, observation.getObservationValue());
						
						for (KeyValue keyValue : observation.getAttributes()) {
							if(checkAttachmentLevel("Observation", keyValue.getConcept())) {
								observationCache.put(keyValue.getConcept(), keyValue.getCode());
							}
						}

						LinkedHashMap<String, String> resultObsFromCache = converterCacheManager.getValueFromKey(xsCache, seriesKeyable);
						if (resultObsFromCache == null) {
							logger.debug(seriesKeyable + " => " + observationCache);
							converterCacheManager.putKeyAndValue(xsCache, seriesKeyable, observationCache);
						} else {
							converterCacheManager.putKeyAndValue(xsCache, seriesKeyable, observationCache);
							logger.debug(seriesKeyable + " => " + observationCache);
							converterCacheManager.putKeyAndValue(xsCache, observationCache, resultObsFromCache);
							logger.debug("Put back not to be erased: " + observationCache + " => " + resultObsFromCache);
						}
						
					}

				}
				if (!currentKey.isSeries() || currentKey.getGroupName() != null) {

					LinkedHashMap<String, String> groupKey = new LinkedHashMap<>();
					groupKey.put(GROUP_NAME, currentKey.getGroupName());
					for (KeyValue keyValue : currentKey.getKey()) {
						groupKey.put(keyValue.getConcept(), keyValue.getCode());
					}
					for (KeyValue keyValue : currentKey.getAttributes()) {
						groupKey.put(keyValue.getConcept(), keyValue.getCode());
					}
					
					if (firstGroup) {
						converterCacheManager.putKeyAndValue(xsCache, datasetGroupKey, groupKey);
						firstGroup = false;
					} else {
						LinkedHashMap<String, String> resultFromCache = converterCacheManager.getValueFromKey(xsCache, groupKey);
						if (resultFromCache == null) {
							converterCacheManager.putKeyAndValue(xsCache, previousGroupKeyable, groupKey);
							previousGroupKeyable = new LinkedHashMap<>();
						} else {
							//put it back, it might be erased from cache
							converterCacheManager.putKeyAndValue(xsCache, groupKey, resultFromCache);
						}
					}
				}
			}
		}
			
	}
	
	public void writeCrossSectionalData(DataWriterEngine dataWriterEngine) {
		if(header != null) {
			dataWriterEngine.writeHeader(header);
		}
		try {
			int dataSetPosition = 0;
			LinkedHashMap<String, String> currentDataSet = startDataSetAtributesElementKey;
			while (currentDataSet != null) {
				DatasetHeaderBean datasetHeader = datasetHeaderBeans.get(dataSetPosition++);
				//TODO check if TIME dimension exists for Compact, Generic and Utility 2.0, StructureSpecific_TS and Generic_TS
				DatasetStructureReferenceBean datasetStructureReference = new DatasetStructureReferenceBeanImpl(datasetHeader.getDataStructureReference().getId(), 
										datasetHeader.getDataStructureReference().getStructureReference(), null, null, DimensionBean.TIME_DIMENSION_FIXED_ID);
				DatasetHeaderBean writerDatasetHeader = datasetHeader.modifyDataStructureReference(datasetStructureReference);
				dataWriterEngine.startDataset(dataflowBean, dataStructureBean, writerDatasetHeader);
				
				for (Entry<String, String> entry:currentDataSet.entrySet()) {
					if (!DATASET.equals(entry.getKey())) {
						dataWriterEngine.writeAttributeValue(entry.getKey(), entry.getValue());
					}
				}

				LinkedHashMap<String, String> currentDataSetForGroup = new LinkedHashMap<>();
				currentDataSetForGroup.putAll(currentDataSet);
				currentDataSetForGroup.put(GROUP, "true");
				LinkedHashMap<String, String> currentGroup = converterCacheManager.getValueFromKey(xsCache, currentDataSetForGroup);
				while (currentGroup !=  null) {
					dataWriterEngine.startGroup(currentGroup.get(GROUP_NAME));
					for (Entry<String, String> entry:currentGroup.entrySet()) {
						dataWriterEngine.writeGroupKeyValue(entry.getKey(), entry.getValue());
					}
				}

				LinkedHashMap<String, String> currentSeries = converterCacheManager.getValueFromKey(xsCache, currentDataSet);
				while (currentSeries != null) {
					dataWriterEngine.startSeries();
					for (Entry<String, String> entry:currentSeries.entrySet()) {
						dataWriterEngine.writeSeriesKeyValue(entry.getKey(), entry.getValue());
					}
					LinkedHashMap<String, String> currentObservation = converterCacheManager.getValueFromKey(xsCache, currentSeries);
					while (currentObservation != null) {
						dataWriterEngine.writeObservation(currentObservation.get(DimensionBean.TIME_DIMENSION_FIXED_ID), 
														  currentObservation.get(OBS_VALUE));
						for (Entry<String, String> entry:currentObservation.entrySet()) {
							if (!DimensionBean.TIME_DIMENSION_FIXED_ID.equals(entry.getKey()) && 
									!OBS_VALUE.equals(entry.getKey())) {
								dataWriterEngine.writeAttributeValue(entry.getKey(), entry.getValue());
							}
						}
						
						currentObservation = converterCacheManager.getValueFromKey(xsCache, currentObservation);
					}
					currentSeries.put(NEXT, "true");
					currentSeries = converterCacheManager.getValueFromKey(xsCache, currentSeries);
				}
				currentDataSet.put(NEXT, "true");
				currentDataSet = converterCacheManager.getValueFromKey(xsCache, currentDataSet);
			}
		} finally {
			dataWriterEngine.close();
			converterCacheManager.closeCache(xsCache);
		}
	}

	public HeaderBean getHeader() {
		return header;
	}
}
