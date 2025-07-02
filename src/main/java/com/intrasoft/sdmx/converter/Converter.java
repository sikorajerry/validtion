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

import com.intrasoft.sdmx.converter.config.FlrOutputConfig;
import com.intrasoft.sdmx.converter.io.data.ComponentBufferWriterEngine;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.csv.FlrDataWriterEngine;
import com.intrasoft.sdmx.converter.services.ConverterDataReaderEngineProvider;
import com.intrasoft.sdmx.converter.services.ConverterDataWriterEngineProvider;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.tranformation.DataReaderToWriter;
import com.intrasoft.sdmx.converter.util.FormatFamily;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.estat.sdmxsource.engine.decorators.ObservationCounterDecorator;
import org.estat.sdmxsource.engine.reader.DataCountingLengthReaderEngine;
import org.estat.sdmxsource.engine.reader.WrapperCrossSectionalDataCachingReaderEngine;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.sdmxsource.util.csv.FlrInColumnMapping;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalMeasureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.RecordReaderCounter;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.dataparser.engine.writer.SdmxDataWriterEngine;
import org.sdmxsource.sdmx.dataparser.engine.writer.WritingDataEngineDecorator;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.util.ObjectUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Main Class of the SDMX Converter Application. This Class accepts the input format, parses it according to the given
 * definitions, invokes the appropriate conversion writer classes and returns the resulting dataset.
 */
public class Converter {
	private static Logger logger = LogManager.getLogger(Converter.class);

	public static LinkedHashMap<String, FlrInColumnMapping> getFinalFlrMappings() {
		return finalFlrMappings;
	}

	private static long conversionDuration; // Static variable to store the conversion duration

	private static LinkedHashMap<String, FlrInColumnMapping> finalFlrMappings;

	public static ConverterMetrics getMetrics() { return metrics; }

	private static ConverterMetrics metrics;

	public static long getConversionDuration() { // Getter for conversion duration
		return conversionDuration;
	}
    /**
     * Performs a conversion from one format to another
     *
     * @param converterInput ConverterInput  the parameters for input:  source format, input data location, input configuration
     * @param converterOutput ConverterOutput the parameters for output: destination format, output stream, output configuration
     * @param converterStructure ConverterStructure: dataStructura and dataflow
     * @throws Exception
     */
    public static void convertWithCurrentImpl( ConverterInput converterInput,
                                ConverterOutput converterOutput,
                                ConverterStructure converterStructure) throws ConverterException {
        logger.info("Starting conversion for input {} and output {} using structure {}", converterInput, converterOutput, converterStructure);

		DataReaderEngine dataReaderEngine = null;
		DataWriterEngine dataWriterEngine = null;
		ExceptionHandler exceptionHandler = new FirstFailureExceptionHandler();
        try {
        	DataStructureBean dataStructure = null;
        	if(converterStructure.getDataStructure()!=null) dataStructure = converterStructure.getDataStructure();
        	
			long before = System.currentTimeMillis();
            // check if there exist unmapped dimensions
            if (converterInput.getInputConfig() instanceof CsvInputConfig) {
                String currentDimension;
                Map<String, CsvInColumnMapping> columnMapping = ((CsvInputConfig) converterInput.getInputConfig()).getMapping();
                if (columnMapping != null) {
                	if(dataStructure!=null) {
	                    for (int i = 0; i < dataStructure.getDimensionList().getDimensions().size(); i++) {
	                        if (dataStructure.getDimensionList().getDimensions().get(i).isTimeDimension()) {
	                            continue;
	                        }
	                        currentDimension = dataStructure.getDimensionList().getDimensions().get(i).getId();
	                        CsvInColumnMapping mapping = columnMapping.get(currentDimension);
	                        if(mapping != null){
	                            if (mapping.getFixedValue() == null && (mapping.getColumns() == null || mapping.getColumns().size() == 0)) {
	                                //check if this dimensionName is the name of the measureDimension
	                                String concept = StructureService.scanForMeasureDimension(dataStructure);
	                                boolean isMeasureDimension = false;
	                                boolean hasCrossXMeasures = false;
	                                if (concept.equals(currentDimension))
	                                    isMeasureDimension = true;
	                                if (dataStructure instanceof CrossSectionalDataStructureBean) {
	                                    List<CrossSectionalMeasureBean> crossXMeasures = ((CrossSectionalDataStructureBean) dataStructure).getCrossSectionalMeasures();
	                                    for (CrossSectionalMeasureBean crossXMeasure : crossXMeasures) {
	                                        String crossConcept = crossXMeasure.getId().toString();
	                                        if (columnMapping.containsKey(crossConcept)) {
	                                            hasCrossXMeasures = true;
	                                            break;
	                                        }
	                                    }
	                                }
	
	                                // if the concept for which the position=null is the measure dimension
	                                // and at least one crossX measure appears in the mapping then do not throw exception.
	                                // because the user has selected to map only the crossX measure and not the measure dimension.
	                                if (!hasCrossXMeasures || !isMeasureDimension)
	                                    throw new InvalidConversionParameters("Dimension: " + currentDimension + " has not been mapped.");
	                            }
	                        }else{
	                            logger.warn(" no mapping found for {}", currentDimension);
	                        }
	                    }
                	}
                }
            }
			LinkedHashMap< ValidationEngineType, ObjectArrayList< DataValidationError >> errorsByEngine = new LinkedHashMap<>();
			//SDMXCONV-1441 After removing ThreadLocal for positions we need to pass a map for positions from readers to Validation Engines.
			Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
            int order = 0;
			DataReaderEngine originReaderEngine = ConverterDataReaderEngineProvider.getDataReaderEngine(converterInput, converterStructure, errorsByEngine, order, 1, exceptionHandler, errorPositions);
            dataReaderEngine = new ObservationCounterDecorator(originReaderEngine,
																converterInput.getInputConfig(),
																converterInput.getInputFormat(),
																converterStructure.getDataflow());
            dataWriterEngine = ConverterDataWriterEngineProvider.getDataWriterEngine(converterOutput, converterStructure.getRetrievalManager());

            //For Csv output
            if(dataWriterEngine instanceof ComponentBufferWriterEngine) {
            	if(converterOutput.getOutputConfig() instanceof MultiLevelCsvOutputConfig) {
    				((ComponentBufferWriterEngine) dataWriterEngine).setMapMeasures(((MultiLevelCsvOutputConfig) converterOutput.getOutputConfig()).isMapMeasure());
    				((ComponentBufferWriterEngine) dataWriterEngine).setMapCrossXMeasures(((MultiLevelCsvOutputConfig) converterOutput.getOutputConfig()).isMapCrossXMeasure());
    			}
            }
            //If the output Format is Flr and the mapping is null we need to count the max length of each value
			//to determine the columns fixed length values.
			//see also https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-775
            if(Formats.FLR.equals(converterOutput.getOutputFormat())
					&& converterOutput.getOutputConfig()!=null) {
				FlrOutputConfig config = (FlrOutputConfig) converterOutput.getOutputConfig();
				LinkedHashMap<String, FlrInColumnMapping> flrMapping = config.getMapping();
            	if(flrMapping==null || config.checkIfAutoExists(flrMapping)) {
					DataCountingLengthReaderEngine countLengthsEngine = new DataCountingLengthReaderEngine(ConverterDataReaderEngineProvider.getDataReaderEngine(converterInput, converterStructure, errorsByEngine, order, 1, exceptionHandler, errorPositions));
					Map<String, Integer> flrLengths = countLengthsEngine.countLengths();
					config.setLengthsCounting(flrLengths);
				}
			}
            
            if (Formats.CROSS_SDMX.equals(converterInput.getInputFormat()) && !FormatFamily.CSV.equals(converterOutput.getOutputFormat().getFamily())
					&& dataStructure!=null && dataStructure.getTimeDimension()!=null) {
					WrapperCrossSectionalDataCachingReaderEngine cachingEngine = new WrapperCrossSectionalDataCachingReaderEngine();
					cachingEngine.prepareCrossSectionalDataInCacheFromReader(dataReaderEngine);
					cachingEngine.writeCrossSectionalData(dataWriterEngine);
	            } else {
				//SDMXCONV-1087, output file's format added in parameter list of copyToWriter method's signature
				DataReaderToWriter.copyToWriter(dataReaderEngine, dataWriterEngine, converterStructure.getDataStructure(), converterInput.getInputConfig(), converterOutput.getOutputFormat());
			 }

			if(dataWriterEngine instanceof WritingDataEngineDecorator) {
				SdmxDataWriterEngine flrWriterEngine = ((WritingDataEngineDecorator) dataWriterEngine).getWriterEngine();
				if((flrWriterEngine instanceof FlrDataWriterEngine)) {
					FlrOutputConfig flrConfig = ((FlrDataWriterEngine) flrWriterEngine).getConfigurations();
					finalFlrMappings = flrConfig.getFinalMapping();
				}
			}
			int ignoredObsCount = 0;
			int obsCount = 0;
			if(dataReaderEngine instanceof ObservationCounterDecorator) {
				obsCount = ((ObservationCounterDecorator)dataReaderEngine).getObservationCounter();
				ignoredObsCount = ((ObservationCounterDecorator)dataReaderEngine).getIgnoredObservationCounter();
			}
            long after = System.currentTimeMillis();
			conversionDuration = after - before; // Set the conversion duration
			// then
			ErrorPosition maxRow = errorPositions.values()
					.stream()
					.max(Comparator.comparing(ErrorPosition::getCurrentRow, Comparator.nullsFirst(Comparator.naturalOrder())))
					.orElse(null);
			if(maxRow!=null && ObjectUtil.validObject(maxRow.getCurrentRow())) {
				metrics = new ConverterMetrics(before, after, ignoredObsCount, obsCount, (maxRow.getCurrentRow()).longValue());
			} else {
				metrics = new ConverterMetrics(before, after, ignoredObsCount, obsCount);
			}
            logger.info("Conversion time: {} msecs", (after - before));
        } catch(IOException ioExc){
        	logger.error("IO exception: ", ioExc);
        	throw new ConverterException("IO error", ioExc);
        }
        finally {
        	//SDMXCONV-1082
            if(dataReaderEngine!=null) dataReaderEngine.close();
			//if(dataWriterEngine!=null) dataWriterEngine.close(); // If we close the dataWriterEngine we get error in the conversion, and the result is an empty file
		}

    }

}
