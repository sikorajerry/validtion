package com.intrasoft.sdmx.converter.io.data.csv;

import com.intrasoft.sdmx.converter.ComponentValuesBuffer;
import com.intrasoft.sdmx.converter.io.data.ComponentBufferWriterEngine;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.sdmx.api.constants.DATASET_LEVEL;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxSuperBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.base.OccurrenceBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptSchemeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.MeasureBean;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.data.ComplexNodeValue;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.superbeans.conceptscheme.ConceptSchemeSuperBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataValidationException;
import org.sdmxsource.util.ObjectUtil;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

import static org.estat.sdmxsource.util.csv.SingleLevelCsvOutColMapping.SINGLE_LEVEL;

public class SingleLevelCsvDataWriterEngine extends ComponentBufferWriterEngine {

	private static Logger logger = LogManager.getLogger(SingleLevelCsvDataWriterEngine.class);

	CsvWriterSettings settings = new CsvWriterSettings();

	/** Univocity CSV writer used after <a href="https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1025">SDMXCONV-1025</a> */
	private CsvWriter writer;

	private OutputStream out;

	/** Output configuration Object */
	private MultiLevelCsvOutputConfig csvOutputConfig;

	private SdmxSuperBeanRetrievalManager beanRetrieval;

	private List<String> dimensions = new ArrayList<String>();

	/**
	 * @param outputStream
	 * @param csvOutputConfig
	 */
	public SingleLevelCsvDataWriterEngine(OutputStream outputStream, MultiLevelCsvOutputConfig csvOutputConfig) {
		super(csvOutputConfig.getTranscoding());
		this.csvOutputConfig = csvOutputConfig;
		this.beanRetrieval = this.csvOutputConfig.getRetrievalManager();
		this.mapMeasures = csvOutputConfig.isMapMeasure();
		this.dsd = csvOutputConfig.getDsd(); //SDMXCONV-1184
		this.out = outputStream;
	}

	/**
	 * Typically used to open IO resources
	 */
	@Override
	public void openWriter() {
		try {
			//SDMXCONV-1080, do not trim spaces
			settings.setIgnoreLeadingWhitespaces(false);
			settings.setIgnoreTrailingWhitespaces(false);
			//SDMXCONV-1023 SDMXCONV-1025
			if (csvOutputConfig.getEscapeValues() == EscapeCsvValues.ESCAPE_ALL) {
				settings.setQuoteEscapingEnabled(true);
				settings.setQuoteAllFields(true);
			}
			//SDMXCONV-1095
			else if (csvOutputConfig.isCsvOutputEnableNeverUseQuotes() && csvOutputConfig.getEscapeValues() == EscapeCsvValues.ESCAPE_NONE) {
				settings.setQuoteEscapingEnabled(false);
				settings.setQuoteAllFields(false);
				settings.setInputEscaped(true);
				settings.setEscapeUnquotedValues(true);
				settings.setNormalizeLineEndingsWithinQuotes(false);
				settings.getFormat().setNormalizedNewline('!');
				settings.setAutoConfigurationEnabled(false);
			} else { //SDMXCONV-1095 ESCAPE_NONE stopped being an option
				settings.setQuoteEscapingEnabled(true);
				settings.setQuoteAllFields(false);
			}
			settings.getFormat().setDelimiter(csvOutputConfig.getDelimiter());
			writer = new CsvWriter(new OutputStreamWriter(out, "UTF-8"), settings);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Typically used to close IO resources
	 */
	@Override
	public void closeWriter() {
		if (writer != null)
			writer.close();
	}

	/**
	 * start for header writing
	 */
	@Override
	public void openHeader() {
		//Find the list of dimensions that must be present on header or mapping
		if (ObjectUtil.validObject(csvOutputConfig.getColumnMapping()) && !csvOutputConfig.getColumnMapping().isEmpty()) {
			dimensions = csvOutputConfig.getColumnMapping().getMappedComponentsForLevel(SINGLE_LEVEL);
		} else {
			if (!this.mapMeasures) {
				dimensions = getStructureScanner().getComponents(this.csvOutputConfig.isMapCrossXMeasure());
			} else {
				dimensions = getComponentsWithExplicitMeasures();
			}
		}
	}

	@Override
	public void doWriteHeader(HeaderBean header) {
		//Write the header row
		if (csvOutputConfig.showCsvHeader() && !isHeaderWritten) {
			String headers[] = new String[dimensions.size()];
			int i = 0;
			for (String dimension : dimensions) {
				headers[i] = dimension;
				i++;
			}
			writer.writeRow(headers);
		}
	}

	@Override
	public void closeHeader() {
	}

	@Override
	protected void doWriteComponentsValues(ComponentValuesBuffer componentValues) {
		List<String> finalRow = new ArrayList<>();
		DataStructureBean dataStructureBean = getStructureScanner().getDataStructure() != null ? getStructureScanner().getDataStructure() : this.dsd;
		if(dataStructureBean !=null
				&& dataStructureBean.hasComplexComponents()
					&& dataStructureBean.isCompatible(SDMX_SCHEMA.VERSION_THREE)) {
			finalRow = new ArrayList<>(Arrays.asList(new String[dimensions.size()]));
			for(String originalDim: getStructureScanner().getComponents(this.csvOutputConfig.isMapCrossXMeasure())) {
				ComponentBean componentBean = getDataStructure().getComponent(originalDim);
				OccurrenceBean occurrenceBean = null;

				if(componentBean instanceof AttributeBean || componentBean instanceof MeasureBean)
					occurrenceBean = componentBean.getRepresentationMaxOccurs();
				
				boolean isComplex = false;
				if(occurrenceBean!=null)
					if(occurrenceBean.isUnbounded() || occurrenceBean.getOccurrences()>1)
						isComplex = true;
				
				int count=0;
				for (String dimension : dimensions) {
					if(!isGroupSeries())
						if(getStructureScanner().getGroupLevelAttributes().contains(dimension))
							continue;
										
					if(dimension.equals(originalDim)) {
                        String appendedValue = null;
                        if (!isComplex)
                            appendedValue = componentValues.getValueFor(dimension);
                        else
                            appendedValue = complexAttributesMap.get(originalDim);

                        if (appendedValue == null) {
                            appendedValue = findComponentFromId(dimension);
                        }
                        if (appendedValue == null) {
                            appendedValue = "";
                        }
                        String transcodedValue = getTranscoding().getValueFromTranscoding(dimension, appendedValue);
                        finalRow.add(dimensions.indexOf(dimension), transcodedValue);
                        finalRow.remove(dimensions.indexOf(dimension) + 1);
                        break;
                    } else if(dimension.startsWith(originalDim) && (StringUtils.isNumeric(dimension.substring(originalDim.length())))) {
						// only complex
						String appendedValue = complexAttributesMap.get(originalDim);
						if(appendedValue == null){
							final String message = String.format("Attribute '%s' has occurences outside the limit of occurences defined by Data Structure", originalDim);
							throw new SdmxDataValidationException(new DataValidationError(ExceptionCode.MIN_OCCURENCES_NOT_REACHED, message, null, DATASET_LEVEL.NONE, originalDim));
						} else {
							try {
								appendedValue = (appendedValue.split(csvOutputConfig.getSubFieldSeparationChar()))[count];
							} catch (Exception e) { 
								// in case we have more header columns than values set "" and continue
								appendedValue = "";
								finalRow.add(dimensions.indexOf(dimension), appendedValue);
								finalRow.remove(dimensions.indexOf(dimension)+1);
								count++;
								continue;
							}
						}
						if(appendedValue == null){
							appendedValue = findComponentFromId(dimension);
						}
						if (appendedValue == null) {
							appendedValue = "";
						}
						String transcodedValue = getTranscoding().getValueFromTranscoding(dimension, appendedValue);
						finalRow.add(dimensions.indexOf(dimension), transcodedValue);
						finalRow.remove(dimensions.indexOf(dimension)+1);
						count++;
					}
				}
			}
		} else {
			for (String dimension : dimensions) {
				String appendedValue = componentValues.getValueFor(dimension);
				if(appendedValue == null){
					appendedValue = findComponentFromId(dimension);
				}
				if (appendedValue == null) {
					appendedValue = "";
				}
				String transcodedValue = getTranscoding().getValueFromTranscoding(dimension, appendedValue);
				finalRow.add(transcodedValue);
			}
		}

		if (finalRow != null) {
			String[] rowArr = new String[finalRow.size()];
			rowArr = finalRow.toArray(rowArr);
			if (rowArr.length >= 0) {
				if (writer != null)
					writer.writeRow(rowArr);
			}
			finalRow.clear();
		}
	}

	/**
	 * Compute the list of concepts and dimensions adding explicit measure concepts.
	 * The resulting list is used only if parameter mapMeasures set to true.
	 *
	 * @return
	 */
	private List<String> getComponentsWithExplicitMeasures() {
		List<String> components = new ArrayList<String>();
		List<String> dims = getStructureScanner().getComponents(false);
		List<DimensionBean> dimMeasure = getStructureScanner().getDataStructure().getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
		if (this.mapMeasures && !dimMeasure.isEmpty()) {
			for (String dim : dims) {
				if (dim.equalsIgnoreCase(dimMeasure.get(0).getId())) {
					CrossReferenceBean ref = dimMeasure.get(0).getRepresentation().getRepresentation();
					//MaintainableBean maintainable = this.beanRetrieval.getMaintainableBean(ref, false, false);
					//SdmxBeans beans = this.beanRetrieval.getSdmxBeans(maintainable.asReference(), RESOLVE_CROSS_REFERENCES.DO_NOT_RESOLVE);
					Set<ConceptSchemeSuperBean> concepts = this.beanRetrieval.getConceptSchemeSuperBeans(ref);
					for (ConceptSchemeSuperBean concept : concepts) {
						ConceptSchemeBean conceptsFrom = concept.getBuiltFrom();
						for (ConceptBean item : conceptsFrom.getItems()) {
							components.add(item.getId());
						}
					}
				} else {
					components.add(dim);
				}
			}
		}
		return components;
	}

	@Override
	public void writeComplexAttributeValue(KeyValue keyValue) {
		if(ObjectUtil.validObject(keyValue) && ObjectUtil.validCollection(keyValue.getComplexNodeValues())){
			StringBuffer sb = new StringBuffer();
			int countNodes = 0;
			for(ComplexNodeValue node: keyValue.getComplexNodeValues()) {
				if(node.getCode() != null) {
					if(countNodes>0) {
						sb.append(csvOutputConfig.getSubFieldSeparationChar());
					}
					sb.append(getTranscoding().getValueFromTranscoding(keyValue.getConcept(), node.getCode()));
					countNodes++;
				} else if(node.getTexts() != null && node.getTexts().size()>0) {
					for(TextTypeWrapper text : node.getTexts()){
						if(countNodes>0)
							sb.append(csvOutputConfig.getSubFieldSeparationChar());
							sb.append(text.getLocale()!=null?text.getLocale()+":"+text.getValue():text.getValue());
						countNodes++;
					}
				}
			}

			complexAttributesMap.put(keyValue.getConcept(), sb.toString());
		} else {
			throw new IllegalArgumentException("Error while writing complex attribues: keyValue.getComplexNodeValues() cannot be null or empty.");
		}
	}

	@Override
	public void writeComplexMeasureValue(KeyValue keyValue) {
		writeComplexAttributeValue(keyValue);
	}

	@Override
	public void writeMeasureValue(String id, String value) {
		for(ComponentValuesBuffer componentValuesBuffer: getComponentValuesBuffers()) {
			componentValuesBuffer.addValueFor(value, id);
		}
	}

	@Override
	public void writeObservation(String obsConceptValue, AnnotationBean... annotations) {
		if (dimensionAtObservation == null) {
			writeObservation(DimensionBean.TIME_DIMENSION_FIXED_ID, obsConceptValue, null, annotations);
			for(ComponentValuesBuffer componentValuesBuffer: getComponentValuesBuffers()) {
				componentValuesBuffer.addValueFor(obsConceptValue, DimensionBean.TIME_DIMENSION_FIXED_ID);
			}
		} else {
			writeObservation(dimensionAtObservation, obsConceptValue, null, annotations);
			for(ComponentValuesBuffer componentValuesBuffer: getComponentValuesBuffers()) {
				componentValuesBuffer.addValueFor(obsConceptValue, dimensionAtObservation);
			}
			
		}
	}

	//SDMXRI-1166
	@Override
	public void close() {
		close(new FooterMessage[]{});
		//closeWriter();
	}
}
