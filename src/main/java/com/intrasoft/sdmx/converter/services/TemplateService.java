package com.intrasoft.sdmx.converter.services;

import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.sdmx.converter.Operation;
import com.intrasoft.sdmx.converter.TemplateData;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.IoUtils;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidColumnMappingException;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidTemplateException;
import com.intrasoft.sdmx.converter.services.exceptions.TemplateException;
import com.intrasoft.sdmx.converter.util.FormatFamily;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.util.csv.*;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.*;
import java.util.*;

/**
 * Created by dbalan on 7/12/2017.
 */
@Service
public class TemplateService {

	private static Logger logger = LogManager.getLogger(TemplateService.class);

    @Autowired
    private CsvService csvService;
    
    @Autowired
    private FlrService flrService;
    
    @Autowired
    private ConfigService configService;
        	
    public void writeTemplate(  File templateFile,
                                TemplateData templateData) throws TemplateException{
        writeTemplate(templateFile, templateData, true);
    }

    public void writeTemplate(  File templateFile,
                                TemplateData templateData,
                                boolean writeConverterVersionAttribute) throws TemplateException{
        try {
            writeTemplate(new FileOutputStream(templateFile), templateData, writeConverterVersionAttribute);
        } catch (FileNotFoundException e) {
            throw new TemplateException("Template file not found!", e);
        }
    }

    public void writeTemplate(  OutputStream templateFile,
                                TemplateData templateData) throws TemplateException{
        writeTemplate(templateFile, templateData, true);
    }

    public void writeTemplate(  OutputStream templateFile,
                                TemplateData templateData,
                                boolean writeConverterVersionAttribute) throws TemplateException{
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document xmlDocument = docBuilder.newDocument();
            xmlDocument.setXmlStandalone(false);
            xmlDocument.setXmlVersion("1.0");
            Element root = xmlDocument.createElement("Template_v6.0");
            //Add the prefix for validation to work SDMXCONV-724
            root.setAttribute("xmlns:nsi", "http://www.w3.org/2001/XMLSchema-instance");
            xmlDocument.appendChild(root);
            if(writeConverterVersionAttribute){
                Attr attr = xmlDocument.createAttribute("createdWith");
                attr.setValue(configService.getProjectVersion());
                root.setAttributeNodeNS(attr);
            }
            //write the Operation element
            root.appendChild(buildOperationElement(templateData, xmlDocument));
            //write the Input_Output element
            List<Element> allInputOutputElements = buildInputOutputElement(templateData, xmlDocument);
            for (Element elem : allInputOutputElements) {
                root.appendChild(elem);
            }
            //write the Structure element
            root.appendChild(buildStructureElement(templateData, xmlDocument));
            //write the Inputconfig element
            root.appendChild(buildInputConfigElement(templateData, xmlDocument));
            
            if (templateData.getOperationType().isConversion()) {
	            //write the OutputConfig element
	            root.appendChild(buildOutputConfigElement(templateData, xmlDocument));
            }
            // write the content into xml file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(xmlDocument);
            StreamResult result = new StreamResult(templateFile);
            transformer.transform(source, result);
        }catch(ParserConfigurationException | TransformerException ex){
            throw new TemplateException("Xml parser or transformer error !", ex);
        }
    }
    
    private Element buildOutputConfigElement(TemplateData templateData, Document document) {   	 
    	Element outputConfig = document.createElement("OutputConfig");   
    	if (templateData.getOutputFormat().getFamily() == FormatFamily.CSV) {
	    	Element cSVOutput = document.createElement("CSVOutput");
	    	cSVOutput.setAttribute("isSDMX", String.valueOf(templateData.isSDMXCsvOutput()));
	    	cSVOutput.setAttribute("level", String.valueOf(templateData.getCsvOutputLevels()));
	    	Element useDoubleQuotes = document.createElement("UseDoubleQuotes");
	    	useDoubleQuotes.setTextContent(String.valueOf(templateData.isUseDoubleQuotes()));
            //SDMXCONV-800
            Element outputMapMeasures = document.createElement("OutputMapMeasures");
            outputMapMeasures.setTextContent(String.valueOf(templateData.isOutputMapMeasures()));
            Element outputTranscodingMeasures = document.createElement("OutputTranscodingMeasures");
            outputTranscodingMeasures.setTextContent(String.valueOf(templateData.isOutputTranscodingMeasures()));
	    	Element csvOutputHeaderRow = document.createElement("CsvOutputHeaderRow");
	    	if (templateData.getCsvOutputColumnHeader() != null) {
	    		csvOutputHeaderRow.setTextContent(templateData.getCsvOutputColumnHeader().getTechnicalValue());
	    	}
			Element outputWriteLabel = document.createElement("WriteLabel");
			outputWriteLabel.setTextContent(String.valueOf(templateData.getWriteLabels()));
	    	Element dateFormat = document.createElement("DateFormat");
	    	dateFormat.setTextContent(templateData.getCsvOutputDateFormat());
	    	Element mappingInfo = buildMappingInfoElement(templateData, document);
//	    	Element mapping = buildMappingElement(templateData, document);
	    	Element mapping = buildOutputMappingElement(templateData, document);
	    	Element transcoding = buildTranscodingElement(templateData, document, templateData.getCsvOutputStructureSet());
	    	Element delimiter = document.createElement("Delimiter");
	    	delimiter.setTextContent(templateData.getCsvOutputDelimiter());
	    	cSVOutput.appendChild(useDoubleQuotes);	
	    	cSVOutput.appendChild(outputMapMeasures);	
	    	cSVOutput.appendChild(outputTranscodingMeasures);	
	    	cSVOutput.appendChild(csvOutputHeaderRow);    	
	    	cSVOutput.appendChild(dateFormat);
	    	cSVOutput.appendChild(mappingInfo);
	    	cSVOutput.appendChild(mapping);
	    	cSVOutput.appendChild(transcoding);	
	    	cSVOutput.appendChild(delimiter);
			cSVOutput.appendChild(outputWriteLabel);
	    	outputConfig.appendChild(cSVOutput);
    	} else if (templateData.getOutputFormat().getFamily() == FormatFamily.EXCEL) {
	    	Element excelOutput = document.createElement("ExcelOutput");
	    	Element excelTemplate = document.createElement("ExcelTemplate");
	    	excelTemplate.setTextContent(templateData.getExcelTemplate());
	    	excelOutput.appendChild(excelTemplate);
	    	outputConfig.appendChild(excelOutput);
    	}
    	Element sdmxOutput = null;
    	if (!templateData.isUseDefaultNamespace()) {
    		sdmxOutput = buildNamespaceElement(templateData, document);
        	if (sdmxOutput != null) {
        		outputConfig.appendChild(sdmxOutput);
        	}
    	}
    	if (templateData.getOutputFormat().getFamily() == FormatFamily.GESMES) {
	    	Element gesTechnique = document.createElement("GesmesOutput");
	    	Element tsTechnique = document.createElement("Ts_Technique");
	    	tsTechnique.setTextContent(templateData.getGesmesTechnique());
	    	gesTechnique.appendChild(tsTechnique);   
	    	outputConfig.appendChild(gesTechnique);
    	}	
    	return outputConfig;   	
    }

    private Element buildOperationElement(TemplateData templateData, Document document) {
    	 Element operation = document.createElement("Operation");
    	 operation.setAttribute("operationType", templateData.getOperationType().getDescription());
    	 return operation;
    }
    
    private List<Element> buildInputOutputElement(TemplateData templateData, Document document){
        List<Element> resultList = new ArrayList<Element>();
        for(String filePath : templateData.getInputFilePaths()) {
            Element inout = document.createElement("Input_Output");
            Element inputFileName = document.createElement("InputFileName");
            inputFileName.setAttribute("format", templateData.getInputFormat().toString());
			inputFileName.setTextContent(filePath);
			inout.appendChild(inputFileName);
			if (templateData.getOperationType().isConversion()) {
				Element outputFileName = document.createElement("OutputFileName");
				if (templateData.getOutputFormat() != null) {
					outputFileName.setAttribute("format", templateData.getOutputFormat().toString());
				}
				if (templateData.getOutputFilePath() != null) {
					outputFileName.setTextContent(templateData.getOutputFilePath());
				}
				inout.appendChild(outputFileName);
			}
			resultList.add(inout);
		}

        return resultList;
    }

    private Element buildStructureElement(TemplateData templateData, Document xmlDocument){
        Element registryParameters = xmlDocument.createElement("Structure");
        Element registry = null;
        if (!templateData.isStructureInRegistry()) {
            registry = xmlDocument.createElement("DSD");
            registry.setAttribute("agency", templateData.getStructureAgency());
            registry.setAttribute("id", templateData.getStructureId());
            registry.setAttribute("version", templateData.getStructureVersion());
            registry.setTextContent(templateData.getStructureFilePath());
        } else { 
            registry = xmlDocument.createElement("Registry");
            registry.setAttribute("url", templateData.getRegistryUrl());
            registry.setAttribute("agency", templateData.getStructureAgency());
            registry.setAttribute("id", templateData.getStructureId());
            registry.setAttribute("version", templateData.getStructureVersion());           
        }
        Element isDataflow = xmlDocument.createElement("isDataFlow"); 
        isDataflow.setTextContent(String.valueOf(templateData.isStructureDataflow()));
        registryParameters.appendChild(registry);
        registryParameters.appendChild(isDataflow);
        return registryParameters;
    }
    
    private Element buildInputConfigElement(TemplateData templateData, Document document) {
    	Element inputConfig = document.createElement("InputConfig");
    	Element header = document.createElement("Header");
    	Element headerFilePath= document.createElement("HeaderFileName");
    	if (templateData.getHeaderFilePath() != null) {
    		 headerFilePath.setTextContent(templateData.getHeaderFilePath());
    		 header.appendChild(headerFilePath);
    	}
    	Element headerInformation = document.createElement("HeaderInformation");
    	// set the header of the conversion
        StringWriter writer = new StringWriter();
        if (templateData.getHeader() != null) {
            templateData.getHeader().list(new PrintWriter(writer));
            headerInformation.setTextContent(writer.getBuffer().toString());
            header.appendChild(headerInformation);
        }   
        inputConfig.appendChild(header);
		Element errorIfEmpty = document.createElement("ErrorIfEmpty");
		errorIfEmpty.setTextContent(String.valueOf(templateData.isErrorIfEmpty()));
		inputConfig.appendChild(errorIfEmpty);
        if (templateData.getInputFormat().getFamily() == FormatFamily.CSV || templateData.getInputFormat().getFamily() == FormatFamily.FLR) {
	    	Element cSVInput = document.createElement("CSVInput");
	    	cSVInput.setAttribute("isSDMX", String.valueOf(templateData.isSDMXCsvInput()));
	    	cSVInput.setAttribute("level", String.valueOf(templateData.getCsvInputLevels()));
	    	Element hasDoubleQuotes = document.createElement("HasDoubleQuotes");
	    	hasDoubleQuotes.setTextContent(String.valueOf(templateData.isHasDoubleQuotes()));
	    	Element inputOrdered = document.createElement("InputOrdered");
	    	inputOrdered.setTextContent(String.valueOf(templateData.isCsvInputOrdered()));
			Element subFieldSeparation = null;
			if(ObjectUtil.validObject(templateData.getFieldSeparationCharacter())) {
				subFieldSeparation = document.createElement("SubFieldSeparator");
				subFieldSeparation.setTextContent(String.valueOf(templateData.getFieldSeparationCharacter()));
			}
            //SDMXCONV-800
            Element inputMapMeasures = document.createElement("InputMapMeasures");
            inputMapMeasures.setTextContent(String.valueOf(templateData.isInputMapMeasures()));
			Element inputAllowAdditionalColumns = document.createElement("AllowAdditionalColumns");
			inputAllowAdditionalColumns.setTextContent(String.valueOf(templateData.isAllowAdditionalColumn()));
            Element inputTranscodingMeasures = document.createElement("InputTranscodingMeasures");
            inputTranscodingMeasures.setTextContent(String.valueOf(templateData.isInputTranscodingMeasures()));
	    	Element cSVInputHeaderRow = document.createElement("CsvInputHeaderRow");
	    	if (templateData.getCsvInputColumnHeader() != null) {	    	
		    	cSVInputHeaderRow.setTextContent(templateData.getCsvInputColumnHeader().getTechnicalValue());
	    	}
	    	Element dateFormat = document.createElement("DateFormat");
	    	dateFormat.setTextContent(templateData.getCsvInputDateFormat());
	    	Element mappingInfo = buildMappingInfoElement(templateData, document);
	    	Element mapping = buildMappingElement(templateData, document);
	    	Element transcoding = buildTranscodingElement(templateData, document, templateData.getCsvInputStructureSet());
	    	Element delimiter = document.createElement("Delimiter");
	    	delimiter.setTextContent(templateData.getCsvInputDelimiter());
	    	cSVInput.appendChild(inputOrdered);
			if(ObjectUtil.validObject(subFieldSeparation))	cSVInput.appendChild(subFieldSeparation);
	    	cSVInput.appendChild(inputMapMeasures);
			cSVInput.appendChild(inputAllowAdditionalColumns);
	    	cSVInput.appendChild(inputTranscodingMeasures);
	    	cSVInput.appendChild(hasDoubleQuotes);
	    	cSVInput.appendChild(cSVInputHeaderRow);
	    	cSVInput.appendChild(dateFormat);
	    	cSVInput.appendChild(mappingInfo);
	    	cSVInput.appendChild(mapping);
	    	cSVInput.appendChild(transcoding);
	    	cSVInput.appendChild(delimiter);
	    	inputConfig.appendChild(cSVInput);
        } else if (templateData.getInputFormat().getFamily() == FormatFamily.EXCEL) {
	    	Element excelInput = document.createElement("ExcelInput");
	    	Element parametersInExternalFile = document.createElement("ParametersInExternalFile");
	    	parametersInExternalFile.setTextContent(templateData.getParametersInExternalFile());
	    	Element parameterSheetMapping = buildParameterSheetMapping(templateData, document);
	    	excelInput.appendChild(parametersInExternalFile);
	    	excelInput.appendChild(parameterSheetMapping);
	    	inputConfig.appendChild(excelInput);
        }
    	return inputConfig;
    }
    
    private Element buildParameterSheetMapping(TemplateData templateData, Document document) {
    	Element parameterSheetMapping = document.createElement("ParameterSheetMapping");
    	if (templateData.getParameterSheetMapping() != null) {
    		for (String dataSheetName: templateData.getParameterSheetMapping().keySet()) {
    			 Element parameterPair = document.createElement("ParameterPair");
    			 parameterPair.setAttribute("dataSheetName", dataSheetName);
    			 String parameterSheetName = templateData.getParameterSheetMapping().get(dataSheetName);
    			 parameterPair.setAttribute("parameterSheetName", parameterSheetName);
    			 parameterSheetMapping.appendChild(parameterPair);
    		}
    	}
    	return parameterSheetMapping;
    }
    
    private Element buildMappingInfoElement(TemplateData templateData, Document document) {
        Element mappingInfo = document.createElement("MappingInfo");
        mappingInfo.setTextContent(templateData.isCsvInputMapCrossSectional() ? TemplateData.MAP_CROSSX_MEASURES : TemplateData.MAP_MEASURE_DIMENSION);
        return mappingInfo;
    }

    private Element buildMappingElement(TemplateData templateData, Document document){
        Element mapping = document.createElement("Mapping");
        // populate the mapping to an xml representation.
        if (templateData.getCsvInputMapping() != null) {
            for (String dim : templateData.getCsvInputMapping().keySet()) {
                Element concept = document.createElement("Concept");
                CsvInColumnMapping data = templateData.getCsvInputMapping().get(dim);
				concept.setAttribute("fixed", String.valueOf(data.isFixed()));
				concept.setAttribute("level", String.valueOf(data.getLevel()));
                concept.setAttribute("name", dim);
                if (data.isFixed()) {
                    if (data.getFixedValue() == null) {
                        concept.setAttribute("value", "");
                    } else {
                        concept.setAttribute("value", String.valueOf(data.getFixedValue()));
                    }
                } else {
                    if (data.getColumns().size() == 1) {
                        concept.setAttribute("value", String.valueOf(data.getColumns().get(0).getUserFriendlyIdx()));
                    } else if (data.getColumns().size() > 1) {
                        String computeValue="";
                        for (CsvColumn column: data.getColumns()) {
                            computeValue += column.getUserFriendlyIdx()+"+";
                        }
                        concept.setAttribute("value", computeValue.substring(computeValue.length()-1));
                    }
                }
                mapping.appendChild(concept);
            }
        }
        // populate the mapping for Flr to an xml representation.
        else if (templateData.getFlrInputMapping() != null) {
            for (String dim : templateData.getFlrInputMapping().keySet()) {
                Element concept = document.createElement("Concept");
                FlrInColumnMapping data = templateData.getFlrInputMapping().get(dim);
                concept.setAttribute("name", dim);
                concept.setAttribute("level", String.valueOf(data.getLevel()));
                concept.setAttribute("fixed", String.valueOf(data.isFixed()));
                if (data.isFixed()) {
                    if (data.getFixedValue() == null) {
                        concept.setAttribute("value", "");
                    } else {
                        concept.setAttribute("value", String.valueOf(data.getFixedValue()));
                    }
                } else {
                	if(data.getPositions().size()==1) {
                		String userFriendlyValues = String.valueOf(data.getPositions().get(0).getStart()) + "-" + String.valueOf(data.getPositions().get(0).getEnd());
                		concept.setAttribute("value", userFriendlyValues);
                	}else {
                		String computeValueFlr="";
                		if(data.getPositions().size()!=0) {
	                		for(FixedWidth pos:data.getPositions()) {
	                			computeValueFlr += String.valueOf(pos.getStart()) + "-" + String.valueOf(pos.getEnd()) + "+";
	                		}
	                		concept.setAttribute("value", computeValueFlr.substring(0,computeValueFlr.length()-1));
                		}else {
                			concept.setAttribute("value", computeValueFlr);
                		}
                	}
                }
                mapping.appendChild(concept);
            }
        }
        return mapping;
    }
    
    private Element buildOutputMappingElement(TemplateData templateData, Document document){
        Element mapping = document.createElement("Mapping");
        // populate the mapping to an xml representation.
        if (templateData.getCsvOutputMapping() != null) {
            for (Integer levelNumber: templateData.getCsvOutputMapping().getDimensionsMappedOnLevel().keySet()) {
                for (Integer colIndex: templateData.getCsvOutputMapping().getDimensionsMappedOnLevel().get(levelNumber).keySet()) {
                    Element concept = document.createElement("Concept");
                    concept.setAttribute("name", templateData.getCsvOutputMapping().getDimensionsMappedOnLevel().get(levelNumber).get(colIndex));
                    concept.setAttribute("level", String.valueOf(levelNumber));
                    concept.setAttribute("value", colIndex+"");
                    concept.setAttribute("fixed", "false");
                    mapping.appendChild(concept);
                }
            }
        }
        return mapping;
    }

    private Element buildNamespaceElement(TemplateData templateData, Document document){    	
        Element sdmxOutput = document.createElement("SdmxOutput");
        Element urn = document.createElement("URN");
        urn.setTextContent(templateData.getNamespaceUri());
        Element prefix = document.createElement("Prefix");
        prefix.setTextContent(templateData.getNamespacePrefix());
        Element reportingPeriod = document.createElement("ReportingPeriod"); 
        Element day = document.createElement("Day");
        day.setTextContent(templateData.getDay());
        Element month = document.createElement("Month");
        month.setTextContent(templateData.getMonth());
        reportingPeriod.appendChild(day);
        reportingPeriod.appendChild(month);
        sdmxOutput.appendChild(urn);
        sdmxOutput.appendChild(prefix);
        sdmxOutput.appendChild(reportingPeriod);
        return sdmxOutput;
    }
    
    private Element buildTranscodingElement(TemplateData templateData, Document xmlDocument, LinkedHashMap<String, LinkedHashMap<String, String>> strucureSet){
        Element transcodingElem = xmlDocument.createElement("Transcoding");
        if (strucureSet != null) {
			LinkedHashMap<String, LinkedHashMap<String, String>> transcoding = strucureSet;
            // populate the structure set to an xml representation
            Element structureSet = xmlDocument.createElement("StructureSet");
            Set<String> listCodelist = transcoding.keySet();
            for (String codelistId : listCodelist) {
                Element codelistMap = xmlDocument.createElement("CodelistMap");
                codelistMap.setAttribute("id", codelistId);

                for (Map.Entry<String, String> codeBean : transcoding.get(codelistId).entrySet()) {
                    Element codeMap = xmlDocument.createElement("CodeMap");
                    Element mapCodeRef = xmlDocument.createElement("MapCodeRef");
                    mapCodeRef.setTextContent(codeBean.getKey());
                    Element mapTargetCodeRef = xmlDocument.createElement("MapTargetCodeRef");
                    mapTargetCodeRef.setTextContent(codeBean.getValue());
                    codeMap.appendChild(mapCodeRef);
                    codeMap.appendChild(mapTargetCodeRef);
                    codelistMap.appendChild(codeMap);
                }
                structureSet.appendChild(codelistMap);
            }
            transcodingElem.appendChild(structureSet);
        }
        return transcodingElem;
    }

	public TemplateData loadTemplate(File templateFile, TemplateData templateData) throws TemplateException{
        try {
            return loadTemplate(new FileInputStream(templateFile), templateData);
        } catch (FileNotFoundException e) {
            throw new TemplateException("template file could not be found !", e);
        }
    }

	public TemplateData loadTemplate(InputStream templateInputStream, TemplateData templateData)
			throws TemplateException {
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			Document document = docBuilder.parse(templateInputStream);
			document.getDocumentElement().normalize();
			Element root = document.getDocumentElement();
			if ("Conversion_Template".equals(root.getNodeName())) {
				NodeList list = root.getChildNodes();
				for (int i = 0; i < list.getLength(); i++) {
					if (list.item(i).getNodeType() == Node.ELEMENT_NODE) {
						Element childElement = (Element) list.item(i);
						if (childElement.getNodeName().contains("Input_Output")) {
							readInputOutputFromTemplateXml(templateData, childElement);
						}
						if (childElement.getNodeName().contains("Registry_Parameters")) {
							if (childElement.getElementsByTagName("DSD").item(0) != null) {
								readDsdFromTemplateXml(templateData, childElement);
							}
							if (childElement.getElementsByTagName("Dataflow").item(0) != null) {
								readDataflowFromTemplateXml(templateData, childElement);
							}
						}
						if (childElement.getElementsByTagName("ErrorIfEmpty").item(0) != null) {
							templateData.setErrorIfEmpty(Boolean.parseBoolean(
									childElement.getElementsByTagName("ErrorIfEmpty").item(0).getTextContent()));
						} else {
							templateData.setErrorIfEmpty(configService.isValidationErrorIfEmpty());
						}
						if (childElement.getNodeName().contains("CSV_Parameters")) {
							readCsvParameterFromOldTemplateXml(templateData, childElement);
						}
						if (childElement.getNodeName().contains("Other_Parameters")) {
							readOtherParametersFromOldTemplateXml(templateData, childElement);
						}
						if (childElement.getNodeName().contains("Namespace")) {
							readNamespaceFromOldTemplateXml(templateData, childElement);
						}
					}
				}
				//because old converter didn't know about multilevel csv and
				// it had only csv simple or on multiple levels,
				// for compatibility we check if we have multiple levels
				if (templateData.getCsvInputLevels() > 1) {
					templateData.setInputFormat(Formats.MULTI_LEVEL_CSV);
				}
				if (templateData.getCsvOutputLevels() > 1) {
					templateData.setOutputFormat(Formats.MULTI_LEVEL_CSV);
				}
			} else {
				// loads the new Template with name "Template_v6.0"
				NodeList list = root.getChildNodes();
				for (int i = 0; i < list.getLength(); i++) {
					if (list.item(i).getNodeType() == Node.ELEMENT_NODE) {
						Element childElement = (Element) list.item(i);
						if (childElement.getNodeName().contains("Operation")) {
							templateData.setOperationType(
									Operation.getOperationByDescrption(childElement.getAttribute("operationType")));
						}
						if (childElement.getNodeName().contains("Input_Output")) {
							readInputOutputFromTemplateXml(templateData, childElement);
						}
						if (childElement.getNodeName().contains("Structure")) {
							if (childElement.getElementsByTagName("DSD").item(0) != null) {
								readStructureFromTemplateXml(templateData, childElement);
							}
							if (childElement.getElementsByTagName("Registry").item(0) != null) {
								readRegistryFromTemplateXml(templateData, childElement);
							}
							if (childElement.getElementsByTagName("isDataFlow").item(0) != null) {
								templateData.setStructureDataflow(Boolean.parseBoolean(
										childElement.getElementsByTagName("isDataFlow").item(0).getTextContent()));
							}
						}
						if (childElement.getNodeName().contains("InputConfig")) {
							if (childElement.getElementsByTagName("Header").item(0) != null) {
								readHeaderParametersFromONewTemplateXml(templateData, childElement);
							}
							if (childElement.getElementsByTagName("ErrorIfEmpty").item(0) != null) {
								templateData.setErrorIfEmpty(Boolean.parseBoolean(
										(childElement.getElementsByTagName("ErrorIfEmpty").item(0).getTextContent())));
							} else {
								templateData.setErrorIfEmpty(configService.isValidationErrorIfEmpty());
							}
							if (childElement.getElementsByTagName("CSVInput").item(0) != null) {
								readInputCsvParametersFromONewTemplateXml(templateData, childElement);
							}
							if (childElement.getElementsByTagName("ExcelInput").item(0) != null) {
								readInputExcelParametersFromNewTemplateXml(templateData, childElement);
							}
						}
						if (childElement.getNodeName().contains("OutputConfig")) {
							if (childElement.getElementsByTagName("CSVOutput").item(0) != null) {
								readOutputCsvParametersFromONewTemplateXml(templateData, childElement);
							}
							if (childElement.getElementsByTagName("ExcelOutput").item(0) != null) {
								templateData.setExcelTemplate(
										childElement.getElementsByTagName("ExcelTemplate").item(0).getTextContent());
							}
							if (childElement.getElementsByTagName("SdmxOutput").item(0) != null) {
								readNamespaceFromNewTemplateXml(templateData, childElement);
							}
							if (childElement.getElementsByTagName("GesmesOutput").item(0) != null) {
								templateData.setGesmesTechnique(
										childElement.getElementsByTagName("Ts_Technique").item(0).getTextContent());
							}
						}
					}
				}

			}
		} catch (ParserConfigurationException | SAXException | IOException | InvalidColumnMappingException e) {
			throw new TemplateException("xml error when loading template", e);
		}
		return templateData;
	}

	private void readStructureFromTemplateXml(TemplateData templateData, Element childElement) {
		templateData.setStructureInRegistry(false);
		templateData.setStructureFilePath(childElement.getElementsByTagName("DSD").item(0).getTextContent());
		templateData.setStructureAgency(
				(((Element) childElement.getElementsByTagName("DSD").item(0).getChildNodes()).getAttribute("agency")));
		templateData.setStructureId(
				(((Element) childElement.getElementsByTagName("DSD").item(0).getChildNodes()).getAttribute("id")));
		templateData.setStructureVersion(
				(((Element) childElement.getElementsByTagName("DSD").item(0).getChildNodes()).getAttribute("version")));
	}

	private void readRegistryFromTemplateXml(TemplateData templateData, Element childElement) {
		templateData.setStructureInRegistry(true);
		templateData.setRegistryUrl(
				(((Element) childElement.getElementsByTagName("Registry").item(0).getChildNodes()).getAttribute(
						"url")));
		templateData.setStructureAgency(
				(((Element) childElement.getElementsByTagName("Registry").item(0).getChildNodes()).getAttribute(
						"agency")));
		templateData.setStructureId(
				(((Element) childElement.getElementsByTagName("Registry").item(0).getChildNodes()).getAttribute("id")));
		templateData.setStructureVersion(
				(((Element) childElement.getElementsByTagName("Registry").item(0).getChildNodes()).getAttribute(
						"version")));
	}
    
    private void readNamespaceFromNewTemplateXml(TemplateData templateData, Element childElement) {
        templateData.setUseDefaultNamespace(false);
        templateData.setNamespaceUri(childElement.getElementsByTagName("URN").item(0).getTextContent());
        templateData.setNamespacePrefix(childElement.getElementsByTagName("Prefix").item(0).getTextContent());
        templateData.setDay(childElement.getElementsByTagName("Day").item(0).getTextContent());
        templateData.setMonth(childElement.getElementsByTagName("Month").item(0).getTextContent());
        if (childElement.getElementsByTagName("ReportingPeriod") != null) {
        	templateData.setReportingPeriod("--" + String.format("%02d", Integer.parseInt(childElement.getElementsByTagName("Month").item(0).getTextContent())) + 
					                        "-" + String.format("%02d", Integer.parseInt(childElement.getElementsByTagName("Day").item(0).getTextContent())));
        }
    }
   private void readHeaderParametersFromONewTemplateXml(TemplateData templateData, Element childElement){
      if (childElement.getElementsByTagName("HeaderFileName").item(0) != null) {
         String headerFilePath = childElement.getElementsByTagName("HeaderFileName").item(0).getTextContent();
         templateData.setHeaderFilePath(headerFilePath);
      }
      if (childElement.getElementsByTagName("HeaderInformation").item(0) != null) {
         String input = childElement.getElementsByTagName("HeaderInformation").item(0).getTextContent();
         templateData.setHeader(buildHeaderProperties(input));
      }
   }
    private void readOutputCsvParametersFromONewTemplateXml(TemplateData templateData, Element childElement) {
    	templateData.setSDMXCsvInput(Boolean.getBoolean((((Element)childElement.getElementsByTagName("CSVOutput").item(0).getChildNodes()).getAttribute("isSDMX"))));
    	if ((((Element)childElement.getElementsByTagName("CSVOutput").item(0).getChildNodes()).getAttribute("level") != null && (((Element)childElement.getElementsByTagName("CSVOutput").item(0).getChildNodes()).getAttribute("level").isEmpty()))) {
    		templateData.setCsvInputLevels(1);
    	} else {
    		templateData.setCsvInputLevels(Integer.parseInt((((Element)childElement.getElementsByTagName("CSVOutput").item(0).getChildNodes()).getAttribute("level"))));
    	}
    	if (childElement.getElementsByTagName("UseDoubleQuotes").item(0) != null) {
            templateData.setHasDoubleQuotes(Boolean.parseBoolean(childElement.getElementsByTagName("UseDoubleQuotes").item(0).getTextContent()));
        }
        //SDMXCONV-799, SDMXCONV-800
    	if (childElement.getElementsByTagName("OutputMapMeasures").item(0) != null) {
            templateData.setOutputMapMeasures(Boolean.parseBoolean(childElement.getElementsByTagName("OutputMapMeasures").item(0).getTextContent()));
        }
    	if (childElement.getElementsByTagName("OutputTranscodingMeasures").item(0) != null) {
            templateData.setOutputTranscodingMeasures(Boolean.parseBoolean(childElement.getElementsByTagName("OutputTranscodingMeasures").item(0).getTextContent()));
        }
        if (childElement.getElementsByTagName("CsvOutputHeaderRow").item(0) != null) {
        	CsvOutputColumnHeader csvOutputColumnHeader = CsvOutputColumnHeader.forTehnicalValue(childElement.getElementsByTagName("CsvOutputHeaderRow").item(0).getTextContent());
    		templateData.setCsvOutputColumnHeader(csvOutputColumnHeader);          	
        }
        if (childElement.getElementsByTagName("DateFormat").item(0) != null) {
        	templateData.setCsvOutputDateFormat(childElement.getElementsByTagName("DateFormat").item(0).getTextContent());
        }
        if (childElement.getElementsByTagName("MappingInfo").item(0) != null) {
        	if (childElement.getElementsByTagName("MappingInfo").item(0).getTextContent().equals(TemplateData.MAP_CROSSX_MEASURES)) {
        		 templateData.setCsvOutputMapCrossSectional(true);
        	}           
        }	                   
        if (childElement.getElementsByTagName("Delimiter").item(0) != null) {
            templateData.setCsvOutputDelimiter(childElement.getElementsByTagName("Delimiter").item(0).getTextContent());
        }
		if (childElement.getElementsByTagName("WriteLabel").item(0) != null) {
			templateData.setWriteLabels(
					CsvLabel.valueOf(childElement.getElementsByTagName("WriteLabel").item(0).getTextContent()));
		}
        readCsvOutputMappingFromXml(templateData, childElement);
        readTranscodingFromXml(templateData, childElement, false);  
    }
    
    private void readInputExcelParametersFromNewTemplateXml(TemplateData templateData, Element childElement) {
    	templateData.setParametersInExternalFile(childElement.getElementsByTagName("ParametersInExternalFile").item(0).getTextContent());
    	Map<String, String> parameterSheetMapping = new HashMap<String, String>();
    	if (childElement.getElementsByTagName("ParameterSheetMapping").item(0) != null && childElement.getElementsByTagName("ParameterSheetMapping").item(0).hasChildNodes()) {
            NodeList listOfMappingConcepts = childElement.getElementsByTagName("ParameterSheetMapping").item(0).getChildNodes();
            for (int j =0; j<listOfMappingConcepts.getLength();j++) {
                if (childElement.getElementsByTagName("ParameterPair").item(j) != null) {
                    Element conceptElement = (Element)childElement.getElementsByTagName("ParameterPair").item(j);
                    String dataSheetName = conceptElement.getAttribute("dataSheetName");
                    String parameterSheetName = conceptElement.getAttribute("parameterSheetName");
                    parameterSheetMapping.put(dataSheetName, parameterSheetName);
                }
            }
        }
    	templateData.setParameterSheetMapping(parameterSheetMapping);
    }
    
    private void readInputCsvParametersFromONewTemplateXml(TemplateData templateData, Element childElement) throws InvalidColumnMappingException {
    	templateData.setSDMXCsvInput(Boolean.getBoolean((((Element)childElement.getElementsByTagName("CSVInput").item(0).getChildNodes()).getAttribute("isSDMX"))));
    	if ((((Element)childElement.getElementsByTagName("CSVInput").item(0).getChildNodes()).getAttribute("level") != null && (((Element)childElement.getElementsByTagName("CSVInput").item(0).getChildNodes()).getAttribute("level").isEmpty()))) {
    		templateData.setCsvInputLevels(1);
    	} else {
    		templateData.setCsvInputLevels(Integer.parseInt((((Element)childElement.getElementsByTagName("CSVInput").item(0).getChildNodes()).getAttribute("level"))));
    	}
    	if (childElement.getElementsByTagName("InputOrdered").item(0) != null) {
            templateData.setCsvInputOrdered(Boolean.parseBoolean(childElement.getElementsByTagName("InputOrdered").item(0).getTextContent()));
        }
        //SDMXCONV-799, SDMXCONV-800
    	if (childElement.getElementsByTagName("InputMapMeasures").item(0) != null) {
            templateData.setInputMapMeasures(Boolean.parseBoolean(childElement.getElementsByTagName("InputMapMeasures").item(0).getTextContent()));
        }
		if (childElement.getElementsByTagName("AllowAdditionalColumns").item(0) != null) {
			templateData.setAllowAdditionalColumn(Boolean.parseBoolean(childElement.getElementsByTagName("AllowAdditionalColumns").item(0).getTextContent()));
		}
    	if (childElement.getElementsByTagName("InputTranscodingMeasures").item(0) != null) {
            templateData.setInputTranscodingMeasures(Boolean.parseBoolean(childElement.getElementsByTagName("InputTranscodingMeasures").item(0).getTextContent()));
        }
    	if (childElement.getElementsByTagName("HasDoubleQuotes").item(0) != null) {
            templateData.setHasDoubleQuotes(Boolean.parseBoolean(childElement.getElementsByTagName("HasDoubleQuotes").item(0).getTextContent()));
        }
        if (childElement.getElementsByTagName("CsvInputHeaderRow").item(0) != null) {
    		CsvInputColumnHeader csvInputColumnHeader = CsvInputColumnHeader.forTehnicalValue(childElement.getElementsByTagName("CsvInputHeaderRow").item(0).getTextContent());
    		templateData.setCsvInputColumnHeader(csvInputColumnHeader);	        	
        }
        if (childElement.getElementsByTagName("DateFormat").item(0) != null) {
        	templateData.setCsvInputDateFormat(childElement.getElementsByTagName("DateFormat").item(0).getTextContent());
        }
        if (childElement.getElementsByTagName("MappingInfo").item(0) != null) {
        	if (childElement.getElementsByTagName("MappingInfo").item(0).getTextContent().equals(TemplateData.MAP_CROSSX_MEASURES)) {
       		 templateData.setCsvInputMapCrossSectional(true);
       	    } 
        }	                   
        if (childElement.getElementsByTagName("Delimiter").item(0) != null) {
            templateData.setCsvInputDelimiter(childElement.getElementsByTagName("Delimiter").item(0).getTextContent());
        }
		if (childElement.getElementsByTagName("SubFieldSeparator").item(0) != null) {
			templateData.setFieldSeparationCharacter(childElement.getElementsByTagName("SubFieldSeparator").item(0).getTextContent());
		}
        readCsvInputMappingFromXml(templateData, childElement);
        readTranscodingFromXml(templateData, childElement, true);        
    }

    private void readInputOutputFromTemplateXml(TemplateData templateData, Element childElement){
        Set<String> filepaths = null;
        if(templateData.getInputFilePaths()==null) {
            filepaths = new HashSet<String>();
        }
        else{
            filepaths = templateData.getInputFilePaths();
        }
    	filepaths.add(childElement.getElementsByTagName("InputFileName").item(0).getTextContent());
        templateData.setInputFilePaths(filepaths);
        templateData.setInputFormat(Formats.forDescription((((Element)childElement.getElementsByTagName("InputFileName").item(0).getChildNodes()).getAttribute("format"))));
        if (childElement.getElementsByTagName("OutputFileName").item(0) != null) {
        	templateData.setOutputFilePath(childElement.getElementsByTagName("OutputFileName").item(0).getTextContent());
        	templateData.setOutputFormat(Formats.forDescription(((Element)childElement.getElementsByTagName("OutputFileName").item(0).getChildNodes()).getAttribute("format")));
        }
    }

    private void readDsdFromTemplateXml(TemplateData templateData, Element childElement){
        NodeList dsdNodeList = childElement.getElementsByTagName("DSD");
        if(dsdNodeList.item(0).getTextContent() != null && !dsdNodeList.item(0).getTextContent().isEmpty()){
            templateData.setStructureFilePath(dsdNodeList.item(0).getTextContent());
            templateData.setStructureInRegistry(false);
        }else{
            templateData.setStructureInRegistry(true);
        }
        Element childrenOfDsdElement = (Element)dsdNodeList.item(0).getChildNodes();
        templateData.setStructureAgency(childrenOfDsdElement.getAttribute("agency"));
        templateData.setStructureId(childrenOfDsdElement.getAttribute("id"));
        templateData.setStructureVersion(childrenOfDsdElement.getAttribute("version"));
    }

    private void readDataflowFromTemplateXml(TemplateData templateData, Element childElement){
        NodeList dataflowNodeList = childElement.getElementsByTagName("Dataflow");
        if(dataflowNodeList.item(0).getTextContent() != null && !dataflowNodeList.item(0).getTextContent().isEmpty()){
            templateData.setStructureInRegistry(false);
            templateData.setStructureFilePath(dataflowNodeList.item(0).getTextContent());
        }else{
            templateData.setStructureInRegistry(true);
        }
        templateData.setStructureDataflow(true);

        Element childrenOfDataflowElement = (Element)childElement.getElementsByTagName("Dataflow").item(0).getChildNodes();
        templateData.setStructureAgency(childrenOfDataflowElement.getAttribute("agency"));
        templateData.setStructureId(childrenOfDataflowElement.getAttribute("id"));
        templateData.setStructureVersion(childrenOfDataflowElement.getAttribute("version"));
    }

    private void readOtherParametersFromOldTemplateXml(TemplateData templateData, Element childElement){
        templateData.setGesmesTechnique(childElement.getElementsByTagName("GesmesTsTechnique").item(0).getTextContent());
        if (Boolean.parseBoolean(childElement.getElementsByTagName("SDMXValidation").item(0).getTextContent())) {
        	templateData.setOperationType(Operation.VALIDATION);
        } else {
        	templateData.setOperationType(Operation.CONVERSION);
        }        
    }

    private void readNamespaceFromOldTemplateXml(TemplateData templateData, Element childElement){
        templateData.setUseDefaultNamespace(false);
        templateData.setNamespaceUri(childElement.getElementsByTagName("URN").item(0).getTextContent());
        templateData.setNamespacePrefix(childElement.getElementsByTagName("Prefix").item(0).getTextContent());
    }

    private void readCsvParameterFromOldTemplateXml(TemplateData templateData, Element childElement) throws InvalidColumnMappingException {
        if (childElement.getElementsByTagName("HeaderInformation").item(0) != null) {
            String input = childElement.getElementsByTagName("HeaderInformation").item(0).getTextContent();
            templateData.setHeader(buildHeaderProperties(input));
        }
        if (childElement.getElementsByTagName("WriteHeader").item(0) != null) {
            templateData.setWriteHeader(Boolean.parseBoolean(childElement.getElementsByTagName("WriteHeader").item(0).getTextContent()));
        }
        // in the old template we have a single set of csv data
        // so must check if the input is csv and set the input parameters 
    	if (FormatFamily.CSV == templateData.getInputFormat().getFamily()) {
	        if (childElement.getElementsByTagName("InputOrdered").item(0) != null) {
	            templateData.setCsvInputOrdered(Boolean.parseBoolean(childElement.getElementsByTagName("InputOrdered").item(0).getTextContent()));
	        }
			if (childElement.getElementsByTagName("AllowAdditionalColumns").item(0) != null) {
				templateData.setAllowAdditionalColumn(Boolean.parseBoolean(childElement.getElementsByTagName("AllowAdditionalColumns").item(0).getTextContent()));
			}
	        if (childElement.getElementsByTagName("UnescapeCSVInputFields").item(0) != null) {
	            templateData.setHasDoubleQuotes(Boolean.parseBoolean(childElement.getElementsByTagName("UnescapeCSVInputFields").item(0).getTextContent()));
	        }
	        if (childElement.getElementsByTagName("OutputDateFormat").item(0) != null) {
	            templateData.setCsvInputDateFormat(childElement.getElementsByTagName("OutputDateFormat").item(0).getTextContent());
	        }
	        if (childElement.getElementsByTagName("WriteHeader").item(0) != null) {
	            templateData.setWriteHeader(Boolean.parseBoolean(childElement.getElementsByTagName("WriteHeader").item(0).getTextContent()));
	        }
	        if (childElement.getElementsByTagName("HeaderRow").item(0) != null) {
	        		CsvInputColumnHeader csvInputColumnHeader = CsvInputColumnHeader.forTehnicalValue(childElement.getElementsByTagName("HeaderRow").item(0).getTextContent());
	        		templateData.setCsvInputColumnHeader(csvInputColumnHeader);	        	
	        }
	        if (childElement.getElementsByTagName("MultiLevelCSV").item(0) != null) {
	            templateData.setCsvInputLevels(Integer.parseInt(childElement.getElementsByTagName("MultiLevelCSV").item(0).getTextContent()));
	        } else {
	            templateData.setCsvInputLevels(1);
	        }
	        if (childElement.getElementsByTagName("MappingInfo").item(0) != null) {
	            templateData.setCsvInputMapCrossSectional(Boolean.parseBoolean(childElement.getElementsByTagName("MappingInfo").item(0).getTextContent()));
	        }	                   
	        if (childElement.getElementsByTagName("CSVDelimiter").item(0) != null) {
	            templateData.setCsvInputDelimiter(childElement.getElementsByTagName("CSVDelimiter").item(0).getTextContent());
	        }
			if (childElement.getElementsByTagName("WriteLabel").item(0) != null) {
				templateData.setWriteLabels(CsvLabel.valueOf(childElement.getElementsByTagName("WriteLabel").item(0).getTextContent()));
			}
	        readCsvInputMappingFromXml(templateData, childElement);
	        readTranscodingFromXml(templateData, childElement, true);
    	} else {
    		// so must check if the output is csv and set the output parameters 
    		if (FormatFamily.CSV == templateData.getOutputFormat().getFamily()) {
    	        if (childElement.getElementsByTagName("EscapeCSVOutputFields").item(0) != null) {
    	            templateData.setUseDoubleQuotes(Boolean.parseBoolean(childElement.getElementsByTagName("EscapeCSVOutputFields").item(0).getTextContent()));
    	        }
    	        if (childElement.getElementsByTagName("OutputDateFormat").item(0) != null) {
    	            templateData.setCsvOutputDateFormat(childElement.getElementsByTagName("OutputDateFormat").item(0).getTextContent());
    	        }
    	        if (childElement.getElementsByTagName("HeaderRow").item(0) != null) {
	        		CsvOutputColumnHeader csvOutputColumnHeader = CsvOutputColumnHeader.forTehnicalValue(childElement.getElementsByTagName("HeaderRow").item(0).getTextContent());
	        		templateData.setCsvOutputColumnHeader(csvOutputColumnHeader);       	
	            }
    	        if (childElement.getElementsByTagName("MultiLevelCSV").item(0) != null) {
    	            templateData.setCsvOutputLevels(Integer.parseInt(childElement.getElementsByTagName("MultiLevelCSV").item(0).getTextContent()));
    	        } else {
    	            templateData.setCsvOutputLevels(1);
    	        }
    	        if (childElement.getElementsByTagName("MappingInfo").item(0) != null) {
    	            templateData.setCsvOutputMapCrossSectional(Boolean.parseBoolean(childElement.getElementsByTagName("MappingInfo").item(0).getTextContent()));
    	        }
    	        if (childElement.getElementsByTagName("CSVDelimiter").item(0) != null) {
    	            templateData.setCsvOutputDelimiter(childElement.getElementsByTagName("CSVDelimiter").item(0).getTextContent());
    	        }
				if (childElement.getElementsByTagName("WriteLabel").item(0) != null) {
					templateData.setWriteLabels(CsvLabel.valueOf(childElement.getElementsByTagName("WriteLabel").item(0).getTextContent()));
				}
    	        readCsvOutputMappingFromXml(templateData, childElement);
    	        readTranscodingFromXml(templateData, childElement, false);
    		}
    	}
    }

    private void readCsvInputMappingFromXml(TemplateData templateData, Element childElement) throws InvalidColumnMappingException {
    	if(templateData.getInputFormat().getFamily() == FormatFamily.CSV ) {
	        Map<String, CsvInColumnMapping> csvInputMapping = new HashMap<>();
	        if (childElement.getElementsByTagName("Mapping").item(0) != null
					&& childElement.getElementsByTagName("Mapping").item(0).hasChildNodes()) {
	            NodeList listOfMappingConcepts = childElement.getElementsByTagName("Mapping").item(0).getChildNodes();
	            int csvLevels = templateData.getCsvInputLevels();
	            csvInputMapping = csvService.buildInputDimensionLevelMapping(listOfMappingConcepts);
	        }
	        templateData.setCsvInputMapping(csvInputMapping);
    	}
    	if(templateData.getInputFormat().getFamily() == FormatFamily.FLR ) {
	        Map<String, FlrInColumnMapping> flrInputMapping = new HashMap<>();
	        if (childElement.getElementsByTagName("Mapping").item(0) != null
					&& childElement.getElementsByTagName("Mapping").item(0).hasChildNodes()) {
	            NodeList listOfMappingConcepts = childElement.getElementsByTagName("Mapping").item(0).getChildNodes();
	            flrInputMapping = flrService.buildInputDimensionLevelMapping(listOfMappingConcepts);
	        }
	        templateData.setFlrInputMapping(flrInputMapping);
    	}
    }

    private void readCsvOutputMappingFromXml(TemplateData templateData, Element childElement){
        MultiLevelCsvOutColMapping multiLevelCsvOutColMapping = new MultiLevelCsvOutColMapping();
        if (childElement.getElementsByTagName("Mapping").item(0) != null && childElement.getElementsByTagName("Mapping").item(0).hasChildNodes()) {
            NodeList listOfMappingConcepts = childElement.getElementsByTagName("Mapping").item(0).getChildNodes();
            for (int j =0; j<listOfMappingConcepts.getLength();j++) {
                if (childElement.getElementsByTagName("Concept").item(j) != null) {
                    Element conceptElement = (Element)childElement.getElementsByTagName("Concept").item(j);
                    String name = conceptElement.getAttribute("name");
                    String value = conceptElement.getAttribute("value");
                    String isFixed = conceptElement.getAttribute("fixed");
                    String level = conceptElement.getAttribute("level");
                    Boolean fixedComponentValue = Boolean.valueOf(isFixed);
                    if (!fixedComponentValue) {
                        Integer levelAsInteger = Integer.valueOf(1);//default value should be one
                        if (level != null && !level.isEmpty()) {
                            levelAsInteger = Integer.valueOf(level);
                        }
                        multiLevelCsvOutColMapping.addMapping(levelAsInteger, Integer.valueOf(value), name);
                    }
                }
            }
        }
        templateData.setCsvOutputMapping(multiLevelCsvOutColMapping);
    }

    private void readTranscodingFromXml(TemplateData templateData, Element childElement, boolean isCsvInput){
		LinkedHashMap<String, LinkedHashMap<String, String>> transcoding = new LinkedHashMap<>();
        String codelistMapId;
        if (childElement.getElementsByTagName("Transcoding").item(0) != null
                && childElement.getElementsByTagName("StructureSet").item(0)!=null) {
            NodeList listOfStructureSet = childElement.getElementsByTagName("StructureSet").item(0).getChildNodes();
            for (int j =0; j < listOfStructureSet.getLength(); j++) {
                if (childElement.getElementsByTagName("CodelistMap").item(j) != null) {
                    codelistMapId = ((Element)childElement.getElementsByTagName("CodelistMap").item(j).getChildNodes()).getAttribute("id");
                    NodeList listOFCodeListMapping = childElement.getElementsByTagName("CodelistMap").item(j).getChildNodes();
					LinkedHashMap<String, String> codeMap = new LinkedHashMap<>();
                    for (int k =0; k < listOFCodeListMapping.getLength(); k++) {
                        if (listOFCodeListMapping.item(k).getNodeType() == Node.ELEMENT_NODE) {
                            Element codeMapElement = (Element) listOFCodeListMapping.item(k);
                            if (codeMapElement.getNodeName().contains("CodeMap")) {
                                if (codeMapElement.getElementsByTagName("MapCodeRef").item(0) != null && codeMapElement.getElementsByTagName("MapTargetCodeRef").item(0) != null) {
                                    codeMap.put(
                                            codeMapElement.getElementsByTagName("MapTargetCodeRef").item(0).getTextContent(),
                                            codeMapElement.getElementsByTagName("MapCodeRef").item(0).getTextContent());
                                }
                            }
                        }
                    }
                    transcoding.put(codelistMapId, codeMap);
                }
            }
        }
        if (isCsvInput) {
          templateData.setCsvInputStructureSet(transcoding);
        } else {
          templateData.setCsvOutputStructureSet(transcoding);
        }  
    }
		
	public void validateTemplate(InputStream templateFile) throws InvalidTemplateException{
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setNamespaceAware(true);    //add this for namespace problems
            DocumentBuilder parser = dbf.newDocumentBuilder();
            Document document = parser.parse(templateFile);
            document.getDocumentElement().normalize();
		    Element root = document.getDocumentElement();
		    
		    Source schemaFile = null;
            if ("Conversion_Template".equals(root.getNodeName())) {
            	// load a WXS schema, represented by a Schema instance
                schemaFile = new StreamSource(IoUtils.getInputStreamFromClassPath("Template.xsd")); 
            } else {
            	schemaFile = new StreamSource(IoUtils.getInputStreamFromClassPath("Template_v6.0.xsd")); 
            }
            // create a SchemaFactory capable of understanding WXS schemas
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = factory.newSchema(schemaFile);
            // create a Validator instance, which can be used to ensureTemplateCanBeWritten an instance document
            javax.xml.validation.Validator validator = schema.newValidator();
            // ensureTemplateCanBeWritten the DOM tree
            validator.validate(new DOMSource(document));
        } catch(IOException ioExc){
            throw new InvalidTemplateException("Exception reading the template file", ioExc);
		} catch (ParserConfigurationException parserExc) {
			throw new InvalidTemplateException("Parser exception", parserExc);
		} catch (SAXException saxException){
		    throw new InvalidTemplateException("Invalid template", saxException);
        }
    }
	
	private Properties buildHeaderProperties(String input) {
		Properties properties = new Properties();
		if (input!=null && !input.isEmpty()) {
			String deleteFirstDataRow = input.substring(input.indexOf("header"));
			String propertiesFromString = deleteFirstDataRow.replaceAll("&#xD;", ";");
			try {
				properties.load(new StringReader(propertiesFromString.replaceAll(";", "\n")));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return properties;
	}	
}

