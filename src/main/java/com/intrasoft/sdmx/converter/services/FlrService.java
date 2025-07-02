package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidColumnMappingException;
import com.intrasoft.sdmx.converter.services.exceptions.WriteMappingException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.util.IOUtils;
import org.estat.sdmxsource.util.csv.FixedWidth;
import org.estat.sdmxsource.util.csv.FlrInColumnMapping;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.MeasureBean;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.*;

@Service
public class FlrService {

	private static final Logger logger = LogManager.getLogger(CsvService.class);
	
	/**
     * the default column header
     */
    public final static String DEFAULT_COLUMN_HEADER_PREFIX = "Column ";

	/**
	 * Method that parses a mapping.xml file and returns a map of dimensions-values
	 *
	 * @param mappingIs Input stream
	 * @return Map<String, FlrInColumnMapping> result
	 * @throws InvalidColumnMappingException
	 */
	public LinkedHashMap<String, FlrInColumnMapping> buildInputDimensionLevelMapping (final InputStream mappingIs,
																					  final DataStructureBean dataStructureBean,
																					  final boolean isSdmx30) throws InvalidColumnMappingException, ArrayIndexOutOfBoundsException {
		LinkedHashMap<String, FlrInColumnMapping> result;
		if(isSdmx30) {
			result = buildInputDimensionLevelMapping(mappingIs, dataStructureBean);
		} else {
			result = buildInputDimensionLevelMapping(mappingIs);
		}
		return result;
	}

	public LinkedHashMap<String, FlrInColumnMapping> buildInputDimensionLevelMapping (final InputStream mappingIs, DataStructureBean dataStructureBean) throws InvalidColumnMappingException, ArrayIndexOutOfBoundsException {
		LinkedHashMap<String, FlrInColumnMapping> result;
		try{
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(mappingIs);
			//normalize text representation
			doc.getDocumentElement().normalize();
			NodeList listOfConcepts = doc.getDocumentElement().getElementsByTagName("Concept");
			result = buildInputDimensionLevelMapping(listOfConcepts);
			if(ObjectUtil.validMap(result)) {
				for (Map.Entry<String, FlrInColumnMapping> entry : result.entrySet()) {
					if(ObjectUtil.validObject(entry)) {
						ComponentBean componentBean = dataStructureBean.getComponent(entry.getKey());
						//Create the position Fixed Width
						if (componentBean instanceof AttributeBean || componentBean instanceof MeasureBean) {
							if(ObjectUtil.validObject(entry.getValue())) {
								entry.getValue().setMinOccurrences(componentBean.getRepresentationMinOccurs());
								entry.getValue().setMaxOccurrences(componentBean.getRepresentationMaxOccurs().isUnbounded() ? Integer.MAX_VALUE : componentBean.getRepresentationMaxOccurs().getOccurrences());
							}
						}
					}
				}
			}
		} catch (ParserConfigurationException e) {
			throw new InvalidColumnMappingException("Invalid configuration mapping file: "+e.getMessage(), e);
		} catch (SAXException e) {
			throw new InvalidColumnMappingException("Invalid parsing the mapping file: "+e.getMessage(), e);
		}catch(IOException e){
			throw new InvalidColumnMappingException("Invalid mapping file: "+e.getMessage(), e);
		}
		return result;
	}
	
	/**
	 * Method that parses a mapping.xml file and returns a map of dimensions-values
	 * 
	 * @param mappingIs Input stream
	 * @return Map<String, FlrInColumnMapping> result
	 * @throws InvalidColumnMappingException
	 */
	public LinkedHashMap<String, FlrInColumnMapping> buildInputDimensionLevelMapping (final InputStream mappingIs) throws InvalidColumnMappingException, ArrayIndexOutOfBoundsException {
		LinkedHashMap<String, FlrInColumnMapping> result;
		try{
		    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(mappingIs);            
            //normalize text representation
            doc.getDocumentElement().normalize();          
            NodeList listOfConcepts = doc.getDocumentElement().getElementsByTagName("Concept");
            result = buildInputDimensionLevelMapping(listOfConcepts);
		} catch (ParserConfigurationException e) {
		    throw new InvalidColumnMappingException("Invalid configuration mapping file: "+e.getMessage(), e);
        } catch (SAXException e) {
            throw new InvalidColumnMappingException("Invalid parsing the mapping file: "+e.getMessage(), e);            
        }catch(IOException e){
            throw new InvalidColumnMappingException("Invalid mapping file: "+e.getMessage(), e);
        }
		return result;		
	}
	
	public LinkedHashMap<String, FlrInColumnMapping> buildInputDimensionLevelMapping (final NodeList conceptsNodeList) throws ArrayIndexOutOfBoundsException, InvalidColumnMappingException {
		LinkedHashMap<String, FlrInColumnMapping> result = new LinkedHashMap<String, FlrInColumnMapping>();
		 for (int temp = 0; temp < conceptsNodeList.getLength(); temp++) {
	            Node firstConceptsNode = conceptsNodeList.item(temp);
	            if (firstConceptsNode.getNodeType() == Node.ELEMENT_NODE) {
	                Element conceptElement = (Element)firstConceptsNode;
	                String name = conceptElement.getAttribute("name");
	                FlrInColumnMapping flrInColumnMapping = buildInputDimensionLevelMapping(conceptElement);
	                result.put(name, flrInColumnMapping);
	            }
	        }
	     return result;
	}
	
	private FlrInColumnMapping buildInputDimensionLevelMapping(Element conceptElement) throws ArrayIndexOutOfBoundsException, InvalidColumnMappingException {
		FlrInColumnMapping result = new FlrInColumnMapping();
		String name = conceptElement.getAttribute("name");
	    String columnFixedValue = conceptElement.getAttribute("value");
	    String isFixed = conceptElement.getAttribute("fixed");
	    String level = conceptElement.getAttribute("level");
	    List<FixedWidth> columnLength = new ArrayList<FixedWidth>();
	    result.setFixed(Boolean.parseBoolean(isFixed));
	    if (level == null || level.isEmpty()) {
            result.setLevel(1);
        } else {
            result.setLevel(Integer.valueOf(level));
        }
	    //case fixed
	    if (result.isFixed()) {
            if (columnFixedValue == null) {
                result.setFixedValue("");
            } else {
                result.setFixedValue(columnFixedValue);
            }
            result.setPositions(null);
	    }
	    //case auto, or position
	    else{
	    	if(columnFixedValue != null && !columnFixedValue.isEmpty()) {
	    		//SDMXCONV-775
	    		if(columnFixedValue.equalsIgnoreCase("AUTO")) {
					result.setFixedValue("AUTO");
					result.setPositions(null);
				} else {
	    			//more than one rows
					if (columnFixedValue.contains("+")) {
						String[] positions = columnFixedValue.split("\\+");
						for (int i = 0; i < positions.length; i++) {
							columnLength.add(getPositionsFromMappingValue(positions[i], name));
						}
					}
					//single row
					else {
						columnLength.add(getPositionsFromMappingValue(columnFixedValue, name));
					}
				}
	    	}
	    }  
	    result.setPositions(columnLength);
		return result;
	}
	
	private FixedWidth getPositionsFromMappingValue(String value, String concept) throws ArrayIndexOutOfBoundsException, InvalidColumnMappingException {
		if(ObjectUtil.validString(value) && !value.contains("-")) {
			throw new InvalidColumnMappingException("Invalid mapping for FLR format.");
		}
		FixedWidth result = new FixedWidth();
		String [] positions = value.split("\\-");
		result.setConceptName(concept);
		result.setStart(Integer.parseInt(positions[0]));
		result.setEnd(Integer.parseInt(positions[1]));
		return result;
	}
	
	/**
	 * 
	 * @param mappingFile
	 * @return
	 * @throws InvalidColumnMappingException
	 */
	public LinkedHashMap<String, FlrInColumnMapping> buildInputDimensionLevelMapping (final File mappingFile)
	throws InvalidColumnMappingException{
		LinkedHashMap<String, FlrInColumnMapping> result = null;
		InputStream inputStream = null; 
		try {
		    inputStream = new FileInputStream(mappingFile); 
			result = buildInputDimensionLevelMapping(inputStream);
		} catch (FileNotFoundException e) {
			throw new InvalidColumnMappingException("The mapping file could not be found: ", e); 
		} finally{
		    IOUtils.closeQuietly(inputStream);
		}
		return result; 
	}	
	
	/**
	 * creates a default mapping for the given components like: dimension 1 ->
	 * column 1 dimension 2 -> column 2 ...
	 * We don't even know how many columns so we create as many as the components are
	 * @return
	 */
	public LinkedHashMap<String, FlrInColumnMapping> initInputColumnMapping(List<String> componentsToMap, DataStructureBean dsd, boolean isSdmx30) {
		LinkedHashMap<String, FlrInColumnMapping> result = new LinkedHashMap<>();
		result.clear();
		if(isSdmx30) {
			result = initInputComplexColumnMapping(componentsToMap, dsd);
		} else {
			result = initInputColumnMapping(componentsToMap);
		}
		return result;
	}

	public LinkedHashMap<String, FlrInColumnMapping> initInputComplexColumnMapping(List<String> componentsToMap, DataStructureBean dsd) {
		//System.out.println("componentToMap: "+componentsToMap);
		LinkedHashMap<String, FlrInColumnMapping> result = new LinkedHashMap<>();
		result.clear();
		int i=1;
		for (String component : componentsToMap) {
			List<FixedWidth> positions = new ArrayList<FixedWidth>();
			FixedWidth position = new FixedWidth();
			position.setConceptName(component);
			position.setStart(i);
			position.setEnd(i);
			positions.add(position);
			FlrInColumnMapping mapping = new FlrInColumnMapping();
			mapping.setFixed(false);
			mapping.setLevel(NumberUtils.INTEGER_ONE);
			componentsToMap.indexOf(component);
			ComponentBean componentBean = dsd.getComponent(component);
			//Create the position Fixed Width
			if(componentBean instanceof AttributeBean || componentBean instanceof MeasureBean) {
				mapping.setMinOccurrences(componentBean.getRepresentationMinOccurs());
				mapping.setMaxOccurrences(componentBean.getRepresentationMaxOccurs().isUnbounded() ? Integer.MAX_VALUE : componentBean.getRepresentationMaxOccurs().getOccurrences());
			}
			mapping.setPositions(positions);
			result.put(component, mapping);
			i++;
		}
		return result;
	}

	public LinkedHashMap<String, FlrInColumnMapping> initInputColumnMapping(List<String> componentsToMap) {
		//System.out.println("componentToMap: "+componentsToMap);
		LinkedHashMap<String, FlrInColumnMapping> result = new LinkedHashMap<>();
		result.clear();
		int i=1;
		for (String component : componentsToMap) {
			List<FixedWidth> positions = new ArrayList<FixedWidth>();
			FixedWidth position = new FixedWidth();
			FlrInColumnMapping mapping = new FlrInColumnMapping();
			mapping.setFixed(false);
			mapping.setLevel(NumberUtils.INTEGER_ONE);
			componentsToMap.indexOf(component);
			position.setConceptName(component);
			position.setStart(i);
			position.setEnd(i);
			positions.add(position);
			mapping.setPositions(positions);
			result.put(component, mapping);
			i++;
		}
		return result;
	}
	
	/**
	 * Method that saves Flr mapping to an xml File.
	 * The output of this method is the xml File.
	 * @param mappings Map<String, FlrInColumnMapping>. The object that holds the mapping info.
	 * @param output File. The File in which we will create the XML DOM.
	 */
	public void saveInMapping(LinkedHashMap<String, FlrInColumnMapping> mappings, File output) throws WriteMappingException {
		try {
			FileOutputStream outputStream = new FileOutputStream(output);
			saveInMapping(mappings, outputStream);
		} catch (FileNotFoundException e) {
			throw new WriteMappingException("Mapping file not found!", e);
		}
	}

	public void saveInMapping(LinkedHashMap<String, FlrInColumnMapping> mappings, OutputStream outputFile) throws WriteMappingException {
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			Document xmlDocument = docBuilder.newDocument();
			xmlDocument.setXmlStandalone(false);
			xmlDocument.setXmlVersion("1.0");
			
			//Create the root element for xml
			Element root = xmlDocument.createElement("Mapping");
			xmlDocument.appendChild(root);

			for (Map.Entry<String, FlrInColumnMapping> entry : mappings.entrySet()) {
				//Every mapping value is a concept tag in xml
				Element concept = xmlDocument.createElement("Concept");
				root.appendChild(concept);
				//Name Attribute in the concept tag
				concept.setAttribute("name", entry.getKey());
				if (!entry.getValue().isFixed()) {
					FlrInColumnMapping myMapping = entry.getValue();
					//case auto
					if(myMapping!=null && myMapping.getFixedValue()!=null && myMapping.getFixedValue().equalsIgnoreCase("auto")){
						concept.setAttribute("value", entry.getValue().getFixedValue());
					}
					//case positions
					else if(myMapping!=null && myMapping.getPositions()!=null && myMapping.getPositions().size()>0){
                        List<FixedWidth> positions = entry.getValue().getPositions();
                        String colsMap = "";
                        int cntr = 0;
                        for (FixedWidth position : positions) {
                            cntr++;
                            colsMap += Integer.toString(position.getStart());
                            colsMap += "-";
                            colsMap += Integer.toString(position.getEnd());
                            if (cntr != positions.size()) {
                                colsMap += "+";
                            }
                        }
                        concept.setAttribute("value", colsMap);
					} else if(myMapping!=null && myMapping.getPositions()!=null && myMapping.getPositions().size()==0){
                        concept.setAttribute("value", "");
                    }
				} else {
					concept.setAttribute("value", entry.getValue().getFixedValue());
				}
				concept.setAttribute("fixed", Boolean.toString(entry.getValue().isFixed()));
				concept.setAttribute("level", entry.getValue().getLevel().toString());
			}
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			DOMSource source = new DOMSource(xmlDocument);
			StreamResult result = new StreamResult(outputFile);
			transformer.transform(source, result);
		} catch (Exception ex) {
			throw new WriteMappingException("Error while writing the mapping File!", ex);
		}
	}

	/**
	 * method for deciding whether auto mapping is enabled, or not
	 * @param flrMappings
	 * @return
	 */
	public boolean isAuto(LinkedHashMap<String, FlrInColumnMapping> flrMappings){
		if(flrMappings!=null) {
			for (Map.Entry<String, FlrInColumnMapping> entry : flrMappings.entrySet()) {
				String key = entry.getKey();
				FlrInColumnMapping value = entry.getValue();
				// do stuff
				if (!value.isFixed() && (value.getFixedValue() != null) && value.getFixedValue().equalsIgnoreCase("AUTO")) {
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Reads only the first line of the input and 
	 * computes the value for the given component.
	 * Used as a preview only.
	 * @param input
	 * @param mapping
	 * @param component
	 * @return value
	 * @throws InvalidColumnMappingException 
	 */
	public String previewValue(File input, Map<String, FlrInColumnMapping> mapping, String component) throws InvalidColumnMappingException{
		String result = "";
		try(BufferedReader reader = new BufferedReader(new FileReader(input))) {	
			String text = reader.readLine();
			FlrInColumnMapping componentMap = mapping.get(component);
			
			if(componentMap!=null) {
				if(componentMap.isFixed()) {
					result=componentMap.getFixedValue();
				}else {
					List<FixedWidth> widths = componentMap.getPositions();
					if(componentMap.getPositions()!=null) {
						for(FixedWidth fixedWidth:widths) {
							if(fixedWidth!=null) {
                                //SDMXCONV-962
								if(text.length()>fixedWidth.getStart()-1 && text.length()>fixedWidth.getEnd())
                                    result +=  text.substring(fixedWidth.getStart()-1, fixedWidth.getEnd());
                                else
                                    break;
							}
						}
					}
				}
			}
		} catch (FileNotFoundException fnfException) {
			throw new RuntimeException("Cannot find Flr file " + fnfException.getMessage(), fnfException);
		} catch (IOException e) {
			throw new RuntimeException("Cannot find Flr file " + e.getMessage(), e);
		} catch (StringIndexOutOfBoundsException stringEx) {
			throw new InvalidColumnMappingException("Invalid configuration mapping file, position out of range: "+ stringEx.getMessage(), stringEx);
		}
		return result;
	}
	/**
	 * Takes the Flr mapping as an argument
	 * and sorts it based on the minWidth of the Positions inside it
	 * @param map
	 * @return sorted map
	 */
	private LinkedHashMap<String, FlrInColumnMapping> sortByValues(Map map) {
		List list = new LinkedList(map.entrySet());
		final int[] i = {0};
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				i[0]++;
				return ((Comparable) minStartWidth((FlrInColumnMapping) ((Map.Entry) (o1)).getValue(), i[0]) )
						.compareTo( minStartWidth((FlrInColumnMapping) ((Map.Entry) (o2)).getValue(), i[0]) );
			}
		});

		// Here I am copying the sorted list in HashMap
		// using LinkedHashMap to preserve the insertion order
		LinkedHashMap sortedHashMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		return sortedHashMap;
	}

	public LinkedHashMap<String, FlrInColumnMapping> sortMapAscWidths(Map<String, FlrInColumnMapping> mapping) {
        return sortByValues(mapping);
	}

	/**
	 * Takes the map for one element
	 * and finds the minimum Start position
	 * @param map
	 * @return minimum Start width of the mapping element
	 */
	private int minStartWidth(FlrInColumnMapping map, int i) {
		int minWidth = 1000 +i;

		if(!map.isFixed() && !"AUTO".equalsIgnoreCase(map.getFixedValue()) && map.getPositions()!=null && !map.getPositions().isEmpty()) {
			for(FixedWidth width:map.getPositions()) {
				if(width.getStart()<minWidth) {
					minWidth=width.getStart();
				}
			}
		}
		return minWidth;
	}
}
