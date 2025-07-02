package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.sdmx.structureparser.SdmxBeansSchemaDecorator;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.exception.SdmxNotImplementedException;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.exception.SdmxSyntaxException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.beans.mapping.*;
import org.sdmxsource.sdmx.api.model.mutable.base.StructureMapMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.base.TextTypeWrapperMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.mapping.*;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.base.TextTypeWrapperMutableBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.mapping.*;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;
import org.sdmxsource.sdmx.util.sdmx.SdmxMessageUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.*;

@Service
public class StructureService {

	private static Logger logger = LogManager.getLogger(StructureService.class);
    
    @Autowired
    private StructureParsingManager structureParsingManager;
    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;

    /**
     * Method that parses the provided dsd file (contains a structure) and creates its memory representation.
     * @param structureInputStream the stream with the structures
     * @return the populated structure bean
     * @throws InvalidStructureException   when exceptions occur during the structure parsing process
     */
    public SdmxBeans readStructuresFromStream(InputStream structureInputStream) throws InvalidStructureException, IOException {
		SdmxBeansSchemaDecorator beansSchemaDecorator;
        //SDMXCONV-1227
        try(BOMInputStream bomStructureInputStream = new BOMInputStream(structureInputStream);
			ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(bomStructureInputStream)) {

	        StructureWorkspace structureWorkspace = structureParsingManager.parseStructures(dataLocation);
			SdmxBeans result = structureWorkspace.getStructureBeans(false);
            //SDMXCONV-792
            SDMX_SCHEMA beansVersion = SdmxMessageUtil.getSchemaVersion(dataLocation);
			beansSchemaDecorator = new SdmxBeansSchemaDecorator(result, beansVersion);
        }catch(SdmxSyntaxException syntaxEx){
        	//SDMXCONV-729, 584
        	String errorMessage = "Error: Structure file cannot be read." + " Technical details: " + syntaxEx.getMessage();
        	throw new SdmxSyntaxException(errorMessage);	
        }catch(SdmxSemmanticException | SdmxNotImplementedException sdmxExc){
            throw sdmxExc;
        }catch(IllegalArgumentException illegalArgumentExc){
            //this is the exception thrown by SdmxSource when you submit a non structure file
            //this is not consistent with the other exceptions but I cannot do anything
            //so here comes the workaround
            if(illegalArgumentExc.getMessage().contains("StructureParsingManagerImpl can not parse document")){
                throw new InvalidStructureException("Structure format is either not supported, or has an invalid syntax", illegalArgumentExc);
            }else{
                //making sure we don't swallow exceptions if the exception is not what we were expecting
                throw illegalArgumentExc;
            }
        }
        return beansSchemaDecorator;
    }
    
    /**
     * @see #readStructuresFromFile(File)
     * 
     * @param filename
     * @return
     * @throws Exception
     */
    public SdmxBeans readStructuresFromFile(String filename) throws InvalidStructureException, IOException {
        return readStructuresFromFile(new File(filename));
    }
    
    /**
     * reads a the structure for the given file 
     * 
     * @param structureFile     the structure file
     * @return                  a SdmxBeans object
     * @throws InvalidStructureException
     */
    public SdmxBeans readStructuresFromFile(File structureFile) throws InvalidStructureException, IOException {
        SdmxBeans result = null; 
        FileInputStream structureStream = null; 
        try{
            structureStream = new FileInputStream(structureFile);
            result = readStructuresFromStream(structureStream);
        }catch(FileNotFoundException fnfExc){
            throw new InvalidStructureException(fnfExc);
        }finally{
            IOUtils.closeQuietly(structureStream);
        }
        return result; 
    }

    /**
     * returns the id of the data structures found in the provided the sdmx beans
     * @param sdmxBeans
     * @return
     */
    public Set<StructureIdentifier> getDataStructureStubs(SdmxBeans sdmxBeans){
    	Set<StructureIdentifier> result = new HashSet<StructureIdentifier>(); 
    	for(DataStructureBean dataStructure: sdmxBeans.getDataStructures()){
			result.add(new StructureIdentifier(dataStructure.getAgencyId(), dataStructure.getId(), dataStructure.getVersion()));
		}    	
    	return result; 
    }
    
    /**
     * returns the dataflow identifiers of the provided sdmxBeans
     * 
     * @param sdmxBeans the sdmx beans
     * @return  a set of dataflow identifiers
     */
    public Set<StructureIdentifier> getDataflowStubs(SdmxBeans sdmxBeans){
    	Set<StructureIdentifier> result = new HashSet<StructureIdentifier>(); 
    	for(DataflowBean dataflow: sdmxBeans.getDataflows()){
			result.add(new StructureIdentifier(dataflow.getAgencyId(), dataflow.getId(), dataflow.getVersion()));
		}    	
    	return result; 
    }
    
    /**
     * reads the dataflows ids from the given file
     * 
     * @param structureFile
     * @return
     */
    public Set<StructureIdentifier> readDataflowStubsFromFile(File structureFile) throws InvalidStructureException, IOException {
       Set<StructureIdentifier> result = new HashSet<StructureIdentifier>(); 
       SdmxBeans sdmxBeans = readStructuresFromFile(structureFile); 
       return getDataflowStubs(sdmxBeans); 
    }
    
    /**
     * reads the data structure ids for the given file
     * 
     * @param structureFile the file containing the structures
     * @return  a set with data structure identifiers
     */
    public Set<StructureIdentifier> readDataStructureStubsFromFile(File structureFile) throws InvalidStructureException, IOException {
    	Set<StructureIdentifier> result = new HashSet<StructureIdentifier>(); 
		SdmxBeans structureAsSdmxBeans = readStructuresFromFile(structureFile);
		result = getDataStructureStubs(structureAsSdmxBeans);
    	return result; 
    }

	/**
	 * reads the sdmx beans from the given input stream and returns the first data structure
	 *
	 * @param structureFilePath	the structure file path
	 * @return		the first data structure found in the file
	 * @throws InvalidStructureException if the structure file provided does not have a valid structure
	 */
	public DataStructureBean readFirstDataStructure(String structureFilePath) throws InvalidStructureException, IOException {
		return readFirstDataStructure(new File(structureFilePath));
	}

    /**
     * reads the sdmx beans from the given input stream and returns the first data structure
     * 
     * @param fis	the file input stream
     * @return		the first data structure found in the file
     * @throws InvalidStructureException if the structure file provided does not have a valid structure
     */
    public DataStructureBean readFirstDataStructure(FileInputStream fis) throws InvalidStructureException, IOException {
        SdmxBeans sdmxBeans = readStructuresFromStream(fis);
        return sdmxBeans.getDataStructures().iterator().next(); 
    }
    
    /**
     * @see #readFirstDataStructure(FileInputStream)
     * 
     * @param file		the structure file
     * @return			the first data structure found in the file
     * @throws InvalidStructureException if the structure file provided does not have a valid structure
     */
    public DataStructureBean readFirstDataStructure(File file) throws InvalidStructureException, IOException {
        DataStructureBean result = null; 
        FileInputStream inputStream = null; 
        try{
            inputStream  = new FileInputStream(file); 
            result = readFirstDataStructure(inputStream);
        }catch(FileNotFoundException fnfExc){
            throw new InvalidStructureException(fnfExc); 
        }finally{
            IOUtils.closeQuietly(inputStream); 
        }
        return result; 
    }
    
    /**
     * 
     * @param dataflow
     * @return
     * @throws UnsupportedEncodingException
     * @deprecated: stupid method (see the use of ByteArrayInputStream below)
     */
    public DataflowBean readDataflowFromString(String dataflow) throws IOException {
        DataflowBean result = null;
		try(InputStream responseIs = new ByteArrayInputStream(dataflow.getBytes("UTF8"));
				ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(responseIs)) {
			StructureWorkspace structureWorkspace = structureParsingManager.parseStructures(dataLocation);
			final SdmxBeans responseSdmxBeans = structureWorkspace.getStructureBeans(true);
			result = (DataflowBean) responseSdmxBeans.getDataflows().iterator().next();
		}
        return result;
    } 
        
    /**
	 * Use this method for parsing structure sets from transcoding files. 
	 * There is a bug in sdmxsources API in parsing the structure sets; 
	 * it parses only the first CodelistMap even if the structure set has more than one.
	 */
	public StructureSetMutableBean readStructureSet(final InputStream transcodingInputStream) throws Exception {		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		DocumentBuilder dBuilder = factory.newDocumentBuilder();				 
		org.w3c.dom.Document doc = dBuilder.parse(transcodingInputStream);			 
		StructureSetMutableBean structureSet = null;	 
		if (doc.hasChildNodes()) {			 					 					 					 
			NodeList structureSetNodes = doc.getElementsByTagNameNS("*","StructureSet");		
			structureSet = new StructureSetMutableBeanImpl();			 
			for (int count = 0; count < structureSetNodes.getLength(); count++) {
				Node structureSetNode = structureSetNodes.item(count);				 						 
				if (structureSetNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Element structureSetElement = (org.w3c.dom.Element) structureSetNode;	
					String agencyId = structureSetElement.getAttribute("agencyID");							
					if(agencyId != null && agencyId.length()>0) {
						structureSet.setAgencyId(agencyId);
					} else {
						structureSet.setAgencyId("emptyAgency");
					}					
					structureSet.setId(structureSetElement.getAttribute("id"));
					structureSet.setVersion(structureSetElement.getAttribute("version"));
					String name = structureSetElement.getElementsByTagNameNS("*","Name").item(0).getFirstChild().getNodeValue();						
					TextTypeWrapperMutableBean structureName = new TextTypeWrapperMutableBeanImpl("en", name);
					List<TextTypeWrapperMutableBean> names = new ArrayList<TextTypeWrapperMutableBean>();
					names.add(structureName);						
					structureSet.setNames(names);	
					//there are two trancoding files, one with CodelistMap element, other with StructureMap
					NodeList codelistMapNodes = structureSetElement.getElementsByTagNameNS("*","CodelistMap");	
					NodeList structureMapNodes = structureSetElement.getElementsByTagNameNS("*","StructureMap");
					// this is the old transcoding with CodelistMap
					//this transcoding accepts only coded components
					if (codelistMapNodes.getLength() > 0) {
							for(int i=0;i<codelistMapNodes.getLength();i++) {							
								CodelistMapMutableBean codelistBean = new CodelistMapMutableBeanImpl();		
								//set thiese fields only to pass validation when getImmutableInstance is called. They are not used anywhere
								codelistBean.addName("en", "empty");
								codelistBean.setSourceRef(new StructureReferenceBeanImpl("agencyId","maintainableId","1.0",SDMX_STRUCTURE_TYPE.CODE_LIST));
								codelistBean.setTargetRef(new StructureReferenceBeanImpl("agencyId","maintainableId","1.0",SDMX_STRUCTURE_TYPE.CODE_LIST));
								Node codelistNode = codelistMapNodes.item(i);							
								if (codelistNode.getNodeType() == Node.ELEMENT_NODE) {
									org.w3c.dom.Element codelistElement = (org.w3c.dom.Element) codelistNode;										
									codelistBean.setId(codelistElement.getAttribute("id"));								
									NodeList codeMapsNodes = codelistElement.getElementsByTagNameNS("*","CodeMap");
									for(int j=0;j<codeMapsNodes.getLength();j++) {									
										org.w3c.dom.Element mapElement = (org.w3c.dom.Element) codeMapsNodes.item(j);
										String mapCodeRef = mapElement.getElementsByTagNameNS("*","MapCodeRef").item(0).getFirstChild().getNodeValue();
										String targetCodeRef = mapElement.getElementsByTagNameNS("*","MapTargetCodeRef").item(0).getFirstChild().getNodeValue();									
										ItemMapMutableBean itemMapBean = new ItemMapMutableBeanImpl();
										itemMapBean.setSourceId(mapCodeRef);
										itemMapBean.setTargetId(targetCodeRef);									
										codelistBean.addItem(itemMapBean);									
									}																				
								}							
									structureSet.addCodelistMap(codelistBean);
								}
					// this is the new transcoding with StructureMap
					// this transcoding accepts non coded components as well, see SDMXCONV-721
					} else if (structureMapNodes.getLength() > 0) {
						for(int i=0;i<structureMapNodes.getLength();i++) {
							StructureMapMutableBean structureMapMutableBean = new StructureMapMutableBeanImpl();
							//set thiese fields only to pass validation when getImmutableInstance is called. They are not used anywhere
							structureMapMutableBean.addName("en", "empty");
							structureMapMutableBean.setSourceRef(new StructureReferenceBeanImpl("agencyId","maintainableId","1.0",SDMX_STRUCTURE_TYPE.DSD));
							structureMapMutableBean.setTargetRef(new StructureReferenceBeanImpl("agencyId","maintainableId","1.0",SDMX_STRUCTURE_TYPE.DSD));
							Node structureMapNode = structureMapNodes.item(i);
							if (structureMapNode.getNodeType() == Node.ELEMENT_NODE) {
								org.w3c.dom.Element  structureMapElement = (org.w3c.dom.Element) structureMapNode;										
								structureMapMutableBean.setId(structureMapElement.getAttribute("id"));
								NodeList componentMapNodes = structureMapElement.getElementsByTagNameNS("*","ComponentMap");
								List<ComponentMapMutableBean> listOfComponentMaps = new ArrayList<>();								
								for(int j=0;j<componentMapNodes.getLength();j++) {
									ComponentMapMutableBean componentMap = new ComponentMapMutableBeanImpl();
									org.w3c.dom.Element mapElement = (org.w3c.dom.Element) componentMapNodes.item(j);
									NodeList sourceNodes = mapElement.getElementsByTagNameNS("*","Source");
									for(int h=0;h<sourceNodes.getLength();h++) {									
										org.w3c.dom.Element sourceElement = (org.w3c.dom.Element) sourceNodes.item(h);
										String sourceCodeRef = sourceElement.getElementsByTagNameNS("*","Ref").item(0).getAttributes().getNamedItem("id").getTextContent();															
										componentMap.setMapConceptRef(sourceCodeRef);								
									}	
									NodeList targetNodes = mapElement.getElementsByTagNameNS("*","Target");
									for(int g=0;g<targetNodes.getLength();g++) {									
										org.w3c.dom.Element targetElement = (org.w3c.dom.Element) targetNodes.item(g);
										String targetCodeRef = targetElement.getElementsByTagNameNS("*","Ref").item(0).getAttributes().getNamedItem("id").getTextContent();															
										componentMap.setMapTargetConceptRef(targetCodeRef);									
									}																												
									RepresentationMapRefMutableBean mapRefMutableBean = new RepresentationMapRefMutableBeanImpl();
									NodeList representationMappingNodes = mapElement.getElementsByTagNameNS("*","RepresentationMapping");									
									for(int k=0;k<representationMappingNodes.getLength();k++) {
										org.w3c.dom.Element representationMappingElement = (org.w3c.dom.Element) representationMappingNodes.item(k);
										NodeList valueMapNodes = representationMappingElement.getElementsByTagNameNS("*","ValueMap");
										for(int n=0;n<valueMapNodes.getLength();n++) {
											org.w3c.dom.Element valueMapElement = (org.w3c.dom.Element) valueMapNodes.item(n);																	
											NodeList valueMappingNodes = valueMapElement.getElementsByTagNameNS("*","ValueMapping");
											for(int q=0;q<valueMappingNodes.getLength();q++) {									
												org.w3c.dom.Element valueMappingElement = (org.w3c.dom.Element) valueMappingNodes.item(q);
												String source = valueMappingElement.getAttribute("source");
												String target =  valueMappingElement.getAttribute("target");									
												mapRefMutableBean.addMapping(source, target);												
											}	
										}
									}
									componentMap.setRepMapRef(mapRefMutableBean);
									listOfComponentMaps.add(componentMap);
								}
								structureMapMutableBean.setComponents(listOfComponentMaps);							
							}
							structureSet.addStructureMap(structureMapMutableBean);
						}						
					}
				 }						 						 						 						 																																			 																		 
			 }
		 }
		return structureSet;				
	}
	
	/**
	 * Method that creates a Map with the transcoding rules
	 * @param structureSetC the StructureSetBean from which the map will be created
	 * @return a LinkedHashMapmap<String, LinkedHashMap<String, String>>
	 * @StructureSetBean structureSetC
	 */
	public LinkedHashMap<String, LinkedHashMap<String, String>> createStructureSetMap(StructureSetBean structureSetC) {
		LinkedHashMap<String, LinkedHashMap<String, String>> transcodingMap = new LinkedHashMap<String, LinkedHashMap<String,String>>();
		List<CodelistMapBean> codelistMaps = structureSetC.getCodelistMapList();
		List<StructureMapBean> structureMapBeanList = structureSetC.getStructureMapList();
		//the old transcoding with CodeListMap
		//accepts only coded components
		if (codelistMaps.size() > 0) {
			for (int i = 0; i < codelistMaps.size(); i++) {
				LinkedHashMap<String, String> rule = new LinkedHashMap<String, String>();
				CodelistMapBean codelistMap = codelistMaps.get(i);
				String name_codelistMap = codelistMap.getId();
				List<ItemMapBean> codeMaps = codelistMap.getItems();
				for (int j = 0; j < codeMaps.size(); j++) {
					ItemMapBean codeMap = codeMaps.get(j);
					String source = codeMap.getSourceId();
					String target = codeMap.getTargetId();
					rule.put(source, target);
				}
				transcodingMap.put(name_codelistMap, rule);
			}
	    //the old transcoding with StructureMapBean	
		//accepts non coded components as well
		} else if (structureMapBeanList.size() > 0){
			for (int i = 0; i < structureMapBeanList.size(); i++) {				
				StructureMapBean structureMap = structureMapBeanList.get(i);			
				for (ComponentMapBean componentMapBean:  structureMap.getComponents()) {
					LinkedHashMap<String, String> rule = new LinkedHashMap<String, String>();
					String name_codelistMap = componentMapBean.getMapTargetConceptRef();
					RepresentationMapRefBean refBean = componentMapBean.getRepMapRef();					
					for (Map.Entry<String, Set<String>> transcoding : refBean.getValueMappings().entrySet()) {
						String source = transcoding.getKey();
						String target = transcoding.getValue().iterator().next();
						rule.put(source, target);
					}
					transcodingMap.put(name_codelistMap, rule);
				}				
			}
		}
		return transcodingMap;
	}
	
	/**
	 * shortcut method reads the structure set from input and applies {@link #createStructureSetMap(StructureSetBean)} to the result
	 * @param transcodingInputStream
	 * @return
	 */
	public LinkedHashMap<String, LinkedHashMap<String, String>> readStructureSetMap(InputStream transcodingInputStream) throws Exception{
		StructureSetMutableBean structureSetMutable = readStructureSet(transcodingInputStream);
		return createStructureSetMap(structureSetMutable.getImmutableInstance());
	}
	
	/**
	 * This method returns the concept name of the measure Dimension.
	 * @param keyFamilyBean
	 * @return measureDimension id
	 */
	public static String scanForMeasureDimension(final DataStructureBean keyFamilyBean) {
		String measureDimension="";
		final List<DimensionBean> dimensionsBean = keyFamilyBean.getDimensions();
		for (final DimensionBean dimBean: dimensionsBean){
			if (dimBean.isMeasureDimension() && !dimBean.isTimeDimension()){
				measureDimension = dimBean.getId();
				break;
			}
		}	
		return measureDimension;
	}

	/**
	 * returns true if hte data structure is cross sectionsl
	 * 
	 * @param dataStructure
	 * @return
	 */
	public boolean isCrossSectionalDataStructure(DataStructureBean dataStructure){
		return dataStructure instanceof CrossSectionalDataStructureBean; 
	}
	
	/**
	 * returns true if the cross x data structure has cross x measures 
	 * 
	 * @param crossXDataStructure	the data structure
	 * @return	true if the structure has cross x measures, false otherwise
	 */
	public boolean hasCrossSectionalMeasures(CrossSectionalDataStructureBean crossXDataStructure){
		List<CrossSectionalMeasureBean> crossXMeasures = crossXDataStructure.getCrossSectionalMeasures(); 
		return crossXMeasures != null && crossXMeasures.size() > 0;
	}
	
	/**
	 * true if the data structure is cross sectional and has cross x measures
	 * @param dataStructure
	 * @return
	 */
	public boolean hasCrossSectionalMeasures(DataStructureBean dataStructure){
		return 	isCrossSectionalDataStructure(dataStructure) 
				&& hasCrossSectionalMeasures((CrossSectionalDataStructureBean)dataStructure);
	}
	//method which converts a set of strings to a list of strings
	public List<String> convertToList(Set<String> set) {
		List<String> list = new LinkedList<>();
		for(String element: set) {
			list.add(element);
		}
		return list;
	}
	//method which converts a list of strings to a set of strings
	public Set<String> convertToSet(List<String> list) {
		Set<String> set = new LinkedHashSet<>();
		for(String element: list) {
			set.add(element);
		}
		return set;
	}

	/**
	 * Method to sort an input Set of Strings.
	 * Converts the input set to linkedList,
	 * sorts the list, and it converts back to set.
	 * Finally it returns the sorted set.
	 * @param inSet
	 * @return
	 */
	public Set<String> sort(Set<String> inSet) {
		//Fist convert Set to List, in order to be able to use Collections.sort method
		List<String> list = new LinkedList<>();
		list = convertToList(inSet);
		//Then sort the list using Collections.sort
		Collections.sort(list);
		//Finally convert list back to set
		inSet = convertToSet(list);
		return inSet;
	}
	
}
