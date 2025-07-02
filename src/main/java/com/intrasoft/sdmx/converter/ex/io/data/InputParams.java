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
package com.intrasoft.sdmx.converter.ex.io.data;

import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.mutable.codelist.CodelistMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.conceptscheme.ConceptSchemeMutableBean;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This Class holds all conversion related input parameters needed by the converter.
 * 
 */
public class InputParams{

	/** properties for reading and writing data of various formats */
	private DataStructureBean keyFamilyBean;
	/** the delimiter of data fileds in CSV format */
	private String delimiter;
	private boolean escapeOutputCSV;
	private boolean unescapeInputCSV;
	/** the number of levels of a flat file, default value equals to 1 */
	private String levelNumber = "1";
	// private InputStream mappingIs;
	private Map<String, String[]> mappingMap;// concept name -> concept order (in some user-provided mappings the order
	// could be a String e.g. 5+8)
	// private InputStream headerBean;
	private HeaderBean headerBean;
	private String namespaceUri;
	private String namespacePrefix;
	private String dateCompatibility;
	/** The comment to be inserted to the generated file (presently only for SDMX files) */
	private String generatedFileComment;
	/** Boolean of whether the flat input file is ordered or not (to save time re-ordering it) */
	private boolean orderedFlatInput;
	/**
	 * String of whether exists a row with arbitrary data in the CSV/FLR input message. 
	 * If disregard column headers then the CSV/FLR reader should ignore the row.
	 * If use column headers then the row will be used for mapping. 
	 * If no column headers then there is not such a row in the message.
	 */
	private String headerRow;
	/** Boolean of whether the converter io should write a header.prop file or not. */
	private boolean headerWriting;
	/** destination path of the header.prop file */
	private String headerOutputPath;
	/** String to hold the writing technique for Gesmes */
	private String gesmesWritingTechnique;
	// holds the transcoding information
	private LinkedHashMap<String, LinkedHashMap<String, String>> structureSetMap;
	/** holds the encoding of a flat file */
	private String flatFileEncoding;
	/** ArrayList with the codelist of the components */
	private ArrayList<CodelistMutableBean> codelist;
	
	/** for transcoding time values from SDMX to native formats and vice versa */
	private TimeTranscoder timeTranscoder;
	/** holds the buffering method for XS reader/writer buffering possible values: {@code Defs.XS_BUFFERING_FILE},
	 *  {@code Defs.XS_BUFFERING_MEMORY}
	 *  <br> defaults to <code>Defs.XS_BUFFERING_FILE</code> 
	 */
	private String xsBuffering;
	/**
	 * variable for whether the leading and trailing spaces should be trimmed or not. If it is set to true then the spaces are 
	 * not considered as part of the data and should be trimmed. This case holds only for data in a DSXML bundle. By default this is
	 * set to false. 
	 */
	private boolean leadTrailSpacesTrimmed;
	private ArrayList<ConceptSchemeMutableBean> concepts;

	private boolean mapCrossXMeasure;
	
	/** Excel parameters */	
	// Parameters map
	private Map<String, ArrayList<String>> excelParameterMultipleMap;
	// Whether the configuration should be read from the excel input (true by default)
    private boolean configInsideExcel = true;
    private boolean mappingInsideExcel = true;
	
	private String excelType = "xlsx";
	private InputStream excelOutputTemplate;
	
	private SdmxBeanRetrievalManager structureRetrievalManager;
	private String structureDefaultVersion;
	
	public InputParams() {
		setXsBuffering(Defs.XS_BUFFERING_FILE);
		setLeadTrailSpacesTrimmed(false);
	}

	/**
	 * This is a getter for keyFamilyBean
	 * @return keyFamilyBean
	 */
	public DataStructureBean getKeyFamilyBean() {
		return keyFamilyBean;
	}

	/**
	 * This is a setter for keyFamilyBean
	 * @param keyFamilyBean, the keyFamilyBean to set
	 * @return itself, updated with the new value
	 */
	public void setKeyFamilyBean(DataStructureBean keyFamilyBean) {
		this.keyFamilyBean = keyFamilyBean;
	}

	/**
	 * This is a getter for delimiter
	 * @return delimiter
	 */
	public String getDelimiter() {
		return delimiter;
	}

	/**
	 * This is a setter for delimiter
	 * @param delimiter, the delimiter to set
	 * @return itself, updated with the new value
	 */
	public void setDelimiter(final String delimiter) {
		this.delimiter = delimiter;
	}

	/**
	 * This is a getter for levelNumber
	 * @return levelNumber
	 */
	public String getLevelNumber() {
		return levelNumber;
	}

	/**
	 * This is a setter for levelNo
	 * @param levelNo, the levelNo to set
	 * @return itself, updated with the new value
	 */
	public void setLevelNumber(final String levelNo) {
		this.levelNumber = levelNo;
	}

	// public InputStream getMappingIs() {
	// return mappingIs;
	// }
	//
	// public void setMappingIs(InputStream mappingIs) {
	// this.mappingIs = mappingIs;
	// }
	//

	// /**
	// * This is a getter for headerIs
	// * @return headerIs
	// */
	// public InputStream getHeaderIs() {
	// return headerIs;
	// }
	/**
	 * This is a getter for headerBean
	 * @return headerBean
	 */
	public HeaderBean getHeaderBean() {
		return headerBean;
	}

	// /**
	// * This is a setter for headerIs
	// * @param headerIs, the headerIs to set
	// * @return itself, updated with the new value
	// */
	// public void setHeaderIs(InputStream headerIs) {
	// this.headerIs = headerIs;
	// }
	/**
	 * This is a setter for headerBean
	 * @param headerBean, the headerBean to set
	 * @return itself, updated with the new value
	 */
	public void setHeaderBean(final HeaderBean headerBean) {
		this.headerBean = headerBean;
	}

	/**
	 * This is a getter for namespaceUri
	 * @return namespaceUri
	 */
	public String getNamespaceUri() {
		return namespaceUri;
	}

	/**
	 * This is a setter for namespaceUri
	 * @param namespaceUri, the namespaceUri to set
	 * @return itself, updated with the new value
	 */
	public void setNamespaceUri(final String namespaceUri) {
		this.namespaceUri = namespaceUri;
	}

	/**
	 * This is a getter for namespacePrefix
	 * @return namespacePrefix
	 */
	public String getNamespacePrefix() {
		return namespacePrefix;
	}

	/**
	 * This is a setter for namespacePrefix
	 * @param namespacePrefix, the namespacePrefix to set
	 * @return itself, updated with the new value
	 */
	public void setNamespacePrefix(final String namespacePrefix) {
		this.namespacePrefix = namespacePrefix;
	}

	/**
	 * This is a setter for dateCompatibility
	 * @param dateCompatibility, the dateCompatibility to set
	 * @return itself, updated with the new value
	 */
	public void setDateCompatibility(final String dateCompatibility) {
		this.dateCompatibility = dateCompatibility;
	}

	/**
	 * This is a getter for dateCompatibility
	 * @return dateCompatibility
	 */
	public String getDateCompatibility() {
		return this.dateCompatibility;
	}

	/**
	 * This is a getter for generatedFileComment
	 * @return generatedFileComment
	 */
	public String getGeneratedFileComment() {
		return generatedFileComment;
	}

	/**
	 * This is a setter for generatedFileComment
	 * @param generatedFileComment, the generatedFileComment to set
	 * @return itself, updated with the new value
	 */
	public void setGeneratedFileComment(final String generatedFileComment) {
		this.generatedFileComment = generatedFileComment;
	}

	/**
	 * This is a getter for orderedFlatInput
	 * @return orderedFlatInput
	 */
	public boolean isOrderedFlatInput() {
		return orderedFlatInput;
	}

	/**
	 * This is a setter for orderedFlatInput
	 * @param orderedFlatInput, the orderedFlatInput to set
	 * @return itself, updated with the new value
	 */
	public void setOrderedFlatInput(final boolean orderedFlatInput) {
		this.orderedFlatInput = orderedFlatInput;
	}

	/**
	 * This is a setter for existHeaderRow
	 * @param headerRow, the existHeaderRow to set
	 * @return itself, updated with the new value
	 */
	public void setHeaderRow(final String headerRow) {
		this.headerRow = headerRow;
	}
	
	/**
	 * This is a getter for headerRow  
	 * @return headerRow 
	 */
	public String getHeaderRow() {
		return headerRow;
	}

	/**
	 * This is a setter for writing or no the Header.prop
	 * @param headerWriting, the headerWriting to set
	 * @return itself, updated with the new value
	 */
	public void setHeaderWriting(final boolean headerWriting) {
		this.headerWriting = headerWriting;
	}

	/**
	 * This is a getter for headerWriting
	 * @return headerWriting
	 */
	public boolean isHeaderWriting() {
		return headerWriting;
	}

	/**
	 * This is a getter for mappingMap
	 * @return mappingMap
	 */
	public Map<String, String[]> getMappingMap() {
		return mappingMap;
	}

	/**
	 * This is a setter for Mapping
	 * @param mappingMap, the mappingMap to set
	 * @return itself, updated with the new value
	 */
	public void setMappingMap(final Map<String, String[]> mappingMap) {
		this.mappingMap = mappingMap;
	}

	/**
	 * This is a getter for codelist
	 * @return codelist
	 */
	public ArrayList<CodelistMutableBean> getCodeList() {
		return codelist;
	}

	/**
	 * This is a setter for Codelist
	 * @param codelist the codelist to set
	 * @return itself, updated with the new value
	 */
	public void setCodeList(final ArrayList<CodelistMutableBean> codelist) {
		this.codelist = codelist;
	}
	/**
	 * This is a getter for conceptscheme
	 * @return concepts
	 */
	public ArrayList<ConceptSchemeMutableBean> getConceptScheme() {
		return concepts;
	}

	/**
	 * This is a setter for ConceptScheme list
	 * @param concepts the conceptscheme to set
	 * @return itself, updated with the new value
	 */
	public void setConceptScheme(final ArrayList<ConceptSchemeMutableBean> concepts) {
		this.concepts = concepts;
	}
	// public boolean isTimeRangeBool() {
	// return timeRangeBool;
	// }
	//
	// public void setTimeRangeBool(boolean useTimeRange) {
	// this.timeRangeBool = useTimeRange;
	// }
	/**
	 * This is a getter for gesmesWritingTechnique
	 * @return gesmesWritingTechnique
	 */
	public String getGesmesWritingTechnique() {
		return gesmesWritingTechnique;
	}

	/**
	 * This is a setter for gesmesWritingTechnique
	 * @param gesmesWritingTechnique the gesmesTechnique to set
	 * @return itself, updated with the new value
	 */
	public void setGesmesWritingTechnique(final String gesmesWritingTechnique) {
		this.gesmesWritingTechnique = gesmesWritingTechnique;
	}

	/**
	 * This is a setter for StructureSet
	 * @param structureSetMap the structureSetMap to set
	 * @return itself, updated with the new value
	 */
	public void setStructureSet(final LinkedHashMap<String, LinkedHashMap<String, String>> structureSetMap) {
		this.structureSetMap = structureSetMap;
	}

	/**
	 * This is a getter for structureSetMap
	 * @return structureSetMap
	 */
	public LinkedHashMap<String, LinkedHashMap<String, String>> getStructureSet() {
		return structureSetMap;
	}

	/**
	 * This is a getter for flatFileEncoding
	 * @return flatFileEncoding
	 */
	public String getFlatFileEncoding() {
		return flatFileEncoding;
	}

	/**
	 * This is a setter for Encoding
	 * @param flatFileEncoding the flatFileEncoding to set
	 * @return itself, updated with the new value
	 */
	public void setFlatFileEncoding(final String flatFileEncoding) {
		this.flatFileEncoding = flatFileEncoding;
	}

	/**
	 * setter for the destination path of the header.prop
	 * @param headerOutputPath
	 */
	public void setHeaderOutputPath(final String headerOutputPath) {
		this.headerOutputPath = headerOutputPath;
	}

	/**
	 * getter of the destination path of the header.prop
	 * @return headerOutputPath
	 */
	public String getHeaderOutputPath() {
		return headerOutputPath;
	}
	
	/**
	 * @return the xsBuffering
	 */
	public String getXsBuffering() {
		return xsBuffering;
	}

	/**
	 * @param xsBuffering the xsBuffering to set
	 */
	public void setXsBuffering(final String xsBuffering) {
		this.xsBuffering = xsBuffering;
	}

	/**
	 * @param leadTrailSpacesTrimmed the leadtrailSpacesTrimmed to set
	 */
	public void setLeadTrailSpacesTrimmed(final boolean leadTrailSpacesTrimmed) {
		this.leadTrailSpacesTrimmed = leadTrailSpacesTrimmed;
	}

	/**
	 * @return the leadtrailSpacesTrimmed
	 */
	public boolean isLeadTrailSpacesTrimmed() {
		return leadTrailSpacesTrimmed;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("InputParams\nKeyFamily: ");
		sb.append(getKeyFamilyBean().getId());
		sb.append("\nMapping size: ");
		sb.append((getMappingMap() == null ? "null" : getMappingMap().size()));
		sb.append("\nHeader size: ");
		// try {
		// sb.append(getHeaderIs().available());
		// } catch (IOException ioe) {
		// sb.append(ioe.getMessage());
		// }
		if (headerBean != null) {
			sb.append(headerBean.getId());
		}
		sb.append("\nDelimiter: \"");
		sb.append(getDelimiter());
		sb.append("\"\nNamespacePrefix: \"");
		sb.append(getNamespacePrefix());
		sb.append("\"\nNamespaceUri: \"");
		sb.append(getNamespaceUri());
		sb.append("\"");
		sb.append("\nEncoding: ");
		sb.append(getFlatFileEncoding());
		return sb.toString();
	}
	
//	public Object clone(){
//		InputParams cloned = null;
//		try{
//	    	cloned = (InputParams)super.clone();	      
//	    }
//	    catch(CloneNotSupportedException e){
//	      System.out.println(e);
//	      return null;
//	    }
//	    cloned.keyFamilyBean = (KeyFamilyBean) keyFamilyBean.clone();    
//	    return cloned;
//	  }

	/**
	 * @param timeTranscoder the timeTranscoder to set
	 */
	public void setTimeTranscoder(final TimeTranscoder timeTranscoder) {
		this.timeTranscoder = timeTranscoder;
	}

	/**
	 * @return the timeTranscoder
	 */
	public TimeTranscoder getTimeTranscoder() {
		return timeTranscoder;
	}


	/**
	 * This is a getter for the mapping between parameter sheets and data sheets, with multiple values per Data Sheet.
	 * 
	 * @return Map<String, ArrayList<String>> - the map between parameter sheets and data sheets
	 */
	public Map<String, ArrayList<String>> getExcelParameterMultipleMap() {
		return excelParameterMultipleMap;
	}

	/**
	 * This is a setter for the mapping between parameter sheets and data sheets, with multiple values per Data Sheet.
	 * 
	 * @param excelParameterMultipleMap
	 */
	public void setExcelParameterMultipleMap(
			Map<String, ArrayList<String>> excelParameterMultipleMap) {
		this.excelParameterMultipleMap = excelParameterMultipleMap;
	}

	/**
	 * This is a getter of the Excel type - old 2003 xls or ooxml xlsx.
	 * 
	 * @return List<ExcelParameter> - the Excel type
	 */
	public String getExcelType() {
		return excelType;
	}

	/**
	 * This is a setter for the Excel type - old 2003 xls or ooxml xlsx.
	 * 
	 * @param excelType
	 */
	public void setExcelType(String excelType) {
		this.excelType = excelType;
	}

	public void setCSVOuputEscape(boolean escapeOutputCSV) {
		this.escapeOutputCSV = escapeOutputCSV;		
	}
	
	public boolean isCSVOuputEscaped() {
		return this.escapeOutputCSV;
	}
	
	public void setCSVInputEscape(boolean unescapeInputCSV) {
		this.unescapeInputCSV = unescapeInputCSV;
	}
	
	public boolean isCSVInputUnescaped() {
		return this.unescapeInputCSV;
	}

	public InputStream getExcelOutputTemplate() {
		return excelOutputTemplate;
	}

	public void setExcelOutputTemplate(InputStream excelOutputTemplate) {
		this.excelOutputTemplate = excelOutputTemplate;
	}
	
	public boolean isMapCrossXMeasure() {
		return mapCrossXMeasure;
	}

	public void setMapCrossXMeasure(boolean mapCrossXMeasure) {
		this.mapCrossXMeasure = mapCrossXMeasure;
	}

	public boolean isConfigInsideExcel() {
		return configInsideExcel;
	}

	public void setConfigInsideExcel(boolean configInsideExcel) {
		this.configInsideExcel = configInsideExcel;
	}
	
	public boolean hasMappingInsideExcel(){
        return mappingInsideExcel;
    }
    public void setMappingInsideExcel(boolean mappingInsideExcel) {
		this.mappingInsideExcel = mappingInsideExcel;
	}
	public void setStructureRetrievalManager(SdmxBeanRetrievalManager structureRetrievalManager) {
		this.structureRetrievalManager = structureRetrievalManager;
		
	}
	public SdmxBeanRetrievalManager getStructureRetrievalManager() {
		return structureRetrievalManager;
	}
	public void setStructureDefaultVersion(String structureDefaultVersion) {
		this.structureDefaultVersion = structureDefaultVersion;
		
	}
	public String getStructureDefaultVersion() {
		return structureDefaultVersion;
	}	
}
