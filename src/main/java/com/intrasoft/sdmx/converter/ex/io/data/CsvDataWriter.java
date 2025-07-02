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
/**

 * © 2009 by the European Community, represented by Eurostat.

 * All rights Reserved.

 */

package com.intrasoft.sdmx.converter.ex.io.data;
import au.com.bytecode.opencsv.CSVWriter;
import com.intrasoft.sdmx.converter.ex.model.data.DataSet;
import com.intrasoft.sdmx.converter.ex.model.data.GroupKey;
import com.intrasoft.sdmx.converter.ex.model.data.Observation;
import com.intrasoft.sdmx.converter.ex.model.data.TimeseriesKey;
import com.intrasoft.sdmx.converter.io.data.TranscodingEngine;
import com.intrasoft.sdmx.converter.ui.exceptions.CSVWriterValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.ATTRIBUTE_ATTACHMENT_LEVEL;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * Implementation of the Writer interface for writing CSV data messages.
 */
public class CsvDataWriter implements Writer {

	private static Logger logger = LogManager.getLogger(CsvDataWriter.class);

	private TranscodingEngine transcoding;

	//default no-arg constructor
	public CsvDataWriter(){
	}
	
	private String lineEnd = System.getProperty("line.separator");
	private PrintWriter pout;
	private int primMeasurePlace;
	/**
	 * contains the place of the primary measure in a csv line
	 */
	private int timeMeasurePlace;

	/**
	 * contains the place of the time dimension in a csv line
	 */

	/** required properties for writing data */
	private DataStructureBean keyFamilyBean;
	
	
	/** optional properties for writing data */
	
	// the default delimiter of data fields
	private String delimiter; 
	
	// the default date format is like SDMX (YYYY-MM)
	private String dateCompatibility = "SDMX"; 
	
	// concept name -> concept data field coordinates. NOTE: In CSV writing
	// coordinates of the form 5-8 or 4+5 are meaningless
	private Map<String, String[]> mappingMap;
	
	private Map<String, String[]> mappingXSMap;
	
	/* attributes that was added with the measures */
	List<AttributeBean> availableCrossAttributes = null;
	List<CrossSectionalMeasureBean> availableCrossDims = null;
	List<String> availableCrossDimsIds = null;
	
	private LinkedHashMap<String, LinkedHashMap<String, String>> transcodingMap;// concept
	
	/** structure that holds level->(position->component) for a multilevel output file */
	final private LinkedHashMap<String, LinkedHashMap<Integer, String>> recordData = new LinkedHashMap<String, LinkedHashMap<Integer, String>>();
	
	/** structure that holds level->data to be written in that level */
	private LinkedHashMap<String, String> recordData2 = new LinkedHashMap<String, String>();
	
	/** integer for keeping the level of the data for a multilevel input file and a default mapping */
	/** boolean if there exits a group key */
	private boolean writeGroup;
	
	/** boolean if there exits a dataset */
	private boolean writeDataset;
	
	/** String holding the previous partial key (i.e. without the crossX measure)  */
	final private StringBuilder prevKey = new StringBuilder();
	
	/** String holding the current partial key (i.e. without the crossX measure)  */
	final private StringBuilder currKey = null;
	final private LinkedHashMap serKeyMap =new LinkedHashMap<String, LinkedHashMap<Integer,String >>();
	
	// partial key, without the crossX measure
	private File tempFlatXSKeyFile;
	private File tempCsvFile;
	private PrintWriter temppout;
	
	/**
	 * list that holds the component names of the dsd
	 */
	final private List dimList=new ArrayList();
	
	/**
	 * metr that correspond to the dimlist. A component in the dimList will appear in the same position (metr) as its value will appear in the lineMap
	 */
	private int metr=0;
	
	/**
	 * metr for the number of dts attributes
	 */
	private int metrDts=0;
	
	/**
	 * boolean whether a first line has been written in the csv file
	 */
	private boolean metadataWritten=false;
	
	/**
	 * map that holds the component names that correspond to the csv columns
	 */
	private LinkedHashMap<Integer, String> metadatalineMap = new LinkedHashMap<Integer, String>();
	private LinkedHashMap<Integer, String> metadatalineXSMap = new LinkedHashMap<Integer, String>();
	
	/*
	 * Hash Map that has as akey the position and as value the value of the component
	 */
	private LinkedHashMap<Integer,String> lineMapX=new LinkedHashMap<Integer,String>();
	private LinkedHashMap<String, LinkedHashMap<Integer, String[]>> flatKeyMapMultilevel = null;
	
	/*
	 * boolean variable whether the mapping has mapped the measure dimension or the crossX measures.
	 */
	private boolean mapCrossXMeasures = false;
	private int keySetLimit;
	
	/*
	 * String holding the name of the measure Dimension
	 */
	private String measureDimension="";
	private ArrayList<DimensionBean> keyFamilyDimensionNames = null;
	
	/* properties required for implementing the Writer interface */
	private InputParams params;
	private OutputStream os;

	/**
	 * Implements Writer interface method.
	 */
	public boolean isReady() throws Exception {
		boolean ready = true;
		String errorMessage = "Writer is not ready. Missing:";
		if (os == null) {
			ready = false;
			errorMessage += "output stream, ";
		}
		if (keyFamilyBean == null) {
			ready = false;
			errorMessage += "DSD, ";
		}
		if (!ready) {
			errorMessage = errorMessage.substring(0, errorMessage.length() - 2) + ".";
			throw new Exception(errorMessage);
		}
		return ready;
		// return (keyFamilyBean != null && os != null);
	}
	
	/**
	 * Implements Writer interface method.
	 */
	public void setOutputStream(final OutputStream os) {
		this.os = os;
	}
	
	/**
	 * Implements Writer interface method.
	 */
	public void setInputParams(final InputParams params) {
		this.params = params;
		setKeyFamilyBean(params.getKeyFamilyBean());
		setDelimiter(params.getDelimiter());
		setDateCompatibility(params.getDateCompatibility());
		setMappingMap(params.getMappingMap());
		setTranscodingMap(params.getStructureSet());
		mapCrossXMeasures = params.isMapCrossXMeasure();
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeHeader(final HeaderBean header) throws Exception {
		try {
			isReady();
			params.setHeaderBean(header);
			if (params.isHeaderWriting()) {
				// write a properties file with the header information
				// the destinaiton path is the path of the output file
				final String header_os = params.getHeaderOutputPath() + "Header.prop";
				IoUtils.printHeader(header, header_os);
			}
			final OutputStreamWriter osw = new OutputStreamWriter(os, params.getFlatFileEncoding());
			pout = new PrintWriter(new BufferedWriter(osw));
			populateDsdRelatedProperties();
		} catch (UnsupportedEncodingException uee) {
			pout.close();
			os.close();
			final String errorMessage = this.getClass().getName() + " exception :" + "unsupported encoding:" + params.getFlatFileEncoding() + lineEnd
			+ "\t";
			throw new Exception(errorMessage + uee.getMessage(), uee);
		} catch (Exception e) {
			pout.close();
			os.close();
			final String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeEmptyDataSet(final DataSet dataSet) throws Exception {
		try {
			OutputStream out;
			if(temppout != null) {temppout.close();}			
			if (mapCrossXMeasures){
				tempFlatXSKeyFile= File.createTempFile("tempFlatXS", null);
				tempCsvFile=File.createTempFile("tempFile", null);
				out = new FileOutputStream(tempFlatXSKeyFile);
				final OutputStreamWriter osw = new OutputStreamWriter(out, params.getFlatFileEncoding());
				temppout = new PrintWriter(new BufferedWriter(osw));
			}
			/*
			 * nothing to do, OutputStream is not used by this Writer if a flat file is to be written however if the
			 * file is multilevel then dataset attributes should be written
			 */
			// if the mapping is the DEFAULT one then print in level 1 the dataset attributes
			if (Integer.parseInt(params.getLevelNumber()) > 1 && params.getMappingMap() == null) {
				// throw exception
				final String message = "Mapping is mandatory for CSV multilevel file";
				throw (new Exception("Please correct the following:\n" + message));
			} else {
				// if the mapping is NOT default then
				String finalvalue = "";
				LinkedHashMap<Integer, String> lineMap = new LinkedHashMap<Integer, String>();
				final StringBuffer stbuf=new StringBuffer();    
				for (final Entry<String, String> entry : dataSet.getAttributeValues().entrySet()) {
					// check if this dimension has transcoding rules
				    final boolean has_rules = this.transcoding.hasTranscodingRules(entry.getKey());
					// Get the value from the transcoding rule
					if (has_rules) {
						finalvalue = this.transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
					} else {
						finalvalue = entry.getValue();
					}
					if (mapCrossXMeasures && Integer.parseInt(params.getLevelNumber()) > 1){
						dimList.add(entry.getKey());
						stbuf.append(entry.getValue());
						stbuf.append(";");
						writeDataset=true;						
						lineMapX.put(metrDts, finalvalue);
						metrDts++;
					}
					
					//if (mapCrossXMeasures){
					final String[] attributeData = mappingMap.get(entry.getKey());
					if (recordData.get(attributeData[2]) != null) {
						lineMap = new LinkedHashMap<Integer, String>();
						lineMap = recordData.get(attributeData[2]);
					} else {
						lineMap = new LinkedHashMap<Integer, String>();
					}
					
					
					//lineMap.put(Integer.valueOf(attributeData[0]) - 1, finalvalue);
					lineMap.put(Integer.valueOf(attributeData[0]) - 1, finalvalue);					
															
					if (Integer.parseInt(params.getLevelNumber()) > 1) {
						metadatalineMap.put(Integer.valueOf(attributeData[0]) - 1, finalvalue);
						lineMap.put(0, attributeData[2]);
					} //else {
						recordData.put(attributeData[2], lineMap);
					//}
					// }
				writeDataset = true;
				}
				if (writeDataset && mapCrossXMeasures){
					temppout.print(stbuf.toString().substring(0, stbuf.length()-1));
					temppout.println();
				}
			}
			// isReady();
		} catch (Exception e) {
			pout.close();
			os.close();
			final String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}
	
	/**
	 * Implements Writer interface method.
	 */
	public void writeGroupKey(final GroupKey grKey) throws Exception {
		try {
			/*
			 * nothing to do, OutputStream is not used by this Writer if a flat file is to be written however if the
			 * file is multilevel then group key values and attributes should be written
			 */
			metr=metrDts;
			if (mapCrossXMeasures){
				String finalvalue = "";
				for (Entry<String, String> entry : grKey.getKeyValues().entrySet()){
					// check if this dimension has transcoding rules
				    final boolean has_rules = this.transcoding.hasTranscodingRules(entry.getKey());
					// Get the value from the transcoding rule
					if (has_rules) {
						finalvalue = this.transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
					} else {
						finalvalue = entry.getValue();
					}
					if(!dimList.contains(entry.getKey())){
						dimList.add(entry.getKey());
						writeGroup=true;
						lineMapX.put(metr, finalvalue);
						metr++;
					}else{
					    final int position=dimList.indexOf(entry.getKey());
						lineMapX.put(position, finalvalue);
					}
				}
				for (Entry<String, String> entry : grKey.getAttributeValues().entrySet()) {
				    final boolean has_rules = this.transcoding.hasTranscodingRules(entry.getKey());
					// Get the value from the transcoding rule
					if (has_rules) {
						finalvalue = this.transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
					} else {
						finalvalue = entry.getValue();
					}
					if(!dimList.contains(entry.getKey())){
						dimList.add(entry.getKey());						
						lineMapX.put(metr, finalvalue);
						metr++;
					}else{
					    final int position=dimList.indexOf(entry.getKey());
						lineMapX.put(position, finalvalue);
					}
				}
			}else{
				recordData2 = new LinkedHashMap<String, String>();
				// if the mapping is the DEFAULT one then print in level 2 the group key and attributes
				
				String finalvalue = "";
				LinkedHashMap<Integer, String> lineMap = new LinkedHashMap<Integer, String>();
				// group keys
				for (Entry<String, String> entry : grKey.getKeyValues().entrySet()) {
					// check if this dimension has transcoding rules
				    final boolean has_rules = this.transcoding.hasTranscodingRules(entry.getKey());
					// Get the value from the transcoding rule
					if (has_rules) {
						finalvalue = this.transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
					} else {
						finalvalue = entry.getValue();
					}
					final String[] keyData = mappingMap.get(entry.getKey());
					if (keyData.length > 2 && recordData.get(keyData[2]) != null) {
						lineMap = new LinkedHashMap<Integer, String>();
						lineMap = recordData.get(keyData[2]);
					} else {
						lineMap = new LinkedHashMap<Integer, String>();
					}
					lineMap.put(0, keyData[2]);
					lineMap.put(Integer.valueOf(keyData[0]) - 1, finalvalue);
					recordData.put(keyData[2], lineMap);
					writeGroup = true;
				}
				// group attributes
				for (Entry<String, String> entry : grKey.getAttributeValues().entrySet()) {
					// check if this dimension has transcoding rules
				    final boolean has_rules = this.transcoding.hasTranscodingRules(entry.getKey());
					// Get the value from the transcoding rule
					if (has_rules) {
						finalvalue = this.transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
					} else {
						finalvalue = entry.getValue();
					}
					final String[] attributeData = mappingMap.get(entry.getKey());
					if (recordData.get(attributeData[2]) != null) {
						lineMap = new LinkedHashMap<Integer, String>();
						lineMap = recordData.get(attributeData[2]);
					} else {
						lineMap = new LinkedHashMap<Integer, String>();
					}
					lineMap.put(0, attributeData[2]);
					lineMap.put(Integer.valueOf(attributeData[0]) - 1, finalvalue);
					recordData.put(attributeData[2], lineMap);
				}
			}

			// isReady();
		} catch (Exception e) {
			pout.close();
			os.close();
			String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}
	
	/**
	 * Implements Writer interface method.
	 */
	public void writeTimeseriesKey(final TimeseriesKey tsKey) throws Exception {
		try {
			isReady();
			printSeriesKey(tsKey);
		} catch (CSVWriterValidationException csve) {
			throw csve;
		} catch (Exception e) {
			pout.close();
			os.close();
			String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}
	
	/**
	 * Implements Writer interface method.
	 */
	public void closeWriter() throws Exception {
		try {
			if (mapCrossXMeasures){
				temppout.flush();
				temppout.close();				
				makeTempCsvFile(Integer.MAX_VALUE);
				if (tempCsvFile != null) {
					tempCsvFile.delete();
				}
				if (tempFlatXSKeyFile != null) {
					tempFlatXSKeyFile.delete();
				}
			}else{
				pout.flush();
				pout.close();
				os.close();
			}
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + ".closeWriter() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}			
	
	/**
	 * This method makes a temporary file with the distinct series keys (without the crossX measure concept). Then parses this file and for each line of it parses
	 * the temporary CSV file which contains the non-flat records i.e. records with one obs value and the measure dimension concept. While reading the
	 * temporary csv file populates a HashMap with records that are the records of the output csv file. In the end it writes the records to the output.
	 * @param _keySetLimit
	 * @throws Exception
	 */
	private void makeTempCsvFile(final int _keySetLimit) throws Exception{
		keySetLimit = _keySetLimit;
		BufferedReader brxs = null;
		BufferedReader brxt = null;
		BufferedReader brd=null;
		Set<String> partialKeys = null;
		String line=null;
		File tempKeysFile=null;
		PrintWriter tempKeys = null;
		LinkedHashMap<String, String[]> flatKeyMap = null;
		keyFamilyDimensionNames = (ArrayList<DimensionBean>) keyFamilyBean.getDimensions();
		//holds the row of the tempFlatXSkeyFile that we read
		int rowCounter;
		//holds the number of keys that have been parsed
		int parsedXKeys;
		String cref=null;
		try{
			parsedXKeys=0;
			if(temppout != null) {temppout.close();}
			//read the temp File
			brxs = new BufferedReader(new InputStreamReader(new FileInputStream(tempFlatXSKeyFile), params.getFlatFileEncoding()));
			temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempCsvFile),params.getFlatFileEncoding()));
			tempKeysFile = File.createTempFile("tempKey", null);
			tempKeys = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempKeysFile), params.getFlatFileEncoding()));
			partialKeys = new TreeSet();
			//read the temp csv File and populate another temp File(tempKeysFile) with the distinct partial keys only (without the crossX measure)
			if (writeDataset){
				line=brxs.readLine();
				final String data[]=line.split(";");
				//get the level number
				final String dtsconcept=dimList.get(0).toString();
				final String mappingData[]=mappingMap.get(dtsconcept);
				final String lev=mappingData[2];
				//print the level
				pout.print(lev);
				pout.print(delimiter);
				for (int k=0;k<data.length;k++){
					if (k!=0) {
						pout.print(delimiter);
					}
					pout.print(data[k]);
				}
				pout.println();
			}

			while ((line = brxs.readLine()) != null) {
			    final String lineKey=getFlatKey(line);
				partialKeys.add(lineKey);
				//System.out.println(line);
				//System.out.println(lineKey);
			}

			final Iterator<String> it = partialKeys.iterator();
			while (it.hasNext()) {
				tempKeys.println(it.next());
			}

			partialKeys = null;
			brxs.close();
			tempKeys.flush();
			tempKeys.close();
			while (true){
				try{
					brxt = new BufferedReader(new InputStreamReader(new FileInputStream(tempKeysFile), params.getFlatFileEncoding()));
					//brxs = new BufferedReader(new InputStreamReader(new FileInputStream(tempFlatXSKeyFile), params.getFlatFileEncoding()));
					flatKeyMap = new LinkedHashMap();
					flatKeyMapMultilevel=new LinkedHashMap<String, LinkedHashMap<Integer,String[]>>();
					String[] values = null;
					LinkedHashMap valuesMap =null;
					rowCounter=0;
					String keyLine=null;
					StringBuilder serkey=new StringBuilder();
					StringBuilder prevkey=new StringBuilder();
					int serLevel=0;
					while ((line = brxt.readLine()) != null) {
						rowCounter++;
						if (Integer.parseInt(params.getLevelNumber()) == 1){
							values = new String[mappingXSMap.size()];
						}
						
						if (rowCounter <= parsedXKeys) {
							continue;
						}
						String data[]=null;
						//the key is the level and the value is the values to be printed in the output file
						valuesMap = flatKeyMapMultilevel.get(line);
						if (valuesMap==null) {
							valuesMap= new LinkedHashMap<Integer,String[]>();
						}
							brxs = new BufferedReader(new InputStreamReader(new FileInputStream(tempFlatXSKeyFile), params.getFlatFileEncoding()));
							if (writeDataset) {
								keyLine=brxs.readLine();
							}

							while ((keyLine = brxs.readLine()) != null) {
								
								List<String> sameKeyLines = collectAllSameLines(keyLine, new FileInputStream(tempFlatXSKeyFile), params.getFlatFileEncoding());
								// For the lines of data with the same keylines
								for(String kl : sameKeyLines) {
									String measureConcept = null;
								    final String keyL=getFlatKey(kl);
									//if partial key of the temporary equals with the partial key of the csv file then process this line
									if (keyL.equals(line)){
										data = kl.split(";");
										//get the position of the crossX measure in the dimList
										int position = dimList.indexOf(measureDimension);
										//get the crossX measure concept
										measureConcept=data[position];
										//get the position  that has in the mapping procedure
										String[] mappingData=mappingMap.get(measureConcept);
										String measureconceptRef="";
										if(mappingData==null){
											//then the code differs from the conceptRef. Call the respective method to get the conceptRef
											measureconceptRef=IoUtils.getCrossXConceptRef(measureConcept,keyFamilyBean);
											mappingData = mappingXSMap.get(measureconceptRef);
											if(mappingData==null) {
												mappingData = mappingMap.get(measureDimension);
											}
											measureConcept = measureconceptRef;
										}
										final int mappos=Integer.valueOf(mappingData[0]) -1;	
										metadatalineXSMap.put(mappos, measureConcept);
										//get the value
										position=dimList.indexOf(keyFamilyBean.getPrimaryMeasure().getConceptRef());
										String obsvalue = null;
										//if exist valaue for the crossX measure
										if (data.length > position ){
											 obsvalue=data[position];
										}
										if (Integer.parseInt(params.getLevelNumber()) == 1){
											values[mappos]=obsvalue;
										} else{
										    final int lev=Integer.parseInt(mappingData[2]);
											String valuesM[]= (String[]) valuesMap.get(lev);
											if (valuesM==null){
												valuesM=new String[IoUtils.getComponentNo(mappingXSMap, mappingData[2])+1];
												valuesM[mappos]=obsvalue;
											}else{
												valuesM[mappos]=obsvalue;
											}
											valuesM[0]=mappingData[2];
											valuesMap.put(lev, valuesM);
										}
									
									
									//populate the rest components that are shared, like dimensions and obs attributes.
									for (AttributeBean attBean : keyFamilyBean.getObservationAttributes()) {
										if (dimList.contains(attBean.getId())){
										    final int positionAttr=dimList.indexOf(attBean.getId());
											String attrValue = "";
											//data table may contain less than the components of dsd
											if (positionAttr<data.length){
												attrValue = data[positionAttr];
											}							
											//get the position  that has in the mapping procedure
											String[] mappingDataAttr;
											if( availableCrossAttributes==null || !availableCrossAttributes.contains(attBean) ) {
												mappingDataAttr=mappingXSMap.get(attBean.getId());
											}else {
												String attributeMapId = measureConcept + "_" + attBean.getId();
												mappingDataAttr=mappingXSMap.get(attributeMapId);
											}
											
											final int mapposAttr=Integer.valueOf(mappingDataAttr[0]) -1;
											//put the attr value in the values List
											if (Integer.parseInt(params.getLevelNumber())== 1){
												values[mapposAttr]=attrValue;
											} else{
											    final int lev=Integer.parseInt(mappingDataAttr[2]);
												String valuesM[]= (String[]) valuesMap.get(lev);
												if (valuesM==null){
													valuesM=new String[IoUtils.getComponentNo(mappingXSMap, mappingDataAttr[2])+1];
													valuesM[mapposAttr]= attrValue;
												}else{
													valuesM[mapposAttr]=attrValue;
												}
												valuesM[0]=mappingDataAttr[2];
												valuesMap.put(lev, valuesM);
											}
										}
									}
								}
							}
						}
						brxs.close();

							//put in the List the rest components
							serkey=new StringBuilder();
							for (final ListIterator iter = keyFamilyBean.getDimensions().listIterator(); iter.hasNext();) {
							    final DimensionBean db = (DimensionBean) iter.next();
							    if (db.isTimeDimension()) {continue;}//time dimension skipped, treated at a later point
								cref = db.getId();
								//if it is not the measure dimension then
								if (!cref.equals(measureDimension)){
								    final int position=dimList.indexOf(cref);
								    final String crefValue=data[position];
									serkey.append(crefValue);
									serkey.append(";");
									//get the position  that has in the mapping procedure
									final String[] mappingData=mappingXSMap.get(cref);
									final int mappos=Integer.valueOf(mappingData[0]) -1;
									if (Integer.parseInt(params.getLevelNumber()) == 1) {
										values[mappos]=crefValue;
									} else {
									    final int lev=Integer.parseInt(mappingData[2].toString());
										serLevel=lev;
										String valuesM[]= (String[]) valuesMap.get(lev);
										if (valuesM==null){
											valuesM=new String[IoUtils.getComponentNo(mappingXSMap, mappingData[2].toString())+1];
											valuesM[mappos]= crefValue;
										}else{
											valuesM[mappos]=crefValue;
										}
										valuesM[0]=mappingData[2];
										valuesMap.put(lev, valuesM);

									}
								}
							}

							//series attributes
							if (Integer.parseInt(params.getLevelNumber()) > 1){
								for (AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getDimensionGroupAttributes()) {
									cref = ab.getId();
									final int position=dimList.indexOf(cref);
									if (position==-1 ||position>=data.length) {
										continue;
									}
									//attribute value
									final String attrValue=data[position];
									//get the position  that has in the mapping procedure
									final String[] mappingData=mappingXSMap.get(cref);
									final int mappos=Integer.valueOf(mappingData[0]) -1;
									final int lev=Integer.parseInt(mappingData[2].toString());
									String valuesM[]= (String[]) valuesMap.get(lev);
									if (valuesM==null){
										valuesM=new String[IoUtils.getComponentNo(mappingXSMap, mappingData[2].toString())+1];
										valuesM[mappos]= attrValue;
									}else{
										valuesM[mappos]=attrValue;
									}
									valuesM[0]=mappingData[2];
									valuesMap.put(lev, valuesM);
								}
							}
							//put time dimension

							if (keyFamilyBean.getTimeDimension() != null) {
								cref = keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
								final int position=dimList.indexOf(cref);
								final String timeValue=data[position];
								//get the position  that has in the mapping procedure
								final String[] mappingData=mappingXSMap.get(cref);
								final int mappos=Integer.valueOf(mappingData[0]) -1;
								if (Integer.parseInt(params.getLevelNumber()) == 1) {
									values[mappos]=timeValue;
								} else {
								    final int lev=Integer.parseInt(mappingData[2].toString());
									String valuesM[]= (String[]) valuesMap.get(lev);
									if (valuesM==null){
										valuesM=new String[IoUtils.getComponentNo(mappingXSMap, mappingData[2].toString())+1];
										valuesM[mappos]= timeValue;
									}else{
										valuesM[mappos]=timeValue;
									}
									valuesM[0]=mappingData[2];
									valuesMap.put(lev, valuesM);
								}
							}

							if (Integer.parseInt(params.getLevelNumber()) == 1) {
								flatKeyMap.put(line, values);
								if (flatKeyMap.size() == keySetLimit) {
									break;
								}
							}else{
								if (prevkey.toString().equals(serkey.toString())){
									valuesMap.remove(serLevel);
								}
								prevkey=new StringBuilder();
								for (int i=0;i<serkey.length();i++){
									prevkey.append(serkey.charAt(i));
								}

								flatKeyMapMultilevel.put(line, valuesMap);
								if (flatKeyMapMultilevel.size() == keySetLimit) {
									break;
								}
							}
					}

					parsedXKeys=rowCounter;
					brxt.close();
					tempKeysFile.delete();
					if (Integer.parseInt(params.getLevelNumber()) == 1) {
						if (flatKeyMap.isEmpty()) {
							// no more keys to process. Exit loop
							break;
						}
					}else{
						if (flatKeyMapMultilevel.isEmpty()) {
							// no more keys to process. Exit loop
							break;
						}
					}

					// print the records of the output (final) csv
					if (Integer.parseInt(params.getLevelNumber()) == 1) {
					    final Iterator<Entry<String, String[]>> iter = flatKeyMap.entrySet().iterator();
						while (iter.hasNext()) {
						    final Entry<String, String[]> entry = iter.next();
							for (int j=0;j<entry.getValue().length;j++){
								if (j!=0) {
									temppout.print(delimiter);
								}
								if (entry.getValue()[j]==null){
									temppout.print("");
								}else{
									temppout.print(entry.getValue()[j]);
								}
							}
							temppout.println();
						}
						temppout.flush();
						if (flatKeyMap.size() < keySetLimit) {
							break;// processed records < keySetLimit i.e. all
							// records have been processed. Exit loop
						}
					}else{
					    final Iterator<Entry<String, LinkedHashMap<Integer,String[]>>> iter = flatKeyMapMultilevel.entrySet().iterator();
						while (iter.hasNext()) {
						    final Entry<String, LinkedHashMap<Integer,String[]>> entry = iter.next();
						    final LinkedHashMap<Integer,String[]> map=entry.getValue();
							for (int j=1;j<=Integer.parseInt(params.getLevelNumber());j++){
							    final String[] data=map.get(j);
								if(data!=null){                                                           
									//print for the specific level the values
									for(int d=0;d<data.length;d++){
										if (d!=0) {
											temppout.print(delimiter);
										}
										if (data[d]!=null) {
											temppout.print(data[d]);
										}
									}
									temppout.println();
								}
							}                      
							//temppout.println();
						}
						temppout.flush();
						if (flatKeyMapMultilevel.size() < keySetLimit) {
							break;// processed records < keySetLimit i.e. all
							// records have been processed. Exit loop
						}
					}
				}catch (OutOfMemoryError e) {
					// on OutOfMemoryError reduce the keySetlimit to its half
					// value
					keySetLimit = flatKeyMap.size() / 2;
					temppout.close();
					brxs.close();
					tempCsvFile = File.createTempFile("tempFile", null);
					temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempCsvFile), params.getFlatFileEncoding()));
					parsedXKeys = 0;
					// if the available memory is small even for a single key
					// then nothing can be done
					if (keySetLimit > 0) {
						//IoUtils.getLogger().warning(prop.getProperty("Caught OutOfMemoryError :") + e.getMessage() + prop.getProperty(". New keySetLimit:") + keySetLimit);
						logger.warn("Caught OutOfMemoryError :" + e.getMessage() + ". New keySetLimit:" + keySetLimit);

					} else {
						tempCsvFile.delete();
						String errorMessage = "Caught OutOfMemoryError. The keySetLimit is already set to 1, cannot decrease further. Please increase the amount of memory available to java."
							+ lineEnd + "\t";
						throw new Exception(errorMessage + e.getMessage(), e);
					}
				}
			}
			temppout.flush();
			temppout.close();
			//the temp CSV file is ready.
			//write the csv
			//if the concept names should be written then print the first line with the concepts
			final StringBuilder strBfr=new StringBuilder();
			if (params.getHeaderRow().equals(Defs.USE_COLUMN_HEADERS)){
				if(!mapCrossXMeasures) {
					for(int i=0;i<computeMaxPositionFromMappingMap();i++){
						if (i != 0) {
							strBfr.append(delimiter);
						}
						if (metadatalineMap.get(i) != (null)){
							//check for target concept in the mapping
							String data[] = null;
							String[] da;
							if( !availableCrossDimsIds.isEmpty()&& availableCrossDimsIds.contains(metadatalineMap.get(i))) {
								da= mappingMap.get(keyFamilyBean.getPrimaryMeasure().getId());
							}else {
								da= mappingMap.get(metadatalineMap.get(i));
							}
							data = da;
							if (data.length == 4 && !data[3].equals("")){
								strBfr.append(data[3]);
							}else{
								strBfr.append(metadatalineMap.get(i));
							}
							
						}else{
							//check for target concept in the mapping
						    final String data[] = mappingMap.get(mappingMap.keySet().toArray()[i]);
							if (data.length == 4 && !data[3].equals("")){
								strBfr.append(data[3]);
							}else{
								strBfr.append(mappingMap.keySet().toArray()[i]);
							}
						}
					}
				} else {
					for(int i=0;i<computeMaxPositionFromMappingXSMap();i++){
						if (i != 0) {
							strBfr.append(delimiter);
						}
						
						String headerStr = (String) mappingXSMap.keySet().toArray()[i];
						
						//Fix the header for attributes that was attached in a XS measure dimension
						for(String dim : availableCrossDimsIds) {
							if(headerStr.contains(dim+"_")) {
								headerStr = headerStr.replace(dim+"_", "");
							}
						}
						
						//check for target concept in the mapping
					    final String data[] = mappingXSMap.get(mappingXSMap.keySet().toArray()[i]);
						if (data.length == 4 && !data[3].equals("")){
							strBfr.append(data[3]);
						}else{
							strBfr.append(headerStr);
						}
						
					}
				}
				pout.println(strBfr.toString());
			}
			brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempCsvFile), params.getFlatFileEncoding()));
			while ((line = brd.readLine()) != null) {
				pout.println(line);
			}
			pout.flush();
			pout.close();
			brd.close();
			os.close();
			tempCsvFile.delete();
			flatKeyMap = null;
			flatKeyMapMultilevel=null;
		}catch (Exception e) {
			String errorMessage = this.getClass().getName() + ".makeTempSdmxFile() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if (brxs != null) {brxs.close();}
			if (brxt != null) {brxt.close();}
			if (brd != null) {brd.close();}
			if (tempKeys != null) {tempKeys.close();}
			if (tempKeysFile != null) {tempKeysFile.delete();}			
		}
	}

	/**
	 * This method takes a line of the csv data 
	 * and finds all the same lines without comparing crossXmeasures
	 * @param keyLine
	 * @param fileInputStream
	 * @param flatFileEncoding
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	private List<String> collectAllSameLines(String keyLine, FileInputStream fileInputStream, String flatFileEncoding) throws UnsupportedEncodingException, IOException {
		List<String> sames = new ArrayList<String>();
		String flatKeyLine = getFlatKey(keyLine);
		
		try(BufferedReader bread = new BufferedReader(new InputStreamReader(fileInputStream, params.getFlatFileEncoding()))){
			String currentKeyLine = null;
			if (writeDataset) {
				currentKeyLine  = bread.readLine();
			}
			while ((currentKeyLine = bread.readLine()) != null) {
				String currentFlatKeyLine = getFlatKey(currentKeyLine);
				if(flatKeyLine.equals(currentFlatKeyLine)) {
					sames.add(currentKeyLine);
				}
			}
		}finally {
			if(fileInputStream!=null) {
				fileInputStream.close();
			}
		}
		return sames;
	}

	/**
	 * Method that returns the partial key of the line, i.e flat key, without the crosX measure
	 * @param line
	 * @return
	 */
	private String getFlatKey(final String line) {
		String cref=null;
		final String data[]=line.split(";");
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < IoUtils.dataStructureDimensionSize(keyFamilyBean); i++) {
			cref = keyFamilyDimensionNames.get(i).getId();
			if (!measureDimension.equals(cref) && !keyFamilyDimensionNames.get(i).isTimeDimension()){
			    final int position=dimList.indexOf(cref);
				sb.append(data[position]);
				sb.append(";");
			}
		}
		if (keyFamilyBean.getTimeDimension()!=null){
			cref=keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
			final int position=dimList.indexOf(cref);
			sb.append(data[position]);
			sb.append(";");
		}
		String lineKey=sb.toString();
		lineKey=lineKey.substring(0,lineKey.length()-1);
		return lineKey;
	}

	public void setKeyFamilyBean(final DataStructureBean keyFamilyBean) {
		this.keyFamilyBean = keyFamilyBean;
	}

	/* Optional for writing data */
	public void setDelimiter(final String delimiter) {
		this.delimiter = delimiter;
	}

	/* Optional for writing data */
	public void setDateCompatibility(final String dateCompatibility) {
		this.dateCompatibility = dateCompatibility;
	}
	
	/* Optional for writing data */
	public void setMappingMap(final Map<String, String[]> mappingMap) {
		this.mappingMap = mappingMap;
	}
	
	private void populateDsdRelatedProperties() {
		if (mappingMap!=null) {
			//mapCrossXMeasures = IoUtils.mapCrossXMeasures(keyFamilyBean , mappingMap);
			mapCrossXMeasures=false;
		}
		if (delimiter == null) {
			delimiter = ";";
		}
		if (dateCompatibility == null) {
			dateCompatibility = "SDMX";
		}
		if (mappingMap == null) {
			mappingMap = new LinkedHashMap<String, String[]>();
			
			//Map is used when mapCrossXMeasures is enabled
			mappingXSMap = new LinkedHashMap<String, String[]>();
			int i = 1;
			int j = 1;
			int count = 1;
			int countLvl = 1;
			String cref = null;
			Boolean xsMeasureFound = false;
			if (Integer.parseInt(params.getLevelNumber()) > 1) {
				// dataset attributes
				int levelNo = 1;
				// which level belongs to groups
				int groupLevel = 1;
				// which level belongs to series
				int seriesLevel;
				for (final AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getDatasetAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(i++), "false", String.valueOf(levelNo) };
					mappingMap.put(cref, attrdata);
					final String[] attrXSdata = { String.valueOf(count++), "false", String.valueOf(levelNo) };
					mappingXSMap.put(cref, attrXSdata);
				}
				if (keyFamilyBean.getDatasetAttributes().size() > 0) {
					levelNo++;
					i = 1;
				}
				// group attributes
				for (final AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getGroupAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(i++), "false", String.valueOf(levelNo) };
					mappingMap.put(cref, attrdata);
					final String[] attrXSdata = { String.valueOf(count++), "false", String.valueOf(levelNo) };
					mappingXSMap.put(cref, attrXSdata);
				}
				final ArrayList<String> groupConceptRefs = new ArrayList<String>();
				for (final GroupBean gb : (List<GroupBean>) keyFamilyBean.getGroups()) {
					for (int k = 0; k < gb.getDimensionRefs().size(); k++) {
						groupConceptRefs.add(gb.getDimensionRefs().get(k).toString());
					}
				}
				if (!keyFamilyBean.getGroupAttributes().isEmpty() || !groupConceptRefs.isEmpty()) {
					groupLevel = levelNo;
					seriesLevel = groupLevel++;
				} else {
					seriesLevel = levelNo;
				}
				// dimensions
				for (final ListIterator<DimensionBean> it = keyFamilyBean.getDimensions().listIterator(); it.hasNext();) {
				    final DimensionBean db = (DimensionBean) it.next();
				    if (db.isTimeDimension()) {continue;} //time dimension is treated at a later point
				    //If the dimension found is Measure and we need to map the XS measures
				    if (db.isMeasureDimension() && mapCrossXMeasures) {
				    	int k = count;
				    	int l = countLvl;
				    	cref = db.getId();
				    	count = mapXmeasuresMultiLvl(k, l, groupConceptRefs, groupLevel, seriesLevel, cref);
				    	
				    	// A cross sectional dim was added
				    	if(k!=count) {
				    		xsMeasureFound = true;
				    	}
				    	final String[] data = { String.valueOf(i++), "false", "" };
				    	mappingMap.put(cref, data);
				    }else {
				    	cref = db.getId();
						final String data[] = new String[3];
						if (groupConceptRefs.contains(cref)) {
							data[0] = String.valueOf(i++);
							data[1] = "false";
							data[2] = String.valueOf(groupLevel);
						} else {
							data[0] = String.valueOf(j++);
							data[1] = "false";
							data[2] = String.valueOf(seriesLevel);
						}
						mappingMap.put(cref, data);
				    }
				    
					
				}
				// series attrinbutes
				for (final AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getDimensionGroupAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(j++), "false", String.valueOf(seriesLevel) };
					mappingMap.put(cref, attrdata);
					//Add it only if observation is not a cross sectional observation and was added before
					if( availableCrossAttributes==null || !availableCrossAttributes.contains(ab) ) {
						final String[] dataXs = { String.valueOf(countLvl++), "false", String.valueOf(seriesLevel) };
				    	mappingXSMap.put(cref, dataXs);
					}
				}
				seriesLevel++;
				j = 1;
				// time dimension
				if (keyFamilyBean.getTimeDimension() != null) {
					cref = keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
					final String[] data = { String.valueOf(j++), "false", String.valueOf(seriesLevel) };
					mappingMap.put(cref, data);
					final String[] dataXs = { String.valueOf(count++), "false", String.valueOf(seriesLevel) };
			    	mappingXSMap.put(cref, dataXs);
				}
				// primary measure
				final String[] data = { String.valueOf(j++), "false", String.valueOf(levelNo) };
				mappingMap.put(keyFamilyBean.getPrimaryMeasure().getId(), data);
				final String[] dataXsPr = { String.valueOf(countLvl++), "false", String.valueOf(levelNo) };
		    	mappingXSMap.put(keyFamilyBean.getPrimaryMeasure().getId(), dataXsPr);
		    	
				// observation attributes
				for (AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getObservationAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(j++), "false", String.valueOf(seriesLevel) };
					mappingMap.put(cref, attrdata);
					//Add it only if observation is not a cross sectional observation and was added before
					if( availableCrossAttributes==null || !availableCrossAttributes.contains(ab) ) {
						final String[] dataXs = { String.valueOf(countLvl++), "false", String.valueOf(seriesLevel) };
				    	mappingXSMap.put(cref, dataXs);
					}
				}
			} else {
				// dimensions
				for (final ListIterator it = keyFamilyBean.getDimensions().listIterator(); it.hasNext();) {
				    final DimensionBean db = (DimensionBean) it.next();
				    if (db.isTimeDimension()) {continue;} //time dimension skipped, treated at a later point

				    //If the dimension found is Measure and we need to map the XS measures
				    if (db.isMeasureDimension() && mapCrossXMeasures) {
				    	int k = count;
				    	count = mapXmeasures(k);
				    	//A cross sectional dim was added
				    	if(k!=count) {
				    		xsMeasureFound = true;
				    	}
				    	cref = db.getId();
				    	final String[] data = { String.valueOf(i++), "false", "" };
				    	mappingMap.put(cref, data);
				    }else {
					    cref = db.getId();
				    	final String[] data = { String.valueOf(i++), "false", "" };
				    	mappingMap.put(cref, data);
				    	final String[] dataXs = { String.valueOf(count++), "false", "" };
				    	mappingXSMap.put(cref, dataXs);
				    }
				}
				// time dimension
				// dsd without time dimension
				if (keyFamilyBean.getTimeDimension() != null) {
					cref = keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
					final String[] data = { String.valueOf(i++), "false", "" };
			    	mappingMap.put(cref, data);
			    	final String[] dataXs = { String.valueOf(count++), "false", "" };
			    	mappingXSMap.put(cref, dataXs);
				}
				
				// primary measure
				final String[] data = { String.valueOf(i++), "false", "" };
				mappingMap.put(keyFamilyBean.getPrimaryMeasure().getId(), data);
				if(!xsMeasureFound) {
					final String[] dataXs = { String.valueOf(count++), "false", "" };
			    	mappingXSMap.put(keyFamilyBean.getPrimaryMeasure().getId(), dataXs);
				}
				
				// observation attributes
				for (AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getObservationAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(i++), "false", "" };
					mappingMap.put(cref, attrdata);
					//Add it only if observation is not a cross sectional observation and was added before
					if( availableCrossAttributes==null || !availableCrossAttributes.contains(ab) ) {
						final String[] dataXs = { String.valueOf(count++), "false", "" };
				    	mappingXSMap.put(cref, dataXs);
					}
				}
				
				// dataset attributes
				for (AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getDatasetAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(i++), "false", "" };
					mappingMap.put(cref, attrdata);
					//Add it only if observation is not a cross sectional observation and was added before
					if( availableCrossAttributes==null || !availableCrossAttributes.contains(ab) ) {
						final String[] dataXs = { String.valueOf(count++), "false", "" };
				    	mappingXSMap.put(cref, dataXs);
					}
				}
				
				// group attributes
				for (AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getGroupAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(i++), "false", "" };
					mappingMap.put(cref, attrdata);
					//Add it only if observation is not a cross sectional observation and was added before
					if( availableCrossAttributes==null || !availableCrossAttributes.contains(ab) ) {
						final String[] dataXs = { String.valueOf(count++), "false", "" };
				    	mappingXSMap.put(cref, dataXs);
					}
				}
				
				// series attributes
				for (AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getDimensionGroupAttributes()) {
					cref = ab.getId();
					final String[] attrdata = { String.valueOf(i++), "false", "" };
					mappingMap.put(cref, attrdata);
					//Add it only if observation is not a cross sectional observation and was added before
					if( availableCrossAttributes==null || !availableCrossAttributes.contains(ab) ) {
						final String[] dataXs = { String.valueOf(count++), "false", "" };
				    	mappingXSMap.put(cref, dataXs);
					}
				}
			}
			// debug
			// for (String c :
			// componentOrder.keySet()){System.out.println(componentOrder.get(c)+" "+c);}
		}
		String[] mappingData;
		String crefOrder;
		String fixedValue;
		if (!mapCrossXMeasures){
			// int containing the place of the primary measure in a csv line
			mappingData = mappingMap.get(keyFamilyBean.getPrimaryMeasure().getId());
			crefOrder = mappingData[0];
			// mappingMap.get(keyFamilyBean.getPrimaryMeasure().getConceptRef());
			fixedValue = mappingData[1];
			if ("false".equals(fixedValue)) {
				if (!"".equals(crefOrder)) {
					primMeasurePlace = Integer.parseInt(mappingData[0]) - 1;
					// primMeasurePlace =
					// Integer.parseInt(mappingMap.get(keyFamilyBean.getPrimaryMeasure().getConceptRef())) - 1;
				} else {
					primMeasurePlace = -1;
				}
			} else {
				primMeasurePlace = -2;
			}
		}
		// int containing the place of the time dimension in a csv line
		if (keyFamilyBean.getTimeDimension() != null) {
			mappingData = mappingMap.get(keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
			crefOrder = mappingData[0];
			fixedValue = mappingData[1];
			if ("false".equals(fixedValue)) {
				timeMeasurePlace = Integer.parseInt(mappingData[0]) - 1;
			} else {
				timeMeasurePlace = -2;
			}
		} else {
			timeMeasurePlace = -2;
		}
	}
	
	public void printSeriesKey(final TimeseriesKey serie) throws Exception {
		String finalvalue = null;// a variable that holds the value from a transcoding rule
		LinkedHashMap<Integer, String> lineMap = new LinkedHashMap<Integer, String>();


		final StringBuilder strB=new StringBuilder();
		final OutputStream out;
		if (!writeDataset && !writeGroup) {
			metr=0;
		} else if (writeDataset && !writeGroup) {
			metr=metrDts;
		}
		//dimList=new ArrayList();
		if (mapCrossXMeasures){
			measureDimension = IoUtils.getMeasureDimension(keyFamilyBean);
		}
		try {
						
			//keep all optional attributes. If an optional attribute will be missed in the next series we need to clean recordData such as its value
			//will not be written in the output.			
			Set<String> optionalAttributeNames = new HashSet<String>();						
			
			for(AttributeBean attrBean:keyFamilyBean.getAttributes()) {
				if(attrBean.getAttachmentLevel() == ATTRIBUTE_ATTACHMENT_LEVEL.DIMENSION_GROUP) {
					optionalAttributeNames.add(attrBean.getId());
				}
			}
			
			for(Object obj:keyFamilyBean.getObservationAttributes()) {
				AttributeBean attrBean = (AttributeBean)obj;								
					optionalAttributeNames.add(attrBean.getId());				
			}
			
			for (final Entry<String, String> entry : serie.getKeyValues().entrySet()) {
				// check if this dimension has transcoding rules
			    final boolean has_rules = this.transcoding.hasTranscodingRules(entry.getKey());
			    
				// Get the value from the transcoding rule
				if (has_rules) {
					finalvalue = this.transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
				} else {
					finalvalue = entry.getValue();
				}
				
				//hold a temp mapping
				if (mapCrossXMeasures){
					if (!dimList.contains(entry.getKey())) {
						dimList.add(entry.getKey());
					}
					lineMapX.put(metr, finalvalue);
					metr++;
                    if (!entry.getKey().equals(measureDimension)) {
                        metadatalineMap.put(Integer.valueOf(mappingMap.get(entry.getKey())[0]) - 1, entry.getKey());
                    }
				}
				//put for example all the keys in the same order with the values.
				//if (mapCrossXMeasures && !measureDimension.equals(entry.getKey()))
				//currKey.append(finalvalue);
				if (!mapCrossXMeasures){
				    final String[] mappingData = mappingMap.get(entry.getKey());
					if (Integer.parseInt(params.getLevelNumber()) == 1) {
						if (mappingData[1].equals("false")) {
							lineMap.put(Integer.valueOf(mappingData[0]) - 1, finalvalue);
							metadatalineMap.put(Integer.valueOf(mappingData[0]) - 1, entry.getKey());
						}
					} else {// only for multilevel data
						if (recordData.get(mappingData[2]) != null) {
							lineMap = new LinkedHashMap<Integer, String>();
							lineMap = recordData.get(mappingData[2]);
						} else {
							lineMap = new LinkedHashMap<Integer, String>();
						}
						lineMap.put(0, mappingData[2]);
						lineMap.put(Integer.valueOf(mappingData[0]) - 1, finalvalue);
						recordData.put(mappingData[2], lineMap);
					}
				}
				// lineMap.put(Integer.valueOf(mappingMap.get(entry.getKey())) - 1, finalvalue);
			}
			
			if (!writeGroup && !writeDataset) {
				recordData2 = new LinkedHashMap<String, String>();
			}
			
			for (final Entry<String, String> entry : serie.getAttributeValues().entrySet()) {
				//check if the current entry is an optional attribute. If it has a value set for it remove the attribute 
				//from the set. The remaining optional attributes which doesn't have a value in the current time series should be cleared before
				//writting.
				optionalAttributeNames.remove(entry.getKey());
				// check if this dimension has transcoding rules
			    final boolean has_rules = this.transcoding.hasTranscodingRules(entry.getKey());
				// Get the value from the transcoding rule
				if (has_rules) {
					finalvalue = this.transcoding.getValueFromTranscoding(entry.getKey(), entry.getValue());
				} else {
					finalvalue = entry.getValue();
				}
				if (mapCrossXMeasures){
					if (!dimList.contains(entry.getKey())) {
						dimList.add(entry.getKey());
					}
					lineMapX.put(metr, finalvalue);
					metr++;
				} else if (Integer.parseInt(params.getLevelNumber()) == 1) {
					if (primMeasurePlace != -1 && primMeasurePlace != -2) {
						if (mappingMap.get(entry.getKey()) != null && mappingMap.get(entry.getKey())[0] != null &&
								!"".equals(mappingMap.get(entry.getKey())[0])) {
							lineMap.put(Integer.parseInt(mappingMap.get(entry.getKey())[0])-1, finalvalue);
							metadatalineMap.put(Integer.parseInt(mappingMap.get(entry.getKey())[0])-1, entry.getKey());
						}
					}
				} else { 
					final String[] mappingData = mappingMap.get(entry.getKey());
					if (recordData.get(mappingData[2]) != null) {
						lineMap = new LinkedHashMap<Integer, String>();
						lineMap = recordData.get(mappingData[2]);
					} else {
						lineMap = new LinkedHashMap<Integer, String>();
					}
					lineMap.put(0, mappingData[2]);
					lineMap.put(Integer.valueOf(mappingData[0]) - 1, finalvalue);
					recordData.put(mappingData[2], lineMap);
				}
			}
			
			for (final Observation ob : serie.getObservations()) {
				
				for(Object obj:keyFamilyBean.getObservationAttributes()) {
					AttributeBean attrBean = (AttributeBean)obj;									
						optionalAttributeNames.add(attrBean.getId());					
				}
				
				String obsValue = ob.getValue();
				if (((obsValue == null)) || (obsValue.equals("null"))) {// ||
					// (obsValue.equalsIgnoreCase
					// ("NaN"))) {
					obsValue = "";
				}
				if (mapCrossXMeasures){
					if (!dimList.contains(keyFamilyBean.getPrimaryMeasure().getConceptRef())) {
						dimList.add(keyFamilyBean.getPrimaryMeasure().getConceptRef());
					}
					primMeasurePlace=metr;
					lineMapX.put(primMeasurePlace, obsValue);
					//metadatalineMap.put(primMeasurePlace, keyFamilyBean.getPrimaryMeasure().getConceptRef());
				}else{
					if (Integer.parseInt(params.getLevelNumber()) == 1) {
						if (primMeasurePlace != -1 && primMeasurePlace != -2) {
							lineMap.put(primMeasurePlace, obsValue);
							metadatalineMap.put(primMeasurePlace, keyFamilyBean.getPrimaryMeasure().getId());
						}
					} else {// only for multilevel data
					    final String[] mappingData = mappingMap.get(keyFamilyBean.getPrimaryMeasure().getId());
						if (recordData.get(mappingData[2]) != null) {
							lineMap = new LinkedHashMap<Integer, String>();
							lineMap = recordData.get(mappingData[2]);
						} else {
							lineMap = new LinkedHashMap<Integer, String>();
						}
						lineMap.put(0, mappingData[2]);
						if (primMeasurePlace != -1 && primMeasurePlace != -2) {
							lineMap.put(primMeasurePlace, obsValue);
						}
						recordData.put(mappingData[2], lineMap);
					}
				}
				String obsTimeValue = ob.getTimeValue();
				//if a Time Transcoder exist, then the csv is included in a DSPL bundle
				if(params.getTimeTranscoder() != null){
					obsTimeValue = params.getTimeTranscoder().timeToDSPLTranscode(ob.getTimeValue());
				}
				if ("GESMES".equals(dateCompatibility)) {
					obsTimeValue = obsTimeValue.replace("-", "");
				}
				if (keyFamilyBean.getTimeDimension() != null) {// no time dimension
					if (mapCrossXMeasures){
						if (timeMeasurePlace != -2) {
							metadatalineMap.put(timeMeasurePlace, keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
							if (!dimList.contains(keyFamilyBean.getTimeDimension().getConceptRef().getFullId())) {
								dimList.add(keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
							}
							timeMeasurePlace=primMeasurePlace+1;
							lineMapX.put(timeMeasurePlace,obsTimeValue);
						}else{
							timeMeasurePlace=primMeasurePlace;
						}
					}else{
						if (timeMeasurePlace != -2) {
							if (Integer.parseInt(params.getLevelNumber()) == 1) {
								lineMap.put(timeMeasurePlace, obsTimeValue);
								metadatalineMap.put(timeMeasurePlace, keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
							} else { // only for multilevel files
							    final String[] mappingData = mappingMap.get(keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
								if (recordData.get(mappingData[2]) != null) {
									lineMap = new LinkedHashMap<Integer, String>();
									lineMap = recordData.get(mappingData[2]);
								} else {
									lineMap = new LinkedHashMap<Integer, String>();
								}
								lineMap.put(0, mappingData[2]);
								lineMap.put(timeMeasurePlace, obsTimeValue);
								recordData.put(mappingData[2], lineMap);
							}
						}
					}
				}//end if time dimension not null
				
				final Map<String, String> attributeValues = ob.getAttributeValues();
				for (final AttributeBean attBean : keyFamilyBean.getObservationAttributes()) {
					
					optionalAttributeNames.remove(attBean.getId());
					
				    final String[] attributeData = mappingMap.get(attBean.getId());
					// check for non mapped component
				    final String attrOrder = attributeData[0];
					// String attrOrder = mappingMap.get(name);
					int attrPlace = 0;
					if (attributeData[1].equals("false")) {
						if (!"".equals(attrOrder)) {
							// attrPlace = Integer.parseInt(mappingMap.get(name)) - 1;
							attrPlace = Integer.parseInt(attributeData[0]) - 1;
						} else {
							attrPlace = -1;
						}
					} else {
						attrPlace = -2;
					}
					// check if this dimension has transcoding rules
					final boolean has_rules = this.transcoding.hasTranscodingRules(attBean.getId());
					// Get the value from the transcoding rule
					if (has_rules) {
						finalvalue = this.transcoding.getValueFromTranscoding(attBean.getId(), attributeValues.get(attBean.getId()));
					} else {
						finalvalue = attributeValues.get(attBean.getId());
					}
					// check for non mapped component
					if (attrPlace != -1 && attrPlace != -2) {
						if(mapCrossXMeasures){
							if (!dimList.contains(attBean.getId())) {
								dimList.add(attBean.getId());
							}
							lineMapX.put(++timeMeasurePlace, (attributeValues.get(attBean.getId()) == null ? "" : finalvalue));
							metadatalineMap.put(attrPlace, attBean.getId());

						}else{
							if (Integer.parseInt(params.getLevelNumber()) == 1) {
								lineMap.put(attrPlace, (attributeValues.get(attBean.getId()) == null ? "" : finalvalue));
								metadatalineMap.put(attrPlace,attBean.getId());
							} else { // only for multilevel files
								if (recordData.get(attributeData[2]) != null) {
									lineMap = new LinkedHashMap<Integer, String>();
									lineMap = recordData.get(attributeData[2]);
								} else {
									lineMap = new LinkedHashMap<Integer, String>();
								}
								lineMap.put(0, attributeData[2]);
								lineMap.put(attrPlace, (attributeValues.get(attBean.getId()) == null ? "" : finalvalue));
								recordData.put(attributeData[2], lineMap);
							}
						}
					}
				}
				
				//before writing check if there are optional attribute which does not have any value in this series. 
				//if so clear their values from recordata (in record date could be stored values from the previous series.)
				for(String optAttrName:optionalAttributeNames) {
					final String[] attributeData = mappingMap.get(optAttrName);
					if (Integer.parseInt(params.getLevelNumber()) == 1) {
						if (attributeData[0] != null && !"".equals(attributeData[0])) {
							lineMap.put(Integer.valueOf(attributeData[0]) - 1, "");
						}
					} else {
						if(attributeData != null && attributeData.length > 2 && recordData.get(attributeData[2])!=null) {	
							lineMap = recordData.get(attributeData[2]);
							if(lineMap !=null) {
								lineMap.put(Integer.valueOf(attributeData[0]) - 1, "");
							}									
						}
					}
				}
				
				
				//check if lineMap misses information from record data and add it - 
				//this means it is flat file and data set attributes or group attributes need to be added
				//SDMXCONV-9
				if (Integer.parseInt(params.getLevelNumber()) == 1) {
					for (String keyRecordData : recordData.keySet()) {
						for (Integer keySubrecordData : recordData.get(keyRecordData).keySet()) {
							if (!lineMap.keySet().contains(keySubrecordData)) {
								lineMap.put(keySubrecordData, recordData.get(keyRecordData).get(keySubrecordData));
							}
						}
					}
				}
				
				//StringBuilder strBfr = new StringBuilder();		
				List<String> strBfr = new ArrayList<String>();
				
				if (Integer.parseInt(params.getLevelNumber()) == 1) {
					if (mapCrossXMeasures){
						for(int i=0;i<dimList.size();i++){
							/*
							if (strBfr.length() > 0) {
								strBfr.append(";");
							}*/							
							//strBfr.append(lineMapX.get(i));
							strBfr.add(lineMapX.get(i));
						}
						String outputLine = getCSVOutputLine(strBfr, false);
						//temppout.println(strBfr.toString());
						temppout.print(outputLine);
					}else{
						//if the concept names should be written then print the first line with the concepts
						if (params.getHeaderRow().equals(Defs.USE_COLUMN_HEADERS) && !metadataWritten){
							for (int i = 0; i < computeMaxPositionFromMappingMap(); i++) {
								/*
								if (strBfr.length() > 0) {
									strBfr.append(delimiter);
								}*/
								if (metadatalineMap.get(i) != (null)){
									//check for target concept in the mapping
								    final String data[] = mappingMap.get(metadatalineMap.get(i));
									if (data.length == 4 && !data[3].equals("")){
										//strBfr.append(data[3]);
										strBfr.add(data[3]);
									}else{
										//strBfr.append(metadatalineMap.get(i));
										strBfr.add(metadatalineMap.get(i));
									}
									
								}else{
									//check for target concept in the mapping
									for (String key: mappingMap.keySet()) {
										final String data[] = mappingMap.get(key);
										if (data != null  && data[0].equals(String.valueOf(i+1))){
											//strBfr.append(key);
											strBfr.add(key);
										}
									}
								}
									
							}
							String outputLine = getCSVOutputLine(strBfr, true);
							//pout.println(strBfr.toString());
							pout.print(outputLine);
							//strBfr = new StringBuilder();
							strBfr = new ArrayList<String>();
							metadataWritten=true;
						}

						for (int i = 0; i < computeMaxPositionFromMappingMap(); i++) {
							/*
							if (strBfr.length() > 0) {
								strBfr.append(delimiter);
							} */
							
							if (lineMap.get(i) != (null)) {
								//strBfr.append(lineMap.get(i));
								strBfr.add(lineMap.get(i));
							} else {
								//strBfr.append("");
								strBfr.add("");
							}
						}
						
						//pout.println(strBfr.toString());
						pout.print(getCSVOutputLine(strBfr, false));												
					}
				} else {// only for multilevel files
					if (mapCrossXMeasures){
						for(int i=0;i<dimList.size();i++){
							/*
							if (i != 0) {
								strBfr.append(";");
							} */
							//strBfr.append(lineMapX.get(i));
							strBfr.add(lineMapX.get(i));
						}
						//temppout.println(strBfr.toString());
						temppout.print(getCSVOutputLine(strBfr, false));
					}else{
						for (int j = 0; j < recordData.size(); j++) {
							LinkedHashMap<Integer, String> dataValues = recordData.get(String.valueOf(j + 1));
							int size = computeMax(dataValues.keySet()) +1;
							for (int i = 0; i < size; i++) {
								/*
								if (i != 0) {
									strBfr.append(delimiter);
								} */
								final LinkedHashMap<Integer, String> data = recordData.get(String.valueOf(j + 1));
								if (data.get(i) != null) {
									//strBfr.append(data.get(i));
									strBfr.add(data.get(i));
								} else {
									//strBfr.append("");
									strBfr.add("");
								}
							}
							//if (strBfr.length() > 0) {
							if (strBfr.size() > 0) {
								if (recordData2.get(String.valueOf(j + 1)) == null) {
									//pout.println(strBfr.toString());
									pout.print(getCSVOutputLine(strBfr, false));
								} else if (!strBfr.toString().equals(recordData2.get(String.valueOf(j + 1)))) {
									//pout.println(strBfr.toString());
									pout.print(getCSVOutputLine(strBfr, false));
								}
							}
							recordData2.put(String.valueOf(j + 1), strBfr.toString());
							//strBfr = new StringBuilder();
							strBfr = new ArrayList<String>();
						}
					}
				}
			}
		} catch (NumberFormatException nfe) {
			String errorMessage = this.getClass().getName() + ".printSeriesKey() exception :" + "Parsing numeric information has failed." + lineEnd
			+ "\t";
			throw new NumberFormatException(errorMessage + nfe.getMessage());
		} catch (CSVWriterValidationException e) {
			throw e;
		}
	}
	
	
	/**
	 * Method called when Measure dimension is found
	 * and mapCrossXMeasures is enabled
	 * adds in the MappingMap crossXdimensions and its attributes
	 */
	private int mapXmeasures(int i) {
		if (keyFamilyBean instanceof CrossSectionalDataStructureBean) {		
		List<ComponentBean> crossSectionalAttachObservations = ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalAttachObservation();
		List<CrossSectionalMeasureBean> crossSectionalAttachDimensionBeans = ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures();
		availableCrossAttributes = new LinkedList<AttributeBean>();
		availableCrossDims = new LinkedList<CrossSectionalMeasureBean>();
		availableCrossDimsIds = new LinkedList<String>();
		for (CrossSectionalMeasureBean measureBean : crossSectionalAttachDimensionBeans) {
		 		final String[] data = { String.valueOf(i++), "false", "" };
				mappingXSMap.put(measureBean.getId(),data);
				availableCrossDims.add(measureBean);	
				availableCrossDimsIds.add(measureBean.getId());
				if(!crossSectionalAttachObservations.isEmpty()) {
					for (ComponentBean measureBeanObs : crossSectionalAttachObservations) {
						AttributeBean attrBean = (AttributeBean) measureBeanObs;
						List<CrossSectionalMeasureBean> measuresFromAttributes = ((CrossSectionalDataStructureBean)keyFamilyBean).getAttachmentMeasures(attrBean);
						if(measuresFromAttributes.contains(measureBean)) {
							final String[] dataAttr = { String.valueOf(i++), "false", "" };
							mappingXSMap.put(measureBean.getId() +"_"+ attrBean.getId(), dataAttr);
							availableCrossAttributes.add(attrBean);
						}
					}
				}
			}
		}
		return i;
	}
	
	private int mapXmeasuresMultiLvl(int i, int l, ArrayList<String> groupConceptRefs, int groupLevel, int seriesLevel, String cref) {
		if (keyFamilyBean instanceof CrossSectionalDataStructureBean) {		
		List<ComponentBean> crossSectionalAttachObservations = ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalAttachObservation();
		List<CrossSectionalMeasureBean> crossSectionalAttachDimensionBeans = ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures();
		availableCrossAttributes = new LinkedList<AttributeBean>();
		availableCrossDims = new LinkedList<CrossSectionalMeasureBean>();
		availableCrossDimsIds = new LinkedList<String>();
		for (CrossSectionalMeasureBean measureBean : crossSectionalAttachDimensionBeans) {
				final String data[] = new String[3];
				if (groupConceptRefs.contains(measureBean.getId())) {
					data[0] = String.valueOf(i++);
					data[1] = "false";
					data[2] = String.valueOf(groupLevel);
				} else {
					data[0] = String.valueOf(l++);
					data[1] = "false";
					data[2] = String.valueOf(seriesLevel);
				}
				mappingXSMap.put(measureBean.getId(), data);	
				availableCrossDims.add(measureBean);	
				availableCrossDimsIds.add(measureBean.getId());
				if(!crossSectionalAttachObservations.isEmpty()) {
					for (ComponentBean measureBeanObs : crossSectionalAttachObservations) {
						AttributeBean attrBean = (AttributeBean) measureBeanObs;
						List<CrossSectionalMeasureBean> measuresFromAttributes = ((CrossSectionalDataStructureBean)keyFamilyBean).getAttachmentMeasures(attrBean);
						if(measuresFromAttributes.contains(measureBean)) {
							final String dataAttr[] = new String[3];
							if (groupConceptRefs.contains(measureBean.getId())) {
								data[0] = String.valueOf(i++);
								data[1] = "false";
								data[2] = String.valueOf(groupLevel);
							} else {
								data[0] = String.valueOf(l++);
								data[1] = "false";
								data[2] = String.valueOf(seriesLevel);
							}
							mappingXSMap.put(measureBean.getId() +"_"+ attrBean.getId(), dataAttr);
							availableCrossAttributes.add(attrBean);
						}
					}
				}
			}
		}
		return i;
	}
	
	private String getCSVOutputLine(List<String> input, boolean isHeader) throws Exception{
		if (this.params.isCSVOuputEscaped()) {			
			StringWriter sw = new StringWriter();
			String[] entries = input.toArray(new String[input.size()]);	
			CSVWriter writer;
			if (isHeader) {
			    writer = new CSVWriter(sw, this.delimiter.charAt(0),'\u0000');
			} else {
				writer = new CSVWriter(sw, this.delimiter.charAt(0));
			}
			writer.writeNext(entries);			
			writer.close();			
			return sw.toString();
		} else {
			StringBuilder output = new StringBuilder();
			for (String entry:input) {
				if(entry.contains(delimiter)) {
					String errorMessage = "The following data contains CSV delimiter: " + entry;					
					throw new CSVWriterValidationException(109,errorMessage);
				} else if ( entry.startsWith("\"") || entry.endsWith("\"")) {
					String errorMessage = "The following data have to be unescaped: " + entry;
					throw new CSVWriterValidationException(109,errorMessage);
				}
				output.append(entry);				
				output.append(delimiter);
			}
			String line = output.toString();
			if (line.length() > 0) {
				line = line.substring(0,line.length()-1);
			}
			return line.concat("\n");
		}
		
	}		
	
	/**
	 * It returns the biggest integer from the set of keys
	 * @param keys
	 * @return
	 */
	private int computeMax(Set<Integer> keys) {
		int max = 0;
		
		mappingMap.keySet();
		//in case mapping map is defined 
		if (mappingMap != null && computeMaxLevelsFromMappingMap() <= 1) { 
			return mappingMap.size();
		}
		
		for (Integer key : keys){
			if (key > max){
				max = key;
			}
		}
		return max;
	}
	
	/**
	 * It returns the biggest level value from the mapping map
	 * @return the biggest level value from mapping map
	 */
	private int computeMaxLevelsFromMappingMap() { 
		int maxLevel = 0;
		
		for (String key: mappingMap.keySet()) {
			String level = mappingMap.get(key)[2];
			if (level != null && !"".equals(level.trim())) {
				int intLevel = Integer.valueOf(level);
				if (maxLevel < intLevel) maxLevel = intLevel;
			}
		}
		
		return maxLevel;
	}
	
	
	/**
	 * It returns the biggest position value from the mapping map
	 * This method should only be used for flat files
	 * @return the biggest position value from mapping map
	 */
	private int computeMaxPositionFromMappingMap() { 
		int maxLevel = 0;
		
		for (String key: mappingMap.keySet()) {
			String position = mappingMap.get(key)[0];
			if (position != null && !"".equals(position.trim())) {
				int intLevel = Integer.valueOf(position);
				if (maxLevel < intLevel) maxLevel = intLevel;
			}
		}
		
		return maxLevel;
	}
	
	/**
	 * It returns the biggest position value from the mapping map
	 * This method should only be used for flat files
	 * @return the biggest position value from mapping map
	 */
	private int computeMaxPositionFromMappingXSMap() { 
		int maxLevel = 0;
		
		for (String key: mappingXSMap.keySet()) {
			String position = mappingXSMap.get(key)[0];
			if (position != null && !"".equals(position.trim())) {
				int intLevel = Integer.valueOf(position);
				if (maxLevel < intLevel) maxLevel = intLevel;
			}
		}
		
		return maxLevel;
	}
		
	public void setTranscodingMap(final LinkedHashMap<String, LinkedHashMap<String, String>> structureSet) {
		this.transcoding = new TranscodingEngine(structureSet);
		this.transcodingMap = structureSet;
	}
	
	public void releaseResources() throws Exception {
		if(temppout != null) {temppout.close();}		
		if (tempCsvFile != null) {
			tempCsvFile.delete();
		}
		if (tempFlatXSKeyFile != null) {
			tempFlatXSKeyFile.delete();
		}
		
	}

	public Map<String, String[]> getMappingMap() {
		return mappingMap;
	}
}