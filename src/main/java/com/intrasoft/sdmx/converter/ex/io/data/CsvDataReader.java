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

import au.com.bytecode.opencsv.CSVReader;
import com.intrasoft.sdmx.converter.ex.model.data.DataSet;
import com.intrasoft.sdmx.converter.ex.model.data.GroupKey;
import com.intrasoft.sdmx.converter.ex.model.data.Observation;
import com.intrasoft.sdmx.converter.ex.model.data.TimeseriesKey;
import com.intrasoft.sdmx.converter.io.data.TranscodingEngine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.*;
import java.util.*;

/**
 * Implementation of the Reader interface for reading CSV data messages.
 */
public class CsvDataReader implements Reader {

	private static Logger logger = LogManager.getLogger(CsvDataReader.class);
	
	//default no-arg constructor
	public CsvDataReader(){
				
	}

	/*
	 * The following are a set of DSD-related temporary properties required for the conversion from the time-series data
	 * model to the cross-sectional data model. These properties are populated by method "populateDsdRelatedProperties"
	 * using information stored in the DSD of the message that needs to be converted.
	 */
	/*
	 * stores the default order of all dsd components. This mapping will be used unless an explicit mapping has been
	 * passed in the arguments of method "readData". The mapping is similar with the mapping used by the
	 * CrossDataReader, with these exceptions: dimensions are placed first, in the order returned by the
	 * "KeyFamilyBean.getDimensions()" method then follows the time dimension then the primary measure and finally the
	 * observation level attributes, in the order returned by the "KeyFamilyBean.getObservationAttributes()" method TO
	 * DO we could enhance this mapping to include higher level attributes, like the cross reader does.
	 */
	
	
	/**
	 * concept name -> concept order
	 *(in some user-provided mappings the order could be a String e.g. 5+8)
	 */
	private Map<String, String[]> componentOrder;
	
	private HeaderBean header;
	private TimeseriesKey currentTimeseriesKey;
	private GroupKey currentGroupKey;
	// private String level = "-1";
	
	final private static int KEYSET_LIMIT = Integer.MAX_VALUE;
	
	/* required properties for reading data */
	private DataStructureBean keyFamilyBean;
	private InputStream headerIs;

	/* optional properties for reading data */
	private String delimiter;// the default delimiter of data files
	
	/**
	 * concept name -> concept order (in some user-provided mappings the order could be a String e.g. 5+8)
	 */
	private Map<String, String[]> mappingMap;
	
	/**
	 * concept name in the input file->concept name int the output file
	 */
	private LinkedHashMap<String, LinkedHashMap<String, String>> transcodingMap;
	
	/**
	 * 'true' indicates that all observations belonging
	 * to the same timeseries are located in consecutive
	 * data records in the input stream
	 */
	private boolean sorted = false;

	/* properties required for implementing the Reader interface */
	private InputStream is;
	private InputParams inputParams;
	private Writer writer;
	private TranscodingEngine transcoding;

	/**
	 * Implements Reader interface method.
	 */
	public void setInputStream(final InputStream is) {
		this.is = is;
	}

	/**
	 * Implements Reader interface method.
	 */
	public void setInputParams(final InputParams params) {
		this.inputParams = params;
		setKeyFamilyBean(params.getKeyFamilyBean());
		setMappingMap(params.getMappingMap());
		setTranscodingMap(params.getStructureSet());
		setHeaderBean(params.getHeaderBean());
		setDelimiter(params.getDelimiter());
		setSorted(params.isOrderedFlatInput());
	}

	/**
	 * Implements Reader interface method.
	 */
	public void setWriter(final Writer writer) {
		this.writer = writer;
	}

	/**
	 * Implements Reader interface method.
	 */
	public boolean isReady() throws Exception {
		boolean ready = true;
		String errorMessage = "Reader is not ready. Missing:";
		if (is == null) {
			ready = false;
			errorMessage += "input stream, ";
		}
		if (writer == null) {
			ready = false;
			errorMessage += "writer, ";
		}
		if (keyFamilyBean == null) {
			ready = false;
			errorMessage += "DSD, ";
		}
		if (header == null) {
			ready = false;
			errorMessage += "header info, ";
		}
		final String writerFormat = this.writer.getClass().getSimpleName().toString();
		// check if a conversion to crossX message can be done with the selected dsd.
		if ("CrossDataWriter".equals(writerFormat)) {
			String complianceXS = IoUtils.checkXsCompliance(keyFamilyBean);
			if (!"".equals(complianceXS)) {
				throw (new Exception("The conversion to the output Cross Sectional message cannot be done because:" + complianceXS));
			}
		}
		if (!ready) {
			errorMessage = errorMessage.substring(0, errorMessage.length() - 2) + ".";
			throw new Exception(errorMessage);
		}
		
		/*
		 * check if the output format is SDMX-ML (except for cross) and if there is no Time Dimension then do not allow
		 * the conversion procedure
		 */
		if (((this.inputParams.getKeyFamilyBean().getTimeDimension() == null))
				&& (("CompactDataWriter".equals(writerFormat)) || ("GenericDataWriter".equals(writerFormat)) || ("UtilityDataWriter".equals(writerFormat
						)))) {
			throw (new Exception("Since DSD has no Time Dimension the conversion from the selected Input  Format to the selected Output Format is not supported"));
		}
		return ready;
	}

	/**
	 * Implements Reader interface method.
	 */
	public void readData(final InputStream is, final InputParams params, final Writer writer) throws Exception {
		setInputStream(is);
		setInputParams(params);
		setWriter(writer);
		readData();
	}

	/**
	 * Implements Reader interface method.
	 */
	public void readData() throws Exception {
		logger.info("reading CSV data ...");
		try {
			isReady();

			if (delimiter == null) {
				delimiter = ";";
			}
			if (mappingMap == null) {
				populateDsdRelatedProperties();
			} else {
				componentOrder = mappingMap;
			}
			// if (headerIs != null) {
			// header = IoUtils.parseHeaderPropertyFile(headerIs);
			// } else {
			// header = new HeaderBean();
			// }

			writer.writeHeader(header);

			// parseCsvDataFileOLD(is, keySetLimit);
			parseCsvDataFile(is, KEYSET_LIMIT, isSorted());
			writer.closeWriter();
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + " exception :" + System.lineSeparator() + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if(writer != null) {
				writer.releaseResources();
			}
		}
	}

	/* Required for reading data */
	public void setKeyFamilyBean(final DataStructureBean keyFamilyBean) {
		this.keyFamilyBean = keyFamilyBean;
	}

	// /* Required for reading data */
	//
	// public void setHeaderIs(InputStream headerIs) {
	// this.headerIs = headerIs;
	// }
	/* Required for reading data */

	public void setHeaderBean(final HeaderBean headerBean) {
		this.header = headerBean;
	}

	/* Optional for reading data */

	public void setMappingMap(final Map<String, String[]> mappingMap) {
		this.mappingMap = mappingMap;
	}

	/* Optional for reading data */

	private void setTranscodingMap(final LinkedHashMap<String, LinkedHashMap<String, String>> structureSet) {
		this.transcoding = new TranscodingEngine(structureSet);
		this.transcodingMap = structureSet;
	}

	/* Optional for reading data */
	public void setDelimiter(final String delimiter) {
		this.delimiter = delimiter;
	}

	/* Optional for reading data */
	public boolean isSorted() {
		return sorted;
	}

	/* Optional for reading data */
	public void setSorted(final boolean sorted) {
		this.sorted = sorted;
	}

	private void populateDsdRelatedProperties() {

		/**
		 * Convention We need a convention for sorting all dsd components' concepts, as well as the primary measure
		 * concept. According to that convention: dimensions are placed first, in the order returned by the
		 * "KeyFamilyBean.getDimensions()" method then follows the time dimension then the primary measure and finally
		 * the observation level attributes, in the order returned by the "KeyFamilyBean.getObservationAttributes()"
		 * method If the input file is multilevel then dataset, group and series attributes are placed also.
		 */
		componentOrder = new LinkedHashMap<String, String[]>();
		int i = 1;
		String cref = null;
		// dimensions
		for (final ListIterator<DimensionBean> it = keyFamilyBean.getDimensions().listIterator(); it.hasNext();) {
			final DimensionBean db = it.next();
			if (!db.isTimeDimension() && !db.getId().equalsIgnoreCase(keyFamilyBean.getPrimaryMeasure().getId()))
			{
				cref = db.getId();
				String[] dimenData = new String[3];
				dimenData[0] = String.valueOf(i++);
				dimenData[1] = "false";
				dimenData[2] = "";
				// componentOrder.put(cref, String.valueOf(i++));
				componentOrder.put(cref, dimenData);
			}
		}
		
		// time dimension
		if (keyFamilyBean.getTimeDimension() != null) {// dsd without time dimension
			cref = keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
			String[] timeData = new String[3];
			timeData[0] = String.valueOf(i++);
			timeData[1] = "false";
			timeData[2] = "1";
			// componentOrder.put(cref, String.valueOf(i++));
			componentOrder.put(cref, timeData);

		}
		// primary measure
		final String[] data = { String.valueOf(i++), "false" };
		componentOrder.put(keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId(), data);
		// observation attributes
		for (AttributeBean ab : (List<AttributeBean>) keyFamilyBean.getObservationAttributes()) {
			cref = ab.getId();
			final String[] attrData = new String[3];
			attrData[0] = String.valueOf(i++);
			attrData[1] = "false";
			attrData[2] = "1";
			componentOrder.put(cref, attrData);
		}

//		if (Integer.parseInt(params.getLevelNumber()) > 1) {

			// add dataset attributes
			for (final ListIterator<AttributeBean> it = keyFamilyBean.getDatasetAttributes().listIterator(); it.hasNext();) {
				final AttributeBean db = it.next();
				cref = db.getId();
				String[] dtsattrData = new String[3];
				dtsattrData[0] = String.valueOf(i++);
				dtsattrData[1] = "false";
				dtsattrData[2] = "";
				componentOrder.put(cref, dtsattrData);

			}
			// add group attributes
			for (final ListIterator<AttributeBean> it = keyFamilyBean.getGroupAttributes().listIterator(); it.hasNext();) {
				final AttributeBean ab = it.next();
				cref = ab.getId();
				String[] attrData = new String[3];
				attrData[0] = String.valueOf(i++);
				attrData[1] = "false";
				attrData[2] = "";
				// componentOrder.put(cref, String.valueOf(i++));
				componentOrder.put(cref, attrData);

			}
			// add series attributes 
			for (final ListIterator<AttributeBean> it = keyFamilyBean.getDimensionGroupAttributes().listIterator(); it.hasNext();) {
				final AttributeBean sb = it.next();
				cref = sb.getId();
				String[] serattrData = new String[3];
				serattrData[0] = String.valueOf(i++);
				serattrData[1] = "false";
				serattrData[2] = "";
				// componentOrder.put(cref, String.valueOf(i++));
				componentOrder.put(cref, serattrData);

			}
//		} //end leveNumber > 1

	}

	/**
	 * Parses the ascii data file, constructs the corresponding time-series data beans (GroupKey, TimeSeriesKey and
	 * Observation) and adds them to the Dataset of the message created by the "parseDataMessage" method.
	 */
	private void parseCsvDataFile(final InputStream in, int keySetLimit, final boolean sorted) throws Exception {
		BufferedReader brdo = null;
		Map<String, TimeseriesKey> sers = null;
		String line = null;
		String serKey = null;
		
		/*
		 * List holding the keys of the timeserieskey to be printed. 
		 * The keys contain the crossX measure concept
		 */
		List<String> tsKeyList=new ArrayList<String>();
		
		/*
		 * List holding the keys of the timeserieskey to be printed. 
		 * The keys do not contain the crossX measure concept
		 */
		final List<String> serKeyList=new ArrayList<String>();
		
		/*
		 * current key with the crossX measure
		 */
		String tsKey="";
		String prevserKey = "";
		String grKey = null;
		String prevgrKey = "";
		GroupKey prevGroupKey = null;
		TimeseriesKey prevTimeSeriesKey = null;
		String cref = null;
		String value = null;
		final ArrayList<DimensionBean> keyFamilyDimensionNames = (ArrayList<DimensionBean>) keyFamilyBean.getDimensions();
		final int levels = Integer.parseInt(inputParams.getLevelNumber());
		// during the first pass, apart from processing series,
		// store the InputStream in a temp file in order to parse that file in
		// the next passes
		PrintWriter pw = null;
		File tempFile = null;
		int parsedTimeSeries = 0;
		int row;
		IoUtils.ParseMonitor mon;
		int lineCounter = 0;
		byte[] rowStatus;
		boolean prevLoopSucceded = true;
		String currentCrossXMeasure="";
		
		/*
		 * boolean variable whether the mapping has mapped the measure dimension or the crossX measures.
		 */
		boolean mapCrossXMeasures=false;
		if (inputParams.getMappingMap() == null){
			mapCrossXMeasures=IoUtils.mapCrossXMeasures(keyFamilyBean, componentOrder);
		}else {
			mapCrossXMeasures=IoUtils.mapCrossXMeasures(keyFamilyBean, inputParams.getMappingMap());
		}
		
		/*
		 * Hash Map with key the series key without the crossX measure and values a hash map with the tsks. 
		 * Used for not sorted files.
		 */
		final Map sersCrossX = new LinkedHashMap<String, LinkedHashMap<String,TimeseriesKey>>();
		 
		//Hash Map with key the series key with the crossX measure and values a list with the tsks
		LinkedHashMap tskMap = new LinkedHashMap<String, TimeseriesKey>();
		 
		// String holding the name of the measure Dimension
		final String measureDimension=IoUtils.getMeasureDimension(keyFamilyBean);
		 
		//Integer holding the number of crossX measures
		int crossXMeasuresNo=1;
		List<CrossSectionalMeasureBean> crossXMeasures = null;
		if (keyFamilyBean instanceof CrossSectionalDataStructureBean) {
			crossXMeasures = ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures();
			if (mapCrossXMeasures) {
				crossXMeasuresNo=crossXMeasures.size();
			}
		}

		//this structure will hold the data for each level. For example we will have 1->A, 2->GR;W 3->A;P1M 4->2000;1.1
		final LinkedHashMap<String, String> recordData = new LinkedHashMap<String, String>();
		 
		//this structure will hold the number of columns per level
		final LinkedHashMap<String, String> columnsPerLevel = new LinkedHashMap<String, String>();
		
		//parse the delimiter
		final String parsedDelimiter = IoUtils.parseDelimiter(delimiter);
		try {
			//constructs a temporary (tempFile) file with the line of the input file + delimiter + series key 
			if (!sorted && Integer.parseInt(inputParams.getLevelNumber()) == 1) {
				try {
					tempFile = File.createTempFile("tempCsv", null);				
					pw = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(tempFile))));
					// set encoding of input
					// TODO make this more generic, an input field, maybe?
					brdo = new BufferedReader(new InputStreamReader(in));
					boolean ignoreFirstLine = false;
					
					while ((line = brdo.readLine()) != null) {
						lineCounter++;
						for (int m=0; m<crossXMeasuresNo; m++){
							// ignore the first line if it contains arbitrary data.
							if (!inputParams.getHeaderRow().equals(Defs.NO_COLUMN_HEADERS) && !ignoreFirstLine) {
								// read the next line
								line = brdo.readLine();
								ignoreFirstLine = true;
							}
							//lineCounter++;
							// create TimeseriesKey
							try {
								final StringBuilder sb = new StringBuilder();
								for (int i = 0; i < keyFamilyDimensionNames.size(); i++) {
									if (keyFamilyDimensionNames.get(i).isTimeDimension()) {continue;} //skip TIME_PERIOD dimension as it is treated separtly
									cref = keyFamilyDimensionNames.get(i).getId();
									if (mapCrossXMeasures &&  measureDimension.equals(cref)){
										//then the value will be the crossX measure
										value=crossXMeasures.get(m).getId();
										currentCrossXMeasure=value;
									}else{
									    value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);
									}
									sb.append("{}");
									sb.append(value);
	 
								}
								sb.deleteCharAt(0);
								sb.deleteCharAt(0);
								if (mapCrossXMeasures){
									//my serkey has to be the key values without the crossX measure
									tsKey=sb.toString();
									if (!tsKeyList.contains(tsKey)) {
										tsKeyList.add(tsKey);
									}
									sb.delete(sb.indexOf(currentCrossXMeasure)-2, sb.indexOf(currentCrossXMeasure)+currentCrossXMeasure.length());
								}						
								serKey = sb.toString();
								if (!serKeyList.contains(serKey)) {
									serKeyList.add(serKey);
								}
							} catch (Exception e) {
								logger.error("Exception parsing data record : " + e.getMessage());
								logger.error("Data record = '" + line + "'");
								continue;
							}
	
						}
						pw.println(line + delimiter + serKey); // store the InputStream in a temp file
					}
				} catch (IOException ioe) {
					String errorMessage = this.getClass().getName() + ".parseCsvDataFile() exception :" + 
							"Could not create the temp file" + System.lineSeparator()+ "\t";
					throw new IOException(errorMessage + ioe.getMessage());
				} finally {
					if(pw !=null) {
						pw.flush();
						pw.close();
					}
					if (brdo != null) {
						brdo.close();
					}
				}
				
			}//end if not sorted and level = 1
			
			rowStatus = new byte[lineCounter];

			parsedTimeSeries = 0;

			// Parsing the source data is done in a multi-pass process,
			// in each pass (loop) parse the entire stream
			// exit the loop either on OutOfMemoryError
			// or when processed series < keySetLimit i.e. all series have been
			// processed
			while (true) {
				logger.info("starting loop: keySetLimit: " + keySetLimit + ", timeseries processed: " + parsedTimeSeries);
				sers = new TreeMap<String, TimeseriesKey>();
				mon = new IoUtils().getParseMonitor(keySetLimit, false);
				int timedRow = 0;
				row = 0;
				try {

					if (!sorted && Integer.parseInt(inputParams.getLevelNumber()) == 1) {
						brdo = new BufferedReader(new InputStreamReader(new FileInputStream(tempFile), inputParams.getFlatFileEncoding()));
					} else {
						brdo = new BufferedReader(new InputStreamReader(in, inputParams.getFlatFileEncoding()));
					}
					boolean ignoreLine = false;
					TimeseriesKey tk = new TimeseriesKey();
					GroupKey gk = new GroupKey();
					DataSet dataset = null;
					String rowdata = "";
					String initialLevel = "";
					if (levels > 1) {
						if (!inputParams.getHeaderRow().equals(Defs.NO_COLUMN_HEADERS) && !ignoreLine && isSorted()) {
							// read the next line
							line = brdo.readLine();
							ignoreLine = true;
						}
						line = brdo.readLine();
						//final String[] data1 = line.split(parsedDelimiter, -1);
						final String[] data1 = splitLine(line,parsedDelimiter);
						initialLevel = data1[0];
						
						//rowdata = line.substring(delimiter.length() + 1, line.length());
						rowdata = line.substring(data1[0].length() + 1, line.length());
						
						rowdata = rowdata + delimiter;
						recordData.put(data1[0], rowdata);						
						//columnsPerLevel.put(initialLevel, String.valueOf((rowdata.split(parsedDelimiter, -1)).length - 1));
						columnsPerLevel.put(initialLevel, String.valueOf(data1.length - 1));

					}
					
					/*
					 * boolean variable indicating if the new mapping has been created in the case we have a multi-level
					 * input file
					 */
					boolean mapping_created = false;
					boolean firstgroup = true;
					// parse the temp file
					while ((line = brdo.readLine()) != null) {
						row++;
						StringBuffer sbrecord = new StringBuffer("");
						for (int m=0; m<crossXMeasuresNo; m++){ 
							/*
							 * ignore the first line if it contains arbitrary data and if the file is sorted. If it is not
							 * then the first row has been already ignored and only data is in the temp file
							 */
							if (!inputParams.getHeaderRow().equals(Defs.NO_COLUMN_HEADERS) && !ignoreLine && isSorted()) {
								// read the next line
								line = brdo.readLine();
								ignoreLine = true;
							}
							//final String[] rdata = line.split(parsedDelimiter, -1);
							final String[] rdata = splitLine(line, parsedDelimiter);
							// if we have a flat input file or we are at the last level of the multilevel input file
							//if ((levels >= 1) || (m>0)) {
							if ((rdata[0].equals(inputParams.getLevelNumber()) && levels > 1) || (levels == 1) || (m>0)) {
								if (levels > 1 && m==0) {

									//rowdata = line.substring(parsedDelimiter.length() + 1, line.length());
									rowdata = line.substring(rdata[0].length() + 1, line.length());
									recordData.put(rdata[0], rowdata);
									//columnsPerLevel.put(rdata[0], String.valueOf((rowdata.split(parsedDelimiter, -1)).length));
									columnsPerLevel.put(rdata[0], String.valueOf(rdata.length - 1));
									sbrecord = new StringBuffer("");
									// get data from each level until you reach the last level.
									for (int i = 0; i < Integer.parseInt(inputParams.getLevelNumber()); i++) {
										final String key = String.valueOf(i + 1);
										sbrecord.append(recordData.get(key));
									}
									line = sbrecord.toString();

									// create the new mapping
									if (!mapping_created) {										
										mapping_created = true;
										//final Map<String, String[]> newMapping = params.getMappingMap();
										if (inputParams.getMappingMap() == null) {
											// throw exception
											String message = "Mapping is mandatory for CSV multilevel file";
											throw (new Exception("Please correct the following:\n" + message));
										}
										
										final Map<String, String[]> newMapping = createMappingForMultilevelFiles(inputParams.getMappingMap());
										
										/*
										// dataset attributes
										final List<AttributeBean> dtsAttrs = keyFamilyBean.getDatasetAttributes();
										for (final AttributeBean dtsAttr : dtsAttrs) {
											IoUtils.createNewMapping(dtsAttr.getId(), columnsPerLevel, newMapping, initialLevel);
										}
										// dimensions

										final List<DimensionBean> dimensions = keyFamilyBean.getDimensions();
										List<String> dimensionNames = new ArrayList<String>();
										for (DimensionBean db : dimensions) {
											if (!db.isTimeDimension()) {
												dimensionNames.add(db.getId());
											}
										}
										
										for (String dimension : dimensionNames) {
											if (mapCrossXMeasures &&  measureDimension.equals(dimension)){//assign the crossX measure to the dimension												
												for (int me=0;me<crossXMeasuresNo;me++){
													dimension=crossXMeasures.get(me).getId();
													IoUtils.createNewMapping(dimension, columnsPerLevel, newMapping, initialLevel);
												}
											}else{
												IoUtils.createNewMapping(dimension, columnsPerLevel, newMapping, initialLevel);
											}
										}

										// group attributes
										final List<AttributeBean> grpAttrs = keyFamilyBean.getGroupAttributes();
										for (final AttributeBean grpAttr : grpAttrs) {
											IoUtils.createNewMapping(grpAttr.getId(), columnsPerLevel, newMapping, initialLevel);

										}
										// series attributes
										final List<AttributeBean> serAttrs = keyFamilyBean.getDimensionGroupAttributes();
										for (final AttributeBean serAttr : serAttrs) {
											IoUtils.createNewMapping(serAttr.getId(), columnsPerLevel, newMapping, initialLevel);

										}
										// time dimension  
										IoUtils.createNewMapping(keyFamilyBean.getTimeDimension().getConceptRef().getFullId(), columnsPerLevel, newMapping,
												initialLevel);

										// Primary Measure
										if (mapCrossXMeasures){
											//the primary measure of the concept does not appear. Its value appears instead.
											//IoUtils.createNewMapping(crossXMeasures.get(m).getConceptRef(), columnsPerLevel, newMapping,
											//	initialLevel);											
										}else{
											IoUtils.createNewMapping(keyFamilyBean.getPrimaryMeasure().getId(), columnsPerLevel, newMapping,
													initialLevel);
										}
										// observation attributes
										final List<AttributeBean> obsAttrs = keyFamilyBean.getObservationAttributes();
										for (final AttributeBean obsAttr : obsAttrs) {
											IoUtils.createNewMapping(obsAttr.getId(), columnsPerLevel, newMapping, initialLevel);
										}
										*/
										// set the new mapping in the InputParams
										inputParams.setMappingMap(newMapping);
										
										componentOrder = newMapping;
									}
									
									
								}
								// This is the real parsing
								if (!sorted && Integer.parseInt(inputParams.getLevelNumber()) == 1) {
								    // input file is NOT sorted, i.e. all series observations are NOT in
									// consecutive rows row++;
									
									if (rowStatus[row - 1] == 1 ) {
									    // the row has been finally parsed
										continue;
									} else if (rowStatus[row - 1] == 2) {
									    // the row has been parsed during the previous loop,
										// mark it as finally parsed
										if (prevLoopSucceded) {
											rowStatus[row - 1] = 1;
											continue;
										} else {
										    // the row has been parsed but the loop
											// failed due to memory error
											rowStatus[row - 1] = 0;
										}
									}
									//final String[] data = line.split(parsedDelimiter, -1);
									final String[] data = splitLine(line,parsedDelimiter);
									
									//write dataset
									if (dataset == null) {
										dataset = new DataSet();
									}
										
									for (final Object dts : keyFamilyBean.getDatasetAttributes()) {
										cref = ((AttributeBean) dts).getId();//check if ok getConceptRef();
										value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);

										if (!"".equals(value)) {
											// check if this dimension has transcoding rules
											final boolean hasTranscoding = transcoding.hasTranscodingRules(cref);
											// check if the value is in a rule
											if (hasTranscoding) {
												final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
												String prevAttrValue = dataset.getAttributeValues().get(cref);
												if(prevAttrValue != null && (!prevAttrValue.equals(finalvalue))) {
													logger.warn("Dataset attribute {} does not have the same value for all entries !", cref );
												}												
												dataset.getAttributeValues().put(cref, finalvalue);
											} else {
												String prevAttrValue = dataset.getAttributeValues().get(cref);
												if(prevAttrValue != null && (!prevAttrValue.equals(value))) {
													logger.warn("Dataset attribute {} does not have the same value for all entries !", cref );
												}
												dataset.getAttributeValues().put(cref, value);
											}
										}
									}
									
																		
									if (firstgroup) {
										// write dataset
										writer.writeEmptyDataSet(dataset);
										firstgroup = false;
									}
									
									//serkey will be the one processed above, i.e value{}value{}value
									serKey = data[data.length - 1];

									// if the series key is the same do nothing, i.e.
									// keep the same currentTimeseriesKey
									if (!prevserKey.equals(serKey) || mapCrossXMeasures) {
										// else either add TimeseriesKey in the series
										// Map, if TimeseriesKey with the same key does
										// not exist,
										//if ((!sers.containsKey(serKey) && !mapCrossXMeasures) ||(mapCrossXMeasures && !sersCrossX.containsKey(serKey))) {
										if ((!sers.containsKey(serKey))) {

											if (sers.size() == keySetLimit) {
												continue;// do not process more series
												// than the keySetLimit
											}

											// create TimeseriesKey
											tk = new TimeseriesKey();
											StringBuilder tskMapKey=new StringBuilder();
											try {									
												for (int i = 0; i < keyFamilyBean.getDimensions().size(); i++) {
													//skip TIME_PERIOD dimension as it is treated separately
													if (keyFamilyDimensionNames.get(i).isTimeDimension()) {continue;} 
													cref = (String) keyFamilyBean.getDimensions().get(i).getId();
													//value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);													
													if (mapCrossXMeasures &&  measureDimension.equals(cref)){
														//then the value will be the crossX measure
														value=crossXMeasures.get(m).getId();
														currentCrossXMeasure=value;
													}else{
														value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);
													}							
													// check if this dimension has transcoding rules
													final boolean hasTranscoding = transcoding.hasTranscodingRules(cref);
													// check if the value is in a rule
													if (hasTranscoding) {
														final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
														tk.getKeyValues().put(cref, finalvalue);
														tskMapKey.append(finalvalue);
														tskMapKey.append("{}");
													} else {
														tk.getKeyValues().put(cref, value);
														tskMapKey.append(value);
														tskMapKey.append("{}");
													}
												}
												
												for (int i = 0; i < keyFamilyBean.getDimensionGroupAttributes().size(); i++) {
													cref = (String) keyFamilyBean.getDimensionGroupAttributes().get(i).getId();
													//value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);													
													value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);
													// check if this attribute has transcoding rules
													final boolean hasTranscoding = transcoding.hasTranscodingRules(cref);
													// check if the value is in a rule
													if (hasTranscoding) {
														final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
														tk.getAttributeValues().put(cref, finalvalue);
													} else {
														tk.getAttributeValues().put(cref, value);
													}
												}
											} catch (Exception e) {
												logger.error("Exception parsing data record : " + e.getMessage());
												logger.error("Data record = '" + line + "'");
												continue;
											}

											currentTimeseriesKey = tk;
											if (mapCrossXMeasures){
												if (!prevserKey.equals(serKey)){
													if (sersCrossX.containsKey(serKey)){
														tskMap =(LinkedHashMap) sersCrossX.get(serKey);
													}else{
														tskMap =new LinkedHashMap<String,TimeseriesKey>();
														sersCrossX.put(serKey,tskMap);
													}
												}
												tsKey=tskMapKey.toString().substring(0, tskMapKey.toString().length()-2);
												if (tskMap.containsKey(tsKey)){
													currentTimeseriesKey=(TimeseriesKey) tskMap.get(tsKey);
												}else{
													tskMap.put(tsKey, currentTimeseriesKey);
												}
												tskMapKey=new StringBuilder();												
												//sersCrossX.put(serKey,tskMap);
											}else{
												sers.put(serKey, currentTimeseriesKey);
											}
											// or get the existing TimeseriesKey from
											// the series Map
										} else {

											if (mapCrossXMeasures){
												final String currentKey=serKey+"{}"+crossXMeasures.get(m).getConceptRef();
												tskMap=(LinkedHashMap<String, TimeseriesKey>) sersCrossX.get(serKey);
												currentTimeseriesKey = (TimeseriesKey) tskMap.get(currentKey);

											}else{
												currentTimeseriesKey = sers.get(serKey);
											}
										}
										prevserKey = serKey;
										prevTimeSeriesKey = currentTimeseriesKey;
									}
									//	//if the serkey is equal with the previous,meaning that we are at the same line then the tskey will change.
									//	if (mapCrossXMeasures && !prevserKey.equals(serKey)){
									//		value=crossXMeasures.get(m).getConceptRef();
									//		currentCrossXMeasure=value;
									//		tskMapKey.append(value);
									//		tskMapKey.append("{}");
									//	}
									if (m==crossXMeasuresNo-1) {
										rowStatus[row - 1] = 2;// mark the row as temp
									}
									// parsed

									// monitor the parsing speed. (when there is not
									// enough memory the speed will slow down
									// significantly)
									timedRow++;
									mon.monitor(timedRow);									

								} else {
								    // input file is sorted, i.e. all series observations are in consecutive rows
									// create GroupKey
									gk = new GroupKey();
									// create TimeseriesKey
									tk = new TimeseriesKey();
									if(dataset==null) {
										dataset = new DataSet();
									}
									try {
										final StringBuilder sb = new StringBuilder();
										// dataset Attr!
										for (final Object dts : keyFamilyBean.getDatasetAttributes()) {
											cref = ((AttributeBean) dts).getId();//check if ok getConceptRef();
											value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);

											if (!"".equals(value)) {
												// check if this dimension has transcoding rules
												final boolean hasTranscoding = transcoding.hasTranscodingRules(cref);
												// check if the value is in a rule
												if (hasTranscoding) {
													final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
													String prevAttrValue = dataset.getAttributeValues().get(cref);
													if(prevAttrValue != null && (!prevAttrValue.equals(finalvalue))) {
														logger.warn("Dataset attribute {} does not have the same value for all entries !", cref );
													}
													dataset.getAttributeValues().put(cref, finalvalue);
												} else {
													String prevAttrValue = dataset.getAttributeValues().get(cref);
													if(prevAttrValue != null && (!prevAttrValue.equals(value))) {
														logger.warn("Dataset attribute {} does not have the same value for all entries !", cref );
													}
													dataset.getAttributeValues().put(cref, value);													
												}
											}
										}
										// group dimensions
										final StringBuilder sbgr = new StringBuilder();
										for (final Object gr : keyFamilyBean.getGroups()) {
											final GroupBean gb = (GroupBean) gr;
											gk.setType(gb.getId());
											for (final Object dref : gb.getDimensionRefs()) {
												cref = (String) dref;
												value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);
												final boolean hasTranscodingRules = transcoding.hasTranscodingRules(cref);
												sbgr.append(';');
												// check if the value is in a rule
												if (hasTranscodingRules) {
													final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
													gk.getKeyValues().put(cref, finalvalue);
													sbgr.append(finalvalue);
												} else {
													gk.getKeyValues().put(cref, value);
													sbgr.append(value);
												}
											}
										}
										// gk.setType(newVal)
										// group attributes
										for (final Object gra : keyFamilyBean.getGroupAttributes()) {
											cref = ((AttributeBean) gra).getId();
											value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);

											if (!"".equals(value)) {
												// check if this dimension has transcoding rules
												final boolean hasTranscodingRules = transcoding.hasTranscodingRules(cref);
												// check if the value is in a rule
												if (hasTranscodingRules == true) {
													final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
													gk.getAttributeValues().put(cref, finalvalue);
												} else {
													gk.getAttributeValues().put(cref, value);
												}
											}
										}
										if (sbgr.length() != 0) {
											sbgr.deleteCharAt(0);
											grKey = sbgr.toString();
										} else {
											grKey = "";
										}
										
										if (prevgrKey.equals(grKey) && prevGroupKey != null) {
											for (String attributeGroupKey : gk.getAttributeValues().keySet()) {
												if (!prevGroupKey.getAttributeValues().get(attributeGroupKey).equals(gk.getAttributeValues().get(attributeGroupKey))) {
													logger.warn("Attribute {} with value {} is not the same for all group series keys: {}", attributeGroupKey, gk.getAttributeValues().get(attributeGroupKey), grKey);
												}
											}
										}
										
										if (firstgroup) {
											// write dataset
											writer.writeEmptyDataSet(dataset);
											if (gk != null && !grKey.equals("")) {
												writer.writeGroupKey(gk);
											}
											firstgroup = false;
											prevgrKey = grKey;
											prevGroupKey = gk;
										}

										for (int i = 0; i < keyFamilyBean.getDimensions().size(); i++) {
											if (!keyFamilyBean.getDimensions().get(i).isTimeDimension() && 
													!keyFamilyBean.getPrimaryMeasure().getId().equalsIgnoreCase(keyFamilyBean.getDimensions().get(i).getId())) {
												cref = (String) keyFamilyBean.getDimensions().get(i).getId();
												if (mapCrossXMeasures &&  measureDimension.equals(cref)){
													//then the value will be the crossX measure
													value=crossXMeasures.get(m).getId();
													currentCrossXMeasure=value;
												}else{
													value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);
												}
												sb.append(';');
												// check if this dimension has transcoding
												// rules
												final boolean hasTranscoding = transcoding.hasTranscodingRules(cref);
												// check if the value is in a rule
												if (hasTranscoding == true) {
													final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
													tk.getKeyValues().put(cref, finalvalue);
													sb.append(finalvalue);
												} else {
													tk.getKeyValues().put(cref, value);
													sb.append(value);
												}
											}//end if dimension != time dimension and dimension != primary measure
										}//end for dimensions

										// series attributes
										for (final Object sea : keyFamilyBean.getDimensionGroupAttributes()) {
											cref = ((AttributeBean) sea).getId();
											value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);

											if (!"".equals(value)) {

												// check if this dimension has transcoding rules
												final boolean hasTranscodingRules = transcoding.hasTranscodingRules(cref);
												// check if the value is in a rule
												if (hasTranscodingRules == true) {
													final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
													tk.getAttributeValues().put(cref, finalvalue);
												} else {
													tk.getAttributeValues().put(cref, value);
												}
											}
										}
										sb.deleteCharAt(0);
										if (mapCrossXMeasures){
											//my serkey has to be the key values without the crossX measure
											tsKey=sb.toString();
											if (!tsKeyList.contains(tsKey)) {
												tsKeyList.add(tsKey);
											}
											sb.delete(sb.indexOf(currentCrossXMeasure)-1, sb.indexOf(currentCrossXMeasure)+currentCrossXMeasure.length());
										}
										serKey = sb.toString();
										prevTimeSeriesKey = tk;

									} catch (Exception e) {
										logger.error("Exception parsing data record : " + e.getMessage());
										logger.error("Data record = '" + line + "'");
										continue;
									}

									if (!prevserKey.equals(serKey)) {

										if (currentTimeseriesKey != null) {
											if (mapCrossXMeasures){
												tsKeyList.remove(tsKey);
												for (String key : tsKeyList){												
													final TimeseriesKey tsks= (TimeseriesKey) tskMap.get(key);
													writer.writeTimeseriesKey(tsks);													
												}
												tskMap =new LinkedHashMap<String,TimeseriesKey>();
												tsKeyList=new ArrayList<String>();
												tsKeyList.add(tsKey);
											}else{
												writer.writeTimeseriesKey(currentTimeseriesKey);
											}
											if (!prevgrKey.equals(grKey)) {
												if (gk != null && !grKey.equals("")) {
													writer.writeGroupKey(gk);
												}
												prevgrKey = grKey;
											}
											prevGroupKey = gk;
										}
										currentTimeseriesKey = tk;
										prevserKey = serKey;
									}
								}

								// add a new Observation to the TimeseriesKey
								final Observation obs = new Observation();
								if (mapCrossXMeasures){
									currentTimeseriesKey=(TimeseriesKey) tskMap.get(tsKey);
									if (currentTimeseriesKey!=null) {
										currentTimeseriesKey.getObservations().add(obs);
									} else {
										currentTimeseriesKey = tk;
										currentTimeseriesKey.getObservations().add(obs);
									}
								}else{
									currentTimeseriesKey.getObservations().add(obs);
								}

								// add the primary measure value to the Observation
								cref = keyFamilyBean.getPrimaryMeasure().getId();
								if ("OBS_VALUE".equals(keyFamilyBean.getPrimaryMeasure().getId()) && 
										!keyFamilyBean.getPrimaryMeasure().getId().equals(keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId())) {
									cref = keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId(); 
								}
								if (mapCrossXMeasures){
									value = extractDataValueFromCsvLine(line, crossXMeasures.get(m).getId(), componentOrder, parsedDelimiter);
								}else{
									value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);
								}
								// if the observation is "-", put null in the value,
								// which will be handled by the writers
								if ("-".equals(value) || "".equals(value)) {// || value.equalsIgnoreCase("NaN")
									obs.setValue(null);
								} else {
									obs.setValue(value);
								}

								// add the time value to the Observation
								if (keyFamilyBean.getTimeDimension() != null) {// dsd without time dimension

									cref = keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
									value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);
									//if a timeTranscoder is a DSPL transcoder then transcode the time
									final TimeTranscoder transcoder = inputParams.getTimeTranscoder();

									if (transcoder!=null){
										value = transcoder.timeToSDMXTranscode(value);
										obs.setTimeValue(value);										
									}else{
										obs.setTimeValue(value);
									}
								}
								// add the observation-level attribute values to the
								// Observation
								for (final Object oba : keyFamilyBean.getObservationAttributes()) {
									cref = ((AttributeBean) oba).getId();
									value = extractDataValueFromCsvLine(line, cref, componentOrder, parsedDelimiter);

									if (!"".equals(value)) {

										// check if this dimension has transcoding rules
										final boolean has_rules = transcoding.hasTranscodingRules(cref);
										// check if the value is in a rule
										if (has_rules == true) {
											final String finalvalue = transcoding.getValueFromTranscoding(cref, value);
											obs.getAttributeValues().put(cref, finalvalue);
										} else {
											obs.getAttributeValues().put(cref, value);
										}
									}
								}
								if (mapCrossXMeasures && sorted){								
									tskMap.put(tsKey, currentTimeseriesKey);
								}
								
								if (prevserKey.equals(serKey)) {
									//check if there are different attributes
									for (String attribute: currentTimeseriesKey.getAttributeValues().keySet()) {
										if (!prevTimeSeriesKey.getAttributeValues().get(attribute).equals(currentTimeseriesKey.getAttributeValues().get(attribute))) {
											logger.warn("Attribute {} with value {} is not the same for all time series keys: {}", attribute, currentTimeseriesKey.getAttributeValues().get(attribute), serKey);
										}
									}
								}
								
								if (mapCrossXMeasures && !sorted){								

									if (!prevserKey.equals(serKey)) {
										tskMap =new LinkedHashMap<String,TimeseriesKey>();
									}
									//tsKey=tskMapKey.toString().substring(0, tskMapKey.toString().length()-2);
									tskMap.put(tsKey, currentTimeseriesKey);
									//tskMapKey=new StringBuilder();												
									sersCrossX.put(serKey,tskMap);
								}
								// test the handling of the OutOfMemoryError
								if (sers.size() == 10000) {
									throw (new OutOfMemoryError("forced out of memory error, current keySetLimit:" + keySetLimit));
								}
								// THIS IS THE END OF THE PARSING
								// System.out.println(line.toString());

							} else {// else we have a multilevel input file in a row with level other than the last one.
								//rowdata = line.substring(delimiter.length() + 1, line.length());
								rowdata = line.substring(rdata[0].length() + 1, line.length());
																																																																																								
								// if (!(line.substring(line.length() - 1, line.length())).equals(parsedDelimiter))
								// append the parsed delimiter
								rowdata = rowdata + delimiter;
								// else
								// rowdata = rowdata + "";
								recordData.put(rdata[0], rowdata);
								//columnsPerLevel.put(rdata[0], String.valueOf((rowdata.split(parsedDelimiter, -1)).length - 1));
								columnsPerLevel.put(rdata[0], String.valueOf(rdata.length - 1));
								m=crossXMeasuresNo;
							}
							//							if (mapCrossXMeasures){
							//								tskList.add(currentTimeseriesKey);
							//								tskMap.put(serKey, tskList);
							//							}
						}//end of for loop of crossX measures 
					}// end of pass

					// IF it was sorted, write last time series that was
					// populated
					if (sorted) {
						if (mapCrossXMeasures){
							for (String key : tsKeyList){												
								final TimeseriesKey tsks= (TimeseriesKey) tskMap.get(key);
								writer.writeTimeseriesKey(tsks);													
							}
						}else{
							writer.writeTimeseriesKey(currentTimeseriesKey);
						}
					} else if (!sorted && Integer.parseInt(inputParams.getLevelNumber()) == 1) {
						// write series
						if (mapCrossXMeasures){
							for (final String key1 : serKeyList){
								final LinkedHashMap<String, TimeseriesKey> map=(LinkedHashMap<String, TimeseriesKey>) sersCrossX.get(key1);
								for (final String key2: tsKeyList){
									if(map.containsKey(key2)){
										final TimeseriesKey tk1 = map.get(key2);
										writer.writeTimeseriesKey(tk1);
									}
								}
							}
						}else{
							for (final TimeseriesKey tk1 : sers.values()) {
								writer.writeTimeseriesKey(tk1);
							}
						}

					}

					brdo.close();// necessary for the temp file to be
					// automatically deleted.					

					parsedTimeSeries += sers.size();
					if (sers.size() < keySetLimit) {
						break;
						// processed series < keySetLimit i.e. all series
						// have been processed. Exit loop
					}
					prevLoopSucceded = true;

				} catch (OutOfMemoryError e) {
					// on OutOfMemoryError reduce the keySetlimit to its half
					// value
					keySetLimit = sers.size() / 2;
					sers = null;
					brdo.close();
					if (keySetLimit > 0) {
						//commented out temporarily until we change the constructors of all readers and writers to take the logger
						//IoUtils.getLogger().warning(prop.getProperty("Caught OutOfMemoryError :") + e.getMessage() + prop.getProperty(". New keySetLimit:") + keySetLimit);
						logger.warn("Caught OutOfMemoryError :" + e.getMessage() + ". New keySetLimit:" + keySetLimit);
					} else {						
						String errorMessage = "Caught OutOfMemoryError. The keySetLimit is already set to 1, cannot decrease further. Please increase the amount of memory available to java."
								+ System.lineSeparator() + "\t";
						throw new Exception(errorMessage + e.getMessage(), e);
					}
					prevLoopSucceded = false;
				} 
			}			
			sers = null;			
		} catch (Exception e) {
			String errorMessage = "exception while parsing csv file:" + System.lineSeparator() + "\t";
			logger.error("Data record = '" + line + "'");
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {	
			if(brdo != null) {
				brdo.close();
			}
			if (tempFile!=null) {
				tempFile.delete();
			}
		}
	}


	private Map<String, String[]> createMappingForMultilevelFiles(Map<String, String[]> map) {
		logger.debug("creating mapping for multilevel files from : {}", map);				
		TreeMap<Integer,Map<String, String[]>> levelToRecord = new TreeMap<Integer,Map<String, String[]>>();
		
		for (Map.Entry<String, String[]> entry : map.entrySet()) {
			logger.debug("iterating through level mapping for key {} and values {}", entry.getKey(), entry.getValue());
			String[] mappingValues = entry.getValue();
			Integer levelInt = Integer.parseInt(mappingValues[2]);
			if(levelToRecord.get(levelInt) == null) {
				Map<String, String[]> m = new LinkedHashMap<String,String[]>();
				levelToRecord.put(levelInt, m);				
			}
			levelToRecord.get(levelInt).put(entry.getKey(), entry.getValue());
		}
		
		
		LinkedHashMap<String, String[]> resultMap = new LinkedHashMap<String, String[]>();
		
		int i = 1;
		for (Integer level : levelToRecord.keySet()) {			
			Map<String, String[]> levelRecord = levelToRecord.get(level);
			
			List<Map.Entry<String, String[]>> levelRecordList = 
					new LinkedList<Map.Entry<String, String[]>>(levelRecord.entrySet());
			
			//Sort the list
			Collections.sort(levelRecordList, new Comparator<Map.Entry<String, String[]>>() {				
				public int compare(Map.Entry<String, String[]> o1,
	                                           Map.Entry<String, String[]> o2) {
					String [] arr1 = o1.getValue();
					String [] arr2 = o2.getValue();
					int int1 = Integer.parseInt(arr1[0]);
					int int2 = Integer.parseInt(arr2[0]);					
					if(int1 < int2) {
						return -1;
					} else if(int1==int2) {					
						return 0;					
					} else {
						return 1;
					}								
				}			
			});						
			
			for (Iterator<Map.Entry<String, String[]>> it = levelRecordList.iterator(); it.hasNext();) {
				Map.Entry<String, String[]> entry = it.next();
				String[] value = entry.getValue();
				value[0] = String.valueOf(i++);				
				resultMap.put(entry.getKey(), entry.getValue());
			}									
		}
		
		return resultMap;
	}
	/**
	 * This method extracts the fixed value of a component or the position of it if it has not fixed value
	 * @param line
	 * @param conceptName
	 * @param csvMapping
	 * @param fieldDelimiter
	 * @return value
	 * 
	 */
	private String extractDataValueFromCsvLine(final String line, 
	                                           final String conceptName, 
	                                           final Map<String, String[]> csvMapping, 
	                                           final String fieldDelimiter) {						
		String value = "";
		String crefOrder = null;
		String[] data = null;
		String[] crefParts = null;
		// parse the delimiter in order to find any special characters
		// fieldDelimiter = parseDelimiter(fieldDelimiter);
		//data = line.split(fieldDelimiter, -1);// the csv file contains an extra
		// (!) delimiter in each line
		data = splitLine(line, fieldDelimiter);
		final String[] mappingData = csvMapping.get(conceptName);
		crefOrder = mappingData[0];

		// if fixed value=false then take the position of the concept
		if (mappingData[1].equals("false")) {
			if (crefOrder.contains("+")) {
				value = "";
				crefParts = crefOrder.split("\\+");// "+" is special regex character
				for (int j = 0; j < crefParts.length; j++) {
					value += data[Integer.parseInt(crefParts[j]) - 1];
				}
			} else if (!"".equals(crefOrder)) {
				value = data[Integer.parseInt(crefOrder) - 1];
			}
		} else {// get the fixed value
			value = crefOrder;
		}
		if (inputParams.isLeadTrailSpacesTrimmed()){
			value = value.trim();
		}
		return value;
	}
	
	/**
	 * splits the given csv line into strings by taking into account the delimiter and the escape characters (if any)
	 * 
	 * @param line
	 * @param fieldDelimiter
	 * @return
	 */
	private String [] splitLine(final String line, final String fieldDelimiter) {
		String[] data = null;
		if (inputParams.isCSVInputUnescaped()) {
			CSVReader csvReader = new CSVReader(new StringReader(line), fieldDelimiter.charAt(0));			
			try {
				data = csvReader.readNext();
			} catch (IOException e) {				
				logger.error("Error reading a csv line ..", e);
			}
		} else {
			data = line.split(fieldDelimiter, -1);
		}
		return data;
	}
}
