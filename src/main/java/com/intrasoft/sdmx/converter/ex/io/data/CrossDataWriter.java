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

import com.intrasoft.sdmx.converter.ex.model.data.*;
import com.intrasoft.sdmx.converter.ex.model.data.cross.XSGroup;
import com.intrasoft.sdmx.converter.ex.model.data.cross.XSObservation;
import com.intrasoft.sdmx.converter.ex.model.data.cross.XSSection;
import com.intrasoft.sdmx.converter.io.data.ComponentBeanUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;


/**
 * Implementation of the Writer interface for writing Cross-sectional SDMX data messages. In order to write the cross
 * file, the writer needs to have been given ALL the series and groups. For this reason, the writer interface functions
 * do not write the output file directly, but create some temp files, where when the closeWriter function is called, the
 * real writing will take place.<br>
 * 
 * The writer works in the following steps:<br>
 * <br>
 * 1. the reader calls:<br>
 * a. writeGroupKey(), which populates the tempTsGroupFile with group keys and group attributes<br>
 * b. writeTimeSeriesKey(), which populates the tempAsciiFile with the dataset attributes, series keys, series
 * attributes, observation time and value (one line per observation)<br>
 * <br>
 * 2. the reader calls closeWriter(), which initiates the real writing.The following methods are called in sequence:<br>
 * a. makeAsciiDataFile()<br>
 * --> parses tempTsGroupFile and populates groupDataMap(grpKey, data[])<br>
 * --> THEN parses tempAsciiFile and, along with the groupDataMap (filled in above) writes tempAsciiFile2, which
 * contains all dimensions, attributes (all levels), observations.<br>
 * --> switches names between tempAsciiFile and tempAsciiFile2<br>
 * <br>
 * b. makeXsDataSet()<br>
 * --> parses first line of tempAsciiFile (previously tempAsciiFile2) to get dataset XS attributes<br>
 * --> calls printDataSet() to write the dataset part of the output file<br>
 * <br>
 * c. makeXSectionFile()<br>
 * --> parses tempAsciiFile (previously tempAsciiFile2), generates SECTION keys and populates an ordered TreeSet of
 * sections. When parsing ends, all sections are found, and it writes the tempXSectionFile, with all the section keys.<br>
 * <br>
 * d. makeXSectionGroupFile()<br>
 * --> parses tempAsciiFile (previously tempAsciiFile2), generates GROUP keys and populates an ordered TreeSet of
 * sections. When parsing ends, all groups are found, and it writes the tempXSGroupFile, with all the group keys.<br>
 * <br>
 * e. makeTempSdmxFile()<br>
 * --> calls buildAndPrintXSGroups, which wraps for outOfMemoryException the following:<br>
 * ----> parses tempXSGroupFile and populates grpTreeMap(groupKey, group), where group contains key and attributes<br>
 * ----> parses tempXSectionFile and populates sctTreeMap(sctKey, section), where section contains key and attributes<br>
 * ----> parses tempAsciiFile (previously tempAsciiFile2) and populates tempSdmxFile. Every processed line of
 * tempAsciiFile is written to tempAsciiFile2.<br>
 * ----> tempSdmxFile is used to write output.
 * 
 * 
 * 
 * 
 */
public class CrossDataWriter implements Writer {

	private static Logger logger = LogManager.getLogger(CrossDataWriter.class);
	
	//default no-arg constructor
	public CrossDataWriter(){
		
	}
	
	/*
	 * The following are a set of DSD-related temporary properties required for the conversion from the time-series data
	 * model to the cross-sectional data model. These properties are populated by method "populateDsdRelatedProperties"
	 * using information stored in the DSD of the message that needs to be converted.
	 */
	/*
	 * stores the order of all dsd components as declared in the dsd, with these exceptions: if it exists, measure
	 * dimension is placed first time dimension is placed after the measure dimesnionnot enforced yet frequency
	 * dimension is placed after the time dimension
	 */
	private LinkedHashMap<String, Integer> componentOrder;
	private LinkedHashMap<String, Integer> dimensionOrder;

	private String[] groupNames;// stores the names of the dsd groups
	private String[][] groupDims;// for each group stores (first[]), stores its
	// dimension names (second[])
	private String[][] groupAtts;// for each group stores (first[]), stores its
	// attribute names (second[])
	private List<String> xobservationDims;// stores the dimension names for the
	// cross-sectional observation
	private List<String> xobservationAtts;// stores the attribute names for the
	// cross-sectional observation
	private String[] xgroupDims;// stores the dimension names for the
	// cross-sectional group
	private List<String> xgroupAtts;// stores the attribute names for the
	// cross-sectional group
	private List<String> xsectionDims;// stores the dimension names for the
	// cross-sectional section
	private List<String> xsectionAtts;// stores the attribute names for the
	// cross-sectional section
	private List<String> xdatasetDims;// stores the dimension names for the
	// cross-sectional dataset
	private List<String> xsDatasetAtts;// stores the attribute names for the
	// cross-sectional dataset
	private LinkedHashMap<String, String> measureDimConcepts;// maps measure
	// dimension
	// values to
	// XSmeasure
	// concept names

	/* Temporary files */
	
	/** contains the entire message data, at the observation level. Data at higher levels are redundantly replicated in each observation record. */
	private File tempAsciiFile; 
	/** used in interchange with "tempAsciiFileName", when more than one pass-through the data message is required (one pass for each DSD group is required) */
	private File tempAsciiFile2;
	/** after parsing "tempAsciiFile" contains all group level data */
	private File tempTsGroupFile;
	/** after parsing "tempAsciiFile" contains all group level data */
	private File tempXGroupFile;
	/** after parsing "tempAsciiFile" contains all section level data */
	private File tempXSectionFile;
	/** after parsing "tempXGroupFile", "tempXSectionFile" and "tempAsciiFileName", contains the sdmx groups and sections. */
	private File tempSdmxFile;
	
	/* ByteArrayOutputStreams replacing Temporary files when memory buffering is selected */
	
	/** contains the entire message data, at the observation level. Data at higher levels are redundantly replicated in each observation record. */
	private ByteArrayOutputStream tempAsciiOS;
	/** used in interchange with "tempAsciiFileName", when more than one pass-through the data message is required (one pass for each DSD group is required) */
	private ByteArrayOutputStream tempAsciiOS2;
	/** after parsing "tempAsciiFile" contains all group level data */
	private ByteArrayOutputStream tempTsGroupOS;
	/** after parsing "tempAsciiFile" contains all group level data */
	private ByteArrayOutputStream tempXGroupOS;
	/** after parsing "tempAsciiFile" contains all section level data */
	private ByteArrayOutputStream tempXSectionOS;
	/** after parsing "tempXGroupFile", "tempXSectionFile" and "tempAsciiFileName", contains the sdmx groups and sections. */
	private ByteArrayOutputStream tempSdmxOS;
	

	final private String lineEnd = System.getProperty("line.separator");
	final private String emptyStr = "";
	final private String colonStr = ";";
	private String tabs;
	private PrintWriter pout;
	private PrintWriter temppout;
	private PrintWriter tempgrpout;
	private int keySetLimit;
	private DataSet ds;
	private HeaderBean header;
	// holds the type of the message update or delete
	private String datasetAction = "";

	/* required properties for writing data */
	private DataStructureBean keyFamilyBean;
	/* optional properties for writing data */
	private String namespaceUri;
	private String namespacePrefix;
	private String generatedFileComment;// The comment to be added to the
	// generated file
	/* properties required for implementing the Writer interface */
	private InputParams params;
	private OutputStream os;

	private boolean delete_timeSeries = false;

	/**
	 * Implements Writer interface method.
	 */
	public void writeData(final DataMessage message, final OutputStream os, final InputParams params) throws Exception {
		try {
			this.setOutputStream(os);
			this.setInputParams(params);
			final DataMessageReader dreader = new DataMessageReader();
			dreader.setDataMessage(message);
			dreader.readData(null, params, this);
		} catch (Exception e) {
			final String errorMessage = this.getClass().getName() + ".writeData() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

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
		// if (this.keyFamilyBean.getCrossSectionalMeasures().size() == 0) {
		// // throw (new Exception("There are no Cross-sectional measures in the DSD"));
		// }
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
		setNamespaceUri(params.getNamespaceUri());
		setNamespacePrefix(params.getNamespacePrefix());
		setGeneratedFileComment(params.getGeneratedFileComment());
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeHeader(final HeaderBean header) throws Exception {
		this.header = header;
		if (header.getAction() != null) {
			datasetAction = header.getAction().getAction();
		}
		try {
			isReady();

			final OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
			pout = new PrintWriter(new BufferedWriter(osw));
			tabs = "";

			// print CompactData tag
			printCrossDataTag();

			// print message header
			final HeaderTagWriter headerWriter = new HeaderTagWriter(pout, tabs, keyFamilyBean);
			headerWriter.printHeader(header);

			populateDsdRelatedProperties();
		} catch (Exception e) {
			final String errorMessage = this.getClass().getName() + ".writeHeader() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Implements Writer interface method.
	 * @throws Exception
	 */
	public void writeEmptyDataSet(final DataSet dataSet) throws Exception {
		try {
			isReady();
			ds = dataSet;
			
			//this method could be called more than once; so clean the temp resources before
			disposeOutputStreams(tempAsciiOS,tempAsciiOS2,tempTsGroupOS,tempXGroupOS,tempXSectionOS);
			disposePrintWriters(temppout,tempgrpout);
			disposeTempFiles(tempAsciiFile,tempAsciiFile2,tempTsGroupFile,tempXGroupFile,tempXSectionFile);
			
			if (params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				tempAsciiFile = File.createTempFile("tempTsDatasetToAscii", null);
				tempAsciiFile2 = File.createTempFile("tempTsDatasetToAscii2", null);
				tempTsGroupFile = File.createTempFile("tempTsGroups", null);
				tempXGroupFile = File.createTempFile("tempXGroups", null);
				tempXSectionFile = File.createTempFile("tempXSections", null);
			} else {
				tempAsciiOS = new ByteArrayOutputStream();
				tempAsciiOS2 = new ByteArrayOutputStream();
				tempTsGroupOS = new ByteArrayOutputStream();
				tempXGroupOS = new ByteArrayOutputStream();
				tempXSectionOS = new ByteArrayOutputStream();
			}

			
			final int m = groupNames.length % 2;
			if (m == 1) {
				if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE) ) {
					switchTempAsciiFiles();
				} else {
					switchTempAsciiOS();
				}
			}

			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
			    final OutputStream out = new FileOutputStream(tempAsciiFile);
			    final OutputStreamWriter osw = new OutputStreamWriter(out, "UTF-8");
				temppout = new PrintWriter(new BufferedWriter(osw));
				tempgrpout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempTsGroupFile), "UTF-8"));
			} else {
				temppout = getPrintWriterForByteArrOS(tempAsciiOS);
				tempgrpout = getPrintWriterForByteArrOS(tempTsGroupOS);
			}

		} catch (FileNotFoundException fnfe) {
			final String errorMessage = this.getClass().getName() + ".writeEmptyDataSet() exception : Could not write the temp File" + lineEnd + "\t";
			throw new FileNotFoundException(errorMessage + fnfe.getMessage());
		} catch (UnsupportedEncodingException uee) {
			final String errorMessage = this.getClass().getName() + ".writeEmptyDataSet() exception :" + "Missing required encoding" + lineEnd + "\t";
			throw new UnsupportedEncodingException(errorMessage + uee.getMessage());
		} catch (IOException ioe) {
			final String errorMessage = this.getClass().getName() + ".writeEmptyDataSet() exception :" + "Could not access the temp file" + lineEnd + "\t";
			throw new IOException(errorMessage + ioe.getMessage());
		} catch (Exception e) {
			final String errorMessage = this.getClass().getName() + ".writeEmptyDataSet() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeGroupKey(final GroupKey grKey) throws Exception {
		try {
			isReady();
			writeTempGroup(grKey);
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + ".writeGroupKey() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeTimeseriesKey(final TimeseriesKey tsKey) throws Exception {
		try {
			isReady();			
			writeTempSeries(tsKey);
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + ".writeTimeseriesKey() exception :" + lineEnd + "\t";			
			throw new Exception(errorMessage + e.getMessage(), e);			
		}
	}
	
	/**
	 * Implements Writer interface method.
	 */
	public void closeWriter() throws Exception {
		try {

			disposePrintWriters(temppout, tempgrpout);

			makeAsciiDataFile(Integer.MAX_VALUE);

			makeXSDataSet();
			makeXSectionFile();
			makeXGroupFile(0);

			makeTempSdmxFile(Integer.MAX_VALUE);
			//processFile();

			// </bisc:DataSet>
			tabs = tabs.substring(1);
			pout.println(tabs + "</" + namespacePrefix + ":DataSet>");

			// </CrossSectionalData>
			tabs = tabs.substring(1);
			pout.println(tabs + "</CrossSectionalData>");

			pout.flush();
			if (!os.getClass().toString().endsWith("java.util.zip.ZipOutputStream")){
				pout.close();
			}
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + ".closeWriter() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	/* Required for writing data */
	public void setKeyFamilyBean(final DataStructureBean keyFamilyBean) {
		this.keyFamilyBean = keyFamilyBean;
	}

	/* Optional for writing data */
	public void setNamespaceUri(final String namespaceUri) {
		this.namespaceUri = namespaceUri;
	}

	/* Optional for writing data */
	public void setNamespacePrefix(final String namespacePrefix) {
		this.namespacePrefix = namespacePrefix;
	}

	/* Optional for writing data */
	public void setGeneratedFileComment(final String generatedFileComment) {
		this.generatedFileComment = generatedFileComment;
	}

	/** Populates the DSD-related properties using info from the "keyFamilyBean" */
	private void populateDsdRelatedProperties() {

		// store xs attachment level for dimensions and attributes
		// (NOTE: store the lowest, i.e. finest, level)
	    final Map<String, List<String>> xsLevelAttachments = new LinkedHashMap<String, List<String>>();
		xsLevelAttachments.put("xobdims", new ArrayList<String>());
		xsLevelAttachments.put("xobatts", new ArrayList<String>());
		xsLevelAttachments.put("xscdims", new ArrayList<String>());
		xsLevelAttachments.put("xscatts", new ArrayList<String>());
		xsLevelAttachments.put("xgrdims", new ArrayList<String>());
		xsLevelAttachments.put("xgratts", new ArrayList<String>());
		xsLevelAttachments.put("xdsdims", new ArrayList<String>());
		xsLevelAttachments.put("xdsatts", new ArrayList<String>());

		/**
		 * Convention We need a convention for sorting all dsd components' concepts, as well as the primary measure
		 * concept. According to that convention: the first concept will be the measure dimension concept, then the time
		 * dimension concept, ***not enforced yet*** then the frequency dimension concept, then the remaining dimension
		 * concepts, then the primary measure concept, and then the attribute concepts in this order: the
		 * observation-level attribute concepts, then the series-level attribute concepts, then the group-level
		 * attribute concepts and finally the dataset-level attribute concepts.
		 */
		componentOrder = new LinkedHashMap<String, Integer>();
		int i = 2;// 0, 1 reserved for measure and time dimensions respectively
		String cref = null;

		// time dimension
		if (keyFamilyBean.getTimeDimension() != null) {// dsd without time dimension
			cref = keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
			componentOrder.put(cref, 1);
			xsLevelAttachments.get("xgrdims").add(cref);
		} else {// dsd without time dimension
			// componentOrder.put("Primary Measure", 0);// dsd with no crossx measure
			i = 1;
		}
		if (((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures().size() == 0) {
			componentOrder.put("Primary Measure", 0);// dsd with no crossx measure
		}

		// dimensions
		for (final ListIterator<DimensionBean> it = keyFamilyBean.getDimensions().listIterator(); it.hasNext();) {
		    final DimensionBean db = it.next();
		    if (db.isTimeDimension()) { continue; }
			cref = db.getId();
			if (db.isMeasureDimension()) {
				componentOrder.put(cref, 0);
			} else if (db.isFrequencyDimension()) {
				componentOrder.put(cref, i++);
				xsLevelAttachments.get("xgrdims").add(cref);
			} else {
				componentOrder.put(cref, i++);
				// store xs attachment level for dimensions and attributes
				// (NOTE: store the lowest, i.e. finest, level)
				Map<String, String> values = new HashMap<String, String>();
				values = ComponentBeanUtils.getDimensionInfo((DimensionBean)db, keyFamilyBean);
				if (values.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION) != null && 
						values.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION).equals("true")) {
					xsLevelAttachments.get("xobdims").add(cref);
				} else if (values.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION) != null && 
						values.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION).equals("true")) {
					xsLevelAttachments.get("xscdims").add(cref);
				} else if (values.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP) != null && 
						values.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP).equals("true")) {
					xsLevelAttachments.get("xgrdims").add(cref);
				} else if (values.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET) != null && 
						values.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET).equals("true")) {
					xsLevelAttachments.get("xdsdims").add(cref);
				}
			}
		}
		// primary measure
		componentOrder.put(keyFamilyBean.getPrimaryMeasure().getId(), i++);

		// attributes
		final List<AttributeBean> allAttrs = new ArrayList<AttributeBean>();
		allAttrs.addAll(keyFamilyBean.getObservationAttributes());
		allAttrs.addAll(keyFamilyBean.getDimensionGroupAttributes());
		allAttrs.addAll(keyFamilyBean.getGroupAttributes());
		allAttrs.addAll(keyFamilyBean.getDatasetAttributes());
		for (final AttributeBean ab : allAttrs) {
			cref = ab.getId();
			componentOrder.put(cref, i++);
			// store xs attachment level for dimensions and attributes
			// (NOTE: store the lowest, i.e. finest, level)
			Map<String, String> values = new HashMap<String, String>();
			values = ComponentBeanUtils.getAttributeInfo((AttributeBean)ab, keyFamilyBean);
			if (values.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION) != null && 
					values.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION).equals("true")) {
				xsLevelAttachments.get("xobatts").add(cref);
			} else if (values.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION) != null && 
					values.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION).equals("true")) {
				xsLevelAttachments.get("xscatts").add(cref);
			} else if (values.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP) != null && 
					values.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP).equals("true")) {
				xsLevelAttachments.get("xgratts").add(cref);
			} else if (values.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET) != null && 
					values.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET).equals("true")) {
				xsLevelAttachments.get("xdsatts").add(cref);
			}
		}

		// //debug
		// for (String c : componentOrder.keySet()){
		// System.out.println(componentOrder.get(c)+" "+c);
		// }
		// for (String c : xsLevelAttachments.keySet()){
		// System.out.println(c+" "+xsLevelAttachments.get(c));
		// }

		// store xs attachment level for dimensions and attributes
		// (NOTE: store the lowest, i.e. finest, level)
		List<String> crefs = null;

		crefs = xsLevelAttachments.get("xobdims");
		// xobservationDims = new String[crefs.size()];
		xobservationDims = new ArrayList<String>(crefs.size());
		// i = 0;
		for (final String c : crefs) {
			xobservationDims.add(c);
			// xobservationDims[i++] = c;
		}
		crefs = xsLevelAttachments.get("xobatts");
		// xobservationAtts = new String[crefs.size()];
		xobservationAtts = new ArrayList<String>(crefs.size());
		// i = 0;
		for (final String c : crefs) {
			xobservationAtts.add(c);
			// xobservationAtts[i++] = c;
		}
		crefs = xsLevelAttachments.get("xscdims");
		xsectionDims = new ArrayList<String>(crefs.size());
		// xsectionDims = new String[crefs.size()];
		// i = 0;
		for (final String c : crefs) {
			xsectionDims.add(c);
			// xsectionDims[i++] = c;
		}
		crefs = xsLevelAttachments.get("xscatts");
		xsectionAtts = new ArrayList<String>(crefs.size());
		// xsectionAtts = new String[crefs.size()];
		// i = 0;
		for (final String c : crefs) {
			xsectionAtts.add(c);
			// xsectionAtts[i++] = c;
		}
		crefs = xsLevelAttachments.get("xgrdims");
		xgroupDims = new String[crefs.size()];
		// xgroupDims = new ArrayList<String>(crefs.size());
		i = 0;
		for (final String c : crefs) {
			// xgroupDims.add(c);
			xgroupDims[i++] = c;
		}
		crefs = xsLevelAttachments.get("xgratts");
		xgroupAtts = new ArrayList<String>(crefs.size());
		// xgroupAtts = new String[crefs.size()];
		// i = 0;
		for (final String c : crefs) {
			xgroupAtts.add(c);
			// xgroupAtts[i++] = c;
		}
		crefs = xsLevelAttachments.get("xdsdims");
		xdatasetDims = new ArrayList<String>(crefs.size());
		// xdatasetDims = new String[crefs.size()];
		// i = 0;
		for (final String c : crefs) {
			xdatasetDims.add(c);
			// xdatasetDims[i++] = c;
		}
		crefs = xsLevelAttachments.get("xdsatts");
		xsDatasetAtts = new ArrayList<String>(crefs.size());
		// xsDatasetAtts = new String[crefs.size()];
		// i = 0;
		for (final String c : crefs) {
			xsDatasetAtts.add(c);
			// xsDatasetAtts[i++] = c;
		}
		// System.out.println("xobservationDims" + xobservationDims.length);
		// System.out.println("xobservationAtts" + xobservationAtts.length);
		// System.out.println("xgroupDims" + xgroupDims.length);
		// System.out.println("xgroupAtts" + xgroupAtts.length);
		// System.out.println("xsectionDims" + xsectionDims.length);
		// System.out.println("xsectionAtts" + xsectionAtts.length);
		// System.out.println("xdatasetDims" + xdatasetDims.length);
		// System.out.println("xdatasetAtts" + xdatasetAtts.length);

		// store group-level attributes per attached group
		final Map<String, List<String>> attachmentGroupRefs = new LinkedHashMap<String, List<String>>();
		for (final Object oba : keyFamilyBean.getGroupAttributes()) {
		    final AttributeBean ab = (AttributeBean) oba;
			cref = ab.getId();
		    final String gr = ab.getAttachmentGroup();
			if (!attachmentGroupRefs.containsKey(gr)) {
				attachmentGroupRefs.put(gr, new ArrayList<String>());
			}
			attachmentGroupRefs.get(gr).add(cref);
		}
		i = 0;
		groupAtts = new String[attachmentGroupRefs.keySet().size()][];
		for (final String gr : attachmentGroupRefs.keySet()) {
			// System.out.println("group: " + gr + " attr: " +
			// attachmentGroupRefs.get(gr));
			crefs = attachmentGroupRefs.get(gr);
			groupAtts[i] = new String[crefs.size()];
			int j = 0;
			for (String c : crefs) {
				groupAtts[i][j++] = c;
			}
			i++;
		}

		// store dimensions per group
		final Map<String, List<String>> groupDimensionRefs = new LinkedHashMap<String, List<String>>();
		for (final Object obg : keyFamilyBean.getGroups()) {
		    final GroupBean gb = (GroupBean) obg;
		    final String gr = gb.getId();
			for (final Object obd : gb.getDimensionRefs()) {
				cref = (String) obd;
				if (!groupDimensionRefs.containsKey(gr)) {
					groupDimensionRefs.put(gr, new ArrayList<String>());
				}
				groupDimensionRefs.get(gr).add(cref);
			}
		}
		i = 0;
		groupNames = new String[groupDimensionRefs.keySet().size()];
		// groupNames = new ArrayList<String>();

		groupDims = new String[groupDimensionRefs.keySet().size()][];
		for (final String gr : groupDimensionRefs.keySet()) {
			// System.out.println("group: " + gr + " dim: " +
			// groupDimensionRefs.get(gr));
			groupNames[i] = gr;
			// String grName = groupNames.get(i).toString();
			// grName = gr;
			// groupNames.get(i)= gr;
			crefs = groupDimensionRefs.get(gr);
			groupDims[i] = new String[crefs.size()];
			int j = 0;
			for (String c : crefs) {
				groupDims[i][j++] = c;
				// System.out.println("i: " + i + " j: " + (j-1) +
				// " :groupDims[i][j] " + groupDims[i][j-1]);
			}
			i++;
		}

		// store xs measures per measure dimension value
		measureDimConcepts = new LinkedHashMap<String, String>();

		// if the DSD has no crossx measures then the Primary measure is the crossx measure.
		if (((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures().size() == 0) {
		    final PrimaryMeasureBean pmb = keyFamilyBean.getPrimaryMeasure();
			measureDimConcepts.put(pmb.getId(), pmb.getId());
		} else {// else get the xs measures
			for (Object obg : ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures()) {
			    final CrossSectionalMeasureBean xmb = (CrossSectionalMeasureBean) obg;
				measureDimConcepts.put(xmb.getCode(), xmb.getId());
			}
		}

	}

	/**
	 * This method receives a time series and writes is to the temp file. It does this by populating the String[] data
	 * with the values of all the components (ser key, ser attributes, obs primary measure, obs time value, obs
	 * attribute, dataset attributes), and then creates a string for each observation and writes it in the
	 * tempAsciifile.
	 * 
	 * @param ts The time series to be written to the temp file
	 * @throws Exception
	 */
	private void writeTempSeries(final TimeseriesKey ts) throws Exception {
		String[] data = null;
		StringBuilder s;
		// for delete dataset messages
		if (datasetAction.equalsIgnoreCase("Delete") && (ts == null)) {

			temppout.flush();
			return;
		} // else if it is a delete time series message or delete sibling group
		else if (datasetAction.equalsIgnoreCase("Delete") && (ts != null) && (ts.getObservations().size() == 0)) {
			delete_timeSeries = true;
			data = new String[componentOrder.size()];

			// time dimension
			if (keyFamilyBean.getTimeDimension() != null) {// dsd without time dimension
				data[componentOrder.get(keyFamilyBean.getTimeDimension().getConceptRef().getFullId())] = "";
			}
			// time value
			data[componentOrder.get(keyFamilyBean.getPrimaryMeasure().getId())] = "";

			// time series key
			Iterator<String> it = ts.getKeyValues().keySet().iterator();
			while (it.hasNext()) {
			    final String comnam = it.next();
				data[componentOrder.get(comnam)] = ts.getKeyValues().get(comnam);
			}
			// time series attributes
			it = ts.getAttributeValues().keySet().iterator();
			while (it.hasNext()) {
			    final String comnam = it.next();
				data[componentOrder.get(comnam)] = ts.getAttributeValues().get(comnam);
			}
			// dataset attributes
			it = ds.getAttributeValues().keySet().iterator();
			while (it.hasNext()) {
			    final String comnam = it.next();
				data[componentOrder.get(comnam)] = ds.getAttributeValues().get(comnam);
			}
			// Now that data is populated with all the components values, create
			// the string to be written to the temp file
			s = new StringBuilder(emptyStr);
			for (int i = 0; i < data.length; i++) {
				s.append(colonStr);
				s.append(data[i]);
			}
			// remove the first ";"
			temppout.println(s.toString().substring(1));

		} else {
			for (Observation ob : ts.getObservations()) {
				data = new String[componentOrder.size()];

				// time dimension
				if (keyFamilyBean.getTimeDimension() != null) {// dsd without time dimension
					data[componentOrder.get(keyFamilyBean.getTimeDimension().getConceptRef().getFullId())] = ob.getTimeValue();
				}
				// time value
				Integer position = componentOrder.get(keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId());
				if (position == null) {
					position = componentOrder.get("Primary Measure");
				}
				if (ob.getValue() != null) {
					data[position] = ob.getValue();
				} else {
					data[position] = "";

				}

				// observation attributes
				Iterator<String> it = ob.getAttributeValues().keySet().iterator();
				while (it.hasNext()) {
				    final String comnam = it.next();
					data[componentOrder.get(comnam)] = ob.getAttributeValues().get(comnam);
				}

				// time series attributes
				it = ts.getAttributeValues().keySet().iterator();
				while (it.hasNext()) {
				    final String comnam = it.next();
					data[componentOrder.get(comnam)] = ts.getAttributeValues().get(comnam);
				}

				// time series key
				it = ts.getKeyValues().keySet().iterator();
				while (it.hasNext()) {
				    final String comnam = it.next();
					data[componentOrder.get(comnam)] = ts.getKeyValues().get(comnam);
				}

				// dataset attributes
				it = ds.getAttributeValues().keySet().iterator();
				while (it.hasNext()) {
				    final String comnam = it.next();
					data[componentOrder.get(comnam)] = ds.getAttributeValues().get(comnam);
				}

				// Now that data is populated with all the components values, create
				// the string to be written to the temp file
				s = new StringBuilder(emptyStr);
				for (int i = 0; i < data.length; i++) {
					s.append(colonStr);
					s.append(data[i]);
				}
				// remove the first ";"
				temppout.println(s.toString().substring(1));
			}
		}
		temppout.flush();
	}

	private void writeTempGroup(final GroupKey gr) throws Exception {
		String[] data = null;
		data = new String[componentOrder.size()];

		// We must store, for later use, the dsd group id info in each data
		// record,
		// this can be stored the in the data array position of the observation,
		// the observation is not included in any group,
		// so there is no chance that an group-level attribute will use that
		// position.
		data[componentOrder.get(keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId())] = gr.getType();
		Iterator<String> it = gr.getAttributeValues().keySet().iterator();
		while (it.hasNext()) {
		    final String comnam = it.next();
			data[componentOrder.get(comnam)] = gr.getAttributeValues().get(comnam);
		}
		it = gr.getKeyValues().keySet().iterator();
		while (it.hasNext()) {
		    final String comnam = it.next();
			data[componentOrder.get(comnam)] = gr.getKeyValues().get(comnam);
		}

		final StringBuilder s = new StringBuilder(emptyStr);
		for (int i = 0; i < data.length; i++) {
			s.append(colonStr);
			s.append(data[i]);
		}

		tempgrpout.println(s.toString().substring(1));
		// tempgrpout.flush();
	}

	/**
	 * Parses the data message and outputs all data at the observation level. Data at higher levels are redundantly
	 * replicated in each observation record.
	 * @throws Exception
	 */
	private void makeAsciiDataFile(final int _keySetLimit) throws Exception {// throws Exception {
		keySetLimit = _keySetLimit;
		BufferedReader brd = null;
		BufferedReader brdg = null;
		String line = null;
		String[] data;
		StringBuilder grpKeySb;
		String grpKey = null;
		TreeMap<String, String[]> groupDataMap = null;
		int rowsProcessed, rowCounter;
		try {
			rowsProcessed = 0;
			while (true) {
				//IoUtils.getLogger().info("starting loop: keySetLimit: " + keySetLimit + ", x-groups processed: " + rowsProcessed);
				logger.info("starting loop: keySetLimit: " + keySetLimit + ", x-groups processed: " + rowsProcessed);

				try {

					// read tempTsGroupFile,
					// put group data in a map
					// (each record contains data for one dsd group)
					String groupType;
					// upon writing the tempTsGroupFile the dsd group info was
					// stored in the first record column
					final int groupTypePosition = componentOrder.get(keyFamilyBean.getPrimaryMeasure().getId());
					groupDataMap = new TreeMap<String, String[]>();
					
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						brdg = new BufferedReader(new InputStreamReader(new FileInputStream(tempTsGroupFile), "UTF-8"));
					} else {
						brdg = getBufferedReaderFromByteArrOS(tempTsGroupOS);
					}

					rowCounter = 0;
					while ((line = brdg.readLine()) != null) {
						rowCounter++;
						if (rowCounter <= rowsProcessed) {
							continue;
						}

						data = line.split(";");
						groupType = data[groupTypePosition];
						// iterate for all dsd groups
						for (int groupNum = 0; groupNum < groupNames.length; groupNum++) {
							// locate the group type this data record refers to
							if (groupType.equals(groupNames[groupNum])) {

								grpKeySb = new StringBuilder("");
								for (int dimensionNum = 0; dimensionNum < groupDims[groupNum].length; dimensionNum++) {
									grpKeySb.append(";");
									grpKeySb.append(data[componentOrder.get(groupDims[groupNum][dimensionNum])]);
								}
								grpKey = grpKeySb.substring(1); // remove first ";"
								groupDataMap.put(grpKey, data);
								break;
							}
						}
						if (groupDataMap.size() == keySetLimit) {
							break;
						}

					}
					brdg.close();
					rowsProcessed = rowCounter;

					// read tempAsciiFile,
					// add group(s) keys, attributes
					// and write to tempAsciiFile2
					disposePrintWriters(temppout);
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempAsciiFile2), "UTF-8"));
						brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempAsciiFile), "UTF-8"));
					} else {
						tempAsciiOS2 = new ByteArrayOutputStream();
						temppout = getPrintWriterForByteArrOS(tempAsciiOS2);
						
						brd = getBufferedReaderFromByteArrOS(tempAsciiOS);
					}

					while ((line = brd.readLine()) != null) {
						// data now will contain observation level data
						data = line.split(";");
						// iterate for all dsd groups
						for (int groupNum = 0; groupNum < groupNames.length; groupNum++) {

							// generate the group in which the observation WOULD
							// belong (there may not be such a group)
							grpKeySb = new StringBuilder("");
							for (int dimensionNum = 0; dimensionNum < groupDims[groupNum].length; dimensionNum++) {
								grpKeySb.append(";");
								grpKeySb.append(data[componentOrder.get(groupDims[groupNum][dimensionNum])]);
							}
							grpKey = grpKeySb.substring(1); // remove first ";"

							// see if there were data retained for the potential group. It there are, get them from the
							// groupDataMap, and fill them out to the data[].
							if (groupDataMap.containsKey(grpKey)) {
							    final String[] groupdata = groupDataMap.get(grpKey);
								for (int d = 0; d < groupAtts[groupNum].length; d++) {
								    final int attrOrder = componentOrder.get(groupAtts[groupNum][d]);
									// fill out the group attributes in the
									// observation level array
									data[attrOrder] = groupdata[attrOrder];
								}
							}
						}

						// Now that data[] also has group attributes, write it to tempAsciiFile2
						final StringBuilder sb = new StringBuilder("");
						for (int j = 0; j < data.length; j++) {
							sb.append(";");
							sb.append(data[j]);
						}

						temppout.println(sb.substring(1)); // take out the first ";"

					}
					// interchange "tempAsciiFileName" and "tempAsciiFileName2"
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE) ) {
						switchTempAsciiFiles();
					} else {
						switchTempAsciiOS();
					}


					disposePrintWriters(temppout);
					brd.close();

					if (groupDataMap.size() < keySetLimit) {
						break;// processed records < keySetLimit i.e. all
						// records have been processed. Exit loop
					}

				} catch (OutOfMemoryError e) {
					// on OutOfMemoryError reduce the keySetlimit to its half
					// value
					keySetLimit = groupDataMap.size() / 2;
					groupDataMap = null;
					// if the available memory is small even for a single group
					// then nothing can be done
					if (keySetLimit > 0) {
						//IoUtils.getLogger().warning("Caught OutOfMemoryError : " + e.getMessage() + ". New keySetLimit: " + keySetLimit);
						logger.warn("Caught OutOfMemoryError : {} New keySetLimit: {}", e.getMessage(), keySetLimit);
					} else {
						disposePrintWriters(temppout);
						brd.close();
						if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
							disposeTempFiles(tempTsGroupFile);
						} else {
							disposeOutputStreams(tempTsGroupOS);
                        }
							String errorMessage = "Caught OutOfMemoryError. The keySetLimit is already set to 1, cannot decrease further. Please increase the amount of memory available to java."
								+ lineEnd + "\t";
						throw new Exception(errorMessage + e.getMessage(), e);
					}
					rowsProcessed = 0;
				}
			}
			
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				disposeTempFiles(tempTsGroupFile);
			} else {
				disposeOutputStreams(tempTsGroupOS);
			}
			
			groupDataMap = null;
		} catch (FileNotFoundException fnfe) {
			String errorMessage = this.getClass().getName() + ".makeAsciiDataFile() exception :" + "Could not access the temp file" + lineEnd + "\t";
			throw new FileNotFoundException(errorMessage + fnfe.getMessage());
		} catch (IOException ioe) {
			String errorMessage = this.getClass().getName() + ".makeAsciiDataFile() exception :" + "Could not read the temp file" + lineEnd + "\t";
			throw new IOException(errorMessage + ioe.getMessage());
		} catch (Exception e) {
			final String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if(brd != null) {brd.close();}
			if(brdg != null) {brdg.close();}
		}
	}

	/**
	 * interchange "tempAsciiFileName" and "tempAsciiFileName2", so that in each pass in method "makeAsciiDataFile" the
	 * first filename is used for output and the second for input.
	 */
	private void switchTempAsciiFiles() {
		File f = tempAsciiFile;
		tempAsciiFile = tempAsciiFile2;
		tempAsciiFile2 = f;
		f = null;
	}

	
	/**
	 * interchange "tempAsciiFileName" and "tempAsciiFileName2", so that in each pass in method "makeAsciiDataFile" the
	 * first filename is used for output and the second for input.
	 */
	private void switchTempAsciiOS() {
	    final ByteArrayOutputStream t = tempAsciiOS;
		tempAsciiOS = tempAsciiOS2;
		tempAsciiOS2 = t;
	}

	/**
	 * Parses the ascii data file created by method "makeAsciiDataFile". Constructs one record for each section,
	 * containing all section "keys". Assumes that the section "key" comprises both dimension and attribute values.
	 * Every distinct key is put in a TreeSet (for ordering), which is parsed and written in the tempXSectionFile in
	 * ascending order
	 * 
	 * @throws Exception
	 */
	private void makeXSectionFile() throws Exception {
		BufferedReader brd = null;
		Set<String> sctKeys = null;
		try {
			disposePrintWriters(temppout);
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) { 
				brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempAsciiFile), "UTF-8"));

				final OutputStream out = new FileOutputStream(tempXSectionFile);
				final OutputStreamWriter osw = new OutputStreamWriter(out, "UTF-8");
				temppout = new PrintWriter(new BufferedWriter(osw));
			} else {
				brd = getBufferedReaderFromByteArrOS(tempAsciiOS);
				
				tempXSectionOS = new ByteArrayOutputStream(); 
				temppout = getPrintWriterForByteArrOS(tempXSectionOS);
			}
			
			// Ascending Section Key
			StringBuffer ascSctKey;
			String line = null;
			String[] data = null;
			// delete time series
			if ("Delete".equals(datasetAction) && delete_timeSeries) {
				// remove time dimension from the xgroupDims
//				for (int j = 0; j < xgroupDims.length; j++) {
//					if (xgroupDims[j].toString().equals(keyFamilyBean.getTimeDimension().getConceptRef())) {
//						// List xgroupDims2 = new ArrayList<String>(xgroupDims.size() - 1);
//						String[] xgroupDims2 = new String[xgroupDims.length - 1];
//						for (int pos = 0; pos < j; pos++) {
//							// xgroupDims2.set(pos, xgroupDims.get(pos));
//							xgroupDims2[pos] = xgroupDims[pos].toString();
//						}
//						for (int k = j; k < xgroupDims.length - 1; k++) {
//							// xgroupDims2.set(k, xgroupDims.get(k + 1));
//							xgroupDims2[k] = xgroupDims[k + 1].toString();
//						}
//						xgroupDims = xgroupDims2;
//						break;
//					}
//				}
			}

			// TreeSet keeps its entries sorted by ascending order
			sctKeys = new TreeSet();

			while ((line = brd.readLine()) != null) {
				data = line.split(";");
				ascSctKey = new StringBuffer("");
				for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {
					// ascSctKey += ";" + data[componentOrder.get(xgroupDims[xsGroupDimNum])];
				
					//if (datasetAction.equals("Delete") && delete_timeSeries == true && xgroupDims[xsGroupDimNum].toString().equals(keyFamilyBean.getTimeDimension().getConceptRef()))
					//	continue;
					//else{
						ascSctKey.append(";");
						ascSctKey.append(data[componentOrder.get(xgroupDims[xsGroupDimNum])]);
				//	}
						
					
				}
				for (int xsGroupAttNum = 0; xsGroupAttNum < xgroupAtts.size(); xsGroupAttNum++) {
					// ascSctKey += ";" + data[componentOrder.get(xgroupAtts[xsGroupAttNum])];
					// if (delete_timeSeries == false) {
					ascSctKey.append(";");
					ascSctKey.append(data[componentOrder.get(xgroupAtts.get(xsGroupAttNum))]);
					// }
				}
				for (int xsSectionDimNum = 0; xsSectionDimNum < xsectionDims.size(); xsSectionDimNum++) {
					// ascSctKey += ";" + data[componentOrder.get(xsectionDims[xsSectionDimNum])];
					// if (delete_timeSeries == false) {
					ascSctKey.append(";");
					ascSctKey.append(data[componentOrder.get(xsectionDims.get(xsSectionDimNum))]);
					// } else {
					// ascSctKey.append(";");
					// ascSctKey.append(data[dimensionOrder.get(xsectionDims[xsSectionDimNum])]);
					// }
				}
				for (int xsSectionAttNum = 0; xsSectionAttNum < xsectionAtts.size(); xsSectionAttNum++) {
					// ascSctKey += ";" + data[componentOrder.get(xsectionAtts[xsSectionAttNum])];
					// if (delete_timeSeries == false) {
					ascSctKey.append(";");
					ascSctKey.append(data[componentOrder.get(xsectionAtts.get(xsSectionAttNum))]);
					// }
				}
				// remove first ";"
				if (ascSctKey.length() != 0) {
					sctKeys.add(ascSctKey.substring(1));
				} else {// there might be no attributes or dimensions in Group or Section level
				    final String value = "";
					ascSctKey.append(";");
					ascSctKey.append(value);
					sctKeys.add(ascSctKey.substring(1));
				}
			}

			// Now that the section keys are ordered, print them in the file
			final Iterator<String> it = sctKeys.iterator();
			while (it.hasNext()) {
				String k = it.next();
				temppout.println(k);
			}

			brd.close();
			disposePrintWriters(temppout);
			sctKeys = null;
			
		} catch (OutOfMemoryError e) { 
			// on OutOfMemoryError reduce the keySetlimit to its half value
			// NOTE: for now, catching out-of-memmory error on method
			// "makeAsciiDataFile" seems to be enough,
			// so we do not handle this here and throw a new exception.
//			IoUtils.getLogger().severe(this.getClass().getName() + prop.getProperty("error.make.x.section.file") + sctKeys.size());
			logger.error(this.getClass().getName() + ".makeXSectionFile() : caught OutOfMemoryError, section keys:" + sctKeys.size());

			String errorMessage = this.getClass().getName() + ".makeXSectionFile() : caught OutOfMemoryError, section keys:" + sctKeys.size() + " :"
					+ lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} catch (IOException ioe) {
			String errorMessage = this.getClass().getName() + ".makeXSectionFile() exception :" + "Could not access the temp file" + lineEnd + "\t";
			throw new IOException(errorMessage + ioe.getMessage());
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if(brd !=null) {
				brd.close();
			}
		}
	}

	/**
	 * Clears the dsd dataset-level attributes from DataSet, adds cross-sectional dataset attributes and prints the xs
	 * dataset tag
	 */
	private void makeXSDataSet() throws Exception {
		BufferedReader brd = null;
		String line = null;
		String[] data = null;
		try {
			// now tempAsciiFile contains all dimensions and attributes at observation level
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempAsciiFile), "UTF-8"));
			} else {
				brd = getBufferedReaderFromByteArrOS(tempAsciiOS);
			}
			
			// for delete dataset messages
			if (datasetAction.equalsIgnoreCase("Delete") && brd.readLine() == null) {

			} else {
				// if (!datasetAction.equalsIgnoreCase("Delete") && brd != null) {
				// read the first line to get dataset XS attributes

				line = brd.readLine();
				if (line != null) {
					data = line.split(";");

					// }
					ds.getAttributeValues().clear();
					for (int xsDatasetAttNum = 0; xsDatasetAttNum < xsDatasetAtts.size(); xsDatasetAttNum++) {
						// when writing from CSV, group attr do not have values
						// String value = "";
						// if (data[componentOrder.get(xsDatasetAtts[xsDatasetAttNum])].toString().equals("null")) {
						// value = "NaN";
						// } else
						// value = data[componentOrder.get(xsDatasetAtts[xsDatasetAttNum])];
						if (!data[componentOrder.get(xsDatasetAtts.get(xsDatasetAttNum))].equals("null")) {

							ds.getAttributeValues().put(xsDatasetAtts.get(xsDatasetAttNum),
									data[componentOrder.get(xsDatasetAtts.get(xsDatasetAttNum))]);

						}
						// ds.getAttributeValues().put(xsDatasetAtts[xsDatasetAttNum],
						// data[componentOrder.get(xsDatasetAtts[xsDatasetAttNum])]);
						// ds.getAttributeValues().put(xsDatasetAtts[xsDatasetAttNum], value);

					}
				}
			}
			printDataSet(ds);			

		} catch (IOException ioe) {
			String errorMessage = this.getClass().getName() + ".makeXSDataSet() exception :" + "Could not read the temp file" + lineEnd + "\t";
			throw new IOException(errorMessage + ioe.getMessage());
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + "exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if (brd !=null) {
				brd.close();
			}
		}
	}

	/**
	 * Parses the ascii data file created by method "makeAsciiDataFile". Constructs one record for each group,
	 * containing all group "keys". Assumes that the group "key" comprises both dimension and attribute values. Every
	 * distinct key is put in a TreeSet (for ordering), which is parsed and written in the tempXSectionFile in
	 * 
	 * @throws Exception
	 */
	private void makeXGroupFile(final int keySetLimit) throws Exception {
		BufferedReader brd = null;
		Set<String> grpKeys = null;
		try {
			disposePrintWriters(temppout);
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempAsciiFile), "UTF-8"));
				temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempXGroupFile), "UTF-8"));
			} else {
				brd = getBufferedReaderFromByteArrOS(tempAsciiOS);
				tempXGroupOS = new ByteArrayOutputStream(); // not needed. (set since new FileOutput stream is called for file) 
				temppout = getPrintWriterForByteArrOS(tempXGroupOS);
			}
			
			
			// brd.readLine();//skip header line
			StringBuffer ascGrpKey;
			String line = null;
			String[] data = null;
			// TreeSet keeps its entries sorted by ascending order
			grpKeys = new TreeSet();
			while ((line = brd.readLine()) != null) {
				data = line.split(";");
				ascGrpKey = new StringBuffer("");
				for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {
					// ascGrpKey += ";" + data[componentOrder.get(xgroupDims[xsGroupDimNum])];
					ascGrpKey.append(";");
					ascGrpKey.append(data[componentOrder.get(xgroupDims[xsGroupDimNum])]);
				}
				for (int d = 0; d < xgroupAtts.size(); d++) {
					// ascGrpKey += ";" + data[componentOrder.get(xgroupAtts[d])];
					// if (delete_timeSeries == false) {
					ascGrpKey.append(";");
					ascGrpKey.append(data[componentOrder.get(xgroupAtts.get(d))]);
					// }
				}

				if (ascGrpKey.length() != 0) {
					grpKeys.add(ascGrpKey.substring(1));
				} else {// there might be no attributes or dimensions in Group level
				    final String value = "";
					ascGrpKey.append(";");
					ascGrpKey.append(value);
					grpKeys.add(ascGrpKey.substring(1));
				}
			}

			final Iterator<String> it = grpKeys.iterator();
			while (it.hasNext()) {

				temppout.println(it.next());
			}

			grpKeys = null; // save memory
			brd.close();
			disposePrintWriters(temppout);

		} catch (IOException ioe) {
			String errorMessage = this.getClass().getName() + ".makeXGroupFile() exception :" + "Could not access the temp file" + lineEnd + "\t";
			throw new IOException(errorMessage + ioe.getMessage());
		} catch (Exception e) {
			String errorMessage = this.getClass().getName() + " exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if (brd !=null) {
				brd.close();
			}
		}
	}

	private void printCrossDataTag() {
	    final DataMessage me = new DataMessage();
		if (namespaceUri == null) {
			// example:
			// xmlns:edu="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=ESTAT:EDUCATION:compact"
			// value:
			// urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=ESTAT:EDUCATION
			setNamespaceUri(me.getProperties().getProperty("specificxsd") + keyFamilyBean.getAgencyId() + ":" + keyFamilyBean.getId() + ":"
					+ keyFamilyBean.getVersion());
		}
		if (namespacePrefix == null) {
			// example:
			// xmlns:edu="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=ESTAT:EDUCATION:compact"
			// value: edu
			String prefix = keyFamilyBean.getId().toLowerCase();
			setNamespacePrefix(prefix.length()>3?prefix.substring(0, 3):prefix);
		}
		// <?xml version="1.0" encoding="UTF-8"?>
		pout.println(tabs + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

		// Generated File comment
		if (generatedFileComment != null) {
			pout.println(tabs + "<!-- " + generatedFileComment + " -->");
		}

		// <CrossSectionalData
		pout.print(tabs + "<CrossSectionalData");

		tabs += "\t";

		// xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message"
		String s = me.getProperties().getProperty("message");
		if (s != null) {
			pout.print(lineEnd + tabs + "xmlns=\"" + s + "\"");
		}

		// xmlns:edu="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=ESTAT:EDUCATION:cross"
		pout.print(lineEnd + tabs + "xmlns:" + namespacePrefix + "=\"" + namespaceUri + ":cross\"");

		// xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		s = me.getProperties().getProperty("xsi");
		if (s != null) {
			pout.print(lineEnd + tabs + "xmlns:xsi=\"" + s + "\"");
		}

		// xsi:schemaLocation="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message SDMXMessage.xsd">
		s = me.getProperties().getProperty("schemaLocation");

		pout.print(lineEnd + tabs + "xsi:schemaLocation=\"");
		if (s != null) {
			pout.print(s + lineEnd + tabs);
		}
		pout.print(namespaceUri + ":cross " + keyFamilyBean.getAgencyId() + "_" + keyFamilyBean.getId() + "_Cross.xsd");
		// >
		pout.print("\">" + lineEnd);

	}

	private void printDataSet(final DataSet ds) {
		// <biscs:DataSet
	    final StringBuilder sb = new StringBuilder("");

		sb.append(tabs);
		sb.append("<");
		sb.append(namespacePrefix);
		sb.append(":DataSet");

		// print DataSet tag attributes (as oposed to dataset DSD attributes)
		String s = "";
		if ((s = ds.getKeyFamilyURI()) != null) {
			sb.append(" keyFamilyURI=\"");
			sb.append(s);
			sb.append("\"");
			// ats += " keyFamilyURI=\"" + s + "\"";
		}
		if ((s = ds.getDatasetID()) != null) {
			sb.append(" datasetID=\"");
			sb.append(s);
			sb.append("\"");
			// ats += " datasetID=\"" + s + "\"";
		}
		if ((s = ds.getDataProviderSchemeAgencyId()) != null) {
			// ats += " dataProviderSchemeAgencyId=\"" + s + "\"";
			sb.append(" dataProviderSchemeAgencyId=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getDataProviderSchemeId()) != null) {
			// ats += " dataProviderSchemeId=\"" + s + "\"";
			sb.append(" dataProviderSchemeId=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getDataProviderID()) != null) {
			// ats += " dataProviderID=\"" + s + "\"";
			sb.append(" dataProviderID=\"");
			sb.append(s);
			sb.append("\"");
		}

		if ((s = ds.getDataflowAgencyID()) != null) {
			// ats += " dataflowAgencyID=\"" + s + "\"";
			sb.append(" dataflowAgencyID=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getDataflowID()) != null) {
			// ats += " dataflowID=\"" + s + "\"";
			sb.append(" dataflowID=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getAction()) != null) {
			// ats += " action=\"" + s + "\"";
			sb.append(" action=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getReportingBeginDate()) != null) {
			// ats += " reportingBeginDate=\"" + s + "\"";
			sb.append(" reportingBeginDate=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getReportingEndDate()) != null) {
			// ats += " reportingEndDate=\"" + s + "\"";
			sb.append(" reportingEndDate=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getValidFromDate()) != null) {
			// ats += " validFromDate=\"" + s + "\"";
			sb.append(" validFromDate=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getValidToDate()) != null) {
			// ats += " validToDate=\"" + s + "\"";
			sb.append(" validToDate=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getPublicationYear()) != null) {
			// ats += " publicationYear=\"" + s + "\"";
			sb.append(" publicationYear=\"");
			sb.append(s);
			sb.append("\"");
		}
		if ((s = ds.getPublicationPeriod()) != null) {
			// ats += " publicationPeriod=\"" + s + "\"";
			sb.append(" publicationPeriod=\"");
			sb.append(s);
			sb.append("\"");
		}
		// pout.print(sb.toString());

		// print dataset attributes
		// printAttributes(ds);//do not use printAttributes because it prints on
		// the temppout
		for (String a : ds.getAttributeValues().keySet()) {
			// pout.print(" " + a + "=\"" + ds.getAttributeValues().get(a) + "\"");

			sb.append(" ");
			sb.append(a);
			sb.append("=\"");
			sb.append(ds.getAttributeValues().get(a));
			sb.append("\"");

		}

		// >
		sb.append(">");
		sb.append(lineEnd);
		pout.print(sb.toString());
		pout.flush();

		tabs += "\t";

	}

	/**
	 * This method basically wraps the buildAndPrintXSGroups and catches any OutOfMemoryErrors thrown by that method.
	 * This is done because in some cases buildAndPrintXSGroups does not catch its own OutOfMemoryErrors. The method
	 * decreases parameter keySetLimit and recursivelly calls itself until no memory error is thrown.
	 */
	protected void makeTempSdmxFile(final int _keySetLimit) throws Exception {
		try {
			buildAndPrintXSGroups(_keySetLimit);
		} catch (OutOfMemoryError me) {

			keySetLimit = (keySetLimit / 2) + (keySetLimit % 2);

//			IoUtils.getLogger().warning(
//					this.getClass().getName() + prop.getProperty("error.make.temp.sdmx.file") + me.getMessage() + prop.getProperty(". New keySetLimit:")
//							+ keySetLimit);
			logger.warn(
					this.getClass().getName() + ".makeTempSdmxFile() caught OutOfMemoryError :" + me.getMessage() + ". New keySetLimit:"
							+ keySetLimit);
			makeTempSdmxFile(keySetLimit);// use recursion
		} catch (Exception e) {

			String errorMessage = this.getClass().getName() + ".makeTempSdmxFile() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Parses the ascii data file, the xsection file and the xgroup files and constructs the corresponding
	 * cross-sectional data beans (XSGroup, XSSection and XSObservation). Then prints out the xgroups (and consequently
	 * the included xsections and xobservations) Assumes that the group and section "keys" comprise both dimension and
	 * attribute values.
	 */
	private void buildAndPrintXSGroups(final int _keySetLimit) throws Exception {

		keySetLimit = _keySetLimit;
		BufferedReader brdg = null;
		BufferedReader brds = null;
		BufferedReader brdo = null;
		Map<String, XSGroup> grpsTreeMap = null;
		Map<String, XSSection> sctsTreeMap = null;
		String line = null;
		StringBuilder grpKey = null;
		StringBuilder sctKey = null;
		String[] data = null;

		int rowCounter;

		PrintWriter tempXSpw = null;
		PrintWriter tempASCIIpw = null;
		File tempXSectionFile2 = null;
		ByteArrayOutputStream tempXSectionOS2 = null;
		int parsedXGroups;

		IoUtils.ParseMonitor mon;

		try {
			disposePrintWriters(temppout);
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				tempXSectionFile2 = File.createTempFile("tempXSections2", null);
				
				tempSdmxFile = File.createTempFile("tempSdmxFile", null);
				temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempSdmxFile), "UTF-8"));
			} else {
				tempXSectionOS2 = new ByteArrayOutputStream();
				
				tempSdmxOS = new ByteArrayOutputStream();
				temppout = getPrintWriterForByteArrOS(tempSdmxOS);
			}
			
			parsedXGroups = 0;
			while (true) {
				//IoUtils.getLogger().info("starting loop: keySetLimit: " + keySetLimit + ", x-groups processed: " + parsedXGroups);
				logger.info("starting loop: keySetLimit: {} , x-groups processed: {}", keySetLimit , parsedXGroups);	
				mon = new IoUtils().getParseMonitor(keySetLimit, false);
				try {
					
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						tempXSpw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempXSectionFile2), "UTF-8"));
						tempASCIIpw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempAsciiFile2), "UTF-8"));
						brdg = new BufferedReader(new InputStreamReader(new FileInputStream(tempXGroupFile), "UTF-8"));
					} else {
						// constructors are called since for Files new FileOutputStream is called thus recreates files. 
						tempXSectionOS2 = new ByteArrayOutputStream();
						tempXSpw = getPrintWriterForByteArrOS(tempXSectionOS2);
						tempAsciiOS2 = new ByteArrayOutputStream();
						tempASCIIpw = getPrintWriterForByteArrOS(tempAsciiOS2);
						brdg = getBufferedReaderFromByteArrOS(tempXGroupOS);
					}
					
					grpsTreeMap = new TreeMap();
					
					rowCounter = 0;
					while ((line = brdg.readLine()) != null) {
						rowCounter++;
						if (rowCounter <= parsedXGroups) {
							continue;
						}

						final XSGroup grp = new XSGroup();
						// don't we already have groupKey from line of tempXGroupFile?
						grpKey = new StringBuilder("");
						data = line.split(";");
						for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {
							grp.getKeyValues().put(xgroupDims[xsGroupDimNum], data[xsGroupDimNum]);
							// grpKey += ";" + cols[d];
							grpKey.append(";");
							grpKey.append(data[xsGroupDimNum]);
						}
						// if (delete_timeSeries == false) {
						for (int d = 0; d < xgroupAtts.size(); d++) {
							// first xgroupDims.length columns are group key values
						    final String s = data[d + xgroupDims.length];
							if (!"null".equals(s)) {
								grp.getAttributeValues().put(xgroupAtts.get(d), s);
							}
							// grpKey += ";" + s;
							grpKey.append(";");
							grpKey.append(s);
						}
						// }
						// grpKey = grpKey.substring(1);

						// if there are no attributes or dimensions in group level
						if (grpKey.length() == 0) {
						    final String s = "";
							grpKey.append(";");
							grpKey.append(s);
						}
						// remove first ";"
						grpsTreeMap.put(grpKey.substring(1), grp);
						if (grpsTreeMap.size() == keySetLimit) {
							break;
						}
					}
					parsedXGroups = rowCounter;
					brdg.close();
					if (grpsTreeMap.isEmpty()) {
						// no more groups to process. Exit loop
						break;
					}

					// if (keySetLimit == Integer.MAX_VALUE) keySetLimit =
					// grps.size();

					sctsTreeMap = new TreeMap();
					
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						brds = new BufferedReader(new InputStreamReader(new FileInputStream(tempXSectionFile), "UTF-8"));
					} else {
						brds = getBufferedReaderFromByteArrOS(tempXSectionOS);
					}
					
					// brds.readLine();//skip header line
					while ((line = brds.readLine()) != null) {
					    final XSSection sct = new XSSection();
						sctKey = new StringBuilder("");
						grpKey = new StringBuilder("");
						data = line.split(";");

						for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {
							// grpKey += ";" + cols[d];
							// sctKey += ";" + cols[d];
							grpKey.append(";");
							grpKey.append(data[xsGroupDimNum]);
							sctKey.append(";");
							sctKey.append(data[xsGroupDimNum]);
						}
						for (int xsGroupAttNum = 0; xsGroupAttNum < xgroupAtts.size(); xsGroupAttNum++) {
							// grpKey += ";" + data[d + xgroupDims.length];
							// sctKey += ";" + data[d + xgroupDims.length];
							grpKey.append(";");
							grpKey.append(data[xsGroupAttNum + xgroupDims.length]);
							sctKey.append(";");
							sctKey.append(data[xsGroupAttNum + xgroupDims.length]);
						}

						// normally, if we don't split the group set, all group keys should be contained in the group
						// map
						String grpKeyStr = "";
						if (grpKey.length() != 0) {
							grpKeyStr = grpKey.substring(1);
						} else {// there might be no attributes or dimensions in Group orlevel
						    final String value = "";
							grpKey.append(";");
							grpKey.append(value);
							grpKeyStr = grpKey.substring(1);
						}

						if (grpsTreeMap.containsKey(grpKeyStr)) {
							for (int xSectionDimNum = 0; xSectionDimNum < xsectionDims.size(); xSectionDimNum++) {
								// sctKey += ";" + data[d + xgroupDims.length + xgroupAtts.length];
								sctKey.append(";");
								sctKey.append(data[xSectionDimNum + xgroupDims.length + xgroupAtts.size()]);
								sct.getKeyValues()
										.put(xsectionDims.get(xSectionDimNum), data[xSectionDimNum + xgroupDims.length + xgroupAtts.size()]);
							}
							for (int xSectionAttNum = 0; xSectionAttNum < xsectionAtts.size(); xSectionAttNum++) {
								// first (xsectionDims.length + xgroupDims.length + xgroupAtts.length)
								// columns are section key values
							    final String s = data[xSectionAttNum + xsectionDims.size() + xgroupDims.length + xgroupAtts.size()];
								// sctKey += ";" + s;
								sctKey.append(";");
								sctKey.append(s);
								// add section attributes only if section is contained in the groups map
								if (!"null".equals(s)) {
									sct.getAttributeValues().put(xsectionAtts.get(xSectionAttNum), s);
								}
							}
							final XSGroup xsgr = grpsTreeMap.get(grpKeyStr);
							final List<XSSection> xss = xsgr.getSections();
							xss.add(sct);

							// grpsTreeMap.get(grpKeyStr).getSections().add(sct);
							if (sctKey.length() != 0) {
								sctsTreeMap.put(sctKey.substring(1), sct);
							} // remove first ";"
							else {
								sctKey.append(";");
								sctKey.append("");
								sctsTreeMap.put(sctKey.substring(1), sct);// remove first ";"
							}
						} else {
							tempXSpw.println(line);// store the unprocessed
							// section in the temp file
						}
					}

					brds.close();
					disposePrintWriters(tempXSpw);
					
					if (params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) { 
						brdo = new BufferedReader(new InputStreamReader(new FileInputStream(tempAsciiFile), "UTF-8"));
					} else {
						brdo = getBufferedReaderFromByteArrOS(tempAsciiOS);
					}
					
					// brdo.readLine();//skip header line
					int row = 0;
					while ((line = brdo.readLine()) != null) {
						row++;

						// monitor the parsing speed. (when there is not enough
						// memory the speed will slow down significantly)
						mon.monitor(row);

						sctKey = new StringBuilder("");
						data = line.split(";");
						final XSObservation obs = new XSObservation();
						// if (delete_timeSeries == false) {

						if (componentOrder.size()>data.length){
							obs.setValue("");
						}else{
							obs.setValue(data[componentOrder.get(keyFamilyBean.getPrimaryMeasure().getId())]);
						}

						if (((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures().size() == 0) {
							obs.setMeasureDimKey(keyFamilyBean.getPrimaryMeasure().getId());
						} else {
							// in this case data[0] always has the crossX measure code
							obs.setMeasureDimKey(data[0]);
						}
						for (int d = 0; d < xobservationDims.size(); d++) {
							obs.getKeyValues().put(xobservationDims.get(d), data[componentOrder.get(xobservationDims.get(d))]);
						}
						for (int d = 0; d < xobservationAtts.size(); d++) {
						    final String s = data[componentOrder.get(xobservationAtts.get(d))];
							if (!"null".equals(s)) {
								obs.getAttributeValues().put(xobservationAtts.get(d), s);
							}
						}
						for (int d = 0; d < xgroupDims.length; d++) {
							// sctKey += ";" + data[componentOrder.get(xgroupDims[d])];
							sctKey.append(";");
							sctKey.append(data[componentOrder.get(xgroupDims[d])]);
						}
						for (int d = 0; d < xgroupAtts.size(); d++) {
							// sctKey += ";" + data[componentOrder.get(xgroupAtts[d])];
							sctKey.append(";");
							sctKey.append(data[componentOrder.get(xgroupAtts.get(d))]);
						}
						for (int d = 0; d < xsectionDims.size(); d++) {
							// sctKey += ";" + data[componentOrder.get(xsectionDims[d])];
							sctKey.append(";");
							sctKey.append(data[componentOrder.get(xsectionDims.get(d))]);
						}
						for (int d = 0; d < xsectionAtts.size(); d++) {
							// sctKey += ";" + data[componentOrder.get(xsectionAtts[d])];
							sctKey.append(";");
							sctKey.append(data[componentOrder.get(xsectionAtts.get(d))]);
						}
						// } else {
						// for (int d = 0; d < xgroupDims.length; d++) {
						// // sctKey += ";" + data[componentOrder.get(xgroupDims[d])];
						// sctKey.append(";");
						// sctKey.append(data[componentOrder.get(xgroupDims[d])]);
						// }
						// for (int d = 0; d < xsectionDims.length; d++) {
						// // sctKey += ";" + data[componentOrder.get(xsectionDims[d])];
						// sctKey.append(";");
						// sctKey.append(data[dimensionOrder.get(xsectionDims[d])]);
						// }
						//
						// }
						String sctKeyStr = "";
						if (sctKey.length() != 0) {// if there are attributes or dimensions in group or section level
							sctKeyStr = sctKey.substring(1);
						} else {// if there do not exist
						    final String value = "";
							sctKey.append(";");
							sctKey.append(value);
							sctKeyStr = sctKey.substring(1);
						}

						if (sctsTreeMap.containsKey(sctKeyStr)) {														
						    final XSSection xsection = sctsTreeMap.get(sctKeyStr);
							final List<XSObservation> xsObs = xsection.getObservations();
							xsObs.add(obs);							
							// sctsTreeMap.get(sctKeyStr).getObservations().add(obs);
						} else {
							// store the unprocessed section in the temp file
							tempASCIIpw.println(line);
						}
						// System.out.println("LALA2");

					}
					brdo.close();
					disposePrintWriters(tempASCIIpw);
					// print xgroups (and consequently the included xsections and xobservations)
					final Iterator<Entry<String, XSGroup>> it = grpsTreeMap.entrySet().iterator();
					while (it.hasNext()) {
					    final Entry<String, XSGroup> entry = it.next();
						printXSGroup(entry.getValue());
						// System.out.println("LALA3");
					}
					temppout.flush();
					
					// switch temp files
					
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						File f = tempXSectionFile;
						tempXSectionFile = tempXSectionFile2;
						tempXSectionFile2 = f;

						f = tempAsciiFile;
						tempAsciiFile = tempAsciiFile2;
						tempAsciiFile2 = f;
						f = null;
					} else {
						ByteArrayOutputStream b = tempXSectionOS;
						tempXSectionOS = tempXSectionOS2;
						tempXSectionOS2 = b;

						b = tempAsciiOS;
						tempAsciiOS = tempAsciiOS2;
						tempAsciiOS2 = b;
						b = null;
					}

					if (grpsTreeMap.size() < keySetLimit) {
						break;// processed records < keySetLimit i.e. all
						// records have been processed. Exit loop
					}
					// keySetLimit++;//slowly increase the limit (careful, when
					// keySetLimit ~ 1 may lead to many iterations that throw
					// OutOfMemoryError)
				} catch (OutOfMemoryError e) {
					// on OutOfMemoryError reduce the keySetlimit to its half
					// value
					keySetLimit = grpsTreeMap.size() / 2;
					grpsTreeMap = null;
					sctsTreeMap = null;
					temppout.close();
					brdg.close();
					brds.close();
					if ( brdo != null ) {
					    brdo.close();
					}

					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						tempSdmxFile = File.createTempFile("tempSdmxFile", null);
						temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempSdmxFile), "UTF-8"));
					} else {
						tempSdmxOS = new ByteArrayOutputStream();
						temppout = getPrintWriterForByteArrOS(tempSdmxOS);
					}

					parsedXGroups = 0;
					tempXSpw.close();
					tempASCIIpw.close();
					// if the available memory is small even for a single group
					// then nothing can be done
					if (keySetLimit > 0) {
						//IoUtils.getLogger().warning(prop.getProperty("Caught OutOfMemoryError :") + e.getMessage() + prop.getProperty(". New keySetLimit:") + keySetLimit);
						logger.warn("Caught OutOfMemoryError :" + e.getMessage() + ". New keySetLimit:" + keySetLimit);

					} else {
						if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
							disposeTempFiles(tempXGroupFile, tempXSectionFile, tempXSectionFile2, tempAsciiFile, tempAsciiFile2, tempSdmxFile);
						} else {
							disposeOutputStreams(tempXGroupOS, tempXSectionOS, tempXSectionOS2, tempAsciiOS, tempAsciiOS2, tempSdmxOS);
						}

						String errorMessage = "Caught OutOfMemoryError. The keySetLimit is already set to 1, cannot decrease further. Please increase the amount of memory available to java."
								+ lineEnd + "\t";
						throw new Exception(errorMessage + e.getMessage(), e);
					}
				}
			}
			
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				disposeTempFiles(tempXGroupFile, tempXSectionFile, tempXSectionFile2, tempAsciiFile, tempAsciiFile2);
			} else {
				disposeOutputStreams(tempXGroupOS, tempXSectionOS, tempXSectionOS2, tempAsciiOS, tempAsciiOS2);
			}

			// copy groups and sections to the final output file
			temppout.flush();
			temppout.close();
			
			BufferedReader brd = null;
			try {								
				if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE))  {
					brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempSdmxFile), "UTF-8"));
				} else {
					brd = getBufferedReaderFromByteArrOS(tempSdmxOS);
				}
	
				while ((line = brd.readLine()) != null) {
					pout.println(line);
	
				}
			} finally {
				brd.close();
			}
			
			pout.flush();			
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				disposeTempFiles(tempSdmxFile);
			} else {
				disposeOutputStreams(tempSdmxOS);
			}
			grpsTreeMap = null;
		} catch (IOException ioe) {
			String errorMessage = this.getClass().getName() + ".buildAndPrintXSGroups() exception :" + "Could not access the temp file" + lineEnd
					+ "\t";
			throw new IOException(errorMessage + ioe.getMessage());

		} catch (Exception e) {

			final String errorMessage = this.getClass().getName() + ".buildAndPrintXSGroups() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if(brdg != null) {brdg.close();}
			if(brds != null) {brds.close();}
			if(brdo != null) {brdo.close();}			
			disposePrintWriters(tempXSpw,tempASCIIpw);	
			disposeTempFiles(tempXSectionFile2);
		}
	}
	
	
	
	private void buildAndPrintXSGroups2(final int _keySetLimit) throws Exception {

		keySetLimit = _keySetLimit;
		BufferedReader brdg = null;
		BufferedReader brds = null;
		BufferedReader brdo = null;
		Map<String, XSGroup> grpsTreeMap = null;
		Map<String, XSSection> sctsTreeMap = null;
		String line = null;
		StringBuilder grpKey = null;
		StringBuilder sctKey = null;
		String[] data = null;

		int rowCounter;

		PrintWriter tempXSpw = null;
		PrintWriter tempASCIIpw = null;
		File tempXSectionFile2 = null;
		ByteArrayOutputStream tempXSectionOS2 = null;
		int parsedXGroups;		

		try {
			disposePrintWriters(temppout);
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				tempXSectionFile2 = File.createTempFile("tempXSections2", null);
				
				tempSdmxFile = File.createTempFile("tempSdmxFile", null);
				temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempSdmxFile), "UTF-8"));
			} else {
				tempXSectionOS2 = new ByteArrayOutputStream();
				
				tempSdmxOS = new ByteArrayOutputStream();
				temppout = getPrintWriterForByteArrOS(tempSdmxOS);
			}
			
			parsedXGroups = 0;
			while (true) {
				//IoUtils.getLogger().info("starting loop: keySetLimit: " + keySetLimit + ", x-groups processed: " + parsedXGroups);
				logger.info("starting loop: keySetLimit: " + keySetLimit + ", x-groups processed: " + parsedXGroups);					
				try {
					
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						tempXSpw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempXSectionFile2), "UTF-8"));
						tempASCIIpw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempAsciiFile2), "UTF-8"));
						brdg = new BufferedReader(new InputStreamReader(new FileInputStream(tempXGroupFile), "UTF-8"));
					} else {
						// constructors are called since for Files new FileOutputStream is called thus recreates files. 
						tempXSectionOS2 = new ByteArrayOutputStream();
						tempXSpw = getPrintWriterForByteArrOS(tempXSectionOS2);
						tempAsciiOS2 = new ByteArrayOutputStream();
						tempASCIIpw = getPrintWriterForByteArrOS(tempAsciiOS2);
						brdg = getBufferedReaderFromByteArrOS(tempXGroupOS);
					}
					
					grpsTreeMap = new TreeMap();
					
					rowCounter = 0;
					while ((line = brdg.readLine()) != null) {
						rowCounter++;
						if (rowCounter <= parsedXGroups) {
							continue;
						}

						final XSGroup grp = new XSGroup();
						// don't we already have groupKey from line of tempXGroupFile?
						grpKey = new StringBuilder("");
						data = line.split(";");
						for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {
							grp.getKeyValues().put(xgroupDims[xsGroupDimNum], data[xsGroupDimNum]);
							// grpKey += ";" + cols[d];
							grpKey.append(";");
							grpKey.append(data[xsGroupDimNum]);
						}						
						for (int d = 0; d < xgroupAtts.size(); d++) {
							// first xgroupDims.length columns are group key values
						    final String s = data[d + xgroupDims.length];
							if (!"null".equals(s)) {
								grp.getAttributeValues().put(xgroupAtts.get(d), s);
							}							
							grpKey.append(";");
							grpKey.append(s);
						}						
						// if there are no attributes or dimensions in group level
						if (grpKey.length() == 0) {
						    final String s = "";
							grpKey.append(";");
							grpKey.append(s);
						}
						// remove first ";"
						grpsTreeMap.put(grpKey.substring(1), grp);
						if (grpsTreeMap.size() == keySetLimit) {
							break;
						}
					}
					parsedXGroups = rowCounter;
					brdg.close();
					if (grpsTreeMap.isEmpty()) {
						// no more groups to process. Exit loop
						break;
					}
					
					sctsTreeMap = new TreeMap();
					
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						brds = new BufferedReader(new InputStreamReader(new FileInputStream(tempXSectionFile), "UTF-8"));
					} else {
						brds = getBufferedReaderFromByteArrOS(tempXSectionOS);
					}
										
					while ((line = brds.readLine()) != null) {
					    final XSSection sct = new XSSection();
						sctKey = new StringBuilder("");
						grpKey = new StringBuilder("");
						data = line.split(";");

						for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {							
							grpKey.append(";");
							grpKey.append(data[xsGroupDimNum]);
							sctKey.append(";");
							sctKey.append(data[xsGroupDimNum]);
						}
						for (int xsGroupAttNum = 0; xsGroupAttNum < xgroupAtts.size(); xsGroupAttNum++) {							
							grpKey.append(";");
							grpKey.append(data[xsGroupAttNum + xgroupDims.length]);
							sctKey.append(";");
							sctKey.append(data[xsGroupAttNum + xgroupDims.length]);
						}
						// normally, if we don't split the group set, all group keys should be contained in the group
						// map
						String grpKeyStr = "";
						if (grpKey.length() != 0) {
							grpKeyStr = grpKey.substring(1);
						} else {// there might be no attributes or dimensions in Group orlevel
						    final String value = "";
							grpKey.append(";");
							grpKey.append(value);
							grpKeyStr = grpKey.substring(1);
						}

						if (grpsTreeMap.containsKey(grpKeyStr)) {
							for (int xSectionDimNum = 0; xSectionDimNum < xsectionDims.size(); xSectionDimNum++) {								
								sctKey.append(";");
								sctKey.append(data[xSectionDimNum + xgroupDims.length + xgroupAtts.size()]);
								sct.getKeyValues()
										.put(xsectionDims.get(xSectionDimNum), data[xSectionDimNum + xgroupDims.length + xgroupAtts.size()]);
							}
							for (int xSectionAttNum = 0; xSectionAttNum < xsectionAtts.size(); xSectionAttNum++) {
								// first (xsectionDims.length + xgroupDims.length + xgroupAtts.length)
								// columns are section key values
							    final String s = data[xSectionAttNum + xsectionDims.size() + xgroupDims.length + xgroupAtts.size()];								
								sctKey.append(";");
								sctKey.append(s);
								// add section attributes only if section is contained in the groups map
								if (!"null".equals(s)) {
									sct.getAttributeValues().put(xsectionAtts.get(xSectionAttNum), s);
								}
							}
							final XSGroup xsgr = grpsTreeMap.get(grpKeyStr);
							final List<XSSection> xss = xsgr.getSections();
							xss.add(sct);
							
							if (sctKey.length() != 0) {
								sctsTreeMap.put(sctKey.substring(1), sct);
							} // remove first ";"
							else {
								sctKey.append(";");
								sctKey.append("");
								sctsTreeMap.put(sctKey.substring(1), sct);// remove first ";"
							}
						} else {
							tempXSpw.println(line);// store the unprocessed section in the temp file
						}
					}

					brds.close();
					disposePrintWriters(tempXSpw);
					
					if (params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) { 
						brdo = new BufferedReader(new InputStreamReader(new FileInputStream(tempAsciiFile), "UTF-8"));
					} else {
						brdo = getBufferedReaderFromByteArrOS(tempAsciiOS);
					}
					
															
					// print xgroups (and consequently the included xsections and xobservations)
					final Iterator<Entry<String, XSGroup>> it = grpsTreeMap.entrySet().iterator();
					while (it.hasNext()) {
					    final Entry<String, XSGroup> entry = it.next();
					    XSGroup gr = entry.getValue();
					    if (datasetAction.equals("Delete") && delete_timeSeries && keyFamilyBean.getTimeDimension() != null && 
								"".equals(gr.getKeyValues().get(keyFamilyBean.getTimeDimension().getConceptRef().getFullId()))){
							gr.getKeyValues().remove(keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
						}						
						temppout.print(tabs + "<" + namespacePrefix + ":Group");											
						printKey(gr);						
						printAttributes(gr);						
						temppout.print(">" + lineEnd);						
						tabs += "\t";
												
						StringBuilder crtKey = new StringBuilder("");
						for (String k:gr.getKeyValues().values()) {
							crtKey.append(k);
							crtKey.append(";");
						}
						for (String k:gr.getAttributeValues().values()) {
							crtKey.append(k);
							crtKey.append(";");
						}
						
						//sections
						for (final XSSection sc : gr.getSections()) {							
							temppout.print(tabs + "<" + namespacePrefix + ":Section");							
							printKey(sc);							
							printAttributes(sc);							
							temppout.print(">" + lineEnd);							
							tabs += "\t";
																					
							String crtSectKeyStr = crtKey.toString();
							StringBuilder crtSectKey = new StringBuilder(crtSectKeyStr);
							for (String k:sc.getKeyValues().values()) {
								crtSectKey.append(k);
								crtSectKey.append(";");
							}
							for (String k:sc.getAttributeValues().values()) {
								crtSectKey.append(k);
								crtSectKey.append(";");
							}
							String sectionKey = crtSectKey.substring(0, crtSectKey.length()-1); 							
							
							//observations															
							while ((line = brdo.readLine()) != null) {					
								sctKey = new StringBuilder("");
								data = line.split(";");
								
								for (int d = 0; d < xgroupDims.length; d++) {
									// sctKey += ";" + data[componentOrder.get(xgroupDims[d])];
									sctKey.append(";");
									sctKey.append(data[componentOrder.get(xgroupDims[d])]);
								}
								for (int d = 0; d < xgroupAtts.size(); d++) {
									// sctKey += ";" + data[componentOrder.get(xgroupAtts[d])];
									sctKey.append(";");
									sctKey.append(data[componentOrder.get(xgroupAtts.get(d))]);
								}
								for (int d = 0; d < xsectionDims.size(); d++) {
									// sctKey += ";" + data[componentOrder.get(xsectionDims[d])];
									sctKey.append(";");
									sctKey.append(data[componentOrder.get(xsectionDims.get(d))]);
								}
								for (int d = 0; d < xsectionAtts.size(); d++) {
									// sctKey += ";" + data[componentOrder.get(xsectionAtts[d])];
									sctKey.append(";");
									sctKey.append(data[componentOrder.get(xsectionAtts.get(d))]);
								}	
								
								String sctKeyStr = "";
								if (sctKey.length() != 0) {// if there are attributes or dimensions in group or section level
									sctKeyStr = sctKey.substring(1);
								} else {// if there do not exist
								    final String value = "";
									sctKey.append(";");
									sctKey.append(value);
									sctKeyStr = sctKey.substring(1);
								}
								
								
								if (sectionKey.equals(sctKeyStr)) {																							    									
									final XSObservation obs = new XSObservation();		
									
									if (componentOrder.size()>data.length){
										obs.setValue("");
									}else{
										obs.setValue(data[componentOrder.get(keyFamilyBean.getPrimaryMeasure().getId())]);
									}

									if (((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures().size() == 0) {
										obs.setMeasureDimKey(keyFamilyBean.getPrimaryMeasure().getId());
									} else {
										// in this case data[0] always has the crossX measure code
										obs.setMeasureDimKey(data[0]);
									}
									for (int d = 0; d < xobservationDims.size(); d++) {
										obs.getKeyValues().put(xobservationDims.get(d), data[componentOrder.get(xobservationDims.get(d))]);
									}
									for (int d = 0; d < xobservationAtts.size(); d++) {
									    final String s = data[componentOrder.get(xobservationAtts.get(d))];
										if (!"null".equals(s)) {
											obs.getAttributeValues().put(xobservationAtts.get(d), s);
										}
									}										
									printXSObservation(obs);
								}																																								
							}	
							//end section
							tabs = tabs.substring(1);
							temppout.println(tabs + "</" + namespacePrefix + ":Section>");																																			
						}				
						//end group
						tabs = tabs.substring(1);
						temppout.println(tabs + "</" + namespacePrefix + ":Group>");					    					    					    					    												
					}
					temppout.flush();
										
					brdo.close();
					disposePrintWriters(tempASCIIpw);
					
					// switch temp files
					
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						File f = tempXSectionFile;
						tempXSectionFile = tempXSectionFile2;
						tempXSectionFile2 = f;

						f = tempAsciiFile;
						tempAsciiFile = tempAsciiFile2;
						tempAsciiFile2 = f;
						f = null;
					} else {
						ByteArrayOutputStream b = tempXSectionOS;
						tempXSectionOS = tempXSectionOS2;
						tempXSectionOS2 = b;

						b = tempAsciiOS;
						tempAsciiOS = tempAsciiOS2;
						tempAsciiOS2 = b;
						b = null;
					}

					if (grpsTreeMap.size() < keySetLimit) {
						break;// processed records < keySetLimit i.e. all
						// records have been processed. Exit loop
					}
					// keySetLimit++;//slowly increase the limit (careful, when
					// keySetLimit ~ 1 may lead to many iterations that throw
					// OutOfMemoryError)
				} catch (OutOfMemoryError e) {
					// on OutOfMemoryError reduce the keySetlimit to its half
					// value
					keySetLimit = grpsTreeMap.size() / 2;
					grpsTreeMap = null;
					sctsTreeMap = null;
					temppout.close();
					brdg.close();
					brds.close();
					if ( brdo != null ) {
					    brdo.close();
					}

					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						tempSdmxFile = File.createTempFile("tempSdmxFile", null);
						temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempSdmxFile), "UTF-8"));
					} else {
						tempSdmxOS = new ByteArrayOutputStream();
						temppout = getPrintWriterForByteArrOS(tempSdmxOS);
					}

					parsedXGroups = 0;
					tempXSpw.close();
					tempASCIIpw.close();
					// if the available memory is small even for a single group
					// then nothing can be done
					if (keySetLimit > 0) {
						//IoUtils.getLogger().warning(prop.getProperty("Caught OutOfMemoryError :") + e.getMessage() + prop.getProperty(". New keySetLimit:") + keySetLimit);
						logger.warn("Caught OutOfMemoryError :" + e.getMessage() + ". New keySetLimit:" + keySetLimit);

					} else {
						if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
							disposeTempFiles(tempXGroupFile, tempXSectionFile, tempXSectionFile2, tempAsciiFile, tempAsciiFile2, tempSdmxFile);
						} else {
							disposeOutputStreams(tempXGroupOS, tempXSectionOS, tempXSectionOS2, tempAsciiOS, tempAsciiOS2, tempSdmxOS);
						}

						String errorMessage = "Caught OutOfMemoryError. The keySetLimit is already set to 1, cannot decrease further. Please increase the amount of memory available to java."
								+ lineEnd + "\t";
						throw new Exception(errorMessage + e.getMessage(), e);
					}
				}
			}
			
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				disposeTempFiles(tempXGroupFile, tempXSectionFile, tempXSectionFile2, tempAsciiFile, tempAsciiFile2);
			} else {
				disposeOutputStreams(tempXGroupOS, tempXSectionOS, tempXSectionOS2, tempAsciiOS, tempAsciiOS2);
			}

			// copy groups and sections to the final output file
			temppout.flush();
			temppout.close();
			
			BufferedReader brd = null;
			try {								
				if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE))  {
					brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempSdmxFile), "UTF-8"));
				} else {
					brd = getBufferedReaderFromByteArrOS(tempSdmxOS);
				}
	
				while ((line = brd.readLine()) != null) {
					pout.println(line);
	
				}
			} finally {
				brd.close();
			}
			
			pout.flush();			
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				disposeTempFiles(tempSdmxFile);
			} else {
				disposeOutputStreams(tempSdmxOS);
			}
			grpsTreeMap = null;
		} catch (IOException ioe) {
			String errorMessage = this.getClass().getName() + ".buildAndPrintXSGroups() exception :" + "Could not access the temp file" + lineEnd
					+ "\t";
			throw new IOException(errorMessage + ioe.getMessage());

		} catch (Exception e) {

			final String errorMessage = this.getClass().getName() + ".buildAndPrintXSGroups() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if(brdg != null) {brdg.close();}
			if(brds != null) {brds.close();}
			if(brdo != null) {brdo.close();}			
			disposePrintWriters(tempXSpw,tempASCIIpw);	
			disposeTempFiles(tempXSectionFile2);
		}
	}
	
	
	
	
	private void processFile() throws Exception {						
		try {							
			
			
			tempSdmxFile = File.createTempFile("tempSdmxFile", null);
			temppout = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempSdmxFile), "UTF-8"));
			
			File fileToBeProcessed = tempAsciiFile;
			
			while(true) {
				File unprocessedRecords = tempFileIteration(fileToBeProcessed);
				if ((unprocessedRecords == null) || (unprocessedRecords.length() == 0)) {
					break;
				} else {
					fileToBeProcessed = unprocessedRecords;
				}				 				
			}			
			
			BufferedReader brd = null;
			try {								
				
				brd = new BufferedReader(new InputStreamReader(new FileInputStream(tempSdmxFile), "UTF-8"));
				String line = null;
				while ((line = brd.readLine()) != null) {
					pout.println(line);
				}
			} finally {
				brd.close();
			}
			
			pout.flush();						
		} catch (Exception e) {
			final String errorMessage = this.getClass().getName() + ".buildAndPrintXSGroups() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {			
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				disposeTempFiles(tempSdmxFile);
			} else {
				disposeOutputStreams(tempSdmxOS);
			}					
		}
	}
	
	private File tempFileIteration(File file) throws Exception {
		
		BufferedReader brdo = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));				
		File unprocessed = null;
		PrintWriter pwUnprocessed = null;
				
		String[] data = null;
		try {																
			String crtGroupKey = null;
			String crtSectKey = null;
			
			String line = brdo.readLine();
			if(line !=null && line.length()>0) { 
							
				data = line.split(";");
				
				XSGroup gr = buildGroupKey(line);
				crtGroupKey = getGroupKeyString(line);
				
				//print first group
			    if (datasetAction.equals("Delete") && delete_timeSeries && keyFamilyBean.getTimeDimension() != null && 
						"".equals(gr.getKeyValues().get(keyFamilyBean.getTimeDimension().getConceptRef().getFullId()))){
					gr.getKeyValues().remove(keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
				}						
				temppout.print(tabs + "<" + namespacePrefix + ":Group");											
				printKey(gr);						
				printAttributes(gr);						
				temppout.print(">" + lineEnd);						
				tabs += "\t";
																														
				crtSectKey = getSectionKeyString(line,crtGroupKey);
				XSSection sec = buildSectionKey(line);
				
				//print first section
				temppout.print(tabs + "<" + namespacePrefix + ":Section");							
				printKey(sec);							
				printAttributes(sec);							
				temppout.print(">" + lineEnd);							
				tabs += "\t";	
				
				//print obs
				final XSObservation obs = new XSObservation();		
				
				if (componentOrder.size()>data.length){
					obs.setValue("");
				}else{
					obs.setValue(data[componentOrder.get(keyFamilyBean.getPrimaryMeasure().getId())]);
				}

				if (((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures().size() == 0) {
					obs.setMeasureDimKey(keyFamilyBean.getPrimaryMeasure().getId());
				} else {
					// in this case data[0] always has the crossX measure code
					obs.setMeasureDimKey(data[0]);
				}
				for (int d = 0; d < xobservationDims.size(); d++) {
					obs.getKeyValues().put(xobservationDims.get(d), data[componentOrder.get(xobservationDims.get(d))]);
				}
				for (int d = 0; d < xobservationAtts.size(); d++) {
				    final String s = data[componentOrder.get(xobservationAtts.get(d))];
					if (!"null".equals(s)) {
						obs.getAttributeValues().put(xobservationAtts.get(d), s);
					}
				}										
				printXSObservation(obs);
				
			}
								
			while ((line = brdo.readLine()) != null) {			
				
				data = line.split(";");
				
				String grKey = getGroupKeyString(line);
				if(!grKey.equals(crtGroupKey)) {
					if(unprocessed == null) {
						unprocessed = File.createTempFile("unprocessedRecs", null);
						pwUnprocessed = new PrintWriter(new OutputStreamWriter(new FileOutputStream(unprocessed), "UTF-8"));
					}					
					pwUnprocessed.println(line);
					continue;
				}																				
				String sectKey = getSectionKeyString(line, crtGroupKey);
				if(!sectKey.equals(crtSectKey)) {
					if(unprocessed == null) {
						unprocessed = File.createTempFile("unprocessedRecs", null);
						pwUnprocessed = new PrintWriter(new OutputStreamWriter(new FileOutputStream(unprocessed), "UTF-8"));
					}
					pwUnprocessed.println(line);
					continue;
				}
												
				
				final XSObservation obs = new XSObservation();		
				
				if (componentOrder.size()>data.length){
					obs.setValue("");
				}else{
					obs.setValue(data[componentOrder.get(keyFamilyBean.getPrimaryMeasure().getId())]);
				}

				if (((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures().size() == 0) {
					obs.setMeasureDimKey(keyFamilyBean.getPrimaryMeasure().getId());
				} else {
					// in this case data[0] always has the crossX measure code
					obs.setMeasureDimKey(data[0]);
				}
				for (int d = 0; d < xobservationDims.size(); d++) {
					obs.getKeyValues().put(xobservationDims.get(d), data[componentOrder.get(xobservationDims.get(d))]);
				}
				for (int d = 0; d < xobservationAtts.size(); d++) {
				    final String s = data[componentOrder.get(xobservationAtts.get(d))];
					if (!"null".equals(s)) {
						obs.getAttributeValues().put(xobservationAtts.get(d), s);
					}
				}										
				printXSObservation(obs);
			}			
			
			//end section
			tabs = tabs.substring(1);
			temppout.println(tabs + "</" + namespacePrefix + ":Section>");																																			
				
			//end group
			tabs = tabs.substring(1);
			temppout.println(tabs + "</" + namespacePrefix + ":Group>");
						
			return unprocessed;	
		} catch (Exception e) {
			return null;
		} finally {
			brdo.close();
			if (pwUnprocessed != null) {
				pwUnprocessed.close();
			}
			temppout.flush();
			temppout.close();
		}
	}
	
	private String getSectionKeyString (String line, String groupKey) {
		
		StringBuilder sctKey = new StringBuilder("");
		StringBuilder grpKey = new StringBuilder("");
		String [] data = line.split(";");

		for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {			
			grpKey.append(";");
			grpKey.append(data[xsGroupDimNum]);
			sctKey.append(";");
			sctKey.append(data[xsGroupDimNum]);
		}
		for (int xsGroupAttNum = 0; xsGroupAttNum < xgroupAtts.size(); xsGroupAttNum++) {			
			grpKey.append(";");
			grpKey.append(data[xsGroupAttNum + xgroupDims.length]);
			sctKey.append(";");
			sctKey.append(data[xsGroupAttNum + xgroupDims.length]);
		}
		
		String grpKeyStr = "";
		if (grpKey.length() != 0) {
			grpKeyStr = grpKey.substring(1);
		} else {// there might be no attributes or dimensions in Group orlevel
		    final String value = "";
			grpKey.append(";");
			grpKey.append(value);
			grpKeyStr = grpKey.substring(1);
		}

		if (groupKey.equals(grpKeyStr)) {
			for (int xSectionDimNum = 0; xSectionDimNum < xsectionDims.size(); xSectionDimNum++) {				
				sctKey.append(";");
				sctKey.append(data[xSectionDimNum + xgroupDims.length + xgroupAtts.size()]);				
			}
			for (int xSectionAttNum = 0; xSectionAttNum < xsectionAtts.size(); xSectionAttNum++) {				
				// columns are section key values
			    final String s = data[xSectionAttNum + xsectionDims.size() + xgroupDims.length + xgroupAtts.size()];				
				sctKey.append(";");
				sctKey.append(s);								
			}
			
			if (sctKey.length() != 0) {
				return sctKey.substring(1);
			} // remove first ";"
			else {
				sctKey.append(";");
				sctKey.append("");
				return sctKey.substring(1);// remove first ";"
			}									
		} else {
			return null;
		}
	}
	private XSSection buildSectionKey(String line) {
				
		XSSection sct = new XSSection();		
		String [] data = line.split(";");
								
		for (int xSectionDimNum = 0; xSectionDimNum < xsectionDims.size(); xSectionDimNum++) {							
			sct.getKeyValues()
					.put(xsectionDims.get(xSectionDimNum), data[xSectionDimNum + xgroupDims.length + xgroupAtts.size()]);
		}
		for (int xSectionAttNum = 0; xSectionAttNum < xsectionAtts.size(); xSectionAttNum++) {				
			// columns are section key values
		    final String s = data[xSectionAttNum + xsectionDims.size() + xgroupDims.length + xgroupAtts.size()];			
			// add section attributes only if section is contained in the groups map
			if (!"null".equals(s)) {
				sct.getAttributeValues().put(xsectionAtts.get(xSectionAttNum), s);
			}
		}		
		
		return sct;					
		
	}
	
	private String getGroupKeyString(String line) {
				
		StringBuilder grpKey = new StringBuilder("");
		String [] data = line.split(";");
		for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {
			grpKey.append(";");
			grpKey.append(data[xsGroupDimNum]);
		}

		for (int d = 0; d < xgroupAtts.size(); d++) {		
			final String s = data[d + xgroupDims.length];
			grpKey.append(";");
			grpKey.append(s);
		}		
		// if there are no attributes or dimensions in group level
		if (grpKey.length() == 0) {
		    final String s = "";
			grpKey.append(";");
			grpKey.append(s);
		}
		// remove first ";"
		return grpKey.substring(1);		
	}
	
	private XSGroup buildGroupKey(String line) {		
		XSGroup grp = new XSGroup();		
		String [] data = line.split(";");
		for (int xsGroupDimNum = 0; xsGroupDimNum < xgroupDims.length; xsGroupDimNum++) {
			grp.getKeyValues().put(xgroupDims[xsGroupDimNum], data[xsGroupDimNum]);						
		}

		for (int d = 0; d < xgroupAtts.size(); d++) {
			// first xgroupDims.length columns are group key values
		    final String s = data[d + xgroupDims.length];
			if (!"null".equals(s)) {
				grp.getAttributeValues().put(xgroupAtts.get(d), s);
			}			
		}		
		
		return grp;
	}
	

	private void printXSGroup(final XSGroup gr) {
		// <biscs:Group TIME="2000" BIS_UNIT="USD" UNIT_MULT="5" DECIMALS="2"
		// AVAILABILITY="A" FREQ="A" >
		
		/*check for the time dimension and delete series message..if the time dimension does not have time value then it should be removed from the xsgroup*/
		if (datasetAction.equals("Delete") && delete_timeSeries && keyFamilyBean.getTimeDimension() != null && 
				"".equals(gr.getKeyValues().get(keyFamilyBean.getTimeDimension().getConceptRef().getFullId()))){
			gr.getKeyValues().remove(keyFamilyBean.getTimeDimension().getConceptRef().getFullId());
		}

		// <biscs:Group
		temppout.print(tabs + "<" + namespacePrefix + ":Group");
	
		// print group key
		printKey(gr);

		// print group attributes
		printAttributes(gr);

		// >
		temppout.print(">" + lineEnd);

		// print sections
		tabs += "\t";
		for (final XSSection sc : gr.getSections()) {
			printXSSection(sc);
		}

		// </biscs:Group>
		tabs = tabs.substring(1);
		temppout.println(tabs + "</" + namespacePrefix + ":Group>");
	}

	private void printXSSection(final XSSection sc) {
		// <biscs:Section COLLECTION="B" TIME_FORMAT="P1Y">

		// <biscs:Section
		temppout.print(tabs + "<" + namespacePrefix + ":Section");

		// print section key
		printKey(sc);

		// print section attributes
		printAttributes(sc);

		// >
		temppout.print(">" + lineEnd);

		// print observations
		tabs += "\t";
		for (final XSObservation ob : sc.getObservations()) {
			printXSObservation(ob);
		}

		// </biscs:Section>
		tabs = tabs.substring(1);
		temppout.println(tabs + "</" + namespacePrefix + ":Section>");
	}

	private void printXSObservation(final XSObservation ob) {
		// <biscs:STOCKS JD_CATEGORY="A" VIS_CTY="MX" value="3.14"
		// OBS_STATUS="A"/>

		// <biscs:STOCKS
		if (measureDimConcepts.get(ob.getMeasureDimKey())==null){
			temppout.print(tabs + "<" + namespacePrefix + ":" + ob.getMeasureDimKey());
		}else{
		    temppout.print(tabs + "<" + namespacePrefix + ":" + measureDimConcepts.get(ob.getMeasureDimKey()));
		}

		// print observation key (if there are dimensions attached to the
		// observation
		printKey(ob);
		if (!(ob.getValue().equals(""))) {
			// value="3.14"
			temppout.print(" value=\"" + IoUtils.handleSpecialCharacters(ob.getValue()) + "\"");
		}
		// else
		// do not write NaN but omit the OBS_VALUE concept
		// print observation attributes
		printAttributes(ob);

		// />
		temppout.print("/>" + lineEnd);
	}

	private void printAttributes(final AttachableArtefact aa) {
		if (aa.getAttributeValues().size() == 0) {
			return;
		}

		// AVAILABILITY="A" DECIMALS="2"
		for (final String s : aa.getAttributeValues().keySet()) {
			temppout.print(" " + IoUtils.handleSpecialCharacters(s) + "=\"" + IoUtils.handleSpecialCharacters(aa.getAttributeValues().get(s)) + "\"");
		}
	}

	private void printKey(final Key ke) {
		// JD_CATEGORY="A" VIS_CTY="MX"
		for (final String s : ke.getKeyValues().keySet()) {
			temppout.print(" " + IoUtils.handleSpecialCharacters(s) + "=\"" + IoUtils.handleSpecialCharacters(ke.getKeyValues().get(s)) + "\"");
		}
	}
	
	/**
	 * This is a housekeeping method for flushing and closing output streams
	 * @param streams a var-args argument for OutputStream objects
	 * @throws IOException
	 */
	private void disposeOutputStreams(final OutputStream... streams) throws IOException {
		if (streams.length > 0) {
			for (int i = 0; i < streams.length; i++) {
				if(streams[i] != null) {
					streams[i].flush();
					streams[i].close();
				}
			}
		}
	}

	/**
	 * This is a housekeeping method for flushing and closing PrintWriters
	 * @param streams a var-args argument for PrintWriter objects
	 * @throws IOException
	 */
	private void disposePrintWriters(final PrintWriter... writers) throws IOException {
		if (writers.length > 0) {
			for (int i = 0; i < writers.length; i++) {
				if(writers[i] != null) {
					writers[i].flush();
					writers[i].close();
				}
			}
		}
	}
	
	/**
	 * This is a housekeeping method for deleting temporary files
	 * @param streams a var-args argument for File objects
	 * @throws IOException
	 */
	private void disposeTempFiles(final File... files) throws IOException {
		if (files.length > 0) {
			for (int i = 0; i < files.length; i++) {
				if(files[i] != null) {
					files[i].delete();
				}
			}
		}
	}

	/**
	 * This is a convenience method for getting a BufferedReader for the byte[] contents of a ByteArrayOutputStream. It
	 * is meant to make the code more readable.
	 * @param bos the ByteArrayOutputStream for the contents of which a BufferedReader will be returned.
	 * @return a BufferedReader for the contents of the stream
	 */
	private BufferedReader getBufferedReaderFromByteArrOS(final ByteArrayOutputStream bos) throws UnsupportedEncodingException {
		return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bos.toByteArray()), "UTF-8"));
	}

	/**
	 * This is a convenience method for getting a PrintWriter for the ByteArrayOutputStream. It is meant to make the
	 * code more readable.
	 * @param bos A ByteArrayOutputDtream
	 * @return a PrintWriter that will write in the input ByteArrayOutputStream
	 * @throws UnsupportedEncodingException
	 */
	private PrintWriter getPrintWriterForByteArrOS(final ByteArrayOutputStream bos) throws UnsupportedEncodingException {
		return new PrintWriter(new BufferedWriter(new OutputStreamWriter(bos, "UTF-8")));
	}
	public void releaseResources() throws Exception {	
		disposeOutputStreams(tempAsciiOS,tempAsciiOS2,tempTsGroupOS,tempXGroupOS,tempXSectionOS,tempSdmxOS);
		
		if (os != null && !os.getClass().toString().endsWith("java.util.zip.ZipOutputStream") && pout!=null){
			pout.close();
		}
		
		disposePrintWriters(temppout,tempgrpout);
		disposeTempFiles(tempAsciiFile,tempAsciiFile2,tempTsGroupFile,tempXGroupFile,tempXSectionFile,tempSdmxFile);
	}

}
