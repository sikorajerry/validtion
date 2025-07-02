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
import com.intrasoft.sdmx.converter.io.data.Defs;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.exceptions.ParseXmlException;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.struval.engine.DefaultVersion;
import org.estat.struval.engine.NodeTracker;
import org.estat.struval.engine.impl.IgnoreErrorsExceptionHandler;
import org.estat.struval.engine.impl.NodeChildTracker;
import org.estat.struval.engine.impl.NodeTrackerComposite;
import org.estat.struval.engine.impl.SdmxHeaderReaderEngine;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.sdmxsource.sdmx.util.beans.reference.MaintainableRefBeanImpl;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * Implementation of the Reader interface for reading Cross-sectional SDMX data messages.
 */
public class CrossDataReader extends DefaultHandler implements Reader, DefaultVersion {

	private static Logger logger = LogManager.getLogger(CrossDataReader.class);

	//default no-arg constructor
	public CrossDataReader(){
		
	}


	final private String lineEnd = System.getProperty("line.separator");
	/*
	 * The following are a set of DSD-related temporary properties required for the conversion from the time-series data
	 * model to the cross-sectional data model. These properties are populated by method "populateDsdRelatedProperties"
	 * using information stored in the DSD of the message that needs to be converted.
	 */
	/*
	 * stores the order of all dsd components as declared in the dsd, with these exceptions: if it exists, measure
	 * dimension is placed first time dimension is placed after the measure dimesnion not enforced yet: frequency
	 * dimension is placed after the time dimension
	 */
	private Map<String, Integer> componentOrder;
	private int obsOrder;// stores the order of the primary measure (for
	// convenience)
	private Map<String, String> measureDimConcepts;// maps XSmeasure
	// concept names
	// to measure
	// dimension
	// values
	private String[] groupNames;// stores the names of the dsd groups
	private String[][] groupDims;// for each group (first[]), stores its
	// dimension names (second[])
	private String[][] groupAtts;// for each group (first[]), stores its
	// attribute names (second[])
	private LinkedHashMap<String, String> xsDatasetAtts;// store attributes
	// attached at xs
	// dataset level

	private boolean verifiedFirstElement = false;
	private boolean parsingHeader = false;
	private HeaderTagReader headerReader;
	private HeaderBean header;
	private XSGroup currentXSGroup;
	private XSSection currentXSSection;
	private XSObservation currentXSObservation;
	private DataSet dataSet;
	private TimeseriesKey currentTimeseriesKey;
	// variable that holds the type of the message, update or delete
	private String datasetAction = "";
	private StructureIdentifier structIdentifier = null;
	private String urn = null;
	private String defaultVersion;
	
	/**
	 * Temporary file that contains the entire message data, at the xobservation level. Data at higher levels are
	 * redundantly replicated in each xobservation record.
	 */
	private File tempAsciiFile;

	/**
	 * Temporary Buffer that contains the entire message data, at the xobservation level. Data at higher levels are
	 * redundantly replicated in each xobservation record.
	 */
	private ByteArrayOutputStream tempAsciiOS;

	// used for writing to the temporary file
	private PrintWriter pw;

	/* properties required for reading data */
	private DataStructureBean keyFamilyBean;

	/* properties required for implementing the Reader interface */
	private InputStream is;
	private InputParams params;
	private Writer writer;
	private boolean writeEmptyData = false;
	private String headerLine;

	/**
	 * Implements Reader interface method.
	 */
	public DataMessage readData(final InputStream is, final InputParams params) throws Exception {
		final DataMessageWriter dwriter = new DataMessageWriter();
		dwriter.setInputParams(params);
		readData(is, params, dwriter);
		return dwriter.getDataMessage();
	}

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
		this.params = params;
		setKeyFamilyBean(params.getKeyFamilyBean());
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
		if (!ready) {
			errorMessage = errorMessage.substring(0, errorMessage.length() - 2) + ".";
			throw new Exception(errorMessage);
		}
		final String writerFormat = this.writer.getClass().getSimpleName().toString();

		if ((keyFamilyBean.getTimeDimension() == null)  && 
				(!"FlrDataWriter".equals(writerFormat)) && (!"CsvDataWriter".equals(writerFormat)) && (!"DataMessageWriter".equals(writerFormat))
				&& (!"Generic21DataWriter".equals(writerFormat)) && (!"StructureSpecificDataWriter".equals(writerFormat))){
			throw (new Exception("Since DSD has no Time Dimension, the conversion from the selected Input Format to the selected Output Format is not supported!"));
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

	
	private DataStructureBean setKeyFamilyBean(StructureIdentifier structIdentifier) {
		if(params.getKeyFamilyBean()!=null) {
			keyFamilyBean = params.getKeyFamilyBean();
			return keyFamilyBean;
		}
		if(structIdentifier!=null) {
			SdmxBeanRetrievalManager retrievalManager = params.getStructureRetrievalManager();
			MaintainableRefBeanImpl maintaibleBean = new MaintainableRefBeanImpl( structIdentifier.getAgency(), 
																				  structIdentifier.getArtefactId(), 
																				  structIdentifier.getArtefactVersion());
			DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, maintaibleBean);
			keyFamilyBean = dataStructure;
			return keyFamilyBean;
		}
		if(structIdentifier==null || structIdentifier.getArtefactVersion()==null) {
			Set<DataStructureBean> latestDsd = null;
			//get the latest dsd
			latestDsd = params.getStructureRetrievalManager().getMaintainableBeans(DataStructureBean.class);
    		for(DataStructureBean dsd:latestDsd) {
    			keyFamilyBean = dsd;
    		}
    		return keyFamilyBean;
		}
		return keyFamilyBean;
	}
	
	/**
	 * We try to identify the version of the datastructure in case we didnt have one
	 * we parse the start of the input file
	 * to find urn and 
	 * @throws ParseXmlException
	 * @throws IOException
	 */
	private void computeKeyFamilyBean() throws ParseXmlException, IOException {	
		//First we search the urn to find a match for version
		is.mark(0);
		parseHeaderFromInput(is, Formats.CROSS_SDMX);
		is.reset();
		StructureIdentifier structIdentifierHeader = StructureIdentifier.computeStructureIdentifier(Formats.CROSS_SDMX, header);
		if(structIdentifierHeader!=null) {
			structIdentifier = structIdentifierHeader;
		}
		
		//If no version found we search header elements
		if(structIdentifier==null) {
			is.mark(1);
			parseUrnFromInput(is, Formats.CROSS_SDMX);
			is.reset();
			StructureIdentifier structIdentifierUrn = StructureIdentifier.computeStructureIdentifier(Formats.CROSS_SDMX, urn);
			if(structIdentifierUrn!=null) {
				structIdentifier = structIdentifierUrn;
			}
		}
		//If structure identifier was found then set params again
		if(structIdentifier!=null) {
			keyFamilyBean = setKeyFamilyBean(structIdentifier);
			params.setKeyFamilyBean(keyFamilyBean);
			//We need to set again the structure for the writer
			writer.setInputParams(params);
		}
	}
	
	/**
	 * Implements Reader interface method.
	 */
	public void readData() throws Exception {
		logger.info(this.getClass().getName() + ".readData");
		try {
			if(keyFamilyBean==null) {
				computeKeyFamilyBean();
			}
			isReady();
			populateDsdRelatedProperties();
			makeAsciiDataFileHeader();
			parseCrossDataMessage(is);
			parseTempAsciiDataFile(Integer.MAX_VALUE);
			writer.closeWriter();
		} catch (Exception e) {
			final String errorMessage = this.getClass().getName() + "exception" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if(pw != null) {
				pw.close();
			}
			if(tempAsciiOS != null) {
				tempAsciiOS.close();
			}
			if(tempAsciiFile != null) {
				tempAsciiFile.delete();
			} 
			if(writer != null) {
				writer.releaseResources();
			}			
		}
	}
	
	/**
	 * In case of xml read the header of the input.
	 * @param inputStream
	 * @param format
	 * @return header
	 * @throws IOException 
	 * @throws WriteHeaderException
	 */
    public void parseHeaderFromInput(InputStream inputStream, Formats format) throws ParseXmlException, IOException {
    	XMLInputFactory factory = XMLInputFactory.newInstance();
    	HeaderBean headerBean = null;
    	try {
			XMLStreamReader parser = factory.createXMLStreamReader(inputStream, "UTF-8");
            NodeTracker nodeTextTracker = new NodeTrackerComposite(new NodeChildTracker(parser, format.getDataType()));
            headerBean = SdmxHeaderReaderEngine.processHeader(parser, nodeTextTracker, null, params.getStructureDefaultVersion(), new IgnoreErrorsExceptionHandler());
            parser.close();
            header = headerBean;
        } catch (XMLStreamException e) {
            throw new ParseXmlException("Error while reading the xml header!", e);
        }
    }
    
    /**
     * Try to find urn for dataStructure from Namespace/Dataset keyFamilyURI
     * @param inputStream
     * @param format
     * @return urn String
     * @throws WriteHeaderException
     */
    public void parseUrnFromInput(InputStream inputStream, Formats format) throws ParseXmlException {
    	XMLInputFactory factory = XMLInputFactory.newInstance();
    	int event = 0;
    	try {
            XMLStreamReader reader = factory.createXMLStreamReader(inputStream, "UTF-8");
            while (reader.hasNext()) {
        	  event = reader.next();
        	  if (XMLStreamConstants.START_ELEMENT == event) {
					if (reader.getNamespaceCount() > 0) {
					  for (int nsIndex = 0; nsIndex < reader.getNamespaceCount(); nsIndex++) {
					    String nsId = reader.getNamespaceURI(nsIndex);
					    if(nsId!=null && nsId.startsWith("urn:")) {
					    	urn = nsId;
					    	logger.info("Urn found: " + nsId + ", an attempt to identify structure version will be made.");
					    }
					  }
					}
				if ("DataSet".equals(reader.getLocalName())) {
					for (int i = 0; i < reader.getAttributeCount(); i++) {  					
					    final String aName = ("".equals(reader.getAttributeLocalName(i)) ? reader.getAttributeName(i).getLocalPart() : reader.getAttributeLocalName(i));
					    final String aValue = reader.getAttributeValue(i);
						if ("keyFamilyURI".equals(aName)) {
							if(aValue!=null && aValue.startsWith("urn:")) {
								urn = aValue;
								logger.info("keyFamilyURI found: " + aValue + ", an attempt to identify structure version will be made.");
							}
						} 																			
					}
					//If DataSet node was parsed and nothing found
					//there is no reason to keep parsing
					break;
				}
        	  }

        	  if(urn!=null) {
        		  //If urn is set we don't need to keep parsing
        		  break;
        	  }
        	}//End of while loop
            reader.close();
        } catch (XMLStreamException e) {
            throw new ParseXmlException("Error while reading the xml header!", e);
        }
    }
    

	/* Required for reading data */
	public void setKeyFamilyBean(final DataStructureBean keyFamilyBean) {
		this.keyFamilyBean = keyFamilyBean;
	}

	private void populateDsdRelatedProperties() throws Exception {
		/*
		 * Convention We need a convention for sorting all dsd components' concepts, as well as the primary measure
		 * concept. According to that convention: the first concept will be the measure dimension concept, then the time
		 * dimension concept,not enforced yet then the frequency dimension concept, then the remaining dimension
		 * concepts, then the primary measure concept, and then the attribute concepts in this order: the
		 * observation-level attribute concepts, then the series-level attribute concepts, then the group-level
		 * attribute concepts and finally the dataset-level attribute concepts.
		 */
		componentOrder = new LinkedHashMap<String, Integer>();
		int i = 2;// 0, 1 reserved for measure and time dimensions respectively
		String cref = null;

		// time dimension
		if (keyFamilyBean.getTimeDimension() != null) {// dsd with time dimension
			cref = keyFamilyBean.getTimeDimension().getConceptRef().getFullId();
			componentOrder.put(cref, 1);
		} else { // dsd without time dimension
			// componentOrder.put("Primary Measure", 0);// dsd with no crossx measure
			i = 1;
		}
		if (!(keyFamilyBean instanceof CrossSectionalDataStructureBean)) {
			componentOrder.put("Primary Measure", 0);// dsd with no crossx measure
		} 

		// primary measure
		componentOrder.put(keyFamilyBean.getPrimaryMeasure().getId(), 0);
		
		// dimensions
		for (final ListIterator<DimensionBean> it = keyFamilyBean.getDimensions().listIterator(); it.hasNext();) {
			final DimensionBean db = it.next();
			
			if (db.isTimeDimension()) {continue;}//time dimension is treated separately
			
			cref = db.getId();
			if (db.isMeasureDimension()) {
				obsOrder = i;
				componentOrder.put(cref, i++);
			} else if (db.isFrequencyDimension()) {
				componentOrder.put(cref, i++);
			} else {
				componentOrder.put(cref, i++);
			}
		}

		// attributes
		final List<AttributeBean> allAttrs = new ArrayList<AttributeBean>();
		xsDatasetAtts = new LinkedHashMap<String, String>();// store attributes
		// attached at xs
		// dataset level
		allAttrs.addAll(keyFamilyBean.getObservationAttributes());
		allAttrs.addAll(keyFamilyBean.getDimensionGroupAttributes());
		allAttrs.addAll(keyFamilyBean.getGroupAttributes());
		allAttrs.addAll(keyFamilyBean.getDatasetAttributes());
		for (final AttributeBean ab : allAttrs) {
			cref = ab.getId();
			componentOrder.put(cref, i++);
			// store attributes attached at xs dataset level
			Map<String, String> valuesAttribute = ComponentBeanUtils.getAttributeInfo(ab, keyFamilyBean); 
			if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET) != null && valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET).equals("true")) {
				xsDatasetAtts.put(cref, null);
			}
		}

		//obsOrder = 0;

		// debug
		// for (String c : componentOrder.keySet()){
		// System.out.println(componentOrder.get(c)+" "+c);
		// }

		// store group-level attributes per attached group
		// ***convention: store each attribute only at its first attachment
		// group
		final Map<String, List<String>> groupAttributeRefs = new LinkedHashMap<String, List<String>>();
		for (final Object oba : keyFamilyBean.getGroupAttributes()) {
			final AttributeBean ab = (AttributeBean) oba;
			cref = ab.getId();
			// ***convention: store each attribute only at its first attachment
			// group
			Object obg = null;
			try{
				obg = ab.getAttachmentGroup();

			}catch (Exception e){
				final String errorMessage = "The DSD reports group attributes without providing attachment group information, i.e. which declared group the attribute is attached to." + lineEnd + "\t";
				throw new Exception(errorMessage, e);
			}

			final String gr = (String) obg;
			if (!groupAttributeRefs.containsKey(gr)) {
				groupAttributeRefs.put(gr, new ArrayList<String>());
			}
			groupAttributeRefs.get(gr).add(cref);
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
		int j;
		groupNames = new String[groupDimensionRefs.keySet().size()];
		groupDims = new String[groupDimensionRefs.keySet().size()][];
		groupAtts = new String[groupDimensionRefs.keySet().size()][];
		List<String> attConceptRefs = null;
		List<String> dimConceptRefs = null;

		// This loop populates the arrays groupNames, groupDims, groupAttrs with
		// the Names, Dimensions and Attributes.
		for (final String gr : groupDimensionRefs.keySet()) {

			// Names
			groupNames[i] = gr;

			// Dimensions
			dimConceptRefs = groupDimensionRefs.get(gr);
			groupDims[i] = new String[dimConceptRefs.size()];

			j = 0;
			for (String c : dimConceptRefs) {
				groupDims[i][j++] = c;
			}

			// Attributes
			attConceptRefs = groupAttributeRefs.get(gr);
			if (attConceptRefs != null) {
				groupAtts[i] = new String[attConceptRefs.size()];
				j = 0;
				for (String c : attConceptRefs) {
					groupAtts[i][j++] = c;
				}
			} else {
				groupAtts[i] = new String[0];
			}

			i++;
		}

		// store xs measures per measure dimension value
		measureDimConcepts = new LinkedHashMap<String, String>();
		// if the DSD has no crossx measures then the Primary measure is the crossx measure.
		if (!(keyFamilyBean instanceof CrossSectionalDataStructureBean) || 
				((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures() == null ||
				((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures().size() < 1) {
			final PrimaryMeasureBean pmb = keyFamilyBean.getPrimaryMeasure();
			measureDimConcepts.put(pmb.getId(), pmb.getId());
		} else {// else get the xs measures
			for (final Object obg : ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures()) {
				final CrossSectionalMeasureBean xmb = (CrossSectionalMeasureBean) obg;
				measureDimConcepts.put(xmb.getId(), xmb.getCode());
			}
		}
		// for (Object obg : keyFamilyBean.getCrossSectionalMeasures()) {
		// CrossSectionalMeasureBean xmb = (CrossSectionalMeasureBean) obg;
		// measureDimConcepts.put(xmb.getConceptRef(), xmb.getCode());
		// // System.out.println(xmb.getConceptRef()+" "+measureDimConcepts.get(xmb.getConceptRef()));
		// }

	}

	private void makeAsciiDataFileHeader() throws Exception {
		try {
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				tempAsciiFile = File.createTempFile("tempXsDataMessageToAscii", null);
				final OutputStream out = new FileOutputStream(tempAsciiFile);
				final OutputStreamWriter osw = new OutputStreamWriter(out, "UTF-8");
				pw = new PrintWriter(new BufferedWriter(osw));
			} else {
				tempAsciiOS = new ByteArrayOutputStream();
				pw = getPrintWriterForByteArrOS(tempAsciiOS);
			}

			// add header
			final StringBuilder sb = new StringBuilder(200);
			for (final Entry<String, Integer> en : componentOrder.entrySet()) {
				if (en.getValue().intValue() == 0) {
					sb.insert(0, en.getKey());
					continue;
				}
				sb.append(';');
				sb.append(en.getKey());
			}
			headerLine = sb.toString();

			pw.println(headerLine);

			pw.flush();

		} catch (Exception e) {
			final String errorMessage = "exception in method makeAsciiDataFileHeader():" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			pw.flush();
		}
	}

	/**
	 * Parses the ascii data file, constructs the corresponding time-series data beans (GroupKey, TimeSeriesKey and
	 * Observation) and adds them to the Dataset of the message created by the "parseDataMessage" method.
	 */
	private void parseTempAsciiDataFile(int keySetLimit) throws Exception {
		BufferedReader brdo = null;
		Map<String, GroupKey> grps = null;
		Map<String, TimeseriesKey> sers = null;
		String line = null;
		String grpKey = null;
		String prevgrpKey = "";
		String serKey = null;
		String prevserKey = "";
		String[] data = null;

		File tempFile2 = null;
		ByteArrayOutputStream tempOS2 = null;

		int parsedTimeSeries;

		IoUtils.ParseMonitor mon;

		try {

			tempFile2 = File.createTempFile("tempAsciiFile2", null);

			parsedTimeSeries = 0;

			// Parsing the source data is done in a multi-pass process,
			// in each pass (loop) parse the entire stream
			// exit the loop either on OutOfMemoryError
			// or when processed series < keySetLimit i.e. all series have been
			// processed
			boolean addedDatasetAttributes = false;
			while (true) {
				//IoUtils.getLogger().info("starting loop: keySetLimit: " + keySetLimit + ", timeseries processed: " + parsedTimeSeries);
				logger.info("starting loop: keySetLimit {} , timeseries processed: {}" , keySetLimit , parsedTimeSeries);
				grps = new TreeMap<String, GroupKey>();
				sers = new TreeMap<String, TimeseriesKey>();
				mon = new IoUtils().getParseMonitor(keySetLimit, false);
				try {
					//close pw before reassign to another file/stream. Otherwise the previous assigned file could not be deleted
					if(pw != null) {
						pw.flush();
						pw.close();
					}
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						pw = new PrintWriter(new BufferedOutputStream(new FileOutputStream(tempFile2)));
						brdo = new BufferedReader(new InputStreamReader(new FileInputStream(tempAsciiFile), "UTF-8"));
					} else {
						tempOS2 = new ByteArrayOutputStream();
						pw = getPrintWriterForByteArrOS(tempOS2);
						brdo = getBufferedReaderFromByteArrOS(tempAsciiOS);
					}

					final String writerFormat = this.writer.getClass().getSimpleName().toString();
					brdo.readLine();// skip header line
					// parse the entire temp file

					// parse the temp file
					int row = 0;
					boolean firstLineToTemp = true;

					while ((line = brdo.readLine()) != null) {
						row++;

						data = line.split(";");
						if (data.length == 24){
							int lala = 0;
							lala++;
						}
						// add dataset attributes
						if (!addedDatasetAttributes) {
							for (Object oba : keyFamilyBean.getDatasetAttributes()) {
								final String cref = ((AttributeBean) oba).getId();
								final String s = data[componentOrder.get(cref)];
								if (!"null".equals(s)) {
									dataSet.getAttributeValues().put(cref, s);
								}
							}
							addedDatasetAttributes = true;
							writer.writeEmptyDataSet(dataSet);
							writeEmptyData = true;
						}

						// monitor the parsing speed. (when there is not enough
						// memory the speed will slow down significantly)
						mon.monitor(row);

						// get series key
						final TimeseriesKey tk = new TimeseriesKey();
						StringBuilder sb = new StringBuilder();
						for (int i = 0; i < keyFamilyBean.getDimensionList().getDimensions().size(); i++) {
							if (keyFamilyBean.getDimensionList().getDimensions().get(i).isTimeDimension()) {continue;} //time dimension is treated separately
							final String cref = (String) keyFamilyBean.getDimensionList().getDimensions().get(i).getId();
							final String s = data[componentOrder.get(cref)];
							sb.append(';');
							sb.append(s);
							if (!"null".equals(s)) {
								tk.getKeyValues().put(cref, s);
							}
						}
						sb.deleteCharAt(0);
						serKey = sb.toString();

						// //do not process already processed series
						// if (parsedTks.contains(serKey)) continue;

						// process groups
						for (int i = 0; i < groupNames.length; i++) {
							// System.out.println("i, groupNames[i] "+i+", "+groupNames[i]);
							final GroupKey gk = new GroupKey();
							gk.setType(groupNames[i]);
							sb = new StringBuilder();
							for (int j = 0; j < groupDims[i].length; j++) {
								final String cref = groupDims[i][j];
								final String s = data[componentOrder.get(cref)];
								sb.append(';');
								sb.append(s);
								if (!"null".equals(s)) {
									gk.getKeyValues().put(cref, s);
								}
							}
							sb.deleteCharAt(0);
							grpKey = sb.toString();

							// if the group key is the same do nothing
							if (!prevgrpKey.equals(grpKey)) {
								// else, if GroupKey with the same key does not
								// exist, add the GroupKey in the groups Map
								if (!grps.containsKey(grpKey)) {
									grps.put(grpKey, gk);
									for (int j = 0; j < groupAtts[i].length; j++) {
										// System.out.println("i: " + i + " j: "
										// + (j) + " :groupAtts[i][j] " +
										// groupAtts[i][j]);
										final String cref = groupAtts[i][j];
										final String s = data[componentOrder.get(cref)];
										if (!"null".equals(s)) {
											gk.getAttributeValues().put(cref, s);
										}
									}
								}
								prevgrpKey = grpKey;
							}
						}

						final Iterator<GroupKey> git = grps.values().iterator();
						while (git.hasNext()) {
							final GroupKey gk = git.next();
							if (gk.getAttributeValues().size() == 0) {
								git.remove();// remove groupKeys with no
								// attributes
							}
						}

						// if the series key is the same do nothing, i.e. keep
						// the same currentTimeseriesKey
						if (!prevserKey.equals(serKey)) {
							// else either add TimeseriesKey in the series Map,
							// if TimeseriesKey with the same key does not
							// exist,
							if (!sers.containsKey(serKey)) {
								if (sers.size() == keySetLimit) {
									if (firstLineToTemp) { // if it's the first record to add add header
										pw.println(headerLine);
										firstLineToTemp = false;
									}

									pw.println(line); // store the unprocessed serie in the temp file
									continue;// do not process more series than the keySetLimit
								}
								currentTimeseriesKey = tk;
								sers.put(serKey, currentTimeseriesKey);
								// or get the existing TimeseriesKey from the
								// series Map
							} else {
								currentTimeseriesKey = sers.get(serKey);
							}
							prevserKey = serKey;
						}

						for (Object oba : keyFamilyBean.getDimensionGroupAttributes()) {
							final String cref = ((AttributeBean) oba).getId();
							final String s = data[componentOrder.get(cref)];
							if (!"null".equals(s)) {
								currentTimeseriesKey.getAttributeValues().put(cref, s);
							}
						}

						final Observation obs = new Observation();
						currentTimeseriesKey.getObservations().add(obs);
						
						if ((data[0].equals("-")) || (data[0].equals("null"))) {							
							obs.setValue(null);
						} else {
							obs.setValue(data[0]);
						}

						
						/*
						if ((data[obsOrder].equals("-")) || (data[obsOrder].equals("null"))) {// (data[obsOrder].
							// equalsIgnoreCase(
							// "NaN")) ||
							obs.setValue(null);
						} else {
							obs.setValue(data[obsOrder]);
						}
						*/

						if (data[1].equals("null")) {
							obs.setTimeValue(null);
						} else {
							obs.setTimeValue(data[1]);
						}
						// ---------------------------------------------------------
						// if (currentTimeseriesKey.getObservations().size() != 0) {
						// currentTimeseriesKey.getObservations().add(obs);
						// if (data[obsOrder].equals("-"))
						// obs.setValue(null);
						// else
						// obs.setValue(data[obsOrder]);
						// obs.setTimeValue(data[1]);
						//
						// }
						for (final Object oba : keyFamilyBean.getObservationAttributes()) {
							
							final String cref = ((AttributeBean) oba).getId();							
							final String s = data[componentOrder.get(cref)];
							if (!"null".equals(s)) {
								obs.getAttributeValues().put(cref, s);
							}
						}

						// //test the handling of the OutOfMemoryError
						// if (sers.size() == 100) {
						// throw (new
						// OutOfMemoryError("forced out of memory error, current keySetLimit: "+keySetLimit));
						// }

					}// end of pass

					// delete dataset messages
					if (datasetAction.equalsIgnoreCase("Delete") && writeEmptyData == false) {
						// if there exist any attributes
						final String[] data2 = new String[componentOrder.size()];

						for (final Entry<String, String> en : xsDatasetAtts.entrySet()) {
							if (en.getValue() != null) {
								data2[componentOrder.get(en.getKey())] = en.getValue();
							}
						}
						for (final Object oba : keyFamilyBean.getDatasetAttributes()) {
							final String cref = ((AttributeBean) oba).getId();
							final String s = data2[componentOrder.get(cref)];
							if (!"null".equals(s)) {
								dataSet.getAttributeValues().put(cref, s);
							}
						}
						writer.writeEmptyDataSet(dataSet);
						final TimeseriesKey tk = null;
						writer.writeTimeseriesKey(tk);
					}

					for (GroupKey gk : grps.values()) {
						writer.writeGroupKey(gk);
					}
					for (TimeseriesKey tk : sers.values()) {
						writer.writeTimeseriesKey(tk);
					}
					// parsedTks.addAll(sers.keySet());//add processed series of
					// this pass to the total of processed series
					disposePrintWriters(pw);
					brdo.close();// necessary for the temp file to be automatically deleted. ??

					// switch temp files
					if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
						final File f = tempAsciiFile;
						tempAsciiFile = tempFile2;
						tempFile2 = f;
					} else {
						final ByteArrayOutputStream b = tempAsciiOS;
						tempAsciiOS = tempOS2;
						tempOS2 = b;
					}

					parsedTimeSeries += sers.size();
					if (sers.size() < keySetLimit) {
						break;// processed series < keySetLimit i.e. all series
						// have been processed. Exit loop
					}

				} catch (OutOfMemoryError e) {
					// on OutOfMemoryError reduce the keySetlimit to its half
					// value
					keySetLimit = sers.size() / 2;
					sers = null;
					grps = null;
					if (keySetLimit > 0) {
						//commented out temporarily until we change the constructors of all readers and writers to take the logger
						//IoUtils.getLogger().warning(prop.getProperty("error.out.of.memory") + e.getMessage() + prop.getProperty("error.new.key.set.limit") + keySetLimit);
						logger.warn("Caught OutOfMemoryError :" + e.getMessage() + ". New keySetLimit:" + keySetLimit);

					} else {
						if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
							disposeTempFiles(tempFile2, tempAsciiFile);
						} else {
							disposeOutputStreams(tempOS2, tempAsciiOS);
						}

						final String errorMessage = "Caught OutOfMemoryError. The keySetLimit is already set to 1, cannot decrease further. Please increase the amount of memory available to java."
								+ lineEnd + "\t";
						throw new Exception(errorMessage + e.getMessage(), e);
					}
				}
			}
			if ( params.getXsBuffering().equals(Defs.XS_BUFFERING_FILE)) {
				disposeTempFiles(tempFile2, tempAsciiFile);
			} else {
				disposeOutputStreams(tempOS2, tempAsciiOS);
			}
		} catch (Exception e) {
			final String errorMessage = this.getClass().getName() + "exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		} finally {
			if(pw != null) {
				pw.close();
			}
			if(brdo !=null) {
				brdo.close();
			}
			if(tempFile2 !=null) {
				tempFile2.delete();
			}
			if(tempOS2 !=null) {
				tempOS2.close();
			}
		}
	}

	/**
	 * Parses a cross-sectional data message from an input stream, and creates an Ascii file
	 * 
	 * @param in
	 * @throws Exception
	 */
	private void parseCrossDataMessage(final InputStream in) throws Exception {
		// Use the default (non-validating) parser
		final SAXParserFactory factory = SAXParserFactory.newInstance();
		try {
			// Parse the input
			factory.setNamespaceAware(true);
			final SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(in, this);
		} catch (SAXParseException spe) {
			// Use the contained exception, if any
			Exception x = spe;
			if (spe.getException() != null) {
				x = spe.getException();
			}
			final String errorMessage = "Error parsing data, SAXParseException at line" + spe.getLineNumber() + " column" + spe.getColumnNumber()
					+ "Error parsing data, SAXParseException at line" + lineEnd + "\t";
			throw new Exception(errorMessage + spe.getMessage(), x);
		} catch (SAXException sxe) {
			// if this wraps our custom exception, use that original exception
			// to generate the stack trace
			Exception x = sxe;
			if (sxe.getException() != null) {
				x = sxe.getException();
				String errorMessage = "Error parsing data, SAXException :" + lineEnd + "\t";
				throw new Exception(errorMessage + sxe.getMessage(), x);
			}
		} 
	}


	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void startDocument() throws SAXException {
		// System.out.println("DOCUMENT");
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void endDocument() throws SAXException {
		// System.out.println("END DOCUMENT");
		pw.flush();
		pw.close();
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void startElement(final String namespaceURI, final String sName, final String qName, final Attributes attrs) throws SAXException {
		if (!verifiedFirstElement) {
			String eName = (sName.equals("") ? qName : sName);
			if (!"CrossSectionalData".equals(eName)) {
				throw new SAXException("Reader exception: The submitted message is not of CrossSectionalData type");
			}
			verifiedFirstElement = true;
			return;
		}

		if (parsingHeader) {
			headerReader.setStructureDefaultVersion(params.getStructureDefaultVersion());
			headerReader.startElement(namespaceURI, sName, qName, attrs);
			return;
		}

		final String eName = ("".equals(sName) ? qName : sName);
		// System.out.println("eName "+eName);
		if ("DataSet".equals(eName)) {
			dataSet = new DataSet();
			dataSet.setKeyFamilyRef(keyFamilyBean.getId());// set
			// <generic:KeyFamilyRef>
			// tag
			for (int i = 0; i < attrs.getLength(); i++) {
				final String aName = ("".equals(attrs.getLocalName(i)) ? attrs.getQName(i) : attrs.getLocalName(i));
				final String aValue = attrs.getValue(i);
				if ("keyFamilyURI".equals(aName)) {
					dataSet.setKeyFamilyURI(aValue);
				} else if ("datasetID".equals(aName)) {
					dataSet.setDatasetID(aValue);
				} else if ("dataProviderSchemeAgencyId".equals(aName)) {
					dataSet.setDataProviderSchemeAgencyId(aValue);
				} else if ("dataProviderSchemeId".equals(aName)) {
					dataSet.setDataProviderSchemeId(aValue);
				} else if ("dataProviderID".equals(aName)) {
					dataSet.setDataProviderID(aValue);
				} else if ("dataflowAgencyID".equals(aName)) {
					dataSet.setDataflowAgencyID(aValue);
				} else if ("dataflowID".equals(aName)) {
					dataSet.setDataflowID(aValue);
				} else if ("action".equals(aName)) {
					dataSet.setAction(aValue);
				} else if ("reportingBeginDate".equals(aName)) {
					dataSet.setReportingBeginDate(aValue);
				} else if ("reportingEndDate".equals(aName)) {
					dataSet.setReportingEndDate(aValue);
				} else if ("validFromDate".equals(aName)) {
					dataSet.setValidFromDate(aValue);
				} else if ("validToDate".equals(aName)) {
					dataSet.setValidToDate(aValue);
				} else if ("publicationYear".equals(aName)) {
					dataSet.setPublicationYear(aValue);
				} else if ("publicationPeriod".equals(aName)) {
					dataSet.setPublicationPeriod(aValue);
				} else if (xsDatasetAtts.containsKey(aName)) {
					xsDatasetAtts.put(aName, aValue);
					// System.out.println("aName "+aName+" aValue "+aValue);
				}
			}
		} else if ("Group".equals(eName)) {
			currentXSGroup = new XSGroup();
			for (int i = 0; i < attrs.getLength(); i++) {
				final String aName = ("".equals(attrs.getLocalName(i)) ? attrs.getQName(i) : attrs.getLocalName(i));
				final String aValue = attrs.getValue(i);
				// System.out.println("aName "+aName+" aValue "+aValue);

				// we put all xml attrs into the keyValues map
				// (it does not matter if they are dimension keys or attribute
				// values)
				currentXSGroup.getKeyValues().put(aName, aValue);
			}
		} else if ("Section".equals(eName)) {
			currentXSSection = new XSSection();
			for (int i = 0; i < attrs.getLength(); i++) {
				final String aName = ("".equals(attrs.getLocalName(i)) ? attrs.getQName(i) : attrs.getLocalName(i));
				final String aValue = attrs.getValue(i);
				// System.out.println("aName "+aName+" aValue "+aValue);

				// we put all xml attrs into the keyValues map
				// (it does not matter if they are dimension keys or attribute
				// values)
				currentXSSection.getKeyValues().put(aName, aValue);
			}
		} else if (measureDimConcepts.keySet().contains(eName)) {
			currentXSObservation = new XSObservation();
			currentXSObservation.setMeasureDimKey(measureDimConcepts.get(eName));
			for (int i = 0; i < attrs.getLength(); i++) {
				final String aName = ("".equals(attrs.getLocalName(i)) ? attrs.getQName(i) : attrs.getLocalName(i));
				final String aValue = attrs.getValue(i);
				// System.out.println("aName "+aName+" aValue "+aValue);

				if ("value".equals(aName)) {
					currentXSObservation.setValue(aValue);
				} else {
					// we put all xml attrs into the keyValues map
					// (it does not matter if they are dimension keys or
					// attribute values)
					currentXSObservation.getKeyValues().put(aName, aValue);
				}
			}

			final String[] data = new String[componentOrder.size()];

			for (final Entry<String, String> en : xsDatasetAtts.entrySet()) {
				if (en.getValue() != null) {
					data[componentOrder.get(en.getKey())] = en.getValue();
				}
			}
			for (final Entry<String, String> en : currentXSGroup.getKeyValues().entrySet()) {
				data[componentOrder.get(en.getKey())] = en.getValue();
			}
			for (final Entry<String, String> en : currentXSSection.getKeyValues().entrySet()) {
				data[componentOrder.get(en.getKey())] = en.getValue();
			}
			for (final Entry<String, String> en : currentXSObservation.getKeyValues().entrySet()) {
				data[componentOrder.get(en.getKey())] = en.getValue();
			}
			data[obsOrder] = currentXSObservation.getMeasureDimKey();
			data[0] = currentXSObservation.getValue();

			// print out record data in the temp ascii file
			final StringBuilder sb = new StringBuilder(200);
			for (int i = 0; i < data.length; i++) {
				sb.append(';');
				sb.append(data[i]);
			}
			sb.deleteCharAt(0);
			pw.println(sb.toString());//

		} else if ("Header".equals(eName)) {
			header = new HeaderBeanImpl("IREF123", "ZZ9");
			headerReader = new HeaderTagReader(header);
			parsingHeader = true;
		} else if ("".equals(eName)) {
		}
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void endElement(final String namespaceURI, final String sName, final String qName) throws SAXException {
		//System.out.println("endElement");
		final String eName = ("".equals(sName) ? qName : sName);
		try {
			// System.out.println("END ELEMENT \t"+eName);
				if ("Header".equals(eName)) {
					parsingHeader = false;
					// write HeaderBean
					header = new HeaderBeanImpl(header.getAdditionalAttributes(), 
							header.getStructures(), 
							header.getDataProviderReference(), 
							header.getAction(), 
							header.getId(), 
							header.getDatasetId(), 
							header.getEmbargoDate(), 
							headerReader.getExtractedDate(), 
							headerReader.getPreparedDate(), 
							header.getReportingBegin(), 
							header.getReportingEnd(), 
							header.getName(), 
							header.getSource(), 
							header.getReceiver(), 
							header.getSender(), 
							headerReader.getTest());
					writer.writeHeader(header);
					// delete messages
					if (header.getAction() != null) {
						datasetAction = header.getAction().getAction().toString();
					}
					// if (datasetAction.equalsIgnoreCase("Delete") && writeEmptyData == false) {
					// try {
					// writer.writeEmptyDataSet(dataSet);
					// TimeseriesKey tk = null;
					// writer.writeTimeseriesKey(tk);
					// } catch (Exception e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// }
					// }
				} else if ("Sender".equals(eName) || "Receiver".equals(eName)) {
					headerReader.endElement(namespaceURI, sName, qName);
				} 
		} catch (Exception e) {
			String errorMessage = "endElement " + eName + " :" + lineEnd + "\t";
			throw new SAXException(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void characters(final char buf[], final int offset, final int len) throws SAXException {
		if (parsingHeader) {
			headerReader.characters(buf, offset, len);
			return;
		}
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void startPrefixMapping(final String prefix, final String uri) throws SAXException {
		// System.out.println("start namespace mapping "+prefix+" - "+uri);
		// if (uri.equals(message.getProperties().getProperty("message"))) {
		// messagePrefix = (prefix.equals("") ? "" : ":" + prefix);
		// System.out.println("messagePrefix W"+messagePrefix+"W");
		// } else if
		// (uri.equals(message.getProperties().getProperty("generic"))) {
		// genericPrefix = (prefix.equals("") ? "" : ":" + prefix);
		// System.out.println("genericPrefix W"+genericPrefix+"W");
		// }

	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void endPrefixMapping(final String prefix) throws SAXException {
		// System.out.println("end namespace mapping "+prefix);
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void error(final SAXParseException e) throws SAXParseException {
		// treat validation errors as fatal
		throw e;
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void warning(final SAXParseException err) throws SAXParseException {
		// dump warnings too
		logger.error("** Warning" + ", line " + err.getLineNumber() + ", uri " + err.getSystemId(),err);
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

	/**
	 * This is a housekeeping method for flushing and closing output streams
	 * @param streams a var-args argument for OutputStream objects
	 * @throws IOException
	 */
	private void disposeOutputStreams(final OutputStream... streams) throws IOException {
		if (streams.length > 0) {
			for (int i = 0; i < streams.length; i++) {
				if(streams[i] !=null) {
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
				if(writers[i] !=null) {
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
				if (files[i] !=null) {
					files[i].delete();
				}
			}
		}
	}

	@Override
    public String getDefaultVersion() {
		return defaultVersion;
	}
	
	@Override
	public void setDefaultVersion(String defaultVersion){
	    this.defaultVersion = defaultVersion; 
	}
}