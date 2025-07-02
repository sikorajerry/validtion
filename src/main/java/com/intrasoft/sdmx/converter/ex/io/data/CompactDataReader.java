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
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.exceptions.ParseXmlException;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import com.intrasoft.sdmx.converter.ui.exceptions.ReaderValidationException;
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
import org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.GroupBean;
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
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Implementation of the Reader interface for reading Compact SDMX data messages.
 */
public class CompactDataReader extends DefaultHandler implements Reader, DefaultVersion {

	private static Logger logger = LogManager.getLogger(CompactDataReader.class);

	final private String lineEnd = System.getProperty("line.separator");
	private HeaderBean header;
	private DataSet dataSet;
	private TimeseriesKey currentTimeseriesKey;
	private boolean verifiedFirstElement = false;
	private boolean parsingHeader = false;
	private boolean existSeries = false;
	
	/*
	 * indicates if the DataSet has been written to the writer. this attribute shifts to "true" when the reader
	 * encounters the first GroupKey or TimeseriesKey (whichever comes first).
	 */
	private boolean datasetWritten;

	/* properties required for reading data */
	private DataStructureBean keyFamilyBean;
	
	/* properties required for implementing the Reader interface */
	private BufferedInputStream is;
	private InputParams params;
	private Writer writer;
	private boolean writeDataSet = false;// delete sibling groups, writeDataSet
	private boolean existGroupKey = false;
	private String datasetAction = "";	
	private StructureIdentifier structIdentifier = null;
	private String urn = null;
	private String defaultVersion;

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
		this.is = new BufferedInputStream(is);
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
		// check if a conversion to crossX message can be done with the selected dsd.
		if (this.writer.getClass().getSimpleName().toString().equals("CrossDataWriter")) {
			final String complianceXS = IoUtils.checkXsCompliance(keyFamilyBean);
			if (!complianceXS.equals("")) {
				throw (new Exception("The conversion to the output Cross Sectional message cannot be done because:" + complianceXS));
			}
		}
		if (((this.params.getKeyFamilyBean().getTimeDimension() == null))) {
			throw (new Exception("Since DSD has no Time Dimension the conversion from the selected Input  Format to the selected Output Format is not supported"));
		}
		return ready;
		// return (keyFamilyBean != null && is != null && writer != null);
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
		logger.info(this.getClass().getName() + ".readData");
		try {
			/*if(keyFamilyBean==null) {*/
				computeKeyFamilyBean();
			/*}*/
			isReady();
			parseCompactDataMessage(is);
		} catch (ReaderValidationException rve) {
			throw rve;
		} catch (Exception e) {
		    final String errorMessage = this.getClass().getName() + "exception :" + lineEnd + "\t";
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
		is.mark(30000);
		parseHeaderFromInput(is, Formats.COMPACT_SDMX);
		is.reset();
		StructureIdentifier structIdentifierHeader = StructureIdentifier.computeStructureIdentifier(Formats.COMPACT_SDMX, header);
		if(structIdentifierHeader!=null) {
			structIdentifier = structIdentifierHeader;
		}
		
		//If no version found we search header elements
		if(structIdentifier==null) {
			is.mark(30001);
			parseUrnFromInput(is, Formats.COMPACT_SDMX);
			is.reset();
			StructureIdentifier structIdentifierUrn = StructureIdentifier.computeStructureIdentifier(Formats.COMPACT_SDMX, urn);
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

	private void parseCompactDataMessage(final BufferedInputStream is) throws Exception {
		datasetWritten = false;		
		// Use the default (non-validating) parser
		final SAXParserFactory factory = SAXParserFactory.newInstance();
		try {
			// Parse the input
			factory.setNamespaceAware(true);
			final SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(is, this);			
		} catch (SAXParseException spe) {
			// Use the contained exception, if any
			Exception x = spe;
			if (spe.getException() != null) {
				x = spe.getException();
            }
			final String errorMessage = "Error parsing data, SAXParseException at line" + spe.getLineNumber() + "column" + spe.getColumnNumber() +
                    " :" + lineEnd + "\t";
			throw new Exception(errorMessage + spe.getMessage(), x);
		} catch (SAXException sxe) {
			// if this wraps our custom exception, use that original exception
			// to generate the stack trace
			Exception x = sxe;
			if (sxe.getException() != null) {
				x = sxe.getException();
				if(x instanceof ReaderValidationException) {
					throw x;
				}
            }
			final String errorMessage = "Error parsing data, SAXException :" + lineEnd + "\t";
			throw new Exception(errorMessage + sxe.getMessage(), x);
		}
	}
	
	/**
	 * In case of xml read the header of the input.
	 * @param inputStream
	 * @param format
	 * @return header
	 * @throws WriteHeaderException
	 */
    public void parseHeaderFromInput(final BufferedInputStream inputStream, Formats format) throws ParseXmlException {
    	XMLInputFactory factory = XMLInputFactory.newInstance();
    	HeaderBean headerBean = null;
    	try {
			XMLStreamReader parser = factory.createXMLStreamReader(inputStream, "UTF-8");
            NodeTracker nodeTextTracker = new NodeTrackerComposite(new NodeChildTracker(parser, format.getDataType()));
            headerBean = SdmxHeaderReaderEngine.processHeader(parser, nodeTextTracker, null, params.getStructureDefaultVersion(), new IgnoreErrorsExceptionHandler());
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
    public void parseUrnFromInput(final BufferedInputStream inputStream, Formats format) throws ParseXmlException {
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
        } catch (XMLStreamException e) {
            throw new ParseXmlException("Error while reading the xml header!", e);
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
		try {
			writer.closeWriter();
		} catch (Exception e) {
			final String errorMessage = "endDocument :" + lineEnd + "\t";
			throw new SAXException(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void startElement(final String namespaceURI, final String sName, final String qName, final Attributes attrs) throws SAXException {

	    final String eName = ("".equals(sName) ? qName : sName);
	    
	    List<DimensionBean> dimensionBeans = keyFamilyBean.getDimensions();
	    List<String> dimensionsNames = new ArrayList<String>();
	    for (DimensionBean dimensionBean : dimensionBeans) {
	    	if (dimensionBean.isTimeDimension()) {
	    		continue;//skip time dimension it; is processed later 
	    	}
	    	dimensionsNames.add(dimensionBean.getId());
	    }
	    
	    List<GroupBean> groupBeans = keyFamilyBean.getGroups();
	    List<String> groupsNames = new ArrayList<String>();
	    for (GroupBean groupBean : groupBeans) {
	    	groupsNames.add(groupBean.getId());
	    }
	    
		if (!verifiedFirstElement) {
			if (!"CompactData".equals(eName)) {
				throw new SAXException("Reader exception. The submitted message is not of CompactData type.");
			}
			verifiedFirstElement = true;
			return;
		}
		try {
			if (parsingHeader) {
				//headerReader.setStructureDefaultVersion(params.getStructureDefaultVersion());
				//headerReader.startElement(namespaceURI, sName, qName, attrs);
				return;
			}

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
					} else {
						for(AttributeBean attr:keyFamilyBean.getDatasetAttributes()) {
							if(attr.getId().equals(aName)) {
								dataSet.getAttributeValues().put(aName, aValue);
								break;
							}
						}
					}																						
				}
			} else if (groupsNames.contains(eName)) {
				if (!datasetWritten) {
					// write dataset (dataset is filled only with its attributes
					// and 'KeyFamilyRef' tag value
					writer.writeEmptyDataSet(dataSet);
					writeDataSet = true;
					datasetWritten = true;
				}
				final GroupKey gk = new GroupKey();
				gk.setType(eName);
				// dataSet.getGroups().add(gk);
				for (int i = 0; i < attrs.getLength(); i++) {
				    final String aName = ("".equals(attrs.getLocalName(i)) ? attrs.getQName(i) : attrs.getLocalName(i));
				    final String aValue = attrs.getValue(i);
					// System.out.println("aName "+aName+" aValue "+aValue);

					if (dimensionsNames.contains(aName)) {
						gk.getKeyValues().put(aName, aValue);
					} else {
						for(AttributeBean attr:keyFamilyBean.getGroupAttributes()) {
							if(attr.getId().equals(aName)) {
								gk.getAttributeValues().put(aName, aValue);
								break;
							}
						}
					}
				}
				existGroupKey = true;
				// write GroupKey
				writer.writeGroupKey(gk);
			} else if ("Series".equals(eName)) {
				if (!datasetWritten) {
					// write dataset (dataset is filled only with its attributes
					// and 'KeyFamilyRef' tag value
					writer.writeEmptyDataSet(dataSet);
					datasetWritten = true;
				}
				currentTimeseriesKey = new TimeseriesKey();
				// dataSet.getSeries().add(currentTimeseriesKey);
				for (int i = 0; i < attrs.getLength(); i++) {
				    final String aName = ("".equals(attrs.getLocalName(i)) ? attrs.getQName(i) : attrs.getLocalName(i));
				    final String aValue = attrs.getValue(i);
					// System.out.println("aName "+aName+" aValue "+aValue);

					if (dimensionsNames.contains(aName)) {
						currentTimeseriesKey.getKeyValues().put(aName, aValue);
					} else {
						for(AttributeBean attr:keyFamilyBean.getDimensionGroupAttributes()) {
							if(attr.getId().equals(aName)) {
								currentTimeseriesKey.getAttributeValues().put(aName, aValue);
								break;
							}
						}
					}						
										
					// // if there are no crossx measures in the DSD then the OBS_VALUE is the crossx
					// // measure
					// if ((keyFamilyBean.getCrossSectionalMeasures().size() == 0)
					// && this.writer.getClass().getSimpleName().toString().equals("CrossDataWriter")) {
					// PrimaryMeasureBean pmb = keyFamilyBean.getPrimaryMeasure();
					// currentTimeseriesKey.getKeyValues().put("Primary Measure", pmb.getConceptRef());
					// }
				}
				
				Set<String> missingDimensions = IoUtils.checkTimeSeriesDimensions(this.keyFamilyBean, currentTimeseriesKey);				
				if(missingDimensions.size() > 0) {
					String errorMessage = "The following dimensions are missing from timeseries:\n";
					for(String dim:missingDimensions) {
						errorMessage += dim + ",";
					}
					errorMessage = errorMessage.substring(0, errorMessage.length()-1);					
					SAXException se = new SAXException(new ReaderValidationException(107,errorMessage));
					throw se;
				}
				
			} else if ("Obs".equals(eName)) {
			    final Observation ob = new Observation();
				currentTimeseriesKey.getObservations().add(ob);
				for (int i = 0; i < attrs.getLength(); i++) {
				    final String aName = ("".equals(attrs.getLocalName(i)) ? attrs.getQName(i) : attrs.getLocalName(i));
				    final String aValue = attrs.getValue(i);
					// System.out.println("aName "+aName+" aValue "+aValue);
					if (keyFamilyBean.getTimeDimension().getConceptRef().getFullId().equals(aName)) {
						ob.setTimeValue(aValue);
					} else if (keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId().equals(aName)) {
						if ("-".equals(aValue)) {// ||aValue.equalsIgnoreCase("NaN") ||
							ob.setValue(null);
						} else {
							ob.setValue(aValue);
						}

					} else {
						for (AttributeBean attr:keyFamilyBean.getObservationAttributes()) {
							if(attr.getId().equals(aName)) {
								ob.getAttributeValues().put(aName, aValue);
								break;
							}
						}
					}
					/*
					else if (keyFamilyBean.getObservationAttributes().contains(aName)) {
						ob.getAttributeValues().put(aName, aValue);
					}*/
				}
			} else if ("Header".equals(eName)) {
				//header = new HeaderBeanImpl("IREF123", "ZZ9");
				//headerReader = new HeaderTagReader(header);
				parsingHeader = true;
			}
		} catch (SAXException se) {						
			throw se;
		} catch (Exception e) {
		    final String errorMessage = "startElement" + eName + " :" + lineEnd + "\t";
			throw new SAXException(errorMessage + e.getMessage(), e);
		}
	}
	

	/**
	 * Overrides DefaultHandler method.
	 */
	@Override
	public void endElement(final String namespaceURI, final String sName, final String qName) throws SAXException {
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
						header.getExtracted(), 
						header.getPrepared(), 
						header.getReportingBegin(), 
						header.getReportingEnd(), 
						header.getName(), 
						header.getSource(), 
						header.getReceiver(), 
						header.getSender(), 
						header.isTest());
				writer.writeHeader(header);
				if (header.getAction() != null) {
					datasetAction = header.getAction().getAction();
				}
			} else if ("Series".equals(eName)) {
				// write TimeseriesKey
				writer.writeTimeseriesKey(currentTimeseriesKey);
				existSeries = true;
			} else if (("DataSet".equals(eName) && !existSeries)) {
				if (existGroupKey && datasetAction.equalsIgnoreCase("Delete")) {
					writer.writeTimeseriesKey(currentTimeseriesKey);
				}

				// write DataSet,for delete dataset messages
				if (!writeDataSet) {
					writer.writeEmptyDataSet(dataSet);
					writer.writeTimeseriesKey(currentTimeseriesKey);
				}
            } else if ("Sender".equals(eName) || "Receiver".equals(eName)) {
				//headerReader.endElement(namespaceURI, sName, qName);
			} 
				// writer.writeTimeseriesKey(currentTimeseriesKey);
		} catch (Exception e) {
		    final String errorMessage = "endElement" + eName + " :" + lineEnd + "\t";
			throw new SAXException(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Overrides DefaultHandler method.
	 */
/*	@Override
	public void characters(final char buf[], final int offset, final int len) throws SAXException {
		if (parsingHeader) {
			headerReader.characters(buf, offset, len);
			return;
		}
	}
*/
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
		System.out.println("** Warning" + ", line " + err.getLineNumber() + ", uri " + err.getSystemId());
		System.out.println("   " + err.getMessage());
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