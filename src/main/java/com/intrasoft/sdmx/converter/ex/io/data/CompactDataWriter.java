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
import com.intrasoft.sdmx.converter.io.data.IoUtils;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

/**
 * Implementation of the Writer interface for writing Compact SDMX data messages.
 */
public class CompactDataWriter implements Writer {
	

	final private String lineEnd = System.getProperty("line.separator");
	private String tabs;
	private PrintWriter pout;

	/* required properties for writing data */
	private DataStructureBean keyFamilyBean;
	private HeaderBean header;
	/* optional properties for writing data */
	private String namespaceUri;
	private String namespacePrefix;
	private String generatedFileComment;// The comment to be added to the
	// generated file
	/* properties required for implementing the Writer interface */
	private InputParams params;
	private OutputStream os;
	private String datasetAction = "";

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
			pout.close();
			os.close();
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
		if (this.header == null) {
			ready = false;
			errorMessage += "header";
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
		setNamespaceUri(params.getNamespaceUri());
		setNamespacePrefix(params.getNamespacePrefix());
		setGeneratedFileComment(params.getGeneratedFileComment());
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeHeader(final HeaderBean header) throws Exception {
		this.header = header;
		if (header!= null && header.getAction() != null) {
			datasetAction = header.getAction().getAction();
		}
		try {
			isReady();
			final OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
			pout = new PrintWriter(new BufferedWriter(osw));
			tabs = "";

			// print CompactData tag
			printCompactDataTag();

			// print message header
			final HeaderTagWriter headerWriter = new HeaderTagWriter(pout, tabs, params.getKeyFamilyBean());
			headerWriter.printHeader(header);
		} catch (Exception e) {
			if(pout!=null) {
				pout.close();
			}
			if(os!=null) {
				os.close();
			}
			final String errorMessage = this.getClass().getName() + ".writeHeader() exception :" + lineEnd + "\t";
			throw new Exception(errorMessage + e.getMessage(), e);
		}
	}

	/**
	 * Implements Writer interface method.
	 */
	public void writeEmptyDataSet(final DataSet dataSet) throws Exception {
		try {
			isReady();
			printDataSet(dataSet);
		} catch (Exception e) {
			pout.close();
			os.close();
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
			printGroupKey(grKey);
		} catch (Exception e) {
			pout.close();
			os.close();
			final String errorMessage = this.getClass().getName() + ".writeGroupKey() exception :" + lineEnd + "\t";
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
		} catch (Exception e) {
			pout.close();
			os.close();
			final String errorMessage = this.getClass().getName() + ".writeTimeseriesKey() exception :" + lineEnd + "\t";						
			throw new Exception(errorMessage + e.getMessage(), e);			
		}
	}
	

	/**
	 * Implements Writer interface method.
	 */
	public void closeWriter() throws Exception {
		try {

			// </bisc:DataSet>
			tabs = tabs.substring(1);
			pout.println(tabs + "</" + namespacePrefix + ":DataSet>");

			// </CompactData>
			// tabs = tabs.substring(1);
			pout.println(tabs + "</CompactData>");

			pout.flush();

			if (!os.getClass().toString().endsWith("java.util.zip.ZipOutputStream")){
				pout.close();
				os.close();
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

	private void printCompactDataTag() {
	    final DataMessage me = new DataMessage();
		if (namespaceUri == null) {
			setNamespaceUri(me.getProperties().getProperty("specificxsd") + keyFamilyBean.getAgencyId() + ":" + keyFamilyBean.getId() + ":"
					+ keyFamilyBean.getVersion());
		}
		if (namespacePrefix == null) {
			String prefix = keyFamilyBean.getId().toLowerCase();
			setNamespacePrefix(prefix.length()>3?prefix.substring(0, 3):prefix);
		}
		// <?xml version="1.0" encoding="UTF-8"?>
		pout.println(tabs + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

		// Generated File comment
		if (generatedFileComment != null) {
			pout.println(tabs + "<!-- " + generatedFileComment + " -->");
		}

		// <CompactData
		pout.print(tabs + "<CompactData");

		tabs += "\t";

		// xmlns="http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message"
		String s = me.getProperties().getProperty("message");
		if (s != null) {
			pout.print(lineEnd + tabs + "xmlns=\"" + s + "\"");
		}

		// xmlns:edu="urn:sdmx:org.sdmx.infomodel.keyfamily.KeyFamily=ESTAT:EDUCATION:compact"
		pout.print(lineEnd + tabs + "xmlns:" + namespacePrefix + "=\"" + namespaceUri + ":compact\"");

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
		pout.print(namespaceUri + ":compact " + keyFamilyBean.getAgencyId() + "_" + keyFamilyBean.getId() + "_Compact.xsd");
		// >
		pout.print("\">" + lineEnd);

	}

	private void printDataSet(final DataSet ds) {
		// <bisc:DataSet
		pout.print(tabs + "<" + namespacePrefix + ":DataSet");

		// print DataSet tag attributes (as oposed to dataset DSD attributes)
		String ats = "";
		String s = "";
		if ((s = ds.getKeyFamilyURI()) != null) {
			ats += " keyFamilyURI=\"" + s + "\"";
		}
		if ((s = ds.getDatasetID()) != null) {
			ats += " datasetID=\"" + s + "\"";
		}
		if ((s = ds.getDataProviderSchemeAgencyId()) != null) {
			ats += " dataProviderSchemeAgencyId=\"" + s + "\"";
		}
		if ((s = ds.getDataProviderSchemeId()) != null) {
			ats += " dataProviderSchemeId=\"" + s + "\"";
		}
		if ((s = ds.getDataProviderID()) != null) {
			ats += " dataProviderID=\"" + s + "\"";
		}
		if ((s = ds.getDataflowAgencyID()) != null) {
			ats += " dataflowAgencyID=\"" + s + "\"";
		}
		if ((s = ds.getDataflowID()) != null) {
			ats += " dataflowID=\"" + s + "\"";
		}
		if ((s = ds.getAction()) != null) {
			ats += " action=\"" + s + "\"";
		}
		if ((s = ds.getReportingBeginDate()) != null) {
			ats += " reportingBeginDate=\"" + s + "\"";
		}
		if ((s = ds.getReportingEndDate()) != null) {
			ats += " reportingEndDate=\"" + s + "\"";
		}
		if ((s = ds.getValidFromDate()) != null) {
			ats += " validFromDate=\"" + s + "\"";
		}
		if ((s = ds.getValidToDate()) != null) {
			ats += " validToDate=\"" + s + "\"";
		}
		if ((s = ds.getPublicationYear()) != null) {
			ats += " publicationYear=\"" + s + "\"";
		}
		if ((s = ds.getPublicationPeriod()) != null) {
			ats += " publicationPeriod=\"" + s + "\"";
		}
		pout.print(ats);

		// print dataset attributes
		printAttributes(ds);

		// >
		pout.print(">" + lineEnd);

		tabs += "\t";

	}

	private void printGroupKey(final GroupKey gk) {
		// <bisc:SiblingGroup VIS_CTY="MX" JD_TYPE="P" JD_CATEGORY="A"
		// AVAILABILITY="A" DECIMALS="2" BIS_UNIT="USD" UNIT_MULT="5"/>

		// <bisc:SiblingGroup
	    final String type = (gk.getType() != null ? IoUtils.handleSpecialCharacters(gk.getType()) : "SiblingGroup");
		pout.print(tabs + "<" + namespacePrefix + ":" + type);

		// print group key
		printKey(gk);

		// print group attributes
		printAttributes(gk);

		// />
		pout.print("/>" + lineEnd);
	}

	private void printSeriesKey(final TimeseriesKey tk) {
		// <bisc:Series FREQ="M" COLLECTION="B" TIME_FORMAT="P1M" VIS_CTY="MX"
		// JD_TYPE="P" JD_CATEGORY="A" >

		// <bisc:Series
		if (tk != null) {// for delete dataset messages tk will be null
			pout.print(tabs + "<" + namespacePrefix + ":Series");
			// print series key
			printKey(tk);
			// print series attributes
			printAttributes(tk);
			// >
			pout.print(">" + lineEnd);
		}
		// print observations
		tabs += "\t";
		// do we need to bring observations? (i.e. delete message with all observations value null)
		boolean printObservations = false;
		// delete datasets
		if (tk != null) {
			for (Observation ob : tk.getObservations()) {
				if ((ob.getValue() != null) || (ob.getTimeValue() != null)) {
					printObservations = true;
					break;
				}
			}

			// print observations
			if (printObservations) {
				for (Observation ob : tk.getObservations()) {
					printObservation(ob);
				}
			}

			// </bisc:Series>
			tabs = tabs.substring(1);
			pout.println(tabs + "</" + namespacePrefix + ":Series>");

		}
	}

	private void printObservation(final Observation ob) {
		// <bisc:Obs TIME_PERIOD="2000-01" OBS_VALUE="3.14" OBS_STATUS="A"/>
		if (!(ob.getTimeValue() == null && ob.getValue() == null)) {

			// <bisc:Obs
			pout.print(tabs + "<" + namespacePrefix + ":Obs");

			// TIME_PERIOD="2000-01" 
			pout.print(" " + IoUtils.handleSpecialCharacters(keyFamilyBean.getTimeDimension().getConceptRef().getFullId()) + "=\""
					+ IoUtils.handleSpecialCharacters(ob.getTimeValue()) + "\"");

			// OBS_VALUE="3.14"
			//added condition for not writing the observation value when it its empty string
			String primaryMeasure = keyFamilyBean.getPrimaryMeasure().getId();
			if ("OBS_VALUE".equals(keyFamilyBean.getPrimaryMeasure().getId()) && 
					!keyFamilyBean.getPrimaryMeasure().getId().equals(keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId())) {
				primaryMeasure = keyFamilyBean.getPrimaryMeasure().getConceptRef().getFullId(); 
			}
			if ((ob.getValue() != null) && (!(ob.getValue().equals("null")))&& !(ob.getValue().equals("")) ) {
				pout.print(" " + IoUtils.handleSpecialCharacters(primaryMeasure) + "=\""
						+ IoUtils.handleSpecialCharacters(ob.getValue()) + "\"");
			}
			// else
			// pout.print(" " + IoUtils.handleSpecialCharacters(keyFamilyBean.getPrimaryMeasure().getConceptRef()) +
			// "=\"NaN\"");
			// OR do not write NaN but omit the OBS_VALUE concept

			// print observation attributes
			printAttributes(ob);

			// />
			pout.print("/>" + lineEnd);
		}
	}

	private void printAttributes(final AttachableArtefact aa) {
		if (aa == null) {
			return;
		}
		if (aa.getAttributeValues().size() == 0) {
			return;
		}

		// AVAILABILITY="A" DECIMALS="2" BIS_UNIT="USD" UNIT_MULT="5"
		for (String s : aa.getAttributeValues().keySet()) {
			pout.print(" " + IoUtils.handleSpecialCharacters(s) + "=\"" + IoUtils.handleSpecialCharacters(aa.getAttributeValues().get(s)) + "\"");
		}
	}

	private void printKey(final Key ke) {
		// VIS_CTY="MX" JD_TYPE="P" JD_CATEGORY="A"
		// if (ke != null) {
		for (final String s : ke.getKeyValues().keySet()) {
			pout.print(" " + IoUtils.handleSpecialCharacters(s) + "=\"" + IoUtils.handleSpecialCharacters(ke.getKeyValues().get(s)) + "\"");
		}
		// }
	}

	public void releaseResources() throws Exception {
		if (os != null && !os.getClass().toString().endsWith("java.util.zip.ZipOutputStream")){			
			os.close();
		}
		if (pout != null) {
			pout.close();
		}
	}

}