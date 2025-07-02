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

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.model.beans.base.ContactBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.header.PartyBean;
import org.sdmxsource.sdmx.util.date.DateUtil;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 * @author gko
 */
public class HeaderTagWriter {
	public final static SimpleDateFormat HEADER_DATE_FROMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

	private static Logger logger = LogManager.getLogger(HeaderTagWriter.class);

	private PrintWriter pout;
	private String tabs;
	
	private DataStructureBean dsdBean;

	// private HeaderBean header;

	/** Creates a new instance of HeaderTagWriter */
	public HeaderTagWriter(final PrintWriter pout, final String tabs, final DataStructureBean dsdBean) {
		this.pout = pout;
		this.tabs = tabs;
		this.dsdBean = dsdBean;
		// this.header = header;
	}

	public void printHeader(final HeaderBean he) throws Exception {
		String s;
		// <Header>
		pout.println(tabs + "<Header>");

		tabs += "\t";

		// <ID>JD014</ID>
		if ((s = he.getId()) != null && !(s = he.getId()).equals("")) {			
			pout.println(tabs + "<ID>" + StringEscapeUtils.escapeXml(s) + "</ID>");
		} else {
			// write warn message that the output file will be invalid because 'id' element is mandatory
			logger.warn("The output SDMX_ML file might not be validated as the Header element 'ID' is mandatory and missing");

		}
		// <Test>true</Test>
		if ((s = String.valueOf(he.isTest())) != null && !(s = String.valueOf(he.isTest())).equals("")) {
			pout.println(tabs + "<Test>" + StringEscapeUtils.escapeXml(s) + "</Test>");
		} else {
			// write warn message that the output file will be invalid because 'prepared' element is mandatory
			logger.warn("The output SDMX_ML file might not be validated as the Header element 'Test' is mandatory and missing");
		}
		// <Truncated>false</Truncated>
		if (he.getAdditionalAttributes()!=null && he.hasAdditionalAttribute("Truncated")) {
			s = String.valueOf(he.getAdditionalAttribute("Truncated"));
			pout.println(tabs + "<Truncated>" + StringEscapeUtils.escapeXml(s) + "</Truncated>");
		}
		// <Name>Name</Name>
		if ((s = String.valueOf(he.getName())) != null && (he.getName().size() > 0)) {
			s = String.valueOf(he.getName().get(0).getValue());
			String lang = he.getName().get(0).getLocale();
			pout.println(tabs + "<Name xml:lang=\"" + lang + "\">" + StringEscapeUtils.escapeXml(s) + "</Name>");
		} else {
			// write warn message that the output file will be invalid because 'prepared' element is mandatory
			logger.warn("The output SDMX_ML file might not be validated as the Header element 'Test' is mandatory and missing");
		}

		// <Prepared>2001-03-11T09:30:47</Prepared>
		if ((s = String.valueOf(he.getPrepared())) != null && !(String.valueOf(he.getPrepared())).equals("")) {
			s =  DateUtil.formatDate(he.getPrepared());
			pout.println(tabs + "<Prepared>" + StringEscapeUtils.escapeXml(DateUtil.formatDate(he.getPrepared())) + "</Prepared>");
		} else {
			pout.println(tabs + "<Prepared>" + StringEscapeUtils.escapeXml(DateUtil.formatDate(new Date())) + "</Prepared>");
		}
		
		// print message senders
		boolean printSender = false;// if false then message is invalid
		PartyBean sender = he.getSender(); {
			if (!StringUtils.isEmpty(sender.getId())) {
				printParty(sender, "Sender");
				printSender = true;
			}
		}
		if (!printSender) {
			// write warn message that the output file will be invalid because 'sender' element is mandatory
			logger.warn("The output SDMX_ML file might not be validated as the Header element 'Sender' is mandatory and missing");

		}
		// print message receivers
		for (final PartyBean receiver : he.getReceiver()) {
			if (!StringUtils.isEmpty(receiver.getId()))
				printParty(receiver, "Receiver");
		}

		if (dsdBean.asReference().getMaintainableReference() != null && dsdBean.asReference().getMaintainableReference().getMaintainableId() != null) {
			s = dsdBean.asReference().getMaintainableReference().getMaintainableId();
			pout.println(tabs + "<KeyFamilyRef>" + StringEscapeUtils.escapeXml(s) + "</KeyFamilyRef>");
		}
		if (dsdBean.asReference().getMaintainableReference() != null && dsdBean.asReference().getMaintainableReference().getAgencyId() != null) {
			s = dsdBean.asReference().getMaintainableReference().getAgencyId();
			pout.println(tabs + "<KeyFamilyAgency>" + StringEscapeUtils.escapeXml(s) + "</KeyFamilyAgency>");
		}
		if (he.getDataProviderReference() != null && (s = he.getDataProviderReference().getAgencyId()) != null) {
			pout.println(tabs + "<DataSetAgency>" + StringEscapeUtils.escapeXml(s) + "</DataSetAgency>");
		}
		if (he.getDatasetId() != null) {
			s = String.valueOf(he.getDatasetId());
			pout.println(tabs + "<DataSetID>" + StringEscapeUtils.escapeXml(s) + "</DataSetID>");
		}
		if ((he.getAction()) != null) {
			pout.println(tabs + "<DataSetAction>" + StringEscapeUtils.escapeXml(he.getAction().getAction()) + "</DataSetAction>");
		}
		if (he.getExtracted() != null && !he.getExtracted().equals("")){
			s =  DateUtil.formatDate(he.getExtracted());
			pout.println(tabs + "<Extracted>" + StringEscapeUtils.escapeXml(s) + "</Extracted>");
		}
		if (he.getReportingBegin() != null && !he.getReportingBegin().equals("")) {
			s =  HEADER_DATE_FROMATTER.format(he.getReportingBegin());
			pout.println(tabs + "<ReportingBegin>" + StringEscapeUtils.escapeXml(s) + "</ReportingBegin>");
		}
		if (he.getReportingEnd() != null && !he.getReportingEnd().equals("")) {
			s =  HEADER_DATE_FROMATTER.format(he.getReportingEnd());
			pout.println(tabs + "<ReportingEnd>" + StringEscapeUtils.escapeXml(s) + "</ReportingEnd>");
		}

		// print message sources
		for (final TextTypeWrapper source : he.getSource()) {
			printTextType(source, "Source");
		}

		// </Header>
		tabs = tabs.substring(1);
		pout.println(tabs + "</Header>");
		
	}

	private void printTextType(final TextTypeWrapper tt, final String ttName) {
		// print the tag name only if it has text
		if (tt!= null && tt.getValue() != null && !tt.getValue().equals("")) {
			if (tt.getLocale() != null && !"".equals(tt.getLocale())) {
				pout.println(tabs + "<" + ttName + " xml:lang=\"" + tt.getLocale() + "\">" + StringEscapeUtils.escapeXml(tt.getValue()) + "</" + ttName + ">");
			} else {
				pout.println(tabs + "<" + ttName + ">" + StringEscapeUtils.escapeXml(tt.getValue()) + "</" + ttName + ">");
			}
		}
	}

	private void printParty(final PartyBean pa, final String paName) throws Exception {
		// <Sender id="BIS">
		pout.println(tabs + "<" + paName + " id=\"" + StringEscapeUtils.escapeXml(pa.getId()) + "\">");

		tabs += "\t";

		// print party names
		for (final TextTypeWrapper tt : pa.getName()) {
			printTextType(tt, "Name");
		}

		// print party contacts
		for (final ContactBean contact : pa.getContacts()) {
			// print the contact tag only if there exists information
			// name info
			if (contact.getName().size() != 0) {
				if (!StringUtils.isEmpty(contact.getName().get(0).getValue())) {
					printContact(contact);
				}
				// department info
			} else if (contact.getDepartments().size() != 0) {
				if (!StringUtils.isEmpty(contact.getDepartments().get(0).getValue())) {
					printContact(contact);
				}// role info
			} else if (contact.getRole().size() != 0) {
				if (!StringUtils.isEmpty(contact.getRole().get(0).getValue())) {
					printContact(contact);
				}
			}

		}

		// </Sender>
		tabs = tabs.substring(1);
		pout.println(tabs + "</" + paName + ">");
	}

	private void printContact(final ContactBean co) {
		// <Contact>
		pout.println(tabs + "<Contact>");
		tabs += "\t";

		// print contact names
		for (final TextTypeWrapper tt : co.getName()) {
			printTextType(tt, "Name");
		}

		// print contact departments
		for (final TextTypeWrapper tt : co.getDepartments()) {
			printTextType(tt, "Department");
		}

		// print contact roles
		for (final TextTypeWrapper tt : co.getRole()) {
			printTextType(tt, "Role");
		}

		// print contact coordinates
		if (co.getTelephone() != null && co.getTelephone().size() > 0) {
			pout.println(tabs + "<Telephone>" + co.getTelephone().get(0) + "</Telephone>");
		}
		if (co.getFax() != null && co.getFax().size() > 0) {
			pout.println(tabs + "<Fax>" + co.getFax().get(0) + "</Fax>");
		}
		if (co.getX400() != null && co.getX400().size() > 0) {
			pout.println(tabs + "<X400>" + co.getX400().get(0) + "</X400>");
		}
		if (co.getUri() != null && co.getUri().size() > 0) {
			pout.println(tabs + "<URI>" + co.getUri().get(0) + "</URI>");
		}
		if (co.getEmail() != null && co.getEmail().size() > 0) {
			pout.println(tabs + "<Email>" + co.getEmail().get(0) + "</Email>");
		}
		
		// </Contact>
		tabs = tabs.substring(1);
		pout.println(tabs + "</Contact>");
	}
}
