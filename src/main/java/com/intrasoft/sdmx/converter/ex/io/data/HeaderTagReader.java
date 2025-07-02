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

import org.sdmxsource.sdmx.api.constants.DATASET_ACTION;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.model.beans.base.ContactBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.mutable.base.ContactMutableBean;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.ContactBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.TextTypeWrapperImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.PartyBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.base.ContactMutableBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.base.TextTypeWrapperMutableBeanImpl;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;
import org.xml.sax.Attributes;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 
 * @author gko
 */
public class HeaderTagReader {
	

	private HeaderBean header;

	private String currentTagName;
	private ContactMutableBean currentContact;
	private String currentPartyId;
	private TextTypeWrapper curentPartyName;
	private List<ContactMutableBean> currentContactsParty;	
	private String currentNameContainerType;// "Header" or "Party" or "Contact"
	private Attributes currentAttrs;
	
	private Date preparedDate;
	private Date extractedDate;
	private Boolean test;
	private List<DatasetStructureReferenceBean> structure = new ArrayList<DatasetStructureReferenceBean>();

	private String version;
	private String keyFamilyRef;
	private String keyFamilyAgency;
	private String dataSetAgency;

	/** Creates a new instance of HeaderTagReader */
	public HeaderTagReader(final HeaderBean header) {
		this.header = header;
		this.currentNameContainerType = "Header";
	}

	public void startElement(final String namespaceURI, final String sName, final String qName, final Attributes attrs) {
		String eName = (sName.equals("") ? qName : sName);
		currentTagName = eName;
		currentAttrs = attrs;

		if ("Sender".equals(eName)) {
			currentNameContainerType = "Party";
			currentPartyId = attrs.getValue("id");
			currentContactsParty = new ArrayList<ContactMutableBean>();
		} else if ("Receiver".equals(eName)) {
			currentNameContainerType = "Party";
			currentPartyId = attrs.getValue("id");
			currentContactsParty = new ArrayList<ContactMutableBean>();
		} else if ("Contact".equals(eName)) {
			currentNameContainerType = "Contact";
			currentContact = new ContactMutableBeanImpl();
			currentContactsParty.add(currentContact);
		} else if ("".equals(eName)) {
		}
	}

	 public void endElement(String namespaceURI, String sName, String qName) {
		 String eName = (sName.equals("") ? qName : sName);
		if ("Sender".equals(eName)) {
			List<ContactBean> currentContactSenders = new ArrayList<ContactBean>();
			for (ContactMutableBean contactMutable : currentContactsParty) {
				currentContactSenders.add(new ContactBeanImpl(contactMutable));
			}
			List<TextTypeWrapper> names = new ArrayList<TextTypeWrapper>();
			if (curentPartyName != null) {
				names.add(curentPartyName);
			}
			header.setSender(new PartyBeanImpl(names, currentPartyId, currentContactSenders, null));
			currentContactsParty = new ArrayList<ContactMutableBean>();
		} else if ("Receiver".equals(eName)) {
			List<ContactBean> currentContactReceivers = new ArrayList<ContactBean>();
			for (ContactMutableBean contactMutable : currentContactsParty) {
				currentContactReceivers.add(new ContactBeanImpl(contactMutable));
			}
			List<TextTypeWrapper> names = new ArrayList<TextTypeWrapper>();
			if (curentPartyName != null) {
				names.add(curentPartyName);
			}
			header.addReceiver(new PartyBeanImpl(names, currentPartyId, currentContactReceivers, null));
			currentContactsParty = new ArrayList<ContactMutableBean>();
		} 
	 }

	public void characters(final char buf[], final int offset, final int len) {
		final String s = new String(buf, offset, len);
		if (s.trim().equals("")) {
			return;
		}

		
		if (currentTagName.equals("ID")) {
			header.setId(s);
		} else if (currentTagName.equals("Test")) {
			header.setTest(Boolean.valueOf(s));
			test = Boolean.valueOf(s);
		} else if (currentTagName.equals("Truncated")) {
//			header.setTruncated(Boolean.valueOf(s)); //not implemented 
		} else if (currentTagName.equals("Name")) {
			final TextTypeWrapper name = new TextTypeWrapperImpl(currentAttrs.getValue("lang"), s, null);
			if (currentNameContainerType.equals("Header")) {
				header.addName(name);
			} else if (currentNameContainerType.equals("Party")) {
				curentPartyName = name;
				/*
				List<TextTypeWrapper> names = new ArrayList<TextTypeWrapper>();
				names.add(name);
				if (currentContact == null) {
					currentContact = new  ContactMutableBeanImpl();
				}
				currentContactsParty.add(currentContact);
				*/
			} else if (currentNameContainerType.equals("Contact")) {
				currentContact.getNames().add(new TextTypeWrapperMutableBeanImpl(name));
			}
		} else if (currentTagName.equals("Prepared")) {
			this.preparedDate = parseHeaderDate(s);
		} else if (currentTagName.equals("Source")) {
			final TextTypeWrapper source = new TextTypeWrapperImpl(currentAttrs.getValue("lang"), s, null);
			header.getSource().add(source);
		} else if (currentTagName.equals("Department")) {
			final TextTypeWrapper department = new TextTypeWrapperImpl(currentAttrs.getValue("lang"), s, null);
			currentContact.getDepartments().add(new TextTypeWrapperMutableBeanImpl(department));
		} else if (currentTagName.equals("Role")) {
			final TextTypeWrapper role = new TextTypeWrapperImpl(currentAttrs.getValue("lang"), s, null);
			currentContact.getRoles().add(new TextTypeWrapperMutableBeanImpl(role));
		} else if (currentTagName.equals("Telephone")) {
			currentContact.getTelephone().add(s);
		} else if (currentTagName.equals("Fax")) {
			currentContact.getFax().add(s);
		} else if (currentTagName.equals("X400")) {
			currentContact.getX400().add(s);
		} else if (currentTagName.equals("URI")) {
			currentContact.getUri().add(s);
		} else if (currentTagName.equals("Email")) {
			currentContact.getEmail().add(s);
		} else if (currentTagName.equals("KeyFamilyRef")) {
			keyFamilyRef =  s; 
			//header.setKeyFamilyRef(s); not impl
		} else if (currentTagName.equals("KeyFamilyAgency")) {
			keyFamilyAgency =  s; 
//			header.setKeyFamilyAgency(s); not impl
		} else if (currentTagName.equals("DataSetAgency")) {
			dataSetAgency =  s;
//			header.setDataSetAgency(s); //not impl
		} else if (currentTagName.equals("DataSetID")) {
			header.setDatasetId(s);
		} else if (currentTagName.equals("DataSetAction")) {
			header.setAction(DATASET_ACTION.getAction(s));
		} else if (currentTagName.equals("Extracted")) {
			extractedDate = parseHeaderDate(s); 
		} else if (currentTagName.equals("ReportingBegin")) {
			header.setReportingBegin(parseHeaderDate(s));
		} else if (currentTagName.equals("ReportingEnd")) {
			header.setReportingEnd(parseHeaderDate(s));
		} else if (currentTagName.equals("")) {
		}
	}

	public Date parseHeaderDate(String dateAsString) {
		return DatatypeConverter.parseDateTime(dateAsString).getTime();			
	}

	public Date getPreparedDate() {
		return preparedDate;
	}

	public Date getExtractedDate() {
		return extractedDate;
	}
	
	public Boolean getTest() {
		return test;
	}

	public String getKeyFamilyRef() {
		return keyFamilyRef;
	}

	public String getKeyFamilyAgency() {
		return keyFamilyAgency;
	}

	public String getDataSetAgency() {
		return dataSetAgency;
	}

	public void setStructureRef(String keyFamilyRef, String keyFamilyAgency, String dataSetAgency) {
		if(keyFamilyRef != null && keyFamilyAgency!=null && dataSetAgency!=null) {
			structure.add(new DatasetStructureReferenceBeanImpl(new StructureReferenceBeanImpl(keyFamilyAgency, keyFamilyRef, version, SDMX_STRUCTURE_TYPE.DSD)));
			HeaderBeanImpl headerNew = new HeaderBeanImpl(header.getAdditionalAttributes(), 
					structure, 
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
			
			header = headerNew;
		}
	}

	public void setStructureDefaultVersion(String version) {
		this.version=version;
	}
}
