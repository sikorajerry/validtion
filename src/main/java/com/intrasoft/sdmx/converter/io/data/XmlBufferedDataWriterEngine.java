package com.intrasoft.sdmx.converter.io.data;

import javanet.staxutils.IndentingXMLStreamWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.base.ContactBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.header.PartyBean;
import org.sdmxsource.sdmx.util.beans.ConceptRefUtil;
import org.sdmxsource.sdmx.util.date.DateUtil;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import java.io.OutputStream;
import java.util.Date;
import java.util.List;

/**
 * Created by dbalan on 6/9/2017.
 */
public abstract class XmlBufferedDataWriterEngine extends BufferedDataWriterEngineAdaptor {

    private static Logger logger = LogManager.getLogger(BufferedDataWriterEngineAdaptor.class);

    public static final String XML_NS_PREFIX = "xml";
    public static final String XML_NS_URI = "http://www.w3.org/XML/1998/namespace";

    public static final String XSI_NS_PREFIX = "xsi";
    public static final String XSI_NS_URI = "http://www.w3.org/2001/XMLSchema-instance";

    private XMLStreamWriter xmlStreamWriter = null;
    
    private boolean isSdmx21 = false;

    public XmlBufferedDataWriterEngine(OutputStream outputStream){
        this(outputStream, false);
    }

    public XmlBufferedDataWriterEngine(OutputStream outputStream, boolean isSdmx21) {
        this.isSdmx21 = isSdmx21;
        try {
            XMLOutputFactory xmlFactory = XMLOutputFactory.newInstance();
            xmlStreamWriter = new IndentingXMLStreamWriter(xmlFactory.createXMLStreamWriter(outputStream, "UTF-8"));
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void openWriter() {
        try{
            xmlStreamWriter.writeStartDocument("UTF-8", "1.0");
        }catch(XMLStreamException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void openHeader() {
        startXmlDocument();
    }

    //TODO: partial implementation of the write header
    @Override
    public void doWriteHeader(HeaderBean header) {
        startXmlElement("Header");  //START HEADER
        //ID
        startXmlElement("ID");
        if (header != null && header.getId() != null) {
            writeXmlCharacters(header.getId());
        } else {
            writeXmlCharacters("DS" + Long.toString(new Date().getTime()));
        }
        endXmlElement();

        //Test
        startXmlElement("Test");
        if(header != null) {
            writeXmlCharacters(""+header.isTest());
        } else {
            writeXmlCharacters("false");
        }
        endXmlElement();//end Test
        
        //Truncated
        if(header != null && !isSdmx21 && header.hasAdditionalAttribute("Truncated")) {
        	startXmlElement("Truncated");
        	writeXmlCharacters(""+header.getAdditionalAttribute("Truncated"));
        	endXmlElement();//end Truncated
        }

        //name for Sdmx 20
        if(!isSdmx21 && header != null && header.getName() != null){
           writeNames("Name", header.getName());
        }

        //Prepared
        startXmlElement("Prepared");
        if(header != null && header.getPrepared() != null) {
            writeXmlCharacters(DateUtil.formatDateUTC(header.getPrepared()));
        } else {
            writeXmlCharacters(DateUtil.formatDateUTC(new Date()));
        }
        endXmlElement();

        //sender
        startXmlElement("Sender");
        if(header != null && header.getSender() != null) {
            writeParty(header.getSender(), isSdmx21); //Only 2.1 has the concept of timeZone in the Sender
        } else {
            writeXmlAttribute("id", "Eurostat");
        }
        endXmlElement();

        //receivers
        if(header != null && header.getReceiver() != null) {
            for(PartyBean currentReceiver : header.getReceiver()) {
                startXmlElement("Receiver");
                writeParty(currentReceiver, isSdmx21);
                endXmlElement();
            }
        }

        //name for sdmx 21
        if(isSdmx21 && header != null && header.getName() != null) {
            //In 2.1 the name goes in this location
            writeNames("Name", header.getName());
        }

        if(!isSdmx21){
            // KeyFamilyRef
            startXmlElement("KeyFamilyRef");
            writeXmlCharacters(getDataStructure().asReference().getMaintainableReference().getMaintainableId());
            endXmlElement();

            // KeyFamilyAgency
            startXmlElement("KeyFamilyAgency");
            writeXmlCharacters(getDataStructure().asReference().getMaintainableReference().getAgencyId());
            endXmlElement();
        }

        if(!isSdmx21){
            //write dataset id first
            if(header != null && header.getDatasetId() != null){
                startXmlElement("DataSetID");
                writeXmlCharacters(header.getDatasetId());
                endXmlElement();
            }
            if(header != null && header.getAction() != null){
                startXmlElement("DataSetAction");
                writeXmlCharacters(header.getAction().getAction());
                endXmlElement();
            }
        }

        if(header != null && header.getExtracted() != null) {
            startXmlElement("Extracted");
            writeXmlCharacters(DateUtil.formatDateUTC(header.getExtracted()));
            endXmlElement();
        }

        if(header != null && header.getReportingBegin() != null){
            startXmlElement("ReportingBegin");
            writeXmlCharacters(DateUtil.formatDateUTC(header.getReportingBegin()));
            endXmlElement();
        }

        if(header != null && header.getReportingEnd() != null){
            startXmlElement("ReportingEnd");
            writeXmlCharacters(DateUtil.formatDateUTC(header.getReportingEnd()));
            endXmlElement();
        }

        endXmlElement();//end header
    }

    @Override
    public void closeHeader() {

    }

    @Override
    public void closeWriter() {
        try {
            endXmlDocument();
            xmlStreamWriter.writeEndDocument();
            xmlStreamWriter.flush();
            xmlStreamWriter.close();
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void startXmlElement(String nsPrefix, String nsUri, String elementName) {
        try {
            xmlStreamWriter.writeStartElement(nsPrefix, elementName, nsUri);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void startXmlElement(String nsUri, String elementName) {
        try {
            xmlStreamWriter.writeStartElement(nsUri, elementName);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void startXmlElement(String elementName) {
        try {
            xmlStreamWriter.writeStartElement(elementName);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeXmlAttribute(String attrName, String attrValue){
        try {
            if(attrName != null ){
                xmlStreamWriter.writeAttribute(attrName, attrValue != null ? attrValue : "");
            }
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeXmlAttribute(String nsPrefix, String nsUri, String attrName, String attrValue){
        try {
            xmlStreamWriter.writeAttribute(nsPrefix, nsUri, attrName, attrValue);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeXmlCharacters(String value){
        try {
        	if (value != null) {
        		xmlStreamWriter.writeCharacters(value);
        	}
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected void writeComment(String value){
        try {
        	if (value != null) {
        		xmlStreamWriter.writeComment(value);
        	}
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void endXmlElement(){
        try {
            xmlStreamWriter.writeEndElement();
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeXmlNamespace(String prefix, String namespaceUri){
        try {
            xmlStreamWriter.writeNamespace(prefix, namespaceUri);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeDefaultNamespace(String namespaceUri){
        try {
            xmlStreamWriter.writeDefaultNamespace(namespaceUri);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    protected void setDefaultNamespace(String namespaceUri){
        try {
            xmlStreamWriter.setDefaultNamespace(namespaceUri);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeParty(PartyBean party, boolean includeTimeZone) {
        if(party.getId() != null ) {
            writeXmlAttribute("id", party.getId());
            if(party.getName() != null) {
                writeNames("Name", party.getName());
            }
            if(party.getContacts() != null) {
                writeContacts(party.getContacts());
            }
            if(includeTimeZone && party.getTimeZone() != null) {
                startXmlElement("Timezone");
                writeXmlCharacters(party.getTimeZone());
                endXmlElement();
            }
        } else {
            writeXmlAttribute("id", "unknown");
        }
    }

    private void writeNames(String elementName, List<TextTypeWrapper> names){
        for (TextTypeWrapper name: names) {
            startXmlElement(elementName);
            writeXmlAttribute(XML_NS_PREFIX, XML_NS_URI, "lang", name.getLocale());
            writeXmlCharacters(name.getValue());
            endXmlElement();
        }
    }

    private void writeListContents(String elementName, List<String> listOfValues) {
       for(String value : listOfValues) {
            if(value != null && value != "") {
                startXmlElement(elementName);
                writeXmlCharacters(value);
                endXmlElement();
            }
        }
    }

    private void writeContacts(List<ContactBean> contacts) {
        if(contacts != null) {
            for(ContactBean currentContact : contacts) {
                startXmlElement("Contact");
                writeContact(currentContact);
                endXmlElement();
            }
        }
    }


    private void writeContact(ContactBean contact) {
        writeNames("Name", contact.getName());
        writeNames("Department", contact.getDepartments());
        writeNames("Role", contact.getRole());
        writeListContents("Telephone", contact.getTelephone());
        writeListContents("Fax", contact.getFax());
        writeListContents("X400", contact.getX400());
        writeListContents("URI", contact.getUri());
        writeListContents("Email", contact.getEmail());
    }
    
	protected String getComponentId(ComponentBean component) {
		if(component == null) {
			return null;
		}
		return ConceptRefUtil.getConceptId(component.getConceptRef());
	}

    public abstract void startXmlDocument();
    public abstract void endXmlDocument();

}
