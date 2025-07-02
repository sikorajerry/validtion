package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.IoUtils;
import com.intrasoft.sdmx.converter.services.exceptions.ParseXmlException;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.util.csv.*;
import org.estat.sdmxsource.util.excel.ExcelInputConfig;
import org.estat.struval.engine.NodeTracker;
import org.estat.struval.engine.impl.IgnoreErrorsExceptionHandler;
import org.estat.struval.engine.impl.NodeChildTracker;
import org.estat.struval.engine.impl.NodeTrackerComposite;
import org.estat.struval.engine.impl.SdmxHeaderReaderEngine;
import org.sdmxsource.sdmx.api.constants.DATASET_ACTION;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.model.beans.base.ContactBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.header.PartyBean;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.HeaderSDMXCsvValues;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.ContactBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.base.TextTypeWrapperImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.PartyBeanImpl;
import org.sdmxsource.sdmx.util.date.DateUtil;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.activation.DataHandler;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class HeaderService {

	//	public final static SimpleDateFormat HEADER_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
	public final static DateFormat HEADER_DATE_FORMATTER = DateUtil.getDateFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Autowired
	private MappingService mappingService;
	private static Logger logger = LogManager.getLogger(HeaderService.class);

	public void saveHeader(HeaderBean headerBean, File outputFile) throws WriteHeaderException {
		try {
			saveHeader(headerBean, new FileOutputStream(outputFile));
		} catch (FileNotFoundException e) {
			throw new WriteHeaderException("Header file not found !", e);
		}
	}

	public void saveHeader(HeaderBean headerBean, OutputStream outputStream) throws WriteHeaderException {
		try {
			Properties prop = retrievePropertiesFileFromHeaderBean(headerBean);
			prop.store(outputStream, null);
			outputStream.flush();
		} catch (IOException ioException) {
			throw new WriteHeaderException("Error writing the header !", ioException);
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * @param inputFile   input file for conversion
	 * @param whereToSave output file
	 * @param format      input format for conversion
	 */
	public void saveHeader(File inputFile, File whereToSave, Formats format) throws WriteHeaderException {
		try {
			saveHeader(inputFile, new FileOutputStream(whereToSave), format);
		} catch (FileNotFoundException e) {
			throw new WriteHeaderException("header file not found !", e);
		}
	}

	/**
	 * @param inputFile    input file for conversion
	 * @param outputStream output stream where to save
	 * @param format       input format for conversion
	 */
	public void saveHeader(File inputFile, OutputStream outputStream, Formats format) throws WriteHeaderException {
		try (FileInputStream inputStream = new FileInputStream(inputFile)) {
			saveHeader(inputStream, outputStream, format);
		} catch (IOException ioExc) {
			logger.error("IOException ", ioExc);
			throw new WriteHeaderException("Error writing the header properties !", ioExc);
		}

	}

	public void saveHeader(InputStream inputStream, OutputStream outputStream, Formats format) throws WriteHeaderException, IOException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		//SDMXCONV-1229
		try (BOMInputStream bomInputStream = new BOMInputStream(inputStream)) {
			XMLStreamReader parser = factory.createXMLStreamReader(bomInputStream, "UTF-8");
			NodeTracker nodeTextTracker = new NodeTrackerComposite(new NodeChildTracker(parser, format.getDataType()));
			HeaderBean headerBean = SdmxHeaderReaderEngine.processHeader(parser, nodeTextTracker, null,
					new IgnoreErrorsExceptionHandler());
			parser.close();
			saveHeader(headerBean, outputStream);

		} catch (XMLStreamException e) {
			logger.error("XMLStreamException", e);
			throw new WriteHeaderException("Error reading the xml header !", e);
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
	}

	/**
	 * @param headerBean
	 * @return
	 */
	public Properties retrievePropertiesFileFromHeaderBean(HeaderBean headerBean) {
		Properties properties = new Properties();
		if (headerBean.getId() != null) {
			properties.setProperty("header.id", headerBean.getId());
		}
		properties.setProperty("header.test", headerBean.isTest() + "");
		if (headerBean.getName() != null && !headerBean.getName().isEmpty() && headerBean.getName().get(0) != null) {
			properties.setProperty("header.name", headerBean.getName().get(0).getValue());
			properties.setProperty("header.lang", headerBean.getName().get(0).getLocale());
		}
		if (headerBean.getSource() != null && !headerBean.getSource().isEmpty() && headerBean.getSource().get(0) != null) {
			properties.setProperty("header.source", headerBean.getSource().get(0).getValue());
		}
		if (headerBean.getDataProviderReference() != null && headerBean.getDataProviderReference().getAgencyId() != null) {
			properties.setProperty("header.datasetagency", headerBean.getDataProviderReference().getAgencyId());
		}
		if (headerBean.getDatasetId() != null) {
			properties.setProperty("header.datasetid", headerBean.getDatasetId());
		}
		if (headerBean.getAction() != null) {
			properties.setProperty("header.datasetaction", headerBean.getAction().getAction());
		}
		if (headerBean.hasAdditionalAttribute("Truncated")) {
			properties.setProperty("header.truncated", headerBean.getAdditionalAttribute("Truncated"));
		}
		if (headerBean.hasAdditionalAttribute("DATASET_AGENCY")) {
			properties.setProperty("header.datasetagency", headerBean.getAdditionalAttribute("DATASET_AGENCY"));
		}
		if (headerBean.getExtracted() != null) {
			//SDMXCONV-947
			properties.setProperty("header.extracted", DateUtil.formatDateUTC(headerBean.getExtracted()));
		}
		if (headerBean.getReportingBegin() != null) {
			properties.setProperty("header.reportingbegin", DateUtil.formatDateUTC(headerBean.getReportingBegin()));
		}
		if (headerBean.getReportingEnd() != null) {
			properties.setProperty("header.reportingend", DateUtil.formatDateUTC(headerBean.getReportingEnd()));
		}
		if (headerBean.getPrepared() != null) {
			properties.setProperty("header.prepared", DateUtil.formatDateUTC(headerBean.getPrepared()));
		}
		if (headerBean.getSender() != null) {
			if (headerBean.getSender().getId() != null) {
				properties.setProperty("header.senderid", headerBean.getSender().getId());
			}
			if (headerBean.getSender().getName() != null && !headerBean.getSender().getName().isEmpty() && headerBean.getSender().getName().get(0) != null) {
				properties.setProperty("header.sendername", headerBean.getSender().getName().get(0).getValue());
			}
			if (headerBean.getSender().getContacts() != null && !headerBean.getSender().getContacts().isEmpty()) {
				ContactBean contactBean = headerBean.getSender().getContacts().get(0);
				if (contactBean != null) {
					if (contactBean.getName() != null && !contactBean.getName().isEmpty() && contactBean.getName().get(0) != null) {
						properties.setProperty("header.sendercontactname", contactBean.getName().get(0).getValue());
					}
					if (contactBean.getDepartments() != null && !contactBean.getDepartments().isEmpty() && contactBean.getDepartments().get(0) != null) {
						properties.setProperty("header.sendercontactdepartment", contactBean.getDepartments().get(0).getValue());
					}
					if (contactBean.getRole() != null && !contactBean.getRole().isEmpty() && contactBean.getRole().get(0) != null) {
						properties.setProperty("header.sendercontactrole", contactBean.getRole().get(0).getValue());
					}
					if (contactBean.getTelephone() != null && !contactBean.getTelephone().isEmpty() && contactBean.getTelephone().get(0) != null) {
						properties.setProperty("header.sendercontacttelephone", contactBean.getTelephone().get(0));
					}
					if (contactBean.getFax() != null && !contactBean.getFax().isEmpty() && contactBean.getFax().get(0) != null) {
						properties.setProperty("header.sendercontactfax", contactBean.getFax().get(0));
					}
					if (contactBean.getX400() != null && !contactBean.getX400().isEmpty() && contactBean.getX400().get(0) != null) {
						properties.setProperty("header.sendercontactx400", contactBean.getX400().get(0));
					}
					if (contactBean.getUri() != null && !contactBean.getUri().isEmpty() && contactBean.getUri().get(0) != null) {
						properties.setProperty("header.sendercontacturi", contactBean.getUri().get(0));
					}
					if (contactBean.getEmail() != null && !contactBean.getEmail().isEmpty() && contactBean.getEmail().get(0) != null) {
						properties.setProperty("header.sendercontactemail", contactBean.getEmail().get(0));
					}
				}
			}
		}
		if (headerBean.getReceiver() != null && !headerBean.getReceiver().isEmpty() && headerBean.getReceiver().get(0) != null) {
			PartyBean partyBean = headerBean.getReceiver().get(0);
			if (partyBean.getId() != null) {
				properties.setProperty("header.receiverid", partyBean.getId());
			}
			if (partyBean.getName() != null && !partyBean.getName().isEmpty() && partyBean.getName().get(0) != null) {
				properties.setProperty("header.receivername", partyBean.getName().get(0).getValue());
			}
			if (partyBean.getContacts() != null && !partyBean.getContacts().isEmpty() && partyBean.getContacts().get(0) != null) {
				ContactBean contactBean = partyBean.getContacts().get(0);
				if (contactBean.getName() != null && !contactBean.getName().isEmpty() && contactBean.getName().get(0) != null) {
					properties.setProperty("header.receivercontactname", contactBean.getName().get(0).getValue());
				}
				if (contactBean.getDepartments() != null && !contactBean.getDepartments().isEmpty() && contactBean.getDepartments().get(0) != null) {
					properties.setProperty("header.receivercontactdepartment", contactBean.getDepartments().get(0).getValue());
				}
				if (contactBean.getDepartments() != null && !contactBean.getDepartments().isEmpty() && contactBean.getDepartments().get(0) != null) {
					properties.setProperty("header.receivercontactdepartment", contactBean.getDepartments().get(0).getValue());
				}
				if (contactBean.getRole() != null && !contactBean.getRole().isEmpty() && contactBean.getRole().get(0) != null) {
					properties.setProperty("header.receivercontactrole", contactBean.getRole().get(0).getValue());
				}
				if (contactBean.getTelephone() != null && !contactBean.getTelephone().isEmpty() && contactBean.getTelephone().get(0) != null) {
					properties.setProperty("header.receivercontacttelephone", contactBean.getTelephone().get(0));
				}
				if (contactBean.getFax() != null && !contactBean.getFax().isEmpty() && contactBean.getFax().get(0) != null) {
					properties.setProperty("header.receivercontactfax", contactBean.getFax().get(0));
				}
				if (contactBean.getX400() != null && !contactBean.getX400().isEmpty() && contactBean.getX400().get(0) != null) {
					properties.setProperty("header.receivercontactx400", contactBean.getX400().get(0));
				}
				if (contactBean.getUri() != null && !contactBean.getUri().isEmpty() && contactBean.getUri().get(0) != null) {
					properties.setProperty("header.receivercontacturi", contactBean.getUri().get(0));
				}
				if (contactBean.getEmail() != null && !contactBean.getEmail().isEmpty() && contactBean.getEmail().get(0) != null) {
					properties.setProperty("header.receivercontactemail", contactBean.getEmail().get(0));
				}
			}
		}

		return properties;
	}

	/**
	 * creates a HeaderBean from properties
	 *
	 * @param properties
	 * @return
	 * @throws WriteHeaderException
	 */
	public org.sdmxsource.sdmx.api.model.header.HeaderBean parseSdmxHeaderProperties(Properties properties) throws WriteHeaderException {
		HeaderBeanImpl header = null;
		if (properties.getProperty("header.id") != null && !properties.getProperty("header.id").isEmpty()) {
			header = new HeaderBeanImpl(properties.getProperty("header.id"), properties.getProperty("header.senderid"));
		} else {
			header = new HeaderBeanImpl("IREF123", properties.getProperty("header.senderid"));
		}

//            DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();

		String language = properties.getProperty("header.lang");

		//TODO
		//header.setDataProviderReference(new StructureReferenceBeanImpl());
		//header.prepared
		//header.extracted

            /*StructureReferenceBeanImpl structureReferenceBean = new StructureReferenceBeanImpl();
             structureReferenceBean.setAgencyId(properties.getProperty("header.datasetagency"));

             header.setDataProviderReference(new StructureReferenceBeanImpl());*/
		String datasetId = properties.getProperty("header.datasetid");
		if (datasetId != null && !datasetId.isEmpty()) {
			header.setDatasetId(datasetId);
		}
		String datasetAction = properties.getProperty("header.datasetaction");
		if (datasetAction != null && !datasetAction.isEmpty()) {
			header.setAction(DATASET_ACTION.getAction(datasetAction));
		}
		String test = properties.getProperty("header.test");
		if (test != null && !test.isEmpty()) {
			header.setTest(Boolean.parseBoolean(test));
		}
		String reportingBegin = properties.getProperty("header.reportingbegin");
		if (reportingBegin != null && !reportingBegin.isEmpty()) {
			header.setReportingBegin(parseHeaderDate(reportingBegin));
		}
		String reportingEnd = properties.getProperty("header.reportingend");
		if (reportingEnd != null && !reportingEnd.isEmpty()) {
			header.setReportingEnd(parseHeaderDate(reportingEnd));
		}
		String prepared = properties.getProperty("header.prepared");
		if (prepared != null && !prepared.isEmpty()) {
			header.setPrepared(parseHeaderDate(prepared));
		}
		String extracted = properties.getProperty("header.extracted");
		Date extractedDate = null;
		if (extracted != null && !extracted.isEmpty()) {
			extractedDate = parseHeaderDate(extracted);
		}
		String source = properties.getProperty("header.source");
		if (source != null && !source.equals("")) {
			TextTypeWrapper wrapper = new TextTypeWrapperImpl(language, source, null);
			header.addSource(wrapper);
		}
		if (properties.getProperty("header.name") != null && !"".equals(properties.getProperty("header.name"))) {
			header.addName(new TextTypeWrapperImpl(language, properties.getProperty("header.name"), null));
		}
		List<TextTypeWrapper> senderNames = null;
		if (properties.getProperty("header.sendername") != null && !"".equals(properties.getProperty("header.sendername"))) {
			senderNames = new ArrayList<TextTypeWrapper>();
			senderNames.add(new TextTypeWrapperImpl(language, properties.getProperty("header.sendername"), null));
		}

		List<ContactBean> senderContacts = null;
		List<TextTypeWrapper> senderContactNames = null;

		if (properties.getProperty("header.sendercontactname") != null && !"".equals(properties.getProperty("header.sendercontactname"))) {
			senderContactNames = new ArrayList<TextTypeWrapper>();
			senderContactNames.add(new TextTypeWrapperImpl(language, properties.getProperty("header.sendercontactname"), null));
		}

		List<TextTypeWrapper> senderContactDepartments = null;
		if (properties.getProperty("header.sendercontactdepartment") != null && !"".equals(properties.getProperty("header.sendercontactdepartment"))) {
			senderContactDepartments = new ArrayList<TextTypeWrapper>();
			senderContactDepartments.add(new TextTypeWrapperImpl(language, properties.getProperty("header.sendercontactdepartment"), null));
		}

		List<TextTypeWrapper> senderContactRoles = null;
		if (properties.getProperty("header.sendercontactrole") != null && !"".equals(properties.getProperty("header.sendercontactrole"))) {
			senderContactRoles = new ArrayList<TextTypeWrapper>();
			senderContactRoles.add(new TextTypeWrapperImpl(language, properties.getProperty("header.sendercontactrole"), null));
		}

		List<String> senderContactDataTelephones = null;
		if (properties.getProperty("header.sendercontacttelephone") != null && !"".equals(properties.getProperty("header.sendercontacttelephone"))) {
			senderContactDataTelephones = new ArrayList<String>();
			senderContactDataTelephones.add(properties.getProperty("header.sendercontacttelephone"));
		}

		List<String> senderContactDataFaxes = null;
		if (properties.getProperty("header.sendercontactfax") != null && !"".equals(properties.getProperty("header.sendercontactfax"))) {
			senderContactDataFaxes = new ArrayList<String>();
			senderContactDataFaxes.add(properties.getProperty("header.sendercontactfax"));
		}

		List<String> senderContactDataX400s = null;
		if (properties.getProperty("header.sendercontactx400") != null && !"".equals(properties.getProperty("header.sendercontactx400"))) {
			senderContactDataX400s = new ArrayList<String>();
			senderContactDataX400s.add(properties.getProperty("header.sendercontactx400"));
		}

		List<String> senderContactDataUris = null;
		if (properties.getProperty("header.sendercontacturi") != null && !"".equals(properties.getProperty("header.sendercontacturi"))) {
			senderContactDataUris = new ArrayList<String>();
			senderContactDataUris.add(properties.getProperty("header.sendercontacturi"));
		}

		List<String> senderContactDataEmails = null;
		if (properties.getProperty("header.sendercontactemail") != null && !"".equals(properties.getProperty("header.sendercontactemail"))) {
			senderContactDataEmails = new ArrayList<String>();
			senderContactDataEmails.add(properties.getProperty("header.sendercontactemail"));
		}

		ContactBean senderContact = null;
		if (senderContactNames != null || senderContactRoles != null || senderContactDepartments != null
				|| senderContactDataEmails != null || senderContactDataFaxes != null || senderContactDataTelephones != null
				|| senderContactDataUris != null || senderContactDataX400s != null) {
			senderContact = new ContactBeanImpl(senderContactNames, senderContactRoles, senderContactDepartments, senderContactDataEmails,
					senderContactDataFaxes, senderContactDataTelephones, senderContactDataUris, senderContactDataX400s);
		}

		if (senderContact != null) {
			senderContacts = new ArrayList<ContactBean>();
			senderContacts.add(senderContact);
		}

		if (senderNames != null
				|| (properties.getProperty("header.senderid") != null && !"".equals(properties.getProperty("header.senderid")) || senderContacts != null)) {
			final PartyBean sender = new PartyBeanImpl(senderNames, properties.getProperty("header.senderid"), senderContacts, null);
			header.setSender(sender);
		}

		//Receiver
		List<TextTypeWrapper> receiverNames = null;
		if (properties.getProperty("header.receivername") != null && !"".equals(properties.getProperty("header.receivername"))) {
			receiverNames = new ArrayList<TextTypeWrapper>();
			receiverNames.add(new TextTypeWrapperImpl(language, properties.getProperty("header.receivername"), null));
		}

		List<TextTypeWrapper> receiverContactNames = null;
		if (properties.getProperty("header.receivercontactname") != null && !"".equals(properties.getProperty("header.receivercontactname"))) {
			TextTypeWrapper name = new TextTypeWrapperImpl(language, properties.getProperty("header.receivercontactname"), null);
			receiverContactNames = new ArrayList<TextTypeWrapper>();
			receiverContactNames.add(name);
		}

		List<TextTypeWrapper> receiverContactDepartments = null;
		if (properties.getProperty("header.receivercontactdepartment") != null && !"".equals(properties.getProperty("header.receivercontactdepartment"))) {
			receiverContactDepartments = new ArrayList<TextTypeWrapper>();
			receiverContactDepartments.add(new TextTypeWrapperImpl(language, properties.getProperty("header.receivercontactdepartment"), null));
		}

		List<TextTypeWrapper> receiverContactRoles = null;
		if (properties.getProperty("header.receivercontactrole") != null && !"".equals(properties.getProperty("header.receivercontactrole"))) {
			receiverContactRoles = new ArrayList<TextTypeWrapper>();
			receiverContactRoles.add(new TextTypeWrapperImpl(language, properties.getProperty("header.receivercontactrole"), null));
		}

		List<String> receiverContactDataTelephones = null;
		if (properties.getProperty("header.receivercontacttelephone") != null && !"".equals(properties.getProperty("header.receivercontacttelephone"))) {
			receiverContactDataTelephones = new ArrayList<String>();
			receiverContactDataTelephones.add(properties.getProperty("header.receivercontacttelephone"));
		}

		List<String> receiverContactDataFaxes = null;
		;
		if (properties.getProperty("header.receivercontactfax") != null && !"".equals(properties.getProperty("header.receivercontactfax"))) {
			receiverContactDataFaxes = new ArrayList<String>();
			receiverContactDataFaxes.add(properties.getProperty("header.receivercontactfax"));
		}

		List<String> receiverContactDataX400s = null;
		if (properties.getProperty("header.receivercontactx400") != null && !"".equals(properties.getProperty("header.receivercontactx400"))) {
			receiverContactDataX400s = new ArrayList<String>();
			receiverContactDataX400s.add(properties.getProperty("header.receivercontactx400"));
		}

		List<String> receiverContactDataUris = null;
		if (properties.getProperty("header.receivercontacturi") != null && !"".equals(properties.getProperty("header.receivercontacturi"))) {
			receiverContactDataUris = new ArrayList<String>();
			receiverContactDataUris.add(properties.getProperty("header.receivercontacturi"));
		}

		List<String> receiverContactDataEmails = null;
		if (properties.getProperty("header.receivercontactemail") != null && !"".equals(properties.getProperty("header.receivercontactemail"))) {
			receiverContactDataEmails = new ArrayList<String>();
			receiverContactDataEmails.add(properties.getProperty("header.receivercontactemail"));
		}

		ContactBean receiverContact = null;
		if (receiverContactNames != null || receiverContactRoles != null || receiverContactDepartments != null
				|| receiverContactDataEmails != null || receiverContactDataFaxes != null || receiverContactDataTelephones != null
				|| receiverContactDataUris != null || receiverContactDataX400s != null) {
			receiverContact = new ContactBeanImpl(receiverContactNames, receiverContactRoles, receiverContactDepartments, receiverContactDataEmails,
					receiverContactDataFaxes, receiverContactDataTelephones, receiverContactDataUris, receiverContactDataX400s);
		}

		List<ContactBean> receiverContacts = null;
		if (receiverContact != null) {
			receiverContacts = new ArrayList<>();
			receiverContacts.add(receiverContact);
		}

		if (receiverNames != null
				|| (properties.getProperty("header.receiverid") != null && !"".equals(properties.getProperty("header.receiverid")) || receiverContacts != null)) {
			final PartyBean receiver = new PartyBeanImpl(receiverNames, properties.getProperty("header.receiverid"), receiverContacts, null);
			header.addReceiver(receiver);
		}

		//Set truncated inside additional Attributes
		Map<String, String> additionalAttributes = header.getAdditionalAttributes();
		final String TRUNCATED = "Truncated";
		String strTruncated = properties.getProperty("header.truncated");
		if (properties.getProperty("header.truncated") != null && !"".equals(properties.getProperty("header.truncated"))) {
			additionalAttributes.put(TRUNCATED, strTruncated);
		}

		//Set Dataset Agency inside additional Attributes
		final String DATASET_AGENCY = "DATASET_AGENCY";
		if (properties.getProperty("header.datasetagency") != null && !"".equals(properties.getProperty("header.datasetagency"))) {
			additionalAttributes.put(DATASET_AGENCY, properties.getProperty("header.datasetagency"));
		}
		//Write HeaderBean
		header = new HeaderBeanImpl(
				additionalAttributes,
				header.getStructures(),
				header.getDataProviderReference(),
				header.getAction(),
				header.getId(),
				header.getDatasetId(),
				header.getEmbargoDate(),
				extractedDate,
				header.getPrepared(),
				header.getReportingBegin(),
				header.getReportingEnd(),
				header.getName(),
				header.getSource(),
				header.getReceiver(),
				header.getSender(),
				header.isTest()
		);

		return header;
	}

	public org.sdmxsource.sdmx.api.model.header.HeaderBean parseSdmxHeaderProperties(File headerPropertiesFile) throws IOException, WriteHeaderException {
		return parseSdmxHeaderProperties(new FileInputStream(headerPropertiesFile));
	}

	public org.sdmxsource.sdmx.api.model.header.HeaderBean parseSdmxHeaderProperties(String headerPropertiesFileName) throws IOException, WriteHeaderException {
		return parseSdmxHeaderProperties(new File(headerPropertiesFileName));
	}


	public org.sdmxsource.sdmx.api.model.header.HeaderBean parseSdmxHeaderProperties(InputStream headerPropertiesIs) throws IOException, WriteHeaderException {
		org.sdmxsource.sdmx.api.model.header.HeaderBean result = null;
		//SDMXCONV-1229
		try (BOMInputStream bomHeaderPropertiesIs = new BOMInputStream(headerPropertiesIs)) {
			Properties headerProperties = readProperties(bomHeaderPropertiesIs);
			result = parseSdmxHeaderProperties(headerProperties);
		} finally {
			IOUtils.closeQuietly(headerPropertiesIs);
		}
		return result;
	}

	/**
	 * Method that gets the Sdmx header either from external file or through the reader.
	 * <p>After we get the reader or if not exists set the default then we set it to input/output configuration.</p>
	 *
	 * @param converterInput      The Object that holds the input config and file
	 * @param converterOutput     The Object that holds the output config and file
	 * @param converterStructure  The structure given by the user
	 * @param headerBean          The headerBean to be set
	 * @param sdmxHeaderToSdmxcsv Param that is used for the mapping of SDMX header to SDMX_CSV output. (default value is false)
	 * @return HeaderBean
	 * @throws IOException
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1292">SDMXCONV-1292</a>
	 */
	public HeaderBean getHeaderBean(ConverterInput converterInput,
									ConverterOutput converterOutput,
									ConverterStructure converterStructure,
									HeaderBean headerBean,
									String sdmxHeaderToSdmxcsv) throws IOException {
		ExceptionHandler exceptionHandler = new FirstFailureExceptionHandler();
		LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();
		Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
		int order = 0;
		//If the user uploaded a header File then we use this.
		if (Formats.SDMX_CSV.equals(converterOutput.getOutputFormat()) ||
				Formats.SDMX_CSV_2_0.equals(converterOutput.getOutputFormat()) ||
				Formats.iSDMX_CSV.equals(converterOutput.getOutputFormat()) && !ObjectUtil.validObject(headerBean)) {
			try (DataReaderEngine originReaderEngine = ConverterDataReaderEngineProvider.getDataReaderEngine(converterInput, converterStructure, errorsByEngine, order, 1, exceptionHandler, errorPositions)) {
				headerBean = originReaderEngine.getHeader();
			}
		}
		if (!ObjectUtil.validObject(headerBean))
			headerBean = new HeaderBeanImpl("IREF123", "ZZ9");

		InputConfig inConfig = converterInput.getInputConfig();
		switch (converterInput.getInputConfig().getClass().getSimpleName()) {
			case "CsvInputConfig":
				((CsvInputConfig) inConfig).setHeader(headerBean);
				break;
			case "FLRInputConfig":
				((FLRInputConfig) inConfig).setHeader(headerBean);
				break;
			case "ExcelInputConfig":
			case "ExcelInputConfigImpl":
				((ExcelInputConfig) inConfig).setHeader(headerBean);
				break;
			case "SdmxCsvInputConfig":
				((SdmxCsvInputConfig) inConfig).setHeader(headerBean);
				break;
		}
		//Set the header beans
		converterInput.setInputConfig(inConfig);
		OutputConfig outConfig = converterOutput.getOutputConfig();
		switch (converterOutput.getOutputConfig().getClass().getSimpleName()) {
			case "SdmxCsvOutputConfig":
				setColumnMapping(headerBean, (SdmxCsvOutputConfig) outConfig, sdmxHeaderToSdmxcsv);
				break;
		}
		converterOutput.setOutputConfig(outConfig);
		return headerBean;
	}

	/**
	 * Method that gets the Sdmx header either from external file or through the reader.
	 * <p>After we get the reader or if not exists set the default then we set it to input/output configuration.</p>
	 *
	 * @param converterInput      The Object that holds the input config and file
	 * @param converterOutput     The Object that holds the output config and file
	 * @param converterStructure  The structure given by the user
	 * @param headerFile          The header File if given
	 * @param sdmxHeaderToSdmxcsv Param that is used for the mapping of SDMX header to SDMX_CSV output. (default value is false)
	 * @return HeaderBean
	 * @throws IOException
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1292">SDMXCONV-1292</a>
	 */
	public HeaderBean getHeaderBean(File headerFile, ConverterInput converterInput, ConverterOutput converterOutput, ConverterStructure converterStructure, String sdmxHeaderToSdmxcsv) throws IOException, WriteHeaderException {
		ExceptionHandler exceptionHandler = new FirstFailureExceptionHandler();
		LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();
		Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
		int order = 0;
		// Set the headerIs
		HeaderBean headerBean = null;
		//If the user uploaded a header File then we use this.
		if (ObjectUtil.validObject(headerFile)) {
			try (InputStream headerIs = new FileInputStream(headerFile)) {
				headerBean = parseSdmxHeaderProperties(headerIs);
			}
			//if the output is sdmx csv or is sdmx csv then we try to get header bean from the original reader
		} else if (Formats.SDMX_CSV.equals(converterOutput.getOutputFormat()) ||
				Formats.SDMX_CSV_2_0.equals(converterOutput.getOutputFormat()) ||Formats.iSDMX_CSV.equals(converterOutput.getOutputFormat())) {
			try (DataReaderEngine originReaderEngine = ConverterDataReaderEngineProvider.getDataReaderEngine(converterInput, converterStructure, errorsByEngine, order, 1, exceptionHandler, errorPositions)) {
				headerBean = originReaderEngine.getHeader();
			}
		}
		if (!ObjectUtil.validObject(headerBean))
			headerBean = new HeaderBeanImpl("IREF123", "ZZ9");

		InputConfig inConfig = converterInput.getInputConfig();
		switch (converterInput.getInputConfig().getClass().getSimpleName()) {
			case "CsvInputConfig":
				((CsvInputConfig) inConfig).setHeader(headerBean);
				break;
			case "FLRInputConfig":
				((FLRInputConfig) inConfig).setHeader(headerBean);
				break;
			case "ExcelInputConfig":
			case "ExcelInputConfigImpl":
				((ExcelInputConfig) inConfig).setHeader(headerBean);
				break;
			case "SdmxCsvInputConfig":
				((SdmxCsvInputConfig) inConfig).setHeader(headerBean);
				break;
		}
		//Set the header beans
		converterInput.setInputConfig(inConfig);
		OutputConfig outConfig = converterOutput.getOutputConfig();
		switch (converterOutput.getOutputConfig().getClass().getSimpleName()) {
			case "SdmxCsvOutputConfig":
				setColumnMapping(headerBean, (SdmxCsvOutputConfig) outConfig, sdmxHeaderToSdmxcsv);
				break;
		}
		converterOutput.setOutputConfig(outConfig);
		return headerBean;
	}

	/**
	 * Method that gets the Sdmx header either from external file or through the reader.
	 * <p>After we get the reader or if not exists set the default then we set it to input/output configuration.</p>
	 *
	 * @param converterInput      The Object that holds the input config and file
	 * @param converterOutput     The Object that holds the output config and file
	 * @param converterStructure  The structure given by the user
	 * @param headerFile          The header File if given in byte array
	 * @param sdmxHeaderToSdmxcsv Param that is used for the mapping of SDMX header to SDMX_CSV output. (default value is false)
	 * @return HeaderBean
	 * @throws IOException
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1292">SDMXCONV-1292</a>
	 */
	public HeaderBean getHeaderBean(ConverterInput converterInput, ConverterOutput converterOutput, ConverterStructure converterStructure, String sdmxHeaderToSdmxcsv, DataHandler headerFile) throws IOException, WriteHeaderException {
		ExceptionHandler exceptionHandler = new FirstFailureExceptionHandler();
		LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> errorsByEngine = new LinkedHashMap<>();
		Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<>();
		int order = 0;
		// Set the headerIs
		HeaderBean headerBean = null;
		//If the user uploaded a header File then we use this.
		if (ObjectUtil.validObject(headerFile)) {
			try (InputStream headerIs = headerFile.getInputStream()) {
				headerBean = parseSdmxHeaderProperties(headerIs);
			}
			//if the output is sdmx csv or is sdmx csv then we try to get header bean from the original reader
		} else if (Formats.SDMX_CSV.equals(converterOutput.getOutputFormat()) ||
				Formats.SDMX_CSV_2_0.equals(converterOutput.getOutputFormat()) ||
				Formats.iSDMX_CSV.equals(converterOutput.getOutputFormat())) {
			try (DataReaderEngine originReaderEngine = ConverterDataReaderEngineProvider.getDataReaderEngine(converterInput, converterStructure, errorsByEngine, order, 1, exceptionHandler, errorPositions)) {
				headerBean = originReaderEngine.getHeader();
			}
		}
		if (!ObjectUtil.validObject(headerBean))
			headerBean = new HeaderBeanImpl("IREF123", "ZZ9");

		InputConfig inConfig = converterInput.getInputConfig();
		switch (converterInput.getInputConfig().getClass().getSimpleName()) {
			case "CsvInputConfig":
				((CsvInputConfig) inConfig).setHeader(headerBean);
				break;
			case "FLRInputConfig":
				((FLRInputConfig) inConfig).setHeader(headerBean);
				break;
			case "ExcelInputConfig":
			case "ExcelInputConfigImpl":
				((ExcelInputConfig) inConfig).setHeader(headerBean);
				break;
			case "SdmxCsvInputConfig":
				((SdmxCsvInputConfig) inConfig).setHeader(headerBean);
				break;
		}
		//Set the header beans
		converterInput.setInputConfig(inConfig);
		OutputConfig outConfig = converterOutput.getOutputConfig();
		switch (converterOutput.getOutputConfig().getClass().getSimpleName()) {
			case "SdmxCsvOutputConfig":
				setColumnMapping(headerBean, (SdmxCsvOutputConfig) outConfig, sdmxHeaderToSdmxcsv);
				break;
		}
		converterOutput.setOutputConfig(outConfig);
		return headerBean;
	}

	/**
	 * Method that sets the mapping of SDMX header in SDMX_SCV output depending on the value of sdmxheadertosdmxcsv.
	 *
	 * @param headerBean          The SDMX header that comes from the input (XML) file.
	 * @param csvOutputConfig     The configuration of the output
	 * @param sdmxheadertosdmxcsv Param that is used for the mapping of SDMX header to SDMX_CSV output. (default value is false)
	 * @return SdmxCsvOutputConfig
	 * @throws IOException
	 */
	private SdmxCsvOutputConfig setColumnMapping(HeaderBean headerBean, SdmxCsvOutputConfig csvOutputConfig, String sdmxheadertosdmxcsv) throws IOException {
		if (ObjectUtil.validString(sdmxheadertosdmxcsv)) {
			if (sdmxheadertosdmxcsv.toLowerCase().equals("true")) {
				List<String> orderedHeaderNames = readOrderedListPropertiesFromPath("ordered-header.properties").stream().filter(s -> s.startsWith("$")).map(s -> s.replace("$", "")).collect(Collectors.toList());
				Map<String, String> orderedHeaderValues = new HashMap<String, String>();
				for (String key : orderedHeaderNames)
					isComponentInXMLHeader(key, headerBean, orderedHeaderValues);

				List<CsvOutColumnMapping> defaultMapping = CsvService.initOutputColumnMapping(orderedHeaderNames);
				Map<Integer, TreeMap<Integer, String>> columMapping = mappingService.populateDimOnLevel(defaultMapping);
				csvOutputConfig.setColumnMapping(new MultiLevelCsvOutColMapping(columMapping));
				csvOutputConfig.setHeaderSDMXCsvValue(HeaderSDMXCsvValues.DEFAULT);

			} else if (sdmxheadertosdmxcsv.contains("Header/")) {
				List<String> orderedHeaderNames = Arrays.asList(sdmxheadertosdmxcsv.split(","));
				Map<String, String> orderedHeaderValues = new HashMap<String, String>();
				for (String key : orderedHeaderNames)
					isComponentInXMLHeader(key, headerBean, orderedHeaderValues);

				List<CsvOutColumnMapping> defaultMapping = CsvService.initOutputColumnMapping(orderedHeaderNames);
				Map<Integer, TreeMap<Integer, String>> dimensionsMappedOnLevel = mappingService.populateDimOnLevel(defaultMapping);
				csvOutputConfig.setColumnMapping(new MultiLevelCsvOutColMapping(dimensionsMappedOnLevel));
				csvOutputConfig.setHeaderSDMXCsvValue(HeaderSDMXCsvValues.MANUAL);
			}
		}
		return csvOutputConfig;
	}

	/**
	 * reads the input stream and creates a properties object
	 *
	 * @param propertiesInputStream
	 * @return
	 * @throws IOException
	 */
	public Properties readProperties(InputStream propertiesInputStream) throws IOException {
		Properties properties = new Properties();
		try {
			properties.load(propertiesInputStream);
		} finally {
			IOUtils.closeQuietly(propertiesInputStream);
		}
		return properties;
	}

	/**
	 * @param propertiesFile
	 * @return
	 * @throws IOException
	 * @see #readProperties(InputStream)
	 */
	public Properties readProperties(File propertiesFile) throws IOException {
		Properties result = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(propertiesFile);
			result = readProperties(is);
		} finally {
			IOUtils.closeQuietly(is);
		}
		return result;
	}

	/**
	 * @param classPath
	 * @return
	 */
	public Properties readPropertiesFromClasspath(String classPath) throws IOException {
		Properties result = new Properties();
		InputStream propertiesInputStream = IoUtils.getInputStreamFromClassPath(classPath);
		try {
			result = readProperties(propertiesInputStream);
		} finally {
			IOUtils.closeQuietly(propertiesInputStream);
		}
		return result;
	}

	/**
	 * Method that loads the properties file from the config folder as a priority.
	 * <small>(When application runs as a package.)</small>
	 * <p>If file not found in external folder then we use the one found in the jar.</p>
	 *
	 * @param classPath
	 * @return
	 * @throws IOException
	 */
	public Properties readPropertiesFromPath(String classPath) throws IOException {
		String path = null;
		InputStream propertiesInputStream = null;
		Properties result = new Properties();
		try {
			try {
//				path = ClassLoader.getSystemClassLoader().getResource(classPath).getPath() + "/" + classPath;
				URL configURL = this.getClass().getResource("/config/" + classPath);
				path = configURL.getPath();
				//SDMXCONV-988(273),  unescape the %20 to spaces
				path = path.replaceAll("%20", " ");
				File file = new File(path);
				boolean empty = !file.exists() || file.length() == 0;
				//case where file does not exist or is empty
				if (empty) {
					propertiesInputStream = IoUtils.getInputStreamFromClassPath(classPath);
				} else {
					propertiesInputStream = new FileInputStream(file);
				}
			} catch (NullPointerException e) {
				propertiesInputStream = IoUtils.getInputStreamFromClassPath(classPath);
			}
			result = readProperties(propertiesInputStream);
		} finally {
			IOUtils.closeQuietly(propertiesInputStream);
		}
		return result;
	}

	public List<String> readOrderedListPropertiesFromPath(String classPath) throws IOException {
		String path = null;
		InputStream propertiesInputStream = null;
		List<String> result = new ArrayList<String>();
		try {
			try {
//				path = ClassLoader.getSystemClassLoader().getResource(classPath).getPath() + "/" + classPath;
				URL configURL = this.getClass().getResource("/config/" + classPath);
				path = configURL.getPath();
				//SDMXCONV-988(273),  unescape the %20 to spaces
				path = path.replaceAll("%20", " ");
				File file = new File(path);
				boolean empty = !file.exists() || file.length() == 0;
				//case where file does not exist or is empty
				if (empty) {
					propertiesInputStream = IoUtils.getInputStreamFromClassPath(classPath);
				} else {
					propertiesInputStream = new FileInputStream(file);
				}
			} catch (NullPointerException e) {
				propertiesInputStream = IoUtils.getInputStreamFromClassPath(classPath);
			}
			result = readListProperties(propertiesInputStream);
		} finally {
			IOUtils.closeQuietly(propertiesInputStream);
		}
		return result;
	}

	public List<String> readListProperties(InputStream propertiesInputStream) throws IOException {
		List<String> properties = null;
		try {
			properties = IOUtils.readLines(propertiesInputStream, StandardCharsets.UTF_8);
		} finally {
			IOUtils.closeQuietly(propertiesInputStream);
		}
		return properties;
	}

	public boolean isComponentInXMLHeader(String componentName, HeaderBean headerBean, Map<String, String> orderedHeaderValues) {
		boolean isFound = false;
		switch (componentName) {
			case "Header/ID":
				if (ObjectUtil.validString(headerBean.getId())) {
					isFound = true;
					orderedHeaderValues.put(componentName, headerBean.getId());
				}
				break;
			case "Header/Test":
				if (ObjectUtil.validObject(headerBean.isTest())) {
					isFound = true;
					orderedHeaderValues.put(componentName, String.valueOf(headerBean.isTest()));
				}
				break;
			case "Header/Prepared":
				if (ObjectUtil.validObject(headerBean.getPrepared())) {
					isFound = true;
					orderedHeaderValues.put(componentName, String.valueOf(headerBean.getPrepared()));
				}
				break;
			case "Header/Sender@Id":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validString(headerBean.getSender().getId())) {
						isFound = true;
						orderedHeaderValues.put(componentName, headerBean.getSender().getId());
					}
				break;
			case "Header/Sender/Name":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getName())) {
						isFound = true;
						orderedHeaderValues.put(componentName, headerBean.getSender().getName().get(0).getValue());
					}
				break;
			case "Header/Sender/Contact/Name":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getName())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getName().get(0).getValue());
							}
						}
				break;
			case "Header/Sender/Contact/Department":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getDepartments())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getDepartments().get(0).getValue());
								break;
							}
						}
				break;
			case "Header/Sender/Contact/Role":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getRole())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getRole().get(0).getValue());
								break;
							}
						}
				break;
			case "Header/Sender/Contact/Telephone":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getTelephone())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getTelephone().get(0));
								break;
							}
						}
				break;
			case "Header/Sender/Contact/Fax":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getFax())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getFax().get(0));
								break;
							}
						}
				break;
			case "Header/Sender/Contact/X400":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getX400())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getX400().get(0));
								break;
							}
						}
				break;
			case "Header/Sender/Contact/Uri":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getUri())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getUri().get(0));
								break;
							}
						}
				break;
			case "Header/Sender/Contact/Email":
				if (ObjectUtil.validObject(headerBean.getSender()))
					if (ObjectUtil.validCollection(headerBean.getSender().getContacts()))
						for (ContactBean cb : headerBean.getSender().getContacts()) {
							if (ObjectUtil.validCollection(cb.getEmail())) {
								isFound = true;
								orderedHeaderValues.put(componentName, cb.getEmail().get(0));
								break;
							}
						}
				break;
			case "Header/Receiver@Id":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validString(pb.getId())) {
							isFound = true;
							orderedHeaderValues.put(componentName, pb.getId());
							break;
						}
				break;


			case "Header/Receiver/Name":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getName())) {
							isFound = true;
							orderedHeaderValues.put(componentName, pb.getName().get(0).getValue());
							break;
						}
				break;
			case "Header/Receiver/Contact/Name":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getName())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getName().get(0).getValue());
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Receiver/Contact/Department":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getDepartments())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getDepartments().get(0).getValue());
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Receiver/Contact/Role":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getRole())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getRole().get(0).getValue());
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Receiver/Contact/Telephone":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getTelephone())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getTelephone().get(0));
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Receiver/Contact/Fax":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getFax())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getFax().get(0));
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Receiver/Contact/X400":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getX400())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getX400().get(0));
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Receiver/Contact/Uri":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getUri())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getUri().get(0));
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Receiver/Contact/Email":
				if (ObjectUtil.validCollection(headerBean.getReceiver()))
					for (PartyBean pb : headerBean.getReceiver())
						if (ObjectUtil.validCollection(pb.getContacts())) {
							for (ContactBean cb : pb.getContacts()) {
								if (ObjectUtil.validCollection(cb.getEmail())) {
									isFound = true;
									orderedHeaderValues.put(componentName, cb.getEmail().get(0));
									break;
								}
							}
							if (isFound)
								break;
						}
				break;
			case "Header/Structure@StructureID":
				if (ObjectUtil.validCollection(headerBean.getStructures()))
					for (DatasetStructureReferenceBean sr : headerBean.getStructures())
						if (ObjectUtil.validString(sr.getId())) {
							isFound = true;
							orderedHeaderValues.put(componentName, sr.getId());
							break;
						}
				break;
			case "Header/Structure@DimensionAtObservation":
				if (ObjectUtil.validCollection(headerBean.getStructures()))
					for (DatasetStructureReferenceBean sr : headerBean.getStructures())
						if (ObjectUtil.validString(sr.getDimensionAtObservation())) {
							isFound = true;
							orderedHeaderValues.put(componentName, sr.getDimensionAtObservation());
							break;
						}
				break;
			case "Header/Structure/Structure/Ref@Id":
				if (ObjectUtil.validCollection(headerBean.getStructures()))
					for (DatasetStructureReferenceBean sr : headerBean.getStructures())
						if (ObjectUtil.validObject(sr.getStructureReference()))
							if (ObjectUtil.validString(sr.getStructureReference().getFullId())) {
								isFound = true;
								orderedHeaderValues.put(componentName, sr.getStructureReference().getFullId());
								break;
							}
				break;
			case "Header/Structure/Structure/Ref@AgencyID":
				if (ObjectUtil.validCollection(headerBean.getStructures()))
					for (DatasetStructureReferenceBean sr : headerBean.getStructures())
						if (ObjectUtil.validObject(sr.getStructureReference()))
							if (ObjectUtil.validString(sr.getStructureReference().getAgencyId())) {
								isFound = true;
								orderedHeaderValues.put(componentName, sr.getStructureReference().getAgencyId());
								break;
							}
				break;
			case "Header/Structure/Structure/Ref@Version":
				if (ObjectUtil.validCollection(headerBean.getStructures()))
					for (DatasetStructureReferenceBean sr : headerBean.getStructures())
						if (ObjectUtil.validObject(sr.getStructureReference()))
							if (ObjectUtil.validString(sr.getStructureReference().getVersion())) {
								isFound = true;
								orderedHeaderValues.put(componentName, sr.getStructureReference().getVersion());
								break;
							}
				break;
			case "Header/KeyFamilyRef":
				if (ObjectUtil.validCollection(headerBean.getStructures()))
					for (DatasetStructureReferenceBean sr : headerBean.getStructures())
						if (ObjectUtil.validObject(sr.getStructureReference()))
							if (ObjectUtil.validString(sr.getStructureReference().getMaintainableId())) {
								isFound = true;
								orderedHeaderValues.put(componentName, sr.getStructureReference().getMaintainableId());
								break;
							}
				break;
			case "Header/KeyFamilyAgency":
				if (ObjectUtil.validCollection(headerBean.getStructures()))
					for (DatasetStructureReferenceBean sr : headerBean.getStructures())
						if (ObjectUtil.validObject(sr.getStructureReference()))
							if (ObjectUtil.validString(sr.getStructureReference().getAgencyId())) {
								isFound = true;
								orderedHeaderValues.put(componentName, sr.getStructureReference().getAgencyId());
								break;
							}
				break;
			case "Header/DataSetID":
				if (ObjectUtil.validString(headerBean.getDatasetId())) {
					isFound = true;
					orderedHeaderValues.put(componentName, headerBean.getDatasetId());
				}
				break;
			case "Header/DataSetAction":
				if (ObjectUtil.validObject(headerBean.getAction())) {
					isFound = true;
					orderedHeaderValues.put(componentName, headerBean.getAction().getAction());
				}
				break;
			case "Header/Extracted":
				if (ObjectUtil.validObject(headerBean.getExtracted())) {
					isFound = true;
					orderedHeaderValues.put(componentName, headerBean.getExtracted().toString());
				}
				break;
			case "Header/Source":
				if (ObjectUtil.validCollection(headerBean.getSource())) {
					isFound = true;
					orderedHeaderValues.put(componentName, headerBean.getSource().get(0).getValue());
				}
				break;
			default:
				break;
		}

		return isFound;
	}

	/**
	 * @param properties
	 * @return the new stripped Properties file
	 * @throws WriteHeaderException
	 */
	public Properties keepOnlyRequiredFields(Properties properties) throws WriteHeaderException {
		//SDMXCONV-768
		Properties retProperties = new Properties();
		if(ObjectUtil.validObject(properties) && !properties.isEmpty()) {
			retProperties.setProperty("header.id", properties.getProperty("header.id"));
			retProperties.setProperty("header.prepared", properties.getProperty("header.prepared"));
			retProperties.setProperty("header.senderid", properties.getProperty("header.senderid"));
		}
		return retProperties;
	}

	//SDMXCONV-835, SDMXCONV-1049

	/**
	 * Method for creating the header properties when manual config is selected
	 *
	 * @param properties
	 * @return
	 */
	public Properties preparePropertiesForManualConfig(Properties properties) {
		Properties retProperties = new Properties();
		long randomNum = new java.sql.Timestamp(new java.util.Date().getTime()).getTime();
		retProperties.setProperty("header.id", "IREF" + randomNum);
		String date = DateUtil.getDateTimeStringNow();
		retProperties.setProperty("header.prepared", date);
		if(ObjectUtil.validObject(properties) && !properties.isEmpty()) {
			retProperties.setProperty("header.senderid", properties.getProperty("header.senderid"));
		}
		return retProperties;
	}


	//SDMXCONV-947, method changed and from now on it uses DateUtil from SdmxSource
	private Date parseHeaderDate(String dateAsString) throws WriteHeaderException {
		Date date = null;
		try {
			date = DateUtil.formatDate(dateAsString);
		} catch (Exception e) {
			logger.error("ParseException", e);
			throw new WriteHeaderException("Error reading the header !", e);
		}
		return date;
	}

	/**
	 * In case of xml read the header of the input.
	 *
	 * @param inputStream
	 * @param format
	 * @return header
	 * @throws WriteHeaderException
	 */
	public HeaderBean parseHeaderFromInput(InputStream inputStream, Formats format) throws ParseXmlException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		HeaderBean headerBean = null;
		try {
			XMLStreamReader parser = factory.createXMLStreamReader(inputStream, "UTF-8");
			NodeTracker nodeTextTracker = new NodeTrackerComposite(new NodeChildTracker(parser, format.getDataType()));
			headerBean = SdmxHeaderReaderEngine.processHeader(parser, nodeTextTracker, null, new IgnoreErrorsExceptionHandler());
			parser.close();
			return headerBean;
		} catch (XMLStreamException e) {
			throw new ParseXmlException("Error while reading the xml header!", e);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
	}

	/**
	 * Try to find urn for dataStructure from Namespace/Dataset keyFamilyURI
	 *
	 * @param inputStream
	 * @param format
	 * @return urn String
	 * @throws WriteHeaderException
	 */
	public String parseUrnFromInput(InputStream inputStream, Formats format) throws ParseXmlException {
		String urn = null;
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
							if (nsId != null && nsId.startsWith("urn:")) {
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
								if (aValue != null && aValue.startsWith("urn:")) {
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

				if (urn != null) {
					//If urn is set we don't need to keep parsing
					break;
				}
			}//End of while loop
			reader.close();
			return urn;
		} catch (XMLStreamException e) {
			throw new ParseXmlException("Error while reading the xml header!", e);
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
	}
}
