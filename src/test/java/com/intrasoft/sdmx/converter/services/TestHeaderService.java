package com.intrasoft.sdmx.converter.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import java.util.Date;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static junit.framework.Assert.assertEquals;
import org.sdmxsource.sdmx.util.date.DateUtil;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestHeaderService {
	
	@Autowired
	private HeaderService headerService;

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
	
	@Test
	public void testHeaderPropertiesParser() throws Exception {
	    Properties mockProperties = new Properties(); 
	    mockProperties.setProperty("header.id", "JD014");
	    mockProperties.setProperty("header.test", "true");
	    mockProperties.setProperty("header.truncated", "false");
	    mockProperties.setProperty("header.name", "Trans46302");
	    mockProperties.setProperty("header.prepared", "2001-03-11T09:30:47-05:00");
	    mockProperties.setProperty("header.senderid", "BIS");
	    mockProperties.setProperty("header.sendername", "Bank for International Settlements");
	    mockProperties.setProperty("header.sendercontactname", "G.B. Smith");
	    mockProperties.setProperty("header.sendercontactdepartment", "Statistics");
	    mockProperties.setProperty("header.sendercontactrole", "SMith");
	    mockProperties.setProperty("header.sendercontacttelephone", "210 2222222");
	    mockProperties.setProperty("header.sendercontactfax", "210 00010999");
	    mockProperties.setProperty("header.sendercontactx400", "sendercontactx400");
	    mockProperties.setProperty("header.sendercontactemail", "senderlala@sdmx.com");

	    mockProperties.setProperty("header.receiverid", "ECB");
	    mockProperties.setProperty("header.receivername", "European Central Bank");
	    mockProperties.setProperty("header.receivercontactname", "John John");
	    mockProperties.setProperty("header.receivercontactdepartment", "Statistics");
	    mockProperties.setProperty("header.receivercontactrole", "Jack Over");
	    mockProperties.setProperty("header.receivercontacttelephone", "210 1234567");
	    mockProperties.setProperty("header.receivercontactfax", "210 3810999");
	    mockProperties.setProperty("header.receivercontactx400", "lalala");
	    mockProperties.setProperty("header.receivercontacturi", "www.sdmx.org");
	    mockProperties.setProperty("header.receivercontactemail", "receiverlala@sdmx.com");

	    mockProperties.setProperty("header.datasetagency", "BIS");
	    mockProperties.setProperty("header.datasetid", "BIS_JD_237");
	    mockProperties.setProperty("header.datasetaction", "Append");
	    mockProperties.setProperty("header.extracted", "2001-03-11T09:30:47-05:00");
	    mockProperties.setProperty("header.reportingbegin", "2000-01-01T00:00:00");
	    mockProperties.setProperty("header.reportingend", "2006-01-01T00:00:00");
	    mockProperties.setProperty("header.source", "intrasoft");
	    mockProperties.setProperty("header.lang", "en");


	    org.sdmxsource.sdmx.api.model.header.HeaderBean result = headerService.parseSdmxHeaderProperties(mockProperties);
	    assertEquals("JD014", result.getId());
	    assertEquals(true, result.isTest());
	    assertEquals("Trans46302", result.getName().get(0).getValue());
		assertEquals("BIS", result.getSender().getId());
		assertEquals("Bank for International Settlements", result.getSender().getName().get(0).getValue());
		assertEquals("G.B. Smith", result.getSender().getContacts().get(0).getName().get(0).getValue());
		assertEquals("Statistics", result.getSender().getContacts().get(0).getDepartments().get(0).getValue());
		assertEquals("SMith", result.getSender().getContacts().get(0).getRole().get(0).getValue());
		assertEquals("210 2222222", result.getSender().getContacts().get(0).getTelephone().get(0));
		assertEquals("210 00010999", result.getSender().getContacts().get(0).getFax().get(0));
		assertEquals("sendercontactx400", result.getSender().getContacts().get(0).getX400().get(0));
		assertEquals("senderlala@sdmx.com", result.getSender().getContacts().get(0).getEmail().get(0));

		assertEquals("ECB", result.getReceiver().get(0).getId());
		assertEquals("European Central Bank", result.getReceiver().get(0).getName().get(0).getValue());
		assertEquals("John John", result.getReceiver().get(0).getContacts().get(0).getName().get(0).getValue());
		assertEquals("Statistics", result.getReceiver().get(0).getContacts().get(0).getDepartments().get(0).getValue());
		assertEquals("Jack Over", result.getReceiver().get(0).getContacts().get(0).getRole().get(0).getValue());
		assertEquals("210 1234567", result.getReceiver().get(0).getContacts().get(0).getTelephone().get(0));
		assertEquals("210 3810999", result.getReceiver().get(0).getContacts().get(0).getFax().get(0));
		assertEquals("lalala", result.getReceiver().get(0).getContacts().get(0).getX400().get(0));
		assertEquals("www.sdmx.org", result.getReceiver().get(0).getContacts().get(0).getUri().get(0));
		assertEquals("receiverlala@sdmx.com", result.getReceiver().get(0).getContacts().get(0).getEmail().get(0));

		assertEquals("BIS_JD_237", result.getDatasetId());
		assertEquals("Append", result.getAction().getAction());

		//the following two asserts do not work in command line ( it seems the time zone is set differently)
		//assertEquals("Sat Jan 01 00:00:00 EET 2000", result.getReportingBegin().toString());
		//assertEquals("Sun Jan 01 00:00:00 EET 2006", result.getReportingEnd().toString());

		assertEquals("intrasoft", result.getSource().get(0).getValue());
	}

	@Test
	public void testHeaderForCompact() throws WriteHeaderException {
		File compact = new File("./test_files/UOE_NON_FINANCE/compact.xml");
		File testOutput = new File("./target/headerServiceExtractHeaderFromCompact.properties");

		headerService.saveHeader(compact, testOutput, Formats.COMPACT_SDMX);

		List<String> keyList = Arrays.asList("header.id",
				"header.test",
				"header.truncated",
				"header.name",
				"header.prepared",
				"header.senderid",
				"header.sendername",
				"header.sendercontactname",
				"header.sendercontactdepartment",
				"header.sendercontactrole",
				"header.sendercontacttelephone",
				"header.sendercontactfax",
				//"header.sendercontactx400",
				"header.sendercontactemail",
				"header.receiverid",
				"header.receivername",
				"header.receivercontactname",
				"header.receivercontactdepartment",
				"header.receivercontactrole",
				"header.receivercontacttelephone",
				"header.receivercontactfax",
				"header.receivercontactx400",
				"header.receivercontacturi",
				"header.receivercontactemail",
				"header.datasetagency",
				"header.datasetid",
				"header.datasetaction",
				"header.extracted",
				"header.reportingbegin",
				"header.reportingend",
				"header.source",
				"header.lang"
				);

		Properties propRef = new Properties();
		Properties generated = new Properties();


		try {
			propRef.load(new FileInputStream("./testfiles/testHeaders/headerServiceExtractHeaderFromCompactRefFile.properties"));
			generated.load(new FileInputStream("./target/headerServiceExtractHeaderFromCompact.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String key: keyList){
			String propRefVal = (String) propRef.get(key);
            String generatedVal = (String) generated.get(key);
            //Editted in order to use DateUtil
            if(DateUtil.isSdmxDate(propRefVal) && DateUtil.isSdmxDate(generatedVal)){
                Date datePropRefVal = DateUtil.formatDate(propRefVal);
                propRefVal = DateUtil.formatDateUTC(datePropRefVal);
                Date dateGeneratedVal = DateUtil.formatDate(generatedVal);
                generatedVal = DateUtil.formatDateUTC(dateGeneratedVal);
            }
			if((propRefVal != null && generatedVal == null) || (propRefVal == null && generatedVal != null)){
				throw new RuntimeException("Properties not equals");
			}else if ((propRefVal != null) && !(propRefVal.equals(generatedVal))){
				throw new RuntimeException("Properties not equals");
			}

		}
	}
}
