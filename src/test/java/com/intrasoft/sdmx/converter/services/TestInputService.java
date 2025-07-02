package com.intrasoft.sdmx.converter.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import com.intrasoft.commons.ui.services.ConfigService;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.InputService;
import com.intrasoft.sdmx.converter.services.exceptions.ParseXmlException;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestInputService {
	
	@Autowired
	private InputService inputService;

	@Autowired
	private ConfigService configService;

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
	public void detectFormatFromFileExtensionForGenericMessage() {
		File genericFile = new File("./test_files/UOE_NON_FINANCE/generic.xml"); 
		Assert.assertNull(inputService.detectFormatFromFileExtension(genericFile)); 
	}
	
	@Test
	public void detectFormatFromFileExtensionForExcel() {
		File excelFile = new File("./test_files/UOE_NON_FINANCE/excel.xlsx"); 
		Assert.assertEquals(Formats.EXCEL, inputService.detectFormatFromFileExtension(excelFile)); 
	}
	
	@Test
	public void detectFormatFromFileExtensionForNonExistentFile() {
		File excelFile = new File("./test_files/UOE_NON_FINANCE/thisFileDoesNotExist.txt"); 
		Assert.assertNull(inputService.detectFormatFromFileExtension(excelFile)); 
	}
	
	@Test
	public void detectFormatFromFileNameForGenericMessage() {
		String genericFile = "generic.xml"; 
		Assert.assertNull(inputService.detectFormatFromFileName(genericFile)); 
	}
	
	@Test
	public void detectFormatFromFileNameForExcel() {
		String excelFile ="excel.xlsx"; 
		Assert.assertEquals(Formats.EXCEL, inputService.detectFormatFromFileName(excelFile)); 
	}
	
	@Test
	public void detectFormatFromFileContentForGeneric() throws Exception{
		File genericFile = new File("./test_files/UOE_NON_FINANCE/generic.xml"); 
		Assert.assertEquals(Formats.GENERIC_SDMX, inputService.detectFormatFromFileContent(genericFile));
	}
	
	@Test
	public void detectFormatFromInputStreamContentForGeneric() throws Exception{
		try(InputStream inputStream = new FileInputStream("./test_files/UOE_NON_FINANCE/generic.xml")) {
		Assert.assertEquals(Formats.GENERIC_SDMX,inputService.detectFormatFromFileContent(inputStream));
		}
	}
			
	@Test
	public void detectFormatFromFileContentForNonSdmxFile() throws Exception{
		File testFile = new File("./test_files/UOE_NON_FINANCE/excel.xlsx"); 
		String message = null;
		try {
			inputService.detectFormatFromFileContent(testFile);
		} catch (Throwable e) {
			message = e.getMessage();
			
		}
		Assert.assertEquals(message, null);
	}
	
	@Test(expected = FileNotFoundException.class)
	public void detectFormatFromFileContentsForNonExistentFile() throws Exception{
		File testFile = new File("./test_files/UOE_NON_FINANCE/thisFileDoesNotExist.txt"); 
		inputService.detectFormatFromFileContent(testFile);
	}	
}
