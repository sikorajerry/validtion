package com.intrasoft.sdmx.converter.io.data.excel;

import com.intrasoft.engine.writer.StandardWriterTest;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelDataWriterEngine;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelOutputConfig;
import com.intrasoft.sdmx.converter.services.ExcelUtils;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ExcelDataWriterEngineTest extends StandardWriterTest{
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
	public void standardTest() throws Exception{
		InputStream excelTemplate = new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx");  
		ExcelConfiguration excelConfig = ExcelUtils.readExcelConfig(
				new FileInputStream("./test_files/ESTAT_STS/excel_parameter_template.xlsx"),false, new FirstFailureExceptionHandler()).get(0);

		//outputFileName
		File outputFile =  new File ("./target/excelWriterStandardTest.xlsx");
		OutputStream outputStream = new FileOutputStream(outputFile);
        performStandardGroupSeriesObservationsTest(new ExcelDataWriterEngine(outputStream, new ExcelOutputConfig(excelTemplate, excelConfig)));
	}
}
