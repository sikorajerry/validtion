package com.intrasoft.sdmx.converter.services;

import static com.intrasoft.sdmx.converter.services.ExcelUtils.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.sdmxsource.util.excel.ExcelDimAttrConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestExcelUtils {

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
    public void testReadParametersInsideExcel() throws Exception{
        File excelWithParamsInside = new File("./test_files/test_excel/NASU_16BP_A_V1.8_2011.xlsx");
        List<ExcelConfiguration> result = ExcelUtils.readExcelConfig(excelWithParamsInside, new FirstFailureExceptionHandler());

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() == 1);
        ExcelConfiguration excelParameter = result.get(0);

        Assert.assertNotNull(excelParameter);
        Assert.assertEquals("K28", excelParameter.getDataStart());
    }


    @Test
    public void testReadExternalParametersFile() throws Exception{
        File externalParamsFile = new File("./test_files/excel_testing/externalParamsFile.txt"); 
        try(InputStream targetStream = new FileInputStream(externalParamsFile)){
        ExcelConfiguration excelParameter = readExcelConfigFromText(targetStream, new FirstFailureExceptionHandler());
        
        Assert.assertNotNull(excelParameter);
        Assert.assertEquals("NaN", excelParameter.getDefaultValue());
        Assert.assertEquals(17, excelParameter.getNumberOfColumns()); 
        Assert.assertEquals("B30", excelParameter.getDataStart());
        
        List<ExcelDimAttrConfig> elements = excelParameter.getParameterElements();
        Assert.assertNotNull(elements);
        System.out.println("elements: "+elements +", " + elements.size());
        Assert.assertTrue(elements.size() == 16);
        
        //check FREQ parameter element
        ExcelDimAttrConfig freq = elements.get(4);
        Assert.assertEquals("FREQ", freq.getId()); 
        Assert.assertEquals("I16", freq.getPosition());
        }
    }
    
    @Test
    public void testReadSheetMappingFile() throws Exception{
        File sheetMappingFile = new File("./test_files/excel_testing/sheetMapping.txt");
        Map<String, ArrayList<String>> sheetMapping = getParametersMap(sheetMappingFile);
        
        Assert.assertNotNull(sheetMapping);
        Assert.assertEquals("Parameter Sheet 1", sheetMapping.get("Sheet1").get(0));
        Assert.assertEquals("Parameter Sheet 2", sheetMapping.get("Sheet2").get(0));
        Assert.assertEquals("Parameter Sheet 3", sheetMapping.get("Sheet3").get(0));
        Assert.assertEquals("ParamSheet", sheetMapping.get("Sheet4").get(0));
    }

}
