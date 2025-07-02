package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.Operation;
import com.intrasoft.sdmx.converter.TemplateData;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.exceptions.TemplateException;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static org.junit.Assert.*;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class TestTemplateService {
	
	@Autowired
	private TemplateService templateService;

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
	public void testWriteTemplate()
			throws TemplateException {
		TemplateData templateData = new TemplateData();
		templateData.setOperationType(Operation.CONVERSION);
		templateData.setInputFormat(Formats.GENERIC_SDMX);
		templateData.setOutputFormat(Formats.UTILITY_SDMX);
		Set<String> filepaths = new HashSet<>();
		filepaths.add("./test_files/UOE_NON_FINANCE/generic.xml");
		templateData.setInputFilePaths(filepaths);
		templateData.setStructureFilePath("./test_files/UOE_NON_FINANCE/UOE_NON_FINANCE+ESTAT+0.4.xml");
		templateData.setUseDefaultNamespace(false);
		templateData.setNamespacePrefix("testPrefix");
		templateData.setNamespaceUri("testUri");
		File templateFile = new File("./target/outputTemplateFile.xml");
		templateService.writeTemplate(templateFile, templateData, false);
		FileAssert.assertEquals("the generated template file is different than what is expected",
				                new File("./testfiles/templates/test_output_template.xml"),
				                templateFile);
	}

	@Test
	public void testLoadTemplate() throws Exception{
	    File templateFile = new File("./testfiles/templates/templateWithMapping_UOE_NON_FINANCE+ESTAT+0.4.xml");
		TemplateData result = templateService.loadTemplate(templateFile, new TemplateData());
		assertEquals("C:\\dev\\converter\\converter-api\\testfiles\\singleCsv_input\\1_input_csv.csv", result.getInputFilePaths().iterator().next());
		assertEquals(Formats.CSV, result.getInputFormat());
		assertEquals("output.xml", result.getOutputFilePath());
		assertEquals(Formats.COMPACT_SDMX, result.getOutputFormat());
		assertEquals("C:\\dev\\converter\\converter-api\\testfiles\\dsds\\UOE_NON_FINANCE+ESTAT+0.4.xml", result.getStructureFilePath());
		assertFalse(result.isStructureInRegistry());
	}

	@Test
	public void testLoadCsvInputColumnHeaders() throws Exception{
		File templateFile = new File("./testfiles/templates/templateWithMapping_UOE_NON_FINANCE+ESTAT+0.4.xml");
		TemplateData result = templateService.loadTemplate(templateFile, new TemplateData());
		assertEquals(CsvInputColumnHeader.NO_HEADER, result.getCsvInputColumnHeader());
	}

	@Test
	public void testLoadTemplateWithoutFilePath() throws Exception{
		File templateFile = new File("./testfiles/templates/templateWithoutInputAndOutputFiles.xml");
		TemplateData result = templateService.loadTemplate(templateFile, new TemplateData());
		assertEquals(Formats.CSV, result.getInputFormat());
		assertEquals(Formats.GENERIC_SDMX, result.getOutputFormat());
		assertTrue(result.isStructureInRegistry());
	}

	@Test
	public void testLoadTemplateWithCsvInputMapping() throws TemplateException {
        File templateFile = new File("./testfiles/templates/templateWithMapping_UOE_NON_FINANCE+ESTAT+0.4.xml");
		TemplateData result = templateService.loadTemplate(templateFile, new TemplateData());
		assertNotNull(result);
		Map<String, CsvInColumnMapping> mapping = result.getCsvInputMapping();
		assertNotNull(mapping);

		assertEquals(29, mapping.size());
        assertNotNull(mapping.get("TABLE_IDENTIFIER"));
        assertEquals(Integer.valueOf(1), mapping.get("TABLE_IDENTIFIER").getLevel());
        assertNotNull(mapping.get("TABLE_IDENTIFIER").getColumns());
        assertEquals(1, mapping.get("TABLE_IDENTIFIER").getColumns().size());
        assertEquals(Integer.valueOf(5), mapping.get("TABLE_IDENTIFIER").getColumns().get(0).getIndex());

        assertNotNull(mapping.get("FREQ"));
        assertEquals(Integer.valueOf(1), mapping.get("FREQ").getLevel());
        assertNotNull(mapping.get("FREQ").getColumns());
        assertEquals(1, mapping.get("FREQ").getColumns().size());
        assertEquals(Integer.valueOf(4), mapping.get("FREQ").getColumns().get(0).getIndex());

        assertNotNull(mapping.get("REF_AREA"));
        assertEquals(Integer.valueOf(1), mapping.get("REF_AREA").getLevel());
        assertNotNull(mapping.get("REF_AREA").getColumns());
        assertEquals(1, mapping.get("REF_AREA").getColumns().size());
        assertEquals(Integer.valueOf(3), mapping.get("REF_AREA").getColumns().get(0).getIndex());

        assertNotNull(mapping.get("REF_SECTOR"));
        assertEquals(Integer.valueOf(1), mapping.get("REF_SECTOR").getLevel());
        assertNotNull(mapping.get("REF_SECTOR").getColumns());
        assertEquals(1, mapping.get("REF_SECTOR").getColumns().size());
        assertEquals(Integer.valueOf(2), mapping.get("REF_SECTOR").getColumns().get(0).getIndex());
	}

    @Test
    public void testLoadTemplateWithTranscoding() throws TemplateException{
        File templateFile = new File("./testfiles/templates/templateWithMappingAndTranscoding_UOE_NON_FINANCE+ESTAT+0.4.xml");
        TemplateData result = templateService.loadTemplate(templateFile, new TemplateData());
        assertNotNull(result);
		LinkedHashMap<String, LinkedHashMap<String, String>> transcoding = result.getCsvInputStructureSet();
        assertNotNull(transcoding);
        assertEquals(3, transcoding.size());
        assertNotNull(transcoding.get("TABLE_IDENTIFIER"));
        assertEquals(2, transcoding.get("TABLE_IDENTIFIER").size());
        assertEquals("test_grad_1", transcoding.get("TABLE_IDENTIFIER").get("GRAD1"));
        assertEquals("test_grad_2", transcoding.get("TABLE_IDENTIFIER").get("GRAD2"));

        assertNotNull(transcoding.get("FREQ"));
        assertEquals(3, transcoding.get("FREQ").size());
        assertEquals("test_a", transcoding.get("FREQ").get("A"));
        assertEquals("test_q", transcoding.get("FREQ").get("Q"));
        assertEquals("test_m", transcoding.get("FREQ").get("M"));

        /*<StructureSet>
        <CodelistMap id="TABLE_IDENTIFIER">
          <CodeMap>
            <MapCodeRef>test_grad_1</MapCodeRef>
            <MapTargetCodeRef>GRAD1</MapTargetCodeRef>
          </CodeMap>
          <CodeMap>
            <MapCodeRef>test_grad_2</MapCodeRef>
            <MapTargetCodeRef>GRAD2</MapTargetCodeRef>
          </CodeMap>
        </CodelistMap>
        <CodelistMap id="FREQ">
          <CodeMap>
            <MapCodeRef>test_a</MapCodeRef>
            <MapTargetCodeRef>A</MapTargetCodeRef>
          </CodeMap>
          <CodeMap>
            <MapCodeRef>test_q</MapCodeRef>
            <MapTargetCodeRef>Q</MapTargetCodeRef>
          </CodeMap>
          <CodeMap>
            <MapCodeRef>test_m</MapCodeRef>
            <MapTargetCodeRef>M</MapTargetCodeRef>
          </CodeMap>
        </CodelistMap>
        <CodelistMap id="REF_AREA">
          <CodeMap>
            <MapCodeRef>test_1d</MapCodeRef>
            <MapTargetCodeRef>1D</MapTargetCodeRef>
          </CodeMap>
          <CodeMap>
            <MapCodeRef>test_4a</MapCodeRef>
            <MapTargetCodeRef>4A</MapTargetCodeRef>
          </CodeMap>
          <CodeMap>
            <MapCodeRef>test_4aa</MapCodeRef>
            <MapTargetCodeRef>4AA</MapTargetCodeRef>
          </CodeMap>
        </CodelistMap>
      </StructureSet> */
    }

}
