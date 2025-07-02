package com.intrasoft.sdmx.converter.io.data.csv;

import com.intrasoft.engine.writer.StandardWriterTest;
import com.intrasoft.sdmx.converter.config.FlrOutputConfig;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.constants.BASE_DATA_FORMAT;
import org.sdmxsource.sdmx.api.constants.DATASET_ACTION;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetHeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.DatasetStructureReferenceBeanImpl;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class FlrDataWriterEngineTest extends StandardWriterTest {

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}

	// Test that throws exception if config does not exist.
	@Test
	public void standardErrorTest() {
		try {
			File resultFile = new File("./target/sdmxFlrUnitTest0.flr");
			FlrDataWriterEngine classUnderTest = new FlrDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE,
					BASE_DATA_FORMAT.CSV,
					new FileOutputStream(resultFile));
			performStandardGroupSeriesObservationsTest(classUnderTest);
		} catch (Exception ex) {
			Assert.assertTrue(ex.getMessage().contains("No configurations found for the FLR writer, a mapping must be present to proceed to writing."));
		}
	}

	@Test
	public void standardTest() throws FileNotFoundException {
		File resultFile = new File("./target/sdmxFlrUnitTest.flr");
		FlrOutputConfig config = new FlrOutputConfig();
		FlrDataWriterEngine classUnderTest = new FlrDataWriterEngine(SDMX_SCHEMA.VERSION_TWO_POINT_ONE,
				BASE_DATA_FORMAT.CSV,
				new FileOutputStream(resultFile),
				config);
		setOutputConfig(config, classUnderTest);
		performStandardGroupSeriesObservationsTest(classUnderTest);
	}

	@Test
	public void standardTestManyObs() throws IOException {
		File resultFile = new File("./target/sdmxFlrUnitTest2.flr");
		SdmxBeans sdmxBeans = readStructuresFromFile(new FileInputStream("./testfiles/dsds/STS+ESTAT+2.0.xml"));
		DataStructureBean dataStructure = sdmxBeans.getDataStructures().iterator().next();
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		DatasetHeaderBean datasetHeaderBean = new DatasetHeaderBeanImpl("unit test",
				DATASET_ACTION.INFORMATION,
				new DatasetStructureReferenceBeanImpl(dataStructure.asReference()));

		try (final FileOutputStream outputStream = new FileOutputStream(resultFile)) {
			FlrOutputConfig config = new FlrOutputConfig();
			FlrDataWriterEngine classUnderTest = new FlrDataWriterEngine(   SDMX_SCHEMA.VERSION_TWO_POINT_ONE,
					BASE_DATA_FORMAT.CSV,
					outputStream, config);
			setOutputConfig(config, classUnderTest);
			classUnderTest.startDataset(dataflow, dataStructure, datasetHeaderBean);
			classUnderTest.startSeries();
			classUnderTest.writeSeriesKeyValue("REF_AREA", "IT");
			classUnderTest.writeSeriesKeyValue("ADJUSTMENT", "C");
			classUnderTest.writeSeriesKeyValue("STS_INDICATOR", "TOVD");
			classUnderTest.writeSeriesKeyValue("STS_ACTIVITY", "NS0040");
			classUnderTest.writeSeriesKeyValue("STS_INSTITUTION", "1");
			classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR", "2000");
			classUnderTest.writeSeriesKeyValue("FREQ", "M");
			classUnderTest.writeAttributeValue("TIME_FORMAT", "P1M");

			for(int i=1; i<= 12; i++) {
				String period = String.format("2005-%1$02d", i);
				classUnderTest.writeObservation("TIME_PERIOD", period, new Float(i).toString());
				classUnderTest.writeAttributeValue("OBS_CONF", "F");
				classUnderTest.writeAttributeValue("OBS_STATUS", "E");
			}
			classUnderTest.startSeries();
			classUnderTest.writeSeriesKeyValue("REF_AREA", "EL");
			classUnderTest.writeSeriesKeyValue("ADJUSTMENT", "C");
			classUnderTest.writeSeriesKeyValue("STS_INDICATOR", "TOVD");
			classUnderTest.writeSeriesKeyValue("STS_ACTIVITY", "NS0040");
			classUnderTest.writeSeriesKeyValue("STS_INSTITUTION", "1");
			classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR", "2000");
			classUnderTest.writeSeriesKeyValue("FREQ", "M");
			classUnderTest.writeAttributeValue("TIME_FORMAT", "P1M");

			classUnderTest.writeObservation("TIME_PERIOD", "2005-01", "1.1");
			classUnderTest.writeAttributeValue("OBS_CONF", "F");
			classUnderTest.writeAttributeValue("OBS_STATUS", "E");

			classUnderTest.writeObservation("TIME_PERIOD", "2005-02", "2.2");
			classUnderTest.writeAttributeValue("OBS_CONF", "F");
			classUnderTest.writeAttributeValue("OBS_STATUS", "E");
			classUnderTest.startSeries();
			classUnderTest.writeSeriesKeyValue("REF_AREA", "DE");
			classUnderTest.writeSeriesKeyValue("ADJUSTMENT", "C");
			classUnderTest.writeSeriesKeyValue("STS_INDICATOR", "TOVD");
			classUnderTest.writeSeriesKeyValue("STS_ACTIVITY", "NS0040");
			classUnderTest.writeSeriesKeyValue("STS_INSTITUTION", "1");
			classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR", "2000");
			classUnderTest.writeSeriesKeyValue("FREQ", "M");
			classUnderTest.writeAttributeValue("TIME_FORMAT", "P1M");

			classUnderTest.writeObservation("TIME_PERIOD", "2005-01", "1.1");
			classUnderTest.writeAttributeValue("OBS_CONF", "F");
			classUnderTest.writeAttributeValue("OBS_STATUS", "E");

			classUnderTest.writeObservation("TIME_PERIOD", "2005-02", "2.2");
			classUnderTest.writeAttributeValue("OBS_CONF", "F");
			classUnderTest.writeAttributeValue("OBS_STATUS", "E");
			classUnderTest.close();
			outputStream.flush();
		}

		try(BufferedReader reader = new BufferedReader(new FileReader(resultFile))) {
			String line = reader.readLine();
			Assert.assertNotNull(line);
			Assert.assertTrue(line.startsWith("MITCTOVDNS0040120002005-011.0    FEP1M"));
			int count = 0;
			while((line = reader.readLine()) != null) {
				count++;
			}
			Assert.assertEquals(15, count);
		}
	}

	@Test
	public void standardTestManyObsNoAttr() throws IOException {
		File resultFile = new File("./target/sdmxFlrUnitTest3.flr");
		SdmxBeans sdmxBeans = readStructuresFromFile(new FileInputStream("./testfiles/dsds/STS+ESTAT+2.0.xml"));
		DataStructureBean dataStructure = sdmxBeans.getDataStructures().iterator().next();
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();
		DatasetHeaderBean datasetHeaderBean = new DatasetHeaderBeanImpl("unit test",
				DATASET_ACTION.INFORMATION,
				new DatasetStructureReferenceBeanImpl(dataStructure.asReference()));

		try (final FileOutputStream outputStream = new FileOutputStream(resultFile)) {
			FlrOutputConfig config = new FlrOutputConfig();
			FlrDataWriterEngine classUnderTest = new FlrDataWriterEngine(
					SDMX_SCHEMA.VERSION_TWO_POINT_ONE,
					BASE_DATA_FORMAT.CSV,
					outputStream, config);
			setOutputConfig(config, classUnderTest);
			classUnderTest.startDataset(dataflow, dataStructure, datasetHeaderBean);
			classUnderTest.startSeries();
			classUnderTest.writeSeriesKeyValue("REF_AREA", "IT");
			classUnderTest.writeSeriesKeyValue("ADJUSTMENT", "C");
			classUnderTest.writeSeriesKeyValue("STS_INDICATOR", "TOVD");
			classUnderTest.writeSeriesKeyValue("STS_ACTIVITY", "NS0040");
			classUnderTest.writeSeriesKeyValue("STS_INSTITUTION", "1");
			classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR", "2000");
			classUnderTest.writeSeriesKeyValue("FREQ", "M");

			for(int i=1; i<= 12; i++) {
				String period = String.format("2005-%1$02d", i);
				classUnderTest.writeObservation("TIME_PERIOD", period, new Float(i).toString());
			}
			classUnderTest.startSeries();
			classUnderTest.writeSeriesKeyValue("REF_AREA", "EL");
			classUnderTest.writeSeriesKeyValue("ADJUSTMENT", "C");
			classUnderTest.writeSeriesKeyValue("STS_INDICATOR", "TOVD");
			classUnderTest.writeSeriesKeyValue("STS_ACTIVITY", "NS0040");
			classUnderTest.writeSeriesKeyValue("STS_INSTITUTION", "1");
			classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR", "2000");
			classUnderTest.writeSeriesKeyValue("FREQ", "M");
			classUnderTest.writeObservation("TIME_PERIOD", "2005-01", "1.1");
			classUnderTest.writeObservation("TIME_PERIOD", "2005-02", "2.2");
			classUnderTest.startSeries();
			classUnderTest.writeSeriesKeyValue("REF_AREA", "DE");
			classUnderTest.writeSeriesKeyValue("ADJUSTMENT", "C");
			classUnderTest.writeSeriesKeyValue("STS_INDICATOR", "TOVD");
			classUnderTest.writeSeriesKeyValue("STS_ACTIVITY", "NS0040");
			classUnderTest.writeSeriesKeyValue("STS_INSTITUTION", "1");
			classUnderTest.writeSeriesKeyValue("STS_BASE_YEAR", "2000");
			classUnderTest.writeSeriesKeyValue("FREQ", "M");
			classUnderTest.writeObservation("TIME_PERIOD", "2005-01", "1.1");
			classUnderTest.writeObservation("TIME_PERIOD", "2005-02", "2.2");
			classUnderTest.close();
			outputStream.flush();
		}

		try(BufferedReader reader = new BufferedReader(new FileReader(resultFile))) {
			String line = reader.readLine();
			Assert.assertNotNull(line);
			Assert.assertTrue(line.startsWith("MITCTOVDNS0040120002005-011.0"));
			int count = 0;
			while((line = reader.readLine()) != null) {
				count++;
			}
			Assert.assertEquals(15, count);
		}
	}

	private void setOutputConfig(FlrOutputConfig config, FlrDataWriterEngine classUnderTest){
		config.setPadding(" ");
		Map<String, Integer> mapping = new HashMap<String, Integer>();
		mapping.put("REF_AREA", 2);
		mapping.put("ADJUSTMENT", 1);
		mapping.put("STS_INDICATOR", 4);
		mapping.put("STS_ACTIVITY", 6);
		mapping.put("STS_INSTITUTION", 1);
		mapping.put("STS_BASE_YEAR", 4);
		mapping.put("UNIT", 2);
		mapping.put("UNIT_MULT", 1);
		mapping.put("TITLE_COMPL", 34);
		mapping.put("DECIMALS", 1);
		mapping.put("FREQ", 1);
		mapping.put("TIME_FORMAT", 3);
		mapping.put("TIME_PERIOD", 7);
		mapping.put("OBS_VALUE", 7);
		mapping.put("OBS_CONF", 1);
		mapping.put("OBS_STATUS", 1);
		config.setLengthsCounting(mapping);
		classUnderTest.setConfigurations(config);
	}
}
