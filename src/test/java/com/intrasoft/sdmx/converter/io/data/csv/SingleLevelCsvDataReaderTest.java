/**
 * Copyright (c) 2015 European Commission.
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl5
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.io.data.csv;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.engine.reader.CsvTestObject;
import org.estat.sdmxsource.util.csv.CsvColumn;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class SingleLevelCsvDataReaderTest {

	private final String resourceRoot= "src/test/resources/csv/";
	private final String stsStructure = resourceRoot + "UOE_NON_FINANCE+ESTAT+0.4_for_csv_reader.xml";

	@Qualifier("readableDataLocationFactory")
	@Autowired
	private ReadableDataLocationFactory dataLocationFactory;
	@Autowired
	private StructureParsingManager parsingManager;

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
	public void testCsvWithMappingOnDifferentColumns() throws FileNotFoundException {				
//		ReadableDataLocation dsdLocation = dataLocationFactory.getReadableDataLocation(stsStructure);
//		StructureWorkspace parseStructures = parsingManager.parseStructures(dsdLocation);
//		SdmxBeans structureBeans = parseStructures.getSdmxBeans(false);
//		SdmxBeanRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBeans);
//
//		CsvInputConfig csvInputConfig = new CsvInputConfig();
//		csvInputConfig.setDelimiter(";");
//		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
//
//		Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
//        csvMapping.put("TABLE_IDENTIFIER", new CsvInColumnMapping(new Integer[]{0}, false, 1, ""));
//        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
//        csvMapping.put("REF_AREA", new CsvInColumnMapping(new Integer[]{2}, false, 1, ""));
//        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{17}, false, 1, ""));
//        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{18}, false, 1, ""));
//        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{19}, false, 1, ""));
//        csvMapping.put("TIME_PER_COLLECT", new CsvInColumnMapping(new Integer[]{21}, false, 1, ""));
//        csvMapping.put("UNIT_MULT", new CsvInColumnMapping(new Integer[]{28}, false, 1, ""));
//        csvInputConfig.setMapping(csvMapping);
//
//        CsvTestObject csvTestObject = parseCsv(retrievalManager, csvInputConfig);
//
//		Assert.assertEquals(csvTestObject.getKeyables().size(), 9);
//		Assert.assertEquals(csvTestObject.getKeyables().get(0), new StringBuffer("TABLE_IDENTIFIER: PERS1;FREQ: A;REF_AREA: MT;UNIT_MULT: 0;").toString());
//		Assert.assertEquals(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).get(0), new StringBuffer("   2013: NaN -    OBS_STATUS: null").toString());
	}
	
	@Test
	public void testCsvWithMappingWithMultipleColumns() throws FileNotFoundException {
//		ReadableDataLocation dsdLocation = dataLocationFactory.getReadableDataLocation(stsStructure);
//		StructureWorkspace parseStructures = parsingManager.parseStructures(dsdLocation);
//		SdmxBeans structureBeans = parseStructures.getSdmxBeans(false);
//		SdmxBeanRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBeans);
//
//		CsvInputConfig csvInputConfig = new CsvInputConfig();
//		csvInputConfig.setDelimiter(";");
//		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
//
//		Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
//        csvMapping.put("TABLE_IDENTIFIER", new CsvInColumnMapping(new Integer[]{0,1}, false, 1, ""));
//        csvMapping.put("REF_AREA", new CsvInColumnMapping(new Integer[]{2}, false, 1, ""));
//        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{17}, false, 1, ""));
//        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{18}, false, 1, ""));
//        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{19}, false, 1, ""));
//        csvMapping.put("TIME_PER_COLLECT", new CsvInColumnMapping(new Integer[]{21}, false, 1, ""));
//        csvMapping.put("UNIT_MULT", new CsvInColumnMapping(new Integer[]{28}, false, 1, ""));
//        csvInputConfig.setMapping(csvMapping);
//
//		CsvTestObject csvTestObject = parseCsv(retrievalManager, csvInputConfig);
//
//		Assert.assertEquals(csvTestObject.getKeyables().size(), 9);
//		Assert.assertEquals(csvTestObject.getKeyables().get(0), new StringBuffer("TABLE_IDENTIFIER: PERS1A;REF_AREA: MT;UNIT_MULT: 0;").toString());
//		Assert.assertEquals(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).get(0), new StringBuffer("   2013: NaN -    OBS_STATUS: null").toString());
	}
	
	@Test
	public void testCsvWithMappingWithFixedValue() throws FileNotFoundException {
//		ReadableDataLocation dsdLocation = dataLocationFactory.getReadableDataLocation(stsStructure);
//		StructureWorkspace parseStructures = parsingManager.parseStructures(dsdLocation);
//		SdmxBeans structureBeans = parseStructures.getSdmxBeans(false);
//		SdmxBeanRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureBeans);
//
//		CsvInputConfig csvInputConfig = new CsvInputConfig();
//		csvInputConfig.setDelimiter(";");
//		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
//
//		Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
//		CsvInColumnMapping columnMapping = new CsvInColumnMapping();
//		List<CsvColumn> columns = new ArrayList<>();
//		columnMapping.setColumns(columns);
//		columnMapping.setFixed(true);
//		columnMapping.setFixedValue("TEST_VALUE");
//		columnMapping.setLevel(1);
//        csvMapping.put("TABLE_IDENTIFIER", columnMapping);
//        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
//        csvMapping.put("REF_AREA", new CsvInColumnMapping(new Integer[]{2}, false, 1, ""));
//        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{17}, false, 1, ""));
//        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{18}, false, 1, ""));
//        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{19}, false, 1, ""));
//        csvMapping.put("TIME_PER_COLLECT", new CsvInColumnMapping(new Integer[]{21}, false, 1, ""));
//        csvMapping.put("UNIT_MULT", new CsvInColumnMapping(new Integer[]{28}, false, 1, ""));
//        csvInputConfig.setMapping(csvMapping);
//
//		CsvTestObject csvTestObject = parseCsv(retrievalManager, csvInputConfig);
//
//		Assert.assertEquals(csvTestObject.getKeyables().size(), 1);
//		Assert.assertEquals(csvTestObject.getKeyables().get(0), new StringBuffer("TABLE_IDENTIFIER: TEST_VALUE;FREQ: A;REF_AREA: MT;UNIT_MULT: 0;").toString());
//		Assert.assertEquals(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).get(0), new StringBuffer("   2013: NaN -    OBS_STATUS: null").toString());

	}
	
	private CsvTestObject parseCsv(SdmxBeanRetrievalManager retrievalManager, CsvInputConfig csvInputConfig) throws FileNotFoundException {		
		CsvTestObject csvTestObject = new CsvTestObject();
		ReadableDataLocation data = dataLocationFactory.getReadableDataLocation("src/test/resources/csv/csv_test_reader.csv");
		DataReaderEngine dataReaderEngine= new SingleLevelCsvDataReaderEngine(data, null, null, retrievalManager, csvInputConfig, new FirstFailureExceptionHandler());
		dataReaderEngine.reset();
		boolean firstCall = dataReaderEngine.moveNextDataset();
		Assert.assertEquals(true, firstCall);				
		while (dataReaderEngine.moveNextKeyable() ) {
			StringBuffer keyable = new StringBuffer("");
			Keyable key = dataReaderEngine.getCurrentKey();			
			// get key concept/code pairs (e.g. FREQ/M)
			for (KeyValue keyValue : key.getKey()) {						
				keyable.append(keyValue.getConcept() + ": " + keyValue.getCode() + ";");
				System.out.println(" " + keyValue.getConcept() + ": " + keyValue.getCode());
			}			
			// get the attributes concept/value pairs (e.g. TITLE=Matrix)
			for (KeyValue keyValue : key.getAttributes()) {
				keyable.append(keyValue.getConcept() + ": " + keyValue.getCode() + ";");
				System.out.println(" " + keyValue.getConcept() + ": " + keyValue.getCode());
			}			
			csvTestObject.addKeyable(keyable.toString());
			// get the observations for this key
			while (dataReaderEngine.moveNextObservation()) {
				Observation observation = dataReaderEngine.getCurrentObservation();
				StringBuffer obsString = new StringBuffer(""); 				
			    obsString.append("   " + observation.getObsTime() + ": " + observation.getObservationValue() + " - ");					
				System.out.println("   " + observation.getObsTime() + ": " + observation.getObservationValue());
				
				// get Obs attributes (e.g. UNIT=KE)
				for (KeyValue keyValue : observation.getAttributes()) {
					obsString.append("   " + keyValue.getConcept() + ": " + keyValue.getCode());
					System.out.println("   " + keyValue.getConcept() + ": " + keyValue.getCode());
				}
			    csvTestObject.addObservation(keyable.toString(), obsString.toString());	
			}
		}		
		return csvTestObject;
	}
	
}
