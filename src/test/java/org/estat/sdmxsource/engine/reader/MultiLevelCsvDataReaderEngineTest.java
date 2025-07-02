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
package org.estat.sdmxsource.engine.reader;

import com.intrasoft.sdmx.converter.io.data.csv.MultiLevelCsvDataReaderEngine;
import com.intrasoft.sdmx.converter.services.StructureService;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.util.CsvReadableDataLocation;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.model.error.FirstFailureExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.Assert;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Test class for MultiLevelCsvDataReaderEngine. 
 * The input data is read from ./test_files/MULTILEVEL_CSV
 * The result is compared to a programmatically built result containing the expected strings. 
 * 
 * The documentation used for calling the api: //http://www.sdmxsource.org/wp-content/uploads/2013/09/ProgrammersGuide.pdf
 * 
 * @author Mihaela Munteanu
 * 
 * @since 22.05.2017
 *
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class MultiLevelCsvDataReaderEngineTest {
	
	@Qualifier("readableDataLocationFactory")
	@Autowired
	private ReadableDataLocationFactory dataLocationFactory;

    @Autowired
    private StructureService structureService;
   
    @Rule
    public ExpectedException thrown = ExpectedException.none();

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
    
    
    /** 
     * Tests a regular multilevel csv input file
     * @throws Exception
     */
    @Test
    public void testSimpleMultipleCsv() throws Exception {

    	//csv input file
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/simpleMultiLevel.csv");

        //dsd
        SdmxBeans structure = structureService.readStructuresFromFile("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

        
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setDelimiter(";");

        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
        csvMapping.put("JD_TYPE", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 2, ""));
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));
        csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{3}, false, 2, ""));
        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{1}, false, 3, ""));
        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        csvMapping.put("OBS_PRE_BREAK", new CsvInColumnMapping(new Integer[]{5}, false, 3, ""));
        
        csvMapping.put("AVAILABILITY", new CsvInColumnMapping(new Integer[]{6}, false, 3, ""));
        csvMapping.put("DECIMALS", new CsvInColumnMapping(new Integer[]{7}, false, 3, ""));
        csvMapping.put("BIS_UNIT", new CsvInColumnMapping(new Integer[]{8}, false, 3, ""));
        csvMapping.put("UNIT_MULT", new CsvInColumnMapping(new Integer[]{9}, false, 3, ""));
        csvMapping.put("TIME_FORMAT", new CsvInColumnMapping(new Integer[]{10}, false, 3, ""));
        csvMapping.put("COLLECTION", new CsvInColumnMapping(new Integer[]{}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);
        
        CsvTestObject csvTestObject = parseCsv(inputStream, kf, csvInputConfig);
        CsvTestObject expectedObject = buildExpectedCsvTestObject();
        
        Assert.isTrue(csvTestObject.getDataAttributes().size()==0, "Expected empty for dataset attributes");
        Assert.isTrue(csvTestObject.getKeyables().size() == expectedObject.getKeyables().size(), 
        		"Should be " + expectedObject.getKeyables().size() + " as number of keyable instead" + csvTestObject.getKeyables().size());
        System.out.println("EXPECTED0:" + csvTestObject.getKeyables().get(0));
        System.out.println("ACTUAL0:"   + expectedObject.getKeyables().get(0));
        System.out.println("EXPECTED0:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(0)));
        System.out.println("ACTUAL0:  " + expectedObject.getObservation(expectedObject.getKeyables().get(0)));
        System.out.println("EXPECTED1:" + csvTestObject.getKeyables().get(1));
        System.out.println("EXPECTED1:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(1)));
        System.out.println("EXPECTED2:" + csvTestObject.getKeyables().get(2));
        System.out.println("EXPECTED2:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(2)));
        
        Assert.isTrue(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).size() ==
        		expectedObject.getObservation(expectedObject.getKeyables().get(0)).size());
        Assert.isTrue(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).get(0).trim().equals(
        		expectedObject.getObservation(expectedObject.getKeyables().get(0)).get(0).trim()));
    }
    
    
    /** 
     * Test with a mapping and dsd containing TIME as time dimension when sdmxsource expects TIME_PERIOD
     * @throws Exception
     */
    @Test
    public void testTimeMultipleCsv() throws Exception{

    	//csv input file
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/simpleMultiLevel.csv");

        //dsd
        SdmxBeans structure = structureService.readStructuresFromFile("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setDelimiter(";");

        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
        csvMapping.put("JD_TYPE", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 2, ""));
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));
        csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{3}, false, 2, ""));
        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{1}, false, 3, ""));
        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        csvMapping.put("OBS_PRE_BREAK", new CsvInColumnMapping(new Integer[]{5}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);
        
        CsvTestObject csvTestObject = parseCsv(inputStream, kf, csvInputConfig);
        CsvTestObject expectedObject = buildExpectedCsvTestObject();
        
        Assert.isTrue(csvTestObject.getDataAttributes().size()==0, "Expected empty for dataset attributes");
        Assert.isTrue(csvTestObject.getKeyables().size() == expectedObject.getKeyables().size(), 
        		"Should be " + expectedObject.getKeyables().size() + " as number of keyables instead " + csvTestObject.getKeyables().size());
        System.out.println("EXPECTED0:" + csvTestObject.getKeyables().get(0));
        System.out.println("EXPECTED0:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(0)));
        System.out.println("ACTUAL0:  " + expectedObject.getObservation(expectedObject.getKeyables().get(0)));
        System.out.println("EXPECTED1:" + csvTestObject.getKeyables().get(1));
        System.out.println("EXPECTED1:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(1)));
        System.out.println("EXPECTED2:" + csvTestObject.getKeyables().get(2));
        System.out.println("EXPECTED2:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(2)));
        
        Assert.isTrue(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).size() ==
        		expectedObject.getObservation(expectedObject.getKeyables().get(0)).size());
        Assert.isTrue(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).get(0).trim().equals(
        		expectedObject.getObservation(expectedObject.getKeyables().get(0)).get(0).trim()));
    }
    
    /** 
     * Test with a mapping and dsd containing TIME as time dimension when sdmxsource expects TIME_PERIOD
     * @throws Exception
     */
    @Test
    public void testConceptMissingInMapping() throws Exception{
    	//csv input file
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/simpleMultiLevel.csv");

        //dsd
        SdmxBeans structure = structureService.readStructuresFromFile("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setDelimiter(";");

        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
        csvMapping.put("JD_TYPE", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 2, ""));
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));
        //csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{3}, false, 2, ""));    //commented out for testing purposes
        csvMapping.put("TIME", new CsvInColumnMapping(new Integer[]{1}, false, 3, ""));
        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        //csvMapping.put("OBS_PRE_BREAK", new CsvInColumnMapping(new Integer[]{6}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);
        
        CsvTestObject csvTestObject = parseCsv(inputStream, kf, csvInputConfig);
        CsvTestObject expectedObject = buildExpectedCsvTestObject();
        
        Assert.isTrue(csvTestObject.getDataAttributes().size()==0, "Expected empty for dataset attributes");
        Assert.isTrue(csvTestObject.getKeyables().size() == expectedObject.getKeyables().size(),
        		"Should be " + expectedObject.getKeyables().size() + " as number of keyable instead " + csvTestObject.getKeyables().size());
        System.out.println("EXPECTED0:" + csvTestObject.getKeyables().get(0));
        System.out.println("EXPECTED0:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(0)));
        System.out.println("ACTUAL0:  " + expectedObject.getObservation(expectedObject.getKeyables().get(0)));
        System.out.println("EXPECTED1:" + csvTestObject.getKeyables().get(1));
        System.out.println("EXPECTED1:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(1)));
        System.out.println("EXPECTED2:" + csvTestObject.getKeyables().get(2));
        System.out.println("EXPECTED2:" + csvTestObject.getObservation(csvTestObject.getKeyables().get(2)));
        
        Assert.isTrue(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).size() ==
        		expectedObject.getObservation(expectedObject.getKeyables().get(0)).size());
        Assert.isTrue(csvTestObject.getObservation(csvTestObject.getKeyables().get(0)).get(0).trim().equals(
        		expectedObject.getObservation(expectedObject.getKeyables().get(0)).get(0).trim()));

    }
    
    /** 
     * Test with a mapping and dsd containing TIME as time dimension when sdmxsource expects TIME_PERIOD
     * @throws Exception
     */
    @Test
    public void testLessLevelThanNumberOfLevels() throws Exception{

    	//csv input file
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/simpleMultiLevel.csv");

        //dsd
        SdmxBeans structure = structureService.readStructuresFromFile("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("4");
        csvInputConfig.setDelimiter(";");

        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
        csvMapping.put("JD_TYPE", new CsvInColumnMapping(new Integer[]{2}, false, 1, ""));
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{3}, false, 2, ""));
        csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{4}, false, 2, ""));        
        csvMapping.put("TIME", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));        
        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{6}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{5}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);
        
        try {
        	parseCsv(inputStream, kf, csvInputConfig);
        } catch (IllegalArgumentException ile) {
        	Assert.isTrue(ile.getMessage().contains("Check if the number of levels inside the file equals the given number of CSv Levels as parameter"));	
        }
    }
     /** 
     * Tests a regular multilevel csv input file
     * @throws Exception
     */
    @Test
    public void testCrossMeasuresDeep() throws Exception{

    	//csv input file
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/CrossPerLineCorrected.csv");

        //dsd
        SdmxBeans structure = structureService.readStructuresFromFile("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

        System.out.println(StructureService.scanForMeasureDimension(kf));
        

        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setMapCrossXMeasure(true);
        

        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
        // In XS FREQ and TIME_PERIOD appear in Group
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{2}, false, 1, ""));        
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{1}, false, 2, ""));
        csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));        
        csvMapping.put("STOCKS", new CsvInColumnMapping(new Integer[]{1}, false, 3, ""));
        csvMapping.put("FLOWS", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);
		ReadableDataLocation sourceData = new CsvReadableDataLocation(this.dataLocationFactory.getReadableDataLocation(inputStream));
        DataReaderEngine dataReaderEngine =  new MultiLevelCsvDataReaderEngine(sourceData, kf,	null, null,	csvInputConfig, null);
        
        //use engine for reading
        dataReaderEngine.reset();
	    assertThat("No dataset found", dataReaderEngine.moveNextDataset(), is(true));
	    
	    int obsCount=0;
	    while(dataReaderEngine.moveNextKeyable()) 
	    {
	    	 Keyable key = dataReaderEngine.getCurrentKey();
	    	 
	    	 if (key.isSeries()) {
	    		 assertThat("It is time series but shouldn't", key.isTimeSeries(),is(false));
	    		 
	    		 assertThat("FREQ should be set to series and should be M", key.getKeyValue("FREQ"), is("M"));
	    		 assertThat("JD_CATEGORY should be set to series and should be A", key.getKeyValue("JD_CATEGORY"), is("A"));
	    		 assertThat("VIS_CTY should be set to series and should be either DE or MX", key.getKeyValue("VIS_CTY"), anyOf(is("DE"),is("MX")));
	    		 assertThat("Cross sectional concept is JD_TYPE", key.getCrossSectionConcept(), is("JD_TYPE"));
	    		 assertThat("Time should be set at series", key.getObsTime(), anyOf(is("2000-01"), is("2000-02"), is("2000-04"), is("2000-09")));
	    		 while(dataReaderEngine.moveNextObservation())
	    		 {
	    			 obsCount++;
	    			 Observation obs = dataReaderEngine.getCurrentObservation();
	    			 assertThat("No observation set", obs.getObservationValue(), is(not(nullValue())));
	    			 assertThat("No observation set", obs.getObservationValue(), is(not(equalTo(""))));
	    			 assertThat("observation is not zero", Double.parseDouble(obs.getObservationValue()), is(not(0.0)));
	    			 assertThat("It is not cross sectional", obs.isCrossSection(), is(true));
	    			 assertThat("Missing Measure dimension at Cross Sectional obs", obs.getCrossSectionalValue(), is(not(nullValue())));
	    			 assertThat("Measure dimension values should be either P or Q", obs.getCrossSectionalValue().getCode(), either(is("P")).or(is("Q")));
	    		 }
	    	 }
	    }
	    
	    assertThat("Observation count is not 10", obsCount, is(10));
    }   
    
    /** 
     * Tests a regular multilevel csv input file
     * @throws Exception
     */
    @Test
    public void testCrossMeasures() throws Exception{

    	//csv input file
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/CrossPerLineCorrected.csv");

        //dsd
        SdmxBeans structure = structureService.readStructuresFromFile("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");
        DataStructureBean kf = (DataStructureBean) structure.getDataStructures().iterator().next();

        System.out.println(StructureService.scanForMeasureDimension(kf));
        

        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setMapCrossXMeasure(true);

        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{1}, false, 1, ""));
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{1}, false, 2, ""));
        csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));        
        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{1}, false, 3, ""));        
        csvMapping.put("STOCKS", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));
        csvMapping.put("FLOWS", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{5}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);
        
        parseCsv(inputStream, kf, csvInputConfig);

    }
       
    private CsvTestObject parseCsv(InputStream inputStream, DataStructureBean kf,  CsvInputConfig csvInputConfig) {
    	//initialize object that will be build from returned reader's value
    	CsvTestObject csvTestObject = new CsvTestObject();
    	StringBuffer dsAttributes = new StringBuffer();
    	
        //readabledatalocation and initialization of reader engine
		ReadableDataLocation sourceData = new CsvReadableDataLocation(this.dataLocationFactory.getReadableDataLocation(inputStream));
        DataReaderEngine dataReaderEngine =  new MultiLevelCsvDataReaderEngine(sourceData, kf,	null, null,	csvInputConfig, new FirstFailureExceptionHandler());
        
        //use engine for reading
        dataReaderEngine.reset();
	    dataReaderEngine.moveNextDataset();
	    
	    System.out.println(dataReaderEngine.getDataStructure().getName());
	    csvTestObject.setDataStructureName(dataReaderEngine.getDataStructure().getName());
	    
	    if (dataReaderEngine.getDatasetAttributes() != null) {
	    	for (KeyValue keyValue : dataReaderEngine.getDatasetAttributes()) {
	    		System.out.println(" " + keyValue.getConcept() + ": " + keyValue.getCode());
	    		dsAttributes.append(" " + keyValue.getConcept() + ": " + keyValue.getCode());
	    		
	    	}
	    	if (!dsAttributes.toString().isEmpty())
	    		csvTestObject.addDataAttribute(dsAttributes.toString());
	    }
	        	
		while (dataReaderEngine.moveNextKeyable() ) {
			StringBuffer keyable = new StringBuffer("");
			Keyable key = dataReaderEngine.getCurrentKey();
			
			if(key.isSeries()) { // series
				
			} else { // group
				System.out.println(key.getGroupName());
			}
			if(!key.isTimeSeries()) {
			}
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
				if (observation.isCrossSection()) { // Non time series
					KeyValue crossSectionalValue = observation.getCrossSectionalValue(); // in 2.1 and in XS 2.0
					if (crossSectionalValue !=  null) {
						crossSectionalValue.getConcept(); // e.g. DEATHS
						crossSectionalValue.getCode(); // e.g. 12535
						System.out.println(" " + crossSectionalValue.getConcept() + ": " + crossSectionalValue.getCode());
					}
				} else { // TimeSeries (e.g. 1999-06 / 634 ) 
					obsString.append("   " + observation.getObsTime() + ": " + observation.getObservationValue() + " - ");
					
					System.out.println("   " + observation.getObsTime() + ": " + observation.getObservationValue());
				}
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
    
    
    private CsvTestObject buildExpectedCsvTestObject() { 
    	CsvTestObject csvTestObject = new CsvTestObject();
    	csvTestObject.setDataStructureName("Joint BIS-IMF-OECD-World Bank stats - external debt");   	
    	String keyable1 = "VIS_CTY: M;JD_CATEGORY: Amounts;FREQ: MON;JD_TYPE: P;";
    	csvTestObject.addKeyable(keyable1);
    	csvTestObject.addObservation(keyable1, "    2000-01: 3.14 -    OBS_STATUS: A");
    	csvTestObject.addObservation(keyable1, "    2000-01: 1.113 -    OBS_STATUS: A");
    	String keyable2 = "VIS_CTY: M;JD_CATEGORY: Amounts;FREQ: ANN;JD_TYPE: P;";
    	csvTestObject.addKeyable(keyable2);
    	csvTestObject.addObservation(keyable2, "   2000-04: 5.2 -    OBS_STATUS: A");
    	csvTestObject.addObservation(keyable2, "    2000-09: 5.2 -    OBS_STATUS: A");
    	String keyable3 = "VIS_CTY: M;JD_CATEGORY: Amounts;FREQ: MON;JD_TYPE: A;";
    	csvTestObject.addKeyable(keyable3);
    	csvTestObject.addObservation(keyable3, "   2000-04: 5.55 -    OBS_STATUS: A");
    	   	
    	return csvTestObject;
    }
    
}
