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

import com.intrasoft.sdmx.converter.DataReaderToWriterTransformation;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.csv.MultiLevelCsvDataReaderEngine;
import com.intrasoft.sdmx.converter.io.data.csv.SingleLevelCsvDataWriterEngine;
import com.intrasoft.sdmx.converter.services.StructureService;

import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.*;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.util.CsvReadableDataLocation;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.transform.DataReaderWriterTransform;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogManager;

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
public class DataReaderToWriterTransformationTest {
	
	@Qualifier("readableDataLocationFactory")
	@Autowired
	private ReadableDataLocationFactory dataLocationFactory;

    @Autowired
    private StructureService structureService;
   
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
	@Autowired
	private DataReaderWriterTransform dataReaderWriterTransform;

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
    public void multipleToSingleLevelCsv() throws Exception {
    	//csv input file
        InputStream inputStream = new FileInputStream("./test_files/MULTILEVEL_CSV/simpleMultiLevel.csv");

        //dsd
        DataStructureBean dataStructure = structureService.readFirstDataStructure("./test_files/MULTILEVEL_CSV/BIS_JOINT_DEBT_v1.0.xml");

        //csv Parameter needed for CSV engine
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setLevelNumber("3");
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
        //csv mapping
        Map<String, CsvInColumnMapping> csvMapping = new LinkedHashMap<>();
        csvMapping.put("JD_TYPE", new CsvInColumnMapping(new Integer[]{2}, false, 1, ""));
        csvMapping.put("FREQ", new CsvInColumnMapping(new Integer[]{2}, false, 2, ""));
        csvMapping.put("JD_CATEGORY", new CsvInColumnMapping(new Integer[]{3}, false, 2, ""));
        csvMapping.put("VIS_CTY", new CsvInColumnMapping(new Integer[]{4}, false, 2, ""));
        csvMapping.put("TIME_PERIOD", new CsvInColumnMapping(new Integer[]{2}, false, 3, ""));    
        csvMapping.put("OBS_VALUE", new CsvInColumnMapping(new Integer[]{3}, false, 3, ""));
        csvMapping.put("OBS_CONF", new CsvInColumnMapping(new Integer[]{4}, false, 3, ""));
        csvMapping.put("OBS_STATUS", new CsvInColumnMapping(new Integer[]{5}, false, 3, ""));
        csvMapping.put("OBS_PRE_BREAK", new CsvInColumnMapping(new Integer[]{6}, false, 3, ""));
        
        csvMapping.put("AVAILABILITY", new CsvInColumnMapping(new Integer[]{7}, false, 3, ""));
        csvMapping.put("DECIMALS", new CsvInColumnMapping(new Integer[]{8}, false, 3, ""));
        csvMapping.put("BIS_UNIT", new CsvInColumnMapping(new Integer[]{9}, false, 3, ""));
        csvMapping.put("UNIT_MULT", new CsvInColumnMapping(new Integer[]{10}, false, 3, ""));
        csvMapping.put("TIME_FORMAT", new CsvInColumnMapping(new Integer[]{11}, false, 3, ""));
        csvMapping.put("COLLECTION", new CsvInColumnMapping(new Integer[]{}, false, 3, ""));
        
        csvInputConfig.setMapping(csvMapping);
        
        ReadableDataLocation sourceDataReader = new CsvReadableDataLocation(this.dataLocationFactory.getReadableDataLocation(inputStream));
        DataReaderEngine dataReaderEngine =  new MultiLevelCsvDataReaderEngine(sourceDataReader, dataStructure, null, null, csvInputConfig, null);

        String fileCsv = "./target/csvFromMultilevelCsv.txt";
        OutputStream outputStream =  new FileOutputStream(fileCsv);

        MultiLevelCsvOutputConfig csvOutputConfig = new MultiLevelCsvOutputConfig();
        csvOutputConfig.setLevels(3);
        csvOutputConfig.setDelimiter(";");
        csvOutputConfig.setOutputHeader(CsvOutputColumnHeader.USE_HEADER);
        
        SingleLevelCsvOutColMapping mappings = new SingleLevelCsvOutColMapping();
        mappings.addMapping(1, "JD_TYPE");
        mappings.addMapping(2, "FREQ");
        mappings.addMapping(3, "JD_CATEGORY");
        mappings.addMapping(4, "VIS_CTY");
        mappings.addMapping(5, "TIME_PERIOD");
        mappings.addMapping(6, "OBS_VALUE");
        mappings.addMapping(7, "OBS_CONF");
        mappings.addMapping(8, "OBS_STATUS");
        mappings.addMapping(9, "OBS_PRE_BREAK");
        mappings.addMapping(10, "AVAILABILITY");
        mappings.addMapping(11, "BIS_UNIT");
        mappings.addMapping(12, "UNIT_MULT");
        csvOutputConfig.setColumnMapping(mappings);
        
        DataWriterEngine csvWriter = new SingleLevelCsvDataWriterEngine(outputStream, csvOutputConfig);
        DataReaderToWriterTransformation dataReaderToWriterTransformation = new DataReaderToWriterTransformation();
        //SDMXCONV-1087, output file's format added in parameter list of copyToWriter method's signature
        dataReaderToWriterTransformation.copyToWriter(dataReaderEngine, csvWriter, Formats.CSV);
    }
}
