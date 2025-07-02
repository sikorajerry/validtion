package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxInputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.excel.ExcelInputConfigImpl;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import junitx.framework.FileAssert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.SdmxCsvOutputConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Handler;
import java.util.logging.LogManager;

/**
 * Tests from all inputs to internal Sdmx Csv 
 * that reports input's position of the observation.
 * @throws Exception
 */
@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITInternalSdmxCsv {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;
	
    @Autowired
    private StructureService structureService;

    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    @Autowired
	private HeaderService headerService; 
    
    private final static String GENERATED_PATH = "internalSdmxCsv/";

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
    public void convertFromExcel() throws Exception{
		String resultFileName = "01_fromExcel_internal_SdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/NASUSample.xlsx");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

        // make the conversion
        TestConverterUtil.convert(Formats.EXCEL, 
        			Formats.iSDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
        			excelInputConfig,
        			csvOutputConfig,
	                kf,
	                dataflow, 
		            converterDelegatorService);
       
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       		" is different than what is expected at " + completeResultTestFilesFileName,
	       		expectedFile, generatedFile);
    }

	// TODO: 10/19/2020 Fix the comparable output file after merging extension SDMXCONV-1095
	@Test
	public void convertToISdmxCsv() throws Exception{
		String resultFileName = "2_output_iSdmxCsv_SDMXCONV-1095.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		InputStream inputStream = new FileInputStream("./testfiles/excel_input/iSdmxCsv_output/NASEC_T0620_A_V1.9_Sample_2.xlsx");

		//keyFamily
		DataStructureBean dataStructure = structureService.readFirstDataStructure("./testfiles/dsds/NASEC_T0620.xml");
		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/dsds/NASEC_T0620.xml");
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);

		//input configuration
		HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
		ExcelInputConfigImpl excelInputConfig = new ExcelInputConfigImpl();
		excelInputConfig.setHeader(header);
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsvAdjustment(true);
		csvOutputConfig.setInternalSdmxCsv(true);

		// make the conversion
		TestConverterUtil.convert(  Formats.EXCEL,
				Formats.iSDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				excelInputConfig,
				csvOutputConfig,
				dataStructure,
				dataflow,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file " + completeResultTargetFileName +
						" is different than what is expected " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

    @Test
    public void convertFromSingleLevel() throws Exception{
		String resultFileName = "02_fromCsv_internal_SdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/NASUSample.csv");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
       //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setHeader(header);
        csvInputConfig.setLevelNumber("1");
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

        //make the conversion
        TestConverterUtil.convert(Formats.CSV, 
        			Formats.iSDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
        			csvInputConfig,
        			csvOutputConfig,
	                kf,
	                dataflow, 
		            converterDelegatorService);
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       						" is different than what is expected at " + completeResultTestFilesFileName,
	       						expectedFile, generatedFile);
    }
    
    @Test
    public void convertFromSdmxCsv() throws Exception{
		String resultFileName = "03_fromSdmxCsv_internal_SdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/NASUSample.sdmx.csv");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
        //input configuration
        HeaderBean header = headerService.parseSdmxHeaderProperties("./testfiles/excel_input/header.prop");
        CsvInputConfig csvInputConfig = new CsvInputConfig();
        csvInputConfig.setDelimiter(";");
        csvInputConfig.setHeader(header);
        csvInputConfig.setLevelNumber("1");
        csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

        //make the conversion
        TestConverterUtil.convert(Formats.SDMX_CSV, 
        			Formats.iSDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
        			csvInputConfig,
        			csvOutputConfig,
	                kf,
	                dataflow, 
		            converterDelegatorService);
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       						" is different than what is expected at " + completeResultTestFilesFileName,
	       						expectedFile, generatedFile);
    }
    
    @Test
    public void convertFromCompact() throws Exception{
		String resultFileName = "04b_fromCompact_internal_SdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/ss21.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

        //make the conversion
        TestConverterUtil.convert(Formats.COMPACT_SDMX, 
        			Formats.iSDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
        			new SdmxInputConfig(),
        			csvOutputConfig,
	                kf,
	                dataflow, 
		            converterDelegatorService);
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       						" is different than what is expected at " + completeResultTestFilesFileName,
	       						expectedFile, generatedFile);
    }
    
    @Test
    public void convertFromSS21() throws Exception{
		String resultFileName = "04_fromSS21_internal_SdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/ss21.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

        //make the conversion
        TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1, 
        			Formats.iSDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
        			new SdmxInputConfig(),
        			csvOutputConfig,
	                kf,
	                dataflow, 
		            converterDelegatorService);
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       						" is different than what is expected at " + completeResultTestFilesFileName,
	       						expectedFile, generatedFile);
    }
    
    @Test
    public void convertFromGeneric() throws Exception{
		String resultFileName = "05_fromGeneric_internal_SdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/generic.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

        //make the conversion
        TestConverterUtil.convert(Formats.GENERIC_SDMX, 
        			Formats.iSDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
        			new SdmxInputConfig(),
        			csvOutputConfig,
	                kf,
	                dataflow, 
		            converterDelegatorService);
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       						" is different than what is expected at " + completeResultTestFilesFileName,
	       						expectedFile, generatedFile);
    }
    
    @Test
    public void convertFromGeneric21() throws Exception{
		String resultFileName = "06_fromGeneric21_internal_SdmxCsv.csv"; 
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;
		
		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));
		
        InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/generic21.xml");
      
        //keyFamily
        DataStructureBean kf = structureService.readFirstDataStructure("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        
        //dataflow
        SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/Dataflows_NASU.xml");
        DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//ouputFileName
        File outputFile =  new File (completeResultTargetFileName);
        OutputStream outputStream = new FileOutputStream(outputFile);
      
		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

        //make the conversion
        TestConverterUtil.convert(Formats.GENERIC_DATA_2_1, 
        			Formats.iSDMX_CSV, 
        			readableDataLocationFactory.getReadableDataLocation(inputStream), 
        			outputStream, 
        			new SdmxInputConfig(),
        			csvOutputConfig,
	                kf,
	                dataflow, 
		            converterDelegatorService);
           
		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
	    FileAssert.assertEquals("the generated file at " + completeResultTargetFileName + 
	       						" is different than what is expected at " + completeResultTestFilesFileName,
	       						expectedFile, generatedFile);
    }

	/**
	 * If the input file has only a header without DataSet element then the converter produces empty file.
	 * SDMXCONV-1184
	 * @throws Exception
	 */
	@Test
	public void convertFromSSEmpty() throws Exception{
		String resultFileName = "07-output.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/07-input-NAMAIN.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/07-dsd.xml");
		DataStructureBean kf = sdmxBeans.getDataStructures().iterator().next();
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);

		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setInternalSdmxCsv(true);

		//make the conversion
		TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				Formats.iSDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				new SdmxInputConfig(),
				csvOutputConfig,
				kf,
				dataflow,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}

	/**
	 * If the input file has only a header without DataSet element
	 * and the dsd is passed as a configuration parameter then the converter produces file with only the header row.
	 * SDMXCONV-1184
	 * @throws Exception
	 */
	@Test
	public void convertFromSSEmptyWriteHeaderRow() throws Exception{
		String resultFileName = "07-headerRowOutput.csv";
		String completeResultTargetFileName = IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH + IntegrationTestsUtils.GENERATED_NAME + resultFileName;
		String completeResultTestFilesFileName = IntegrationTestsUtils.TEST_FILES_NAME + GENERATED_PATH + resultFileName;

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME + GENERATED_PATH));

		InputStream inputStream = new FileInputStream("./testfiles/internalSdmxCsv/07-input-NAMAIN.xml");

		//dataflow
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile("./testfiles/internalSdmxCsv/07-dsd.xml");
		DataStructureBean dsd = sdmxBeans.getDataStructures().iterator().next();
		DataflowBean dataflow = sdmxBeans.getDataflows().iterator().next();

		//outputFileName
		File outputFile =  new File (completeResultTargetFileName);
		OutputStream outputStream = new FileOutputStream(outputFile);

		//output configuration
		SdmxCsvOutputConfig csvOutputConfig = new SdmxCsvOutputConfig();
		csvOutputConfig.setDsd(dsd);
		csvOutputConfig.setInternalSdmxCsv(true);

		//make the conversion
		TestConverterUtil.convert(Formats.STRUCTURE_SPECIFIC_DATA_2_1,
				Formats.iSDMX_CSV,
				readableDataLocationFactory.getReadableDataLocation(inputStream),
				outputStream,
				new SdmxInputConfig(),
				csvOutputConfig,
				dsd,
				dataflow,
				converterDelegatorService);

		File expectedFile = new File(completeResultTestFilesFileName);
		File generatedFile = outputFile;
		FileAssert.assertEquals("the generated file at " + completeResultTargetFileName +
						" is different than what is expected at " + completeResultTestFilesFileName,
				expectedFile, generatedFile);
	}
}
