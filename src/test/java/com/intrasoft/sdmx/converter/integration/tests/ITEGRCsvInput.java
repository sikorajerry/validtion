package com.intrasoft.sdmx.converter.integration.tests;

import com.intrasoft.sdmx.converter.config.SdmxOutputConfig;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.io.data.csv.SingleLevelCsvDataReaderEngine;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.HeaderService;
import com.intrasoft.sdmx.converter.services.StructureService;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import com.intrasoft.sdmx.converter.util.FormatFamily;
import com.intrasoft.sdmx.converter.util.TestConverterUtil;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import junit.framework.Assert;
import org.apache.logging.log4j.core.config.Configurator;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.engine.decorators.ObservationCounterDecorator;
import org.estat.sdmxsource.extension.model.ErrorReporter;
import org.estat.sdmxsource.sdmx.api.constants.DATA_VERSION;
import org.estat.sdmxsource.sdmx.structureparser.SdmxBeansSchemaDecorator;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.CsvInputConfig;
import org.estat.struval.engine.impl.*;
import org.estat.struval.factory.*;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ErrorLimitException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.ErrorPosition;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DataValidationErrorHolder;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.DeduplicatorDecorator;
import org.sdmxsource.sdmx.dataparser.engine.reader.deduplication.ValidationEngineType;
import org.sdmxsource.sdmx.dataparser.manager.DataInformationManager;
import org.sdmxsource.sdmx.dataparser.manager.impl.DataValidationManagerImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
import org.sdmxsource.sdmx.structureretrieval.manager.SdmxSuperBeanRetrievalManagerImpl;
import org.sdmxsource.sdmx.util.beans.reference.MaintainableRefBeanImpl;
import org.sdmxsource.sdmx.util.sdmx.SdmxMessageUtil;
import org.sdmxsource.sdmx.validation.exceptions.DataValidationError;
import org.sdmxsource.sdmx.validation.model.MultipleFailureHandlerEngine;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;


@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/test-spring-context.xml"})
public class ITEGRCsvInput {
	private final static String GENERATED_PATH = "egrCsv_input/";
	private final static String INPUT_PATH = "egr_csv";
	@Rule
	public final ExpectedException expectedException = ExpectedException.none();
	@Autowired
	private ConverterDelegatorService converterDelegatorService;
	@Autowired
	private HeaderService headerService;
	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;
	@Autowired
	private DataInformationManager dataInformationManager;
	@Autowired
	private StructureService structureService;

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
	public void testCsvReaderEGR_IS_F2() throws InvalidStructureException, IOException {
		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "F2_EGROUT_ISNORLE_N_BG_2016_0000_V9022_17072017_NSA_BR_NSI.csv").toFile();

		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "SDMX_20_ESTAT+EGR_IS_F2_ATT+2.0-v2.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);
		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_IS_F2_ATT", "2.0"));
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(inputFile);
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setHeader(new HeaderBeanImpl("IREF" + Calendar.getInstance().getTimeInMillis(), "ZZ9"));
		SingleLevelCsvDataReaderEngine reader = new SingleLevelCsvDataReaderEngine(datasetLocation, dataStructure, null, null, csvInputConfig, null);
		reader.reset();
		assertThat(reader.moveNextDataset(), is(true));
		int seriesCount = 0;
		int obsCount = 0;
		while (reader.moveNextKeyable()) {
			Keyable key = reader.getCurrentKey();
			assertThat(key.isSeries(), is(true));
			assertThat(key.isTimeSeries(), is(false));
			assertThat(key.getCrossSectionConcept(), is(not(nullValue())));
			assertThat(key.getCrossSectionConcept(), is(not("LEU_COUNTRY_CODE")));
			assertThat(key.getObsTime(), either(is("")).or(is(nullValue())));
			assertThat(key.getKeyValue("LEU_NAME"), is(not("")));
			assertThat(key.getKeyValue("LEU_COUNTRY_CODE"), either(is("AE")).or(is("AU")));

			// This DSD has 3 dimension, one has to play the dimension at observation role
			assertThat(key.getKey().size(), is(3));
			seriesCount++;
			int obsInSeries = 0;
			while (reader.moveNextObservation()) {
				Observation obs = reader.getCurrentObservation();
				assertThat(obs, is(not(nullValue())));
				assertThat(obs.getCrossSectionalValue(), is(not(nullValue())));

				// we have no time dimesnion
				assertThat("We have should not have a obs time since we have no time dimension", obs.getObsTime(), either(is("")).or(is(nullValue())));
				assertThat("There should not be a obs value", obs.getObservationValue(), is(nullValue()));
				obsInSeries++;
			}
			assertThat("At series:" + key.getShortCode() + " at pos  " + seriesCount, obsInSeries, either(is(1)).or(is(2)));
			obsCount += obsInSeries;
		}


		//assertThat(seriesCount, is(3848)); //When setting dimensionAtObservation= AllDimensions this number goes 3851 SDMXCONV-945
		assertThat(obsCount, is(3852));
	}

	@Test
	public void testEGROUT_CSV_to_XS() throws Exception {
		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "F2_EGROUT_ISNORLE_N_BG_2016_0000_V9022_17072017_NSA_BR_NSI.csv").toFile();
		final String resultFileName = inputFile.getName() + ".xml";
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "SDMX_20_ESTAT+EGR_IS_F2_ATT+2.0-v2.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_IS_F2_ATT", "2.0"));
		final File outFile = csvToCrossSectional(inputFile, resultFileName, dataStructure);

		int expectedDataSet = 1;
		int expectedSeries = 3852;
		int expectedObs = 3852;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalCrossSectionalDataReaderEngine(datasetLocation, retrievalManager, null, null);
		checkCountVariableTotalObs(expectedDataSet, expectedSeries, expectedObs, reader);
		List<Throwable> errors = struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
		assertEquals(2, errors.size());
	}

	@Ignore
	//We Ignore it because it needs >1min to run but remove ignore to test it
	@Test
	public void testEntCSVToXS() throws Exception {

		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "EGR_ENT_A_EE_2015_0000_V0003_15112016_NSA_BR.csv").toFile();
		final String resultFileName = inputFile.getName() + ".xml";
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "ESTAT+EGR_CORE_ENT+1.0_Standard.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_CORE_ENT", "1.0"));
		final File outFile = csvToCrossSectional(inputFile, resultFileName, dataStructure);

		int expectedObsAttributes = 7;
		int expectedSeriesAttributes = 0;
		int expectedDataSetAttributes = 0;
		int expectedDataSet = 1;
		int expectedSeries = 5200;
		int expectedObs = 1;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalCrossSectionalDataReaderEngine(datasetLocation, retrievalManager, null, null);
		checkCount(expectedObsAttributes, expectedSeriesAttributes, expectedDataSetAttributes, expectedDataSet,
				expectedSeries, expectedObs, reader);

		struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
	}

	@Test
	public void testLELCSVToXS() throws Exception {

		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "EGR_LEL_A_EE_2015_0000_V0003_15112016_NSA_BR.csv").toFile();
		final String resultFileName = inputFile.getName() + ".xml";
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "ESTAT+EGR_CORE_LEL+1.0_Standard.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_CORE_LEL", "1.0"));
		final File outFile = csvToCrossSectional(inputFile, resultFileName, dataStructure);

		int expectedObsAttributes = 0;
		int expectedSeriesAttributes = 0;
		int expectedDataSetAttributes = 0;
		int expectedDataSet = 1;
		int expectedSeries = 5200;
		int expectedObs = 1;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalCrossSectionalDataReaderEngine(datasetLocation, retrievalManager, null, null);
		checkCount(expectedObsAttributes, expectedSeriesAttributes, expectedDataSetAttributes, expectedDataSet,
				expectedSeries, expectedObs, reader);

		struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
	}

	@Ignore
	//We Ignore it because it needs >1min to run but remove ignore to test it
	@Test
	public void testLEUCSVToXS() throws Exception {

		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "EGR_LEU_A_EE_2015_0000_V0003_15112016_NSA_BR.csv").toFile();
		final String resultFileName = inputFile.getName() + ".xml";
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "ESTAT+EGR_CORE_LEU+1.0_Standard.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_CORE_LEU", "1.0"));
		final File outFile = csvToCrossSectional(inputFile, resultFileName, dataStructure);

		int expectedObsAttributes = 15;
		int expectedSeriesAttributes = 0;
		int expectedDataSetAttributes = 0;
		int expectedDataSet = 1;
		int expectedSeries = 5200;
		int expectedObs = 1;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalCrossSectionalDataReaderEngine(datasetLocation, retrievalManager, null, null);
		checkCount(expectedObsAttributes, expectedSeriesAttributes, expectedDataSetAttributes, expectedDataSet,
				expectedSeries, expectedObs, reader);

		struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
	}

	@Test
	public void testLEUCSVToSS21() throws Exception {

		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "EGR_LEU_N_BE_2012_0001_V0001_01022013_NSA.CSV").toFile();
		//System.out.println(inputFile.getName());
		final String resultFileName = inputFile.getName() + ".xml";
		//System.out.println(resultFileName);
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "ESTAT+EGR_CORE_LEU+1.0_2.1.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_CORE_LEU", "1.0"));
		final File outFile = csvToSS21(inputFile, resultFileName, dataStructure);

		int expectedObsAttributes = 16;
		int expectedSeriesAttributes = 0;
		int expectedDataSetAttributes = 0;
		int expectedDataSet = 1;
		int expectedSeries = 3;
		int expectedObs = 1;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalStructureSpecificDataReaderEngine(datasetLocation, retrievalManager, null, null);
		//System.out.println("reader: "+reader.getClass());
		checkCount(expectedObsAttributes, expectedSeriesAttributes, expectedDataSetAttributes, expectedDataSet,
				expectedSeries, expectedObs, reader);
		struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
	}

	@Test
	public void testIS_F1_CSVToXS() throws Exception {

		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "EGR_ISNORLE_N_BG_2016_0000_V9022_07072017_NSA_TEST.csv").toFile();
		final String resultFileName = inputFile.getName() + ".xml";
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "ESTAT+EGR_IS_F1_ATT+2.0_Standard.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_IS_F1_ATT", "2.0"));
		final File outFile = csvToCrossSectional(inputFile, resultFileName, dataStructure);

		int expectedObsAttributes = 2;
		int expectedSeriesAttributes = 0;
		int expectedDataSetAttributes = 0;
		int expectedDataSet = 1;
		int expectedSeries = 3852;
		int expectedObs = 1;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalCrossSectionalDataReaderEngine(datasetLocation, retrievalManager, null, null);
		checkCount(expectedObsAttributes, expectedSeriesAttributes, expectedDataSetAttributes, expectedDataSet, expectedSeries, expectedObs, reader);

		struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
	}

	@Test
	public void testIS_F2_WithObsValueCSVToXS() throws Exception {

		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "INPUT_With_Obs_VAL_F2.csv").toFile();
		final String resultFileName = inputFile.getName() + ".xml";
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "ESTAT+EGR_IS_F2_ATT+2.0_Standard.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_IS_F2_ATT", "2.0"));
		final File outFile = csvToCrossSectional(inputFile, resultFileName, dataStructure);

		int expectedDataSet = 1;
		int expectedSeries = 3852;
		int expectedObs = 1;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalCrossSectionalDataReaderEngine(datasetLocation, retrievalManager, null, null);

		checkCountVariable(expectedDataSet, expectedSeries, expectedObs, reader);
		struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
	}

	@Test
	public void testIS_F2_WithoutObsValueCSVToXS() throws Exception {

		final File inputFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "INPUT_Without_Obs_VAL_F2.csv").toFile();
		final String resultFileName = inputFile.getName() + ".xml";
		final File dsdFile = Paths.get(IntegrationTestsUtils.TEST_FILES_NAME, INPUT_PATH, "ESTAT+EGR_IS_F2_ATT+2.0_Standard.xml").toFile();
		SdmxBeans sdmxBeans = structureService.readStructuresFromFile(dsdFile);
		InMemoryRetrievalManager retrievalManager = new InMemoryRetrievalManager(sdmxBeans);

		final DataStructureBean dataStructure = retrievalManager.getMaintainableBean(DataStructureBean.class, new MaintainableRefBeanImpl("ESTAT", "EGR_IS_F2_ATT", "2.0"));
		final File outFile = csvToCrossSectional(inputFile, resultFileName, dataStructure);

		int expectedDataSet = 1;
		int expectedSeries = 3852;
		int expectedObs = 1;
		final CsvInputConfig csvInputConfig = new CsvInputConfig();
		csvInputConfig.setDelimiter(";");
		csvInputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
		csvInputConfig.setStructureSchemaVersion(((SdmxBeansSchemaDecorator) sdmxBeans).getSdmxSchema());
		ReadableDataLocation datasetLocation = this.readableDataLocationFactory.getReadableDataLocation(outFile);
		DataReaderEngine reader = new StruvalCrossSectionalDataReaderEngine(datasetLocation, retrievalManager, null, null);
		checkCountVariable(expectedDataSet, expectedSeries, expectedObs, reader);

		struvalValidation(retrievalManager, datasetLocation, reader, Formats.CSV, csvInputConfig);
	}

	private void checkCountVariableTotalObs(int expectedDataSet, int expectedSeries, int expectedObs, DataReaderEngine reader) {
		int dataSetCount = 0;

		int obsCount = 0;
		int keyCount = 0;
		while (reader.moveNextDataset()) {
			dataSetCount++;
			assertNotNull(reader.getDatasetAttributes());
			while (reader.moveNextKeyable()) {
				keyCount++;
				Keyable key = reader.getCurrentKey();
				assertNotNull(key);
				if (key.isSeries()) {
					while (reader.moveNextObservation()) {
						obsCount++;
						Observation obs = reader.getCurrentObservation();
						assertNotNull(obs);
					}

				}
			}

		}

		assertEquals(expectedSeries, keyCount);
		assertEquals(expectedObs, obsCount);
		assertEquals(expectedDataSet, dataSetCount);
	}

	private void checkCountVariable(int expectedDataSet, int expectedSeries, int expectedObs, DataReaderEngine reader) {
		int dataSetCount = 0;

		while (reader.moveNextDataset()) {
			dataSetCount++;
			assertNotNull(reader.getDatasetAttributes());
			int keyCount = 0;
			while (reader.moveNextKeyable()) {
				keyCount++;
				Keyable key = reader.getCurrentKey();
				assertNotNull(key);
				if (key.isSeries()) {
					int obsCount = 0;
					while (reader.moveNextObservation()) {
						obsCount++;
						Observation obs = reader.getCurrentObservation();
						assertNotNull(obs);
					}

					assertEquals(expectedObs, obsCount);
				}
			}

			assertEquals(expectedSeries, keyCount);
		}

		assertEquals(expectedDataSet, dataSetCount);
	}

	private void checkCount(int expectedObsAttributes, int expectedSeriesAttributes, int expectedDataSetAttributes,
							int expectedDataSet, int expectedSeries, int expectedObs, DataReaderEngine reader) {
		int dataSetCount = 0;
		while (reader.moveNextDataset()) {
			dataSetCount++;
			assertNotNull(reader.getDatasetAttributes());
			assertEquals(expectedDataSetAttributes, reader.getDatasetAttributes().size());
			int keyCount = 0;
			while (reader.moveNextKeyable()) {
				keyCount++;
				Keyable key = reader.getCurrentKey();
				assertNotNull(key);
				if (key.isSeries()) {
					int obsCount = 0;
					assertEquals(key.getAttributes().toString(), expectedSeriesAttributes, key.getAttributes().size());
					while (reader.moveNextObservation()) {
						obsCount++;
						Observation obs = reader.getCurrentObservation();
						assertNotNull(obs);
						assertThat(obs.getAttributes().toString(), obs.getAttributes().size() >= expectedObsAttributes, is(true));
					}
					assertEquals(expectedObs, obsCount);
				}
			}
			assertEquals(expectedSeries, keyCount);
		}
		assertEquals(expectedDataSet, dataSetCount);
	}

	private List<Throwable> struvalValidation(SdmxBeanRetrievalManager retrievalManager,
											  ReadableDataLocation datasetLocation,
											  DataReaderEngine dataReaderEngine,
											  Formats sourceFormat,
											  InputConfig inputConfig) {
		final MultipleFailureHandlerEngine exceptionHandler = new MultipleFailureHandlerEngine(10);
		if (dataReaderEngine instanceof ErrorReporter) {
			((ErrorReporter) dataReaderEngine).setExceptionHandler(exceptionHandler);
		}
		int order = 0;
		//SDMXCONV-1363 A map that holds all the errors from the validators. Used to deduplicate the errors.
		LinkedHashMap<ValidationEngineType, ObjectArrayList<DataValidationError>> dataValidationErrors = new LinkedHashMap<>();
		Object2ObjectLinkedOpenHashMap<String, ErrorPosition> errorPositions = new Object2ObjectLinkedOpenHashMap<String, ErrorPosition>();
		if (dataReaderEngine instanceof DataValidationErrorHolder) {
			((DataValidationErrorHolder) dataReaderEngine).setMaximumErrorOccurrencesNumber(5);
			((DataValidationErrorHolder) dataReaderEngine).setErrorsByEngine(dataValidationErrors);
			((DataValidationErrorHolder) dataReaderEngine).setOrder(order);
			((DataValidationErrorHolder) dataReaderEngine).setErrorLimit(exceptionHandler.getLimit());
			errorPositions = ((DataValidationErrorHolder) dataReaderEngine).getErrorPositions();
		}
		dataReaderEngine = new ObservationCounterDecorator(dataReaderEngine, inputConfig, sourceFormat,null);
		//SDMXCONV-1338
		DataReaderEngine validatingDataReaderEngine;
		if(sourceFormat != null && sourceFormat.getFamily() == FormatFamily.SDMX || inputConfig==null) {
			validatingDataReaderEngine = new ValidateHeaderDataReaderEngine(dataInformationManager,
					dataReaderEngine, datasetLocation, exceptionHandler, true);
		} else {
			validatingDataReaderEngine = new ValidateCSVDataReaderEngine(dataInformationManager, dataReaderEngine,
					datasetLocation, exceptionHandler);
		}
		DataValidationManagerImpl dataValidationManager = new DataValidationManagerImpl();
		dataValidationManager.setSuperBeanRetrievalManager(new SdmxSuperBeanRetrievalManagerImpl(retrievalManager));
		//Add Struval Deep Validation Factory
		{
			// SDMXCONV-693 Set Boolean value to determine if we will report Time Period
			// if SDMX_SCHEMA.VERSION_TWO and not Csv report time_period = true
			boolean isTwoPointZero = false;
			if(ObjectUtil.validObject(sourceFormat)
					&& sourceFormat.getFamily() == FormatFamily.SDMX
					&& (SdmxMessageUtil.getSchemaVersion(datasetLocation) == SDMX_SCHEMA.VERSION_TWO)) {
				isTwoPointZero = true;
			}
			String isConfidential = "false";
			StruvalDeepValidatorFactory struvalDeep = new StruvalDeepValidatorFactory();
			DataValidationErrorDeduplicator deduplicatorDeep = new DataValidationErrorDeduplicator(struvalDeep, isTwoPointZero, isConfidential, DATA_VERSION.SDMX_21_COMPATIBLE, dataValidationErrors,5, order, 10, errorPositions, inputConfig.getStructureSchemaVersion());
			dataValidationManager.addValidatorFactory(deduplicatorDeep);
		}
		//Add StruvalMandatoryAttributesFactory that checks if mandatory attributes have values
		{
			DataValidationErrorDeduplicator deduplicatorMandatory = new DataValidationErrorDeduplicator(new StruvalMandatoryAttributesFactory(), dataValidationErrors, 5, order, 10, errorPositions, inputConfig.getStructureSchemaVersion());
			dataValidationManager.addValidatorFactory(deduplicatorMandatory);
		}
		//Add StruvalDuplicateObsValidationFactory that checks for duplicate observations
		{
			DataValidationErrorDeduplicator deduplicatorDuplicateObs = new DataValidationErrorDeduplicator(new StruvalDuplicateObsValidationFactory(), dataValidationErrors, 5, order, 10, errorPositions, inputConfig.getStructureSchemaVersion());
			dataValidationManager.addValidatorFactory(deduplicatorDuplicateObs);
		}
		//Add StruvalConstraintValidatorFactory that checks if constraints are followed
		{
			final StruvalConstraintValidatorFactory constraintValidatorFactory = new StruvalConstraintValidatorFactory();
			constraintValidatorFactory.setBeanRetrievalManager(retrievalManager);
			DataValidationErrorDeduplicator deduplicatorConstraints = new DataValidationErrorDeduplicator(constraintValidatorFactory, retrievalManager, dataValidationErrors, 5, order, 10, errorPositions, inputConfig.getStructureSchemaVersion());
			dataValidationManager.addValidatorFactory(deduplicatorConstraints);
		}
		int totalNumberOfErrors = 0;
		String msg =null;
		try {
			dataValidationManager.validateData(dataReaderEngine, exceptionHandler);
		} catch (ErrorLimitException e) {
			msg = e.getMessage();
		} catch (RuntimeException e) {
			throw e;
		} finally {
			//throw only the deduplicated errors
			totalNumberOfErrors = DeduplicatorDecorator.handleExceptions(dataValidationErrors, exceptionHandler);
			if(msg!=null)
				Assert.assertTrue(msg + " vs " + totalNumberOfErrors,totalNumberOfErrors == exceptionHandler.getLimit());
		}
		//throw only the deduplicated errors
		totalNumberOfErrors = DeduplicatorDecorator.handleExceptions(dataValidationErrors, exceptionHandler);
		return exceptionHandler.getExceptions();
	}

	private SdmxBeanRetrievalManager getRetrievalManager(final File dsdFile) {
		ReadableDataLocation structureLocation = readableDataLocationFactory.getReadableDataLocation(dsdFile);
		try {
			SdmxBeanRetrievalManager retrievalManager = new InMemoryRetrievalManager(structureLocation);
			return retrievalManager;
		} finally {
			structureLocation.close();
		}
	}

	private File csvToCrossSectional(File inputFile, String resultFileName, final DataStructureBean dataStructure)
			throws IOException, InvalidStructureException, FileNotFoundException, Exception {
		File outputFile = Paths.get(IntegrationTestsUtils.TARGET_NAME, GENERATED_PATH, IntegrationTestsUtils.GENERATED_NAME, resultFileName).toFile();

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME, GENERATED_PATH, IntegrationTestsUtils.GENERATED_NAME));

		try(FileInputStream headerStream = new FileInputStream("./testfiles/singleCsv_input/header.prop");
		ReadableDataLocation inputLocation = readableDataLocationFactory.getReadableDataLocation(inputFile);
		FileOutputStream outputStream = new FileOutputStream(outputFile)) {
			//input
			CsvInputConfig inputConfig = new CsvInputConfig();
			HeaderBean header = headerService.parseSdmxHeaderProperties(headerStream);
			inputConfig.setHeader(header);
			inputConfig.setDelimiter(";");
			//inputConfig.setMapCrossXMeasure(true);
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);

			TestConverterUtil.convert(Formats.CSV,
					Formats.CROSS_SDMX,
					inputLocation,
					outputStream,
					inputConfig,
					new SdmxOutputConfig(),
					dataStructure,
					null,
					converterDelegatorService);

			outputStream.flush();
			return outputFile;
		}
	}

	private File csvToSS21(File inputFile, String resultFileName, final DataStructureBean dataStructure)
			throws IOException, InvalidStructureException, FileNotFoundException, Exception {
		File outputFile = Paths.get(IntegrationTestsUtils.TARGET_NAME, GENERATED_PATH, IntegrationTestsUtils.GENERATED_NAME, resultFileName).toFile();

		Files.createDirectories(Paths.get(IntegrationTestsUtils.TARGET_NAME, GENERATED_PATH, IntegrationTestsUtils.GENERATED_NAME));


		try(FileInputStream headerStream = new FileInputStream("./testfiles/singleCsv_input/header.prop");
		ReadableDataLocation inputLocation = readableDataLocationFactory.getReadableDataLocation(inputFile);
		FileOutputStream outputStream = new FileOutputStream(outputFile)) {
			//input
			CsvInputConfig inputConfig = new CsvInputConfig();
			HeaderBean header = headerService.parseSdmxHeaderProperties(headerStream);
			inputConfig.setHeader(header);
			inputConfig.setDelimiter(";");
			inputConfig.setInputColumnHeader(CsvInputColumnHeader.USE_HEADER);
			inputConfig.setIsEscapeCSV(false);

			TestConverterUtil.convert(Formats.CSV,
						Formats.STRUCTURE_SPECIFIC_DATA_2_1,
						inputLocation,
						outputStream,
						inputConfig,
						new SdmxOutputConfig(),
						dataStructure,
						null,
						converterDelegatorService);

			outputStream.flush();
			return outputFile;
		}
	}
}
