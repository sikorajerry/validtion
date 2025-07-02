package com.intrasoft.sdmx.converter.integration.tests;

import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.output.StructureWriterManager;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.manager.query.ComplexStructureQueryBuilderManager;
import org.sdmxsource.sdmx.api.manager.query.QueryStructureRequestBuilderManager;
import org.sdmxsource.sdmx.api.manager.query.StructureQueryBuilderManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import com.intrasoft.sdmx.converter.services.SoapRegistryService;
import com.intrasoft.sdmx.converter.services.exceptions.RegistryConnectionException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
public class ITTestSdmxSoapService {
	
    @Autowired
    private ConverterDelegatorService converterDelegatorService;

	@Autowired
	StructureParsingManager structureParsingManager; 

	@Autowired
	StructureWriterManager structureWritingManager;

	@Autowired
	QueryStructureRequestBuilderManager queryStructureRequestBuilderManager;

	@Autowired
	ReadableDataLocationFactory readableDataLocationFactory;

	@Autowired
	ComplexStructureQueryBuilderManager complexStructureQueryBuilderManager;

	@Autowired
	StructureQueryBuilderManager structureQueryBuilderManager;

	@Autowired
	private SoapRegistryService registryService;

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}

	private static final String EURO_REGISTRY_V21 = "https://ec.europa.eu/tools/cspa_services_global/sdmxregistry/v2.1/SdmxRegistryServicePS?wsdl";
	private static final String EURO_REGISTRY_V20 = "https://ec.europa.eu/tools/cspa_services_global/sdmxregistry/v2.0/SdmxRegistryServicePS?wsdl";
	private static final String FUSION_REGISTRY_V21 = "https://registry.sdmx.org/FusionRegistry/ws/soap/sdmxSoap.wsdl?wsdl";

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	//Tests are commented out as they only run with a server connection
	
	@Test
	public void testDefault() throws RegistryConnectionException{
		Assert.assertTrue(true);
	}
//	@Test
	public void testRetrieveAllDataStructureStubs() throws RegistryConnectionException{
		Set<DataStructureBean> dsds = registryService.retrieveAllDataStructureStubs(EURO_REGISTRY_V21);
		Assert.assertEquals(162, dsds.size());
	}

//	@Test
	public void testRetrieveAllStructureStubsDSD() throws RegistryConnectionException{
		Set<DataStructureBean> dsds = registryService.retrieveAllStructureStubs(EURO_REGISTRY_V21, DataStructureBean.class);
		Assert.assertEquals(162, dsds.size());
	}

//	@Test
	public void testRetrieveStructureStubsNotDataflow() throws RegistryConnectionException{
		Set<StructureIdentifier> dsds = registryService.retrieveStructureStubs(false, EURO_REGISTRY_V21);
		Assert.assertEquals(162, dsds.size());
	}

//	@Test
	public void testRetrieveStructureStubsDataflow() throws RegistryConnectionException{
		Set<StructureIdentifier> dsds = registryService.retrieveStructureStubs(true, EURO_REGISTRY_V21);
		Assert.assertEquals(88, dsds.size());
	}

//	@Test
	public void testRetrieveAllDataflowStubs() throws RegistryConnectionException{
		Set<DataflowBean> dataflows = registryService.retrieveAllDataflowStubs(EURO_REGISTRY_V21);
		Assert.assertEquals(88, dataflows.size());
	}

//	@Test
	public void testRetrieveAllStructureStubsDataflow() throws RegistryConnectionException{
		Set<DataflowBean> dsds = registryService.retrieveAllStructureStubs(EURO_REGISTRY_V21, DataflowBean.class);
		Assert.assertEquals(88, dsds.size());
	}

//	@Test
	public void testRetrieveFullDetailsForSingleDSDNoResultFound() throws RegistryConnectionException{
		expectedException.expect(RegistryConnectionException.class);
		expectedException.expectMessage("SERVER RESPONSE ERROR: Received a SOAP FAULT:");

		StructureIdentifier structIdentifier = new StructureIdentifier("ESTAT", "ECB", "1.0");
		SdmxBeans sdmxBeans = registryService.retrieveFullDetailsForSingleDSD(EURO_REGISTRY_V21, structIdentifier);
		Assert.assertEquals("1.0", sdmxBeans.getDataStructures().iterator().next().getAgencyId());
	}

//	@Test
	public void testRetrieveFullDetailsForSingleDSD() throws RegistryConnectionException{
		StructureIdentifier structIdentifier = new StructureIdentifier("ECB", "ECB_AME1", "1.0");
		SdmxBeans sdmxBeans = registryService.retrieveFullDetailsForSingleDSD(EURO_REGISTRY_V21, structIdentifier);
		Assert.assertEquals("ECB", sdmxBeans.getDataStructures().iterator().next().getAgencyId());
	}

//	@Test
	public void testRetrieveFullDetailsForSingleDataflow() throws RegistryConnectionException{
		StructureIdentifier structIdentifier = new StructureIdentifier("ESTAT", "DEMOGRAPHY_RQ", "1.0");
		SdmxBeans sdmxBeans = registryService.retrieveFullDetailsForSingleDataflow(EURO_REGISTRY_V21, structIdentifier);
		Assert.assertEquals("1.0", sdmxBeans.getDataflows().iterator().next().getVersion());
	}
	
//	@Test
	public void testIncorrectUrl() throws RegistryConnectionException{
		expectedException.expect(RegistryConnectionException.class);
		expectedException.expectMessage("CONFIG ERROR:Cannot load the WSDL for endpoint");
		StructureIdentifier structIdentifier = new StructureIdentifier("ESTAT", "DEMOGRAPHY_RQ", "1.0");
		SdmxBeans sdmxBeans = registryService.retrieveFullDetailsForSingleDataflow(EURO_REGISTRY_V21+"xyz", structIdentifier);
		Assert.assertEquals("1.0", sdmxBeans.getDataflows().iterator().next().getVersion());
	}
	
//	@Test
	public void testRetrieveStructureStubsNotDataflowV20() throws RegistryConnectionException{
		Set<StructureIdentifier> dsds = registryService.retrieveStructureStubs(false, EURO_REGISTRY_V20);
		Assert.assertEquals(162, dsds.size());
	}
	
//	@Test
	public void testRetrieveFullDetailsForSingleDataflowV20NotFound() throws RegistryConnectionException{
		expectedException.expect(RegistryConnectionException.class);
		
		StructureIdentifier structIdentifier = new StructureIdentifier("ECB", "ECB_BOP1", "1.0");
		SdmxBeans sdmxBeans = registryService.retrieveFullDetailsForSingleDataflow(EURO_REGISTRY_V20, structIdentifier);
		Assert.assertEquals("1.0", sdmxBeans.getDataflows().iterator().next().getVersion());
	}
	
//	@Test
	public void testRetrieveFullDetailsForSingleDsdV20() throws RegistryConnectionException{
		StructureIdentifier structIdentifier = new StructureIdentifier("ECB", "ECB_BOP1", "1.0");
		SdmxBeans sdmxBeans = registryService.retrieveFullDetailsForSingleDSD(EURO_REGISTRY_V20, structIdentifier);
		Assert.assertEquals("1.0", sdmxBeans.getDataStructures().iterator().next().getVersion());
	}
	
//	@Test
	public void testRetrieveStructureStubsDataflowV20() throws RegistryConnectionException{
		Set<StructureIdentifier> dsds = registryService.retrieveStructureStubs(true, EURO_REGISTRY_V20);
		Assert.assertEquals(88, dsds.size());
	}
	
//	@Test
	public void testRetrieveFullDetailsForSingleDataflowFusionV21() throws RegistryConnectionException{
		StructureIdentifier structIdentifier = new StructureIdentifier("ESTAT", "NAMAIN_IDC_N", "1.6");
		SdmxBeans sdmxBeans = registryService.retrieveFullDetailsForSingleDataflow(FUSION_REGISTRY_V21, structIdentifier);
		Assert.assertEquals("1.6", sdmxBeans.getDataStructures().iterator().next().getVersion());
	}
	
//	@Test
	public void testRetrieveAllStructureStubsDataflowFusion21() throws RegistryConnectionException{
		Set<DataflowBean> dsds = registryService.retrieveAllStructureStubs(FUSION_REGISTRY_V21, DataflowBean.class);
		Assert.assertTrue(dsds != null && dsds.size() > 1);
	}
}
