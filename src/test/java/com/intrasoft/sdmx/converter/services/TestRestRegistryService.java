package com.intrasoft.sdmx.converter.services;

import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.commons.ui.services.JsonEndpoint;
import com.intrasoft.sdmx.converter.services.exceptions.RegistryConnectionException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import com.intrasoft.sdmx.converter.util.TypeOfVersion;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Set;
import java.util.logging.Handler;
import java.util.logging.LogManager;

import static junit.framework.Assert.*;

@DirtiesContext
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/test-spring-context.xml"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestRestRegistryService {

   @Autowired
   private RestRegistryService registryService;

    @Autowired
    private RegistryServiceFactory registryServiceFactory;

    @Autowired
    private ConfigService configService;

    @BeforeClass
    public static void testSetup() {
        Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
        java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(java.util.logging.Level.OFF);
        for (Handler h : rootLogger.getHandlers()) {
            h.setLevel(java.util.logging.Level.OFF);
        }
    }

    private final String UNIT_TEST_REGISTRY_URL = "https://registry.sdmx.org/ws/public/sdmxapi/rest";
    private final String EURO_SOAP_REGISTRY_URL_v20 = "https://ec.europa.eu/tools/cspa_services_global/sdmxregistry/v2.0/SdmxRegistryServicePS?wsdl";

    @Test
    //@Ignore
   public void test01RetrieveOfDataStrutureStubs() throws RegistryConnectionException{
	   Set<DataStructureBean> result = registryService.retrieveAllDataStructureStubs(UNIT_TEST_REGISTRY_URL);
       assertNotNull(result);
       assertTrue(result.size() > 0);
       System.out.println(result);
   }
   
   @Rule
   public ExpectedException expectedException = ExpectedException.none();
   
   @Test
   public void test02RetrieveDataStructuresWithInvalidUrl() throws RegistryConnectionException{
       expectedException.expect(RegistryConnectionException.class);
       expectedException.expectMessage("Could not open a connexion to URL: wrong.URL");
       
       registryService.retrieveAllDataStructureStubs("wrong.URL");
   }
   
   @Test
   public void test03RetrieveDataStructuresWithAlmostGoodUrl() throws RegistryConnectionException{
       expectedException.expect(RegistryConnectionException.class);
       expectedException.expectMessage("Could not connect to URL:");
       
       registryService.retrieveAllDataStructureStubs("https://registry.sdmxcloud.org/ws/caca/");
   }

    /**
     * SDMXCONV-1204
     * Test case for ensuring that when a v2.0 registry gets queried the Codelists are included in the SdmxBean
     * @throws RegistryConnectionException
     */
   @Test
   @Ignore
   public void test04RetrieveDataFlowWithEuroRegistry20() throws RegistryConnectionException {
       StructureIdentifier structIdentifier = new StructureIdentifier("ESTAT", "BCS_QBD_Q", "1.0");
       JsonEndpoint endpoint = new JsonEndpoint(EURO_SOAP_REGISTRY_URL_v20, TypeOfVersion.SOAP, "Euro Soap Registry V2.0");
       SdmxBeans sdmxBeans = registryServiceFactory.getRegistryService(endpoint).retrieveFullDetailsForSingleDataflow(EURO_SOAP_REGISTRY_URL_v20, structIdentifier);
       Assert.assertTrue(sdmxBeans.getCodelists().size()>0);
   }

   @Test
   //@Ignore
   public void test05RetrieveDataflowStubs() throws RegistryConnectionException{
	   Set<DataflowBean> result = registryService.retrieveAllDataflowStubs(UNIT_TEST_REGISTRY_URL);
       assertNotNull(result);
       assertTrue(result.size() > 0);
   }
   
   @Test
   //@Ignore
   public void test06RetrieveSingleDataStructure() throws RegistryConnectionException{
	   SdmxBeans result = registryService.retrieveFullDetailsForSingleDSD(	UNIT_TEST_REGISTRY_URL, 
			   																new StructureIdentifier("ESTAT", "NA_MAIN", "1.4"));
	   assertNotNull(result);
	   assertTrue(result.hasDataStructures());
	   assertTrue(result.getDataStructures().size() == 1);
	   DataStructureBean theOnlyDsd = result.getDataStructures().iterator().next(); 
	   assertEquals("1.4", theOnlyDsd.getVersion());
	   assertEquals("NA_MAIN", theOnlyDsd.getId());
	   assertEquals("ESTAT", theOnlyDsd.getAgencyId());
	   
	   assertTrue(result.hasCodelists());
	   assertEquals(30, result.getCodelists().size());
   }
   
   @Test
   //@Ignore
   public void test07RetrieveSingleDataflow() throws RegistryConnectionException{
       SdmxBeans result = registryService.retrieveFullDetailsForSingleDataflow(UNIT_TEST_REGISTRY_URL, 
    		   													new StructureIdentifier("ESTAT", "NAMAIN_IDC_N", "1.10"));
       assertNotNull(result);
       assertTrue(result.hasDataflows());
       assertTrue(result.getDataflows().size() == 1);
       
       //we make sure the dependencies have been downloaded as well
       assertTrue(result.hasDataStructures());
       assertTrue(result.getDataStructures().size() == 1);
       
       DataflowBean theOnlyDataflow = result.getDataflows().iterator().next(); 
       assertEquals("1.10", theOnlyDataflow.getVersion());
       assertEquals("NAMAIN_IDC_N", theOnlyDataflow.getId());
       assertEquals("ESTAT", theOnlyDataflow.getAgencyId());            
   }

    private final String LOCAL_FUSION_URL_V30 = "http://10.240.126.65:8080/sdmx/v2";
    private final String FUSION_USER = "root";
    private final String FUSION_PASSWORD = "password";
    private final String PROXY_HOST = "http://10.240.126.59:3128";
    private final String PROXY_USER = "sdmx";
    private final String PROXY_PASSWORD = "d_UseThisAsACITNET_2FA_workaround";

    /**
     * <p>Query for v3 SDMX. Test with proxy authentication.</p>
     * @see "https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1335"
     * @throws RegistryConnectionException
     */
    @Test
    public void test08RetrieveSingleDsd30() throws RegistryConnectionException{
        JsonEndpoint endpoint = new JsonEndpoint("https://registry.sdmx.org/sdmx/v2", TypeOfVersion.REST_V2, "Org Sdmx V2");
        RegistryService registryService = registryServiceFactory.getRegistryService(endpoint);
        SdmxBeans result = registryService.retrieveFullDetailsForSingleDSD("https://registry.sdmx.org/sdmx/v2",
                new StructureIdentifier("ESTAT", "NA_MAIN", "1.8"));
        assertNotNull(result);
        assertTrue(result.hasDataStructures());
        assertTrue(result.getDataStructures().size() == 1);
        DataStructureBean theOnlyDsd = result.getDataStructures().iterator().next();
        assertEquals("1.8", theOnlyDsd.getVersion());
        assertEquals("NA_MAIN", theOnlyDsd.getId());
        assertEquals("ESTAT", theOnlyDsd.getAgencyId());
    }

    /**
     * <p>Query for v3 SDMX.</p>
     * @see "https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1335"
     * @throws RegistryConnectionException
     */
    @Test
    public void test09RetrieveSingleDataflow30() throws RegistryConnectionException {
        JsonEndpoint endpoint = new JsonEndpoint("https://registry.sdmx.org/sdmx/v2", TypeOfVersion.REST_V2, "Org Sdmx V2");
        RegistryService registryService = registryServiceFactory.getRegistryService(endpoint);
        SdmxBeans result = registryService.retrieveFullDetailsForSingleDataflow("https://registry.sdmx.org/sdmx/v2",
                new StructureIdentifier("ESTAT", "NAMAIN_IDC_N", "1.9"));
        assertNotNull(result);
        assertTrue(result.hasDataflows());
        assertTrue(result.getDataflows().size() == 1);
        DataflowBean theOnlyDataflow = result.getDataflows().iterator().next();
        assertEquals("1.9", theOnlyDataflow.getVersion());
        assertEquals("NAMAIN_IDC_N", theOnlyDataflow.getId());
        assertEquals("ESTAT", theOnlyDataflow.getAgencyId());
    }
   
}
