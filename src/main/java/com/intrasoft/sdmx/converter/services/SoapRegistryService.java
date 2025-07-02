/**
 *
 * Copyright 2015 EUROSTAT
 *
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * 	https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.services;

import com.ibm.wsdl.extensions.http.*;
import com.ibm.wsdl.extensions.mime.MIMEConstants;
import com.ibm.wsdl.extensions.mime.MIMEContentSerializer;
import com.ibm.wsdl.extensions.mime.MIMEMimeXmlSerializer;
import com.ibm.wsdl.extensions.mime.MIMEMultipartRelatedSerializer;
import com.ibm.wsdl.extensions.soap.*;
import com.ibm.wsdl.extensions.soap12.*;
import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.sdmx.converter.sdmxsource.SoapSdmxBeanRetrievalManager;
import com.intrasoft.sdmx.converter.sdmxsource.SoapSdmxBeanRetrievalManagerV20;
import com.intrasoft.sdmx.converter.sdmxsource.SoapSdmxBeanRetrievalManagerV21;
import com.intrasoft.sdmx.converter.services.exceptions.RegistryConnectionException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.factory.ProxySettings;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationByProxyFactory;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.manager.query.ComplexStructureQueryBuilderManager;
import org.sdmxsource.sdmx.api.manager.query.QueryStructureRequestBuilderManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.util.beans.reference.MaintainableRefBeanImpl;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;
import org.sdmxsource.util.ObjectUtil;
import org.sdmxsource.util.factory.CertificatesHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.xml.sax.InputSource;

import javax.wsdl.Definition;
import javax.wsdl.WSDLException;
import javax.wsdl.extensions.ExtensionRegistry;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Soap Registry service
 * 
 * @author Mihaela Munteanu
 * @since 6th August 2017
 */
@Service
public class SoapRegistryService implements RegistryService {

	private static Logger logger = LogManager.getLogger(SoapRegistryService.class);
	
    @Autowired
    QueryStructureRequestBuilderManager queryStructureRequestBuilderManager;
	
    @Autowired
	private StructureParsingManager structureParsingManager;
    
    @Autowired
    private ComplexStructureQueryBuilderManager complexStructureQueryBuilderManager;
    
    @Autowired
    @Qualifier("readableDataLocationFactory")
	private ReadableDataLocationFactory readableLocationFactory;
    
    @Autowired
    @Qualifier("readableDataLocationByProxyFactory")
	private ReadableDataLocationByProxyFactory readableDataLocationByProxyFactory;
    
	@Autowired
    private ConfigService configService;
	 
    /**
     * Retrieves the structure stubs from the provided registry rest url
     * 
     * @param registryUrl - the rest endpoint of the registry
     * @param structureType - the type of structure to be retrieved from registry
     * @return
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
	public <T extends MaintainableBean> Set<T> retrieveAllStructureStubs(String registryUrl, Class<T> structureType) 
            throws RegistryConnectionException{
        Set<T> setOfResults = null; 
        try{
        	
    		SDMX_STRUCTURE_TYPE type = SDMX_STRUCTURE_TYPE.ANY;
        	if(structureType != null) {
    			type = SDMX_STRUCTURE_TYPE.parseClass(structureType);
    		}
            try {
            	//build empty identifier structure reference bean
            	StructureReferenceBean referenceBean = buildStructureReferenceBean(null, type);
            	//SDMXCONV-1288
            	if(!configService.isProxyEnabled()
						|| (configService.isProxyEnabled() && ObjectUtil.validCollection(configService.getProxyExclusions()) && configService.getProxyExclusions().contains(registryUrl))) {
					SoapSdmxBeanRetrievalManager retrievalManager = createRetrievalManagerInstance(registryUrl, null);
					setOfResults = (Set<T>)retrievalManager.getSdmxBeans(
							referenceBean,
							false,
							false,
							configService.getJksPath(),
							configService.getJksPassword()).getMaintainables(type);
				} else {
					ProxySettings proxySet = new ProxySettings( configService.getProxyUsername(),
							configService.getProxyPassword(),
							configService.getProxyHost(),
							configService.getProxyPort(),
							configService.getProxyExclusions());
					SoapSdmxBeanRetrievalManager retrievalManager = createRetrievalManagerInstance(registryUrl, proxySet);
					setOfResults = (Set<T>)retrievalManager.getSdmxBeans(
							referenceBean,
							false,
							false,
							proxySet,
							configService.getJksPath(),
							configService.getJksPassword()).getMaintainables(type);
				}
			} catch (Exception e) {
				throw new RegistryConnectionException(e.getMessage(), e);
			}
        }catch(RuntimeException runtimeExc){
            //workaround used for Rest
            throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
        }
    	
        return setOfResults;        
    }
    
    /**
     * retrieves the stubs for all datastructures found at the given registry url
     * @param url
     * @return
     * @throws IOException 
     */
    public Set<DataStructureBean> retrieveAllDataStructureStubs(String url) throws RegistryConnectionException{
    	logger.info("retrieving all data structures from url {}", url);
    	return retrieveAllStructureStubs(url, DataStructureBean.class);
    }
    
    /**
     * retrieves the stubs for all dataflows found in the given registry
     * @param url
     * @return
     * @throws IOException 
     */
    public Set<DataflowBean> retrieveAllDataflowStubs(String url) throws RegistryConnectionException{
    	logger.info("retrieving all dataflows from url {}", url);
    	return retrieveAllStructureStubs(url, DataflowBean.class);
    }
    
    /**
     * 
     * @param isDataflow
     * @param registryUrl
     * @return
     * @throws IOException 
     */
    public Set<StructureIdentifier> retrieveStructureStubs(boolean isDataflow, String registryUrl) 
            throws RegistryConnectionException {
        Set<? extends MaintainableBean> artefactStubs = null;
        if(isDataflow){
            artefactStubs = retrieveAllDataflowStubs(registryUrl);
        }else{
            //data structure
            artefactStubs = retrieveAllDataStructureStubs(registryUrl);
        }
        Set<StructureIdentifier> structureIdentifiers = new HashSet<StructureIdentifier>(artefactStubs.size());
        for(MaintainableBean stub: artefactStubs){
            logger.trace("parsing structure {} ", stub);
            structureIdentifiers.add(new StructureIdentifier(stub.getAgencyId(), stub.getId(), stub.getVersion()));
        }
        return structureIdentifiers;
    }
    
    /**
     * 
     * @param registryUrl
     * @param structIdentifier
     * @return
     */
    public SdmxBeans retrieveFullDetailsForSingleDSD(String registryUrl, StructureIdentifier structIdentifier)
    throws RegistryConnectionException{
    	logger.info("retrieving data structure from registry {} for identifier = {}", registryUrl, structIdentifier);
    	SdmxBeans result = null; 
    	try{
    		
    	    StructureReferenceBean structureReferenceBean = new StructureReferenceBeanImpl(
                        new MaintainableRefBeanImpl(structIdentifier.getAgency(), 
                        							structIdentifier.getArtefactId(), 
                        							structIdentifier.getArtefactVersion()), 
                        							SDMX_STRUCTURE_TYPE.DSD); 
    	    //compute the result
			//SDMXCONV-1288
			if(!configService.isProxyEnabled()
					|| (configService.isProxyEnabled() && ObjectUtil.validCollection(configService.getProxyExclusions()) && configService.getProxyExclusions().contains(registryUrl))) {
				SoapSdmxBeanRetrievalManager retrievalManager = createRetrievalManagerInstance(registryUrl, null);
				result = retrievalManager.getSdmxBeans(
						structureReferenceBean, true, true,
						configService.getJksPath(),
						configService.getJksPassword());
    	    } else {
				ProxySettings proxySet = new ProxySettings( configService.getProxyUsername(),
						configService.getProxyPassword(),
						configService.getProxyHost(),
						configService.getProxyPort(),
						configService.getProxyExclusions());
				SoapSdmxBeanRetrievalManager retrievalManager = createRetrievalManagerInstance(registryUrl, proxySet);
				result = retrievalManager.getSdmxBeans(
						structureReferenceBean, true, true,
						proxySet,
						configService.getJksPath(),
						configService.getJksPassword());

    	    }
    	} catch(RuntimeException runtimeExc){
    	    throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
    	} catch(Exception exception) {
    		throw new RegistryConnectionException(exception.getMessage(), exception);
    	}
        return result; 
    }
    
    /**
     * 
     * @param registryUrl
     * @param structIdentifier
     * @return
     */
    public SdmxBeans retrieveFullDetailsForSingleDataflow(String registryUrl, StructureIdentifier structIdentifier)
    throws RegistryConnectionException{
    	logger.info("retrieving dataflow from registry {} for identifier={}", registryUrl, structIdentifier);
    	SdmxBeans result = null; 
    	try{
    	    StructureReferenceBean structureReferenceBean = new StructureReferenceBeanImpl(
                        new MaintainableRefBeanImpl(structIdentifier.getAgency(), 
                        							structIdentifier.getArtefactId(), 
                        							structIdentifier.getArtefactVersion()), 
                        							SDMX_STRUCTURE_TYPE.DATAFLOW);
    	    //compute the result
			//SDMXCONV-1288
			if(!configService.isProxyEnabled()
					|| (configService.isProxyEnabled() && ObjectUtil.validCollection(configService.getProxyExclusions()) && configService.getProxyExclusions().contains(registryUrl))) {
				SoapSdmxBeanRetrievalManager retrievalManager = createRetrievalManagerInstance(registryUrl, null);
				result = retrievalManager.getSdmxBeans(
						structureReferenceBean, true, true,
						configService.getJksPath(),
						configService.getJksPassword());
    	    }else {
				ProxySettings proxySet = new ProxySettings( configService.getProxyUsername(),
						configService.getProxyPassword(),
						configService.getProxyHost(),
						configService.getProxyPort(),
						configService.getProxyExclusions());
				SoapSdmxBeanRetrievalManager retrievalManager = createRetrievalManagerInstance(registryUrl, proxySet);
				result = retrievalManager.getSdmxBeans(
						structureReferenceBean, true, true,
						proxySet,
						configService.getJksPath(),
						configService.getJksPassword());

    	    }
    	} catch(RuntimeException runtimeExc){
    	    throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
    	} catch(Exception exception) {
    		throw new RegistryConnectionException(exception.getMessage(), exception);
    	}
        return result; 
    }

	/**
	 * This is not implemented for SOAP registry.
	 * We do not fetch constraints for 2.0, 2.1
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1323">SDMXCONV-1323</a>,
	 * <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1314">SDMXCONV-1314</a>
	 * @param registryUrl
	 * @param structIdentifier
	 * @return SdmxBeans
	 */
	@Override
	public SdmxBeans retrieveReferencesForSingleDataflow(String registryUrl, StructureIdentifier structIdentifier) {
		return null;
	}

	private StructureReferenceBean buildStructureReferenceBean(StructureIdentifier structureIdentifier, SDMX_STRUCTURE_TYPE type) {
    	StructureReferenceBean structureReferenceBean = new StructureReferenceBeanImpl(type);
    	if (structureIdentifier != null) {
		    structureReferenceBean = new StructureReferenceBeanImpl(
	                new MaintainableRefBeanImpl(structureIdentifier.getAgency(), 
	                		structureIdentifier.getArtefactId(), 
	                		structureIdentifier.getArtefactVersion()), 
	                		type); 
	    }
	    return structureReferenceBean;
    }
    
    /**
     * 
     * @param endpointUrl
	 * @param proxySet
     * @return SoapSdmxBeanRetrievalManager
     * @throws IOException 
     */
    private SoapSdmxBeanRetrievalManager createRetrievalManagerInstance(String endpointUrl, ProxySettings proxySet) {
    	SoapSdmxBeanRetrievalManager retrievalManager = null;
    	//Read only once the definition of wsdl
    	Definition wsdlDefinition = getWsdlDefinition(endpointUrl, proxySet);
    	//We only need the targetNamespace
    	String namespace = wsdlDefinition.getTargetNamespace();
    	if (isSdmxV21Endpoint(namespace)) {
        	retrievalManager = new SoapSdmxBeanRetrievalManagerV21(); 
        	retrievalManager.setSoapURL(endpointUrl);
        	retrievalManager.setStructureParsingManager(structureParsingManager);
        	retrievalManager.setReadableDataLocationFactory(readableLocationFactory);
        	retrievalManager.setReadableLocationByProxyFactory(readableDataLocationByProxyFactory);
        	retrievalManager.setEndpoint(getEndpointFromWsdl(endpointUrl));
        	retrievalManager.setNsiNamespace(namespace);
        	retrievalManager.setV21Type(true);
        	((SoapSdmxBeanRetrievalManagerV21)retrievalManager).setComplexStructureQueryBuilderManager(complexStructureQueryBuilderManager);
        	return retrievalManager; 
    	} else {
        	retrievalManager = new SoapSdmxBeanRetrievalManagerV20(); 
        	retrievalManager.setSoapURL(endpointUrl);
        	retrievalManager.setStructureParsingManager(structureParsingManager);
        	retrievalManager.setReadableDataLocationFactory(readableLocationFactory);
        	retrievalManager.setReadableLocationByProxyFactory(readableDataLocationByProxyFactory);
        	retrievalManager.setEndpoint(getEndpointFromWsdl(endpointUrl));
        	retrievalManager.setNsiNamespace(namespace);
        	retrievalManager.setV21Type(false);
        	((SoapSdmxBeanRetrievalManagerV20)retrievalManager).setQueryStructureRequestBuilderManager(queryStructureRequestBuilderManager);
        	return retrievalManager; 
    	}
    }
    
    
    /**
     * Connect to url given to read the wsdl and get the definition.
     * This method doesn't use certificates or proxy settings to connect.
     * @param registryUrl
     * @return definition
     */
    private Definition getWsdlDefinition(String registryUrl) {
		try {
			final WSDLFactory factory = WSDLFactory.newInstance();
			WSDLReader reader = factory.newWSDLReader();
			// Avoid importing external documents
			WSVersionExtensionRegistry extensionErgistry = new WSVersionExtensionRegistry();
			reader.setExtensionRegistry(extensionErgistry);
			Definition definition =  reader.readWSDL(registryUrl);
			logger.info("-- WSDL loaded successfully!");
			return definition;
		} catch (WSDLException e) {
			final String message = String.format("CONFIG ERROR:Cannot load the WSDL for endpoint '%1$s'",registryUrl);
			logger.error("`-- " + message, e);
			throw new RuntimeException(message,e);
		}
    }
    
    private static String getEndpointFromWsdl(String registryUrl) {
    	if (registryUrl.contains("?wsdl")) {
    		return registryUrl.substring(0, registryUrl.indexOf("?wsdl"));
    	} 
    	// Fix for SDMXCONV-695
    	if (registryUrl.contains("?WSDL")) {
    		return registryUrl.substring(0, registryUrl.indexOf("?WSDL"));
    	}
    	return registryUrl;
    }
    
    private boolean isSdmxV21Endpoint(String targetNamespace) {
    	if (targetNamespace != null && (targetNamespace.contains("2.1") || targetNamespace.contains("2_1"))) {
    		return true;
    	}
    	
    	return false;
    }
    
    private boolean isSdmxV21Endpoint(Definition definition) {
    	String targetNamespace = definition.getTargetNamespace();
    	if (targetNamespace != null && (targetNamespace.contains("2.1") || targetNamespace.contains("2_1"))) {
    		return true;
    	}
    	return false;
    }
    
    
    /**
     * Connect to url given to read the wsdl and get the definition.
     * This method uses certificates and proxy settings to connect, 
     * downloading the stream of the wsdl as xml.
     * @param urlString
     * @param proxySet
     * @return definition
     * @see 'https://webgate.ec.europa.eu/CITnet/jira/browse/SDMXCONV-695'
     */
    private Definition getWsdlDefinition(String urlString, ProxySettings proxySet) {
    	
    	//Simple old method without proxy authentication
    	if(proxySet == null) return getWsdlDefinition(urlString);
    	
    	String jksPath = configService.getJksPath();
    	String jksPassword = configService.getJksPassword();
    	URL url = null;
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e) {
			logger.error("Malformed Url Exception", e);
		}
		HttpClient client;
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(100 * 1000) // 100 s
				.setConnectionRequestTimeout(100 * 1000) // 100 s
				.build();

		HttpClientBuilder httpClientBuilder = HttpClients.custom().setDefaultRequestConfig(requestConfig);
		HttpHost proxy = new HttpHost(proxySet.getHost(), proxySet.getPort());
		final List<String> exclusions = proxySet.getExclusions();
		HttpRoutePlanner routePlanner = new DefaultProxyRoutePlanner(proxy) {
		    @Override
		    public HttpRoute determineRoute(
		            final HttpHost host,
		            final HttpRequest request,
		            final HttpContext context) throws HttpException {
		        String hostname = host.getHostName(); 
		        for (String hst : exclusions) {
		        	if (hostname.equalsIgnoreCase(hst)) {
		            // Return direct route
		            return new HttpRoute(host);
		        	}
		        }
		        return super.determineRoute(host, request, context);
		    }
		};
		
		//Use the credential for proxy server
		Credentials credentials = new UsernamePasswordCredentials(proxySet.getUsername(), proxySet.getPassword());
		AuthScope authScope = new AuthScope(proxySet.getHost(), proxySet.getPort());

		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(authScope, credentials);
		httpClientBuilder.setDefaultCredentialsProvider(credsProvider);

		//Only if protocol is https load the cacerts file
		if(url.getProtocol().toString().equalsIgnoreCase("https")){
			client = httpClientBuilder
					.setRoutePlanner(routePlanner)
			        .setSSLSocketFactory(CertificatesHandler.loadCertificates(jksPath, jksPassword)).build();
		} else {
			client = httpClientBuilder
					.setRoutePlanner(routePlanner)
			        .build();
		}

		//We just want to take the wsdl xml
		HttpGet get = new HttpGet(url.toString());
		get.setConfig(RequestConfig.custom().setRedirectsEnabled(false).build());
		get.setHeader("Content-Type", "text/xml;");
		get.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;");
		
		HttpResponse response = null;
		InputStream initialStream = null;
		Definition definition = null;
		InputSource source = null;
		try {
			response = client.execute(get);
			initialStream = response.getEntity().getContent();
			source = new InputSource(initialStream);
			WSDLFactory factory;
			
			factory = WSDLFactory.newInstance();
			WSDLReader reader = factory.newWSDLReader();
			// Avoid importing external documents
			// Possible solution to SDMXCONV-695
			WSVersionExtensionRegistry extensionErgistry = new WSVersionExtensionRegistry();
			reader.setExtensionRegistry(extensionErgistry);
			definition =  reader.readWSDL(null, source);
			logger.info("WSDL loaded successfully!");		
		} catch (WSDLException | UnsupportedOperationException e) {
			final String message = String.format("CONFIG ERROR:Cannot load the WSDL for endpoint '%1$s'.",urlString);
			logger.error("`-- " + message, e);
			throw new RuntimeException(message,e);
		} catch (ClientProtocolException e) {
			final String message = String.format("CONFIG ERROR:Cannot load the WSDL for endpoint '%1$s'. Client protocol error.",urlString);
			logger.error("`-- " + message, e);
			throw new RuntimeException(message,e);
		} catch (IOException e) {
			final String message = String.format("CONFIG ERROR:Cannot load the WSDL for endpoint '%1$s'",urlString);
			logger.error("`-- " + message, e);
			throw new RuntimeException(message,e);
		} finally {
			IOUtils.closeQuietly(initialStream);
		}
		return definition;
    }
    
    private class WSVersionExtensionRegistry extends ExtensionRegistry {

		private static final long serialVersionUID = -1954870879593320281L;

		public WSVersionExtensionRegistry() {
            SOAPAddressSerializer soapaddressserializer = new SOAPAddressSerializer();
            registerSerializer(javax.wsdl.Port.class,
                    SOAPConstants.Q_ELEM_SOAP_ADDRESS, soapaddressserializer);
            registerDeserializer(javax.wsdl.Port.class,
                    SOAPConstants.Q_ELEM_SOAP_ADDRESS, soapaddressserializer);
            mapExtensionTypes(javax.wsdl.Port.class,
                    SOAPConstants.Q_ELEM_SOAP_ADDRESS,
                    com.ibm.wsdl.extensions.soap.SOAPAddressImpl.class);
            SOAPBindingSerializer soapbindingserializer = new SOAPBindingSerializer();
            registerSerializer(javax.wsdl.Binding.class,
                    SOAPConstants.Q_ELEM_SOAP_BINDING, soapbindingserializer);
            registerDeserializer(javax.wsdl.Binding.class,
                    SOAPConstants.Q_ELEM_SOAP_BINDING, soapbindingserializer);
            mapExtensionTypes(javax.wsdl.Binding.class,
                    SOAPConstants.Q_ELEM_SOAP_BINDING,
                    com.ibm.wsdl.extensions.soap.SOAPBindingImpl.class);
            SOAPHeaderSerializer soapheaderserializer = new SOAPHeaderSerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    SOAPConstants.Q_ELEM_SOAP_HEADER, soapheaderserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    SOAPConstants.Q_ELEM_SOAP_HEADER, soapheaderserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    SOAPConstants.Q_ELEM_SOAP_HEADER,
                    com.ibm.wsdl.extensions.soap.SOAPHeaderImpl.class);
            registerSerializer(javax.wsdl.BindingOutput.class,
                    SOAPConstants.Q_ELEM_SOAP_HEADER, soapheaderserializer);
            registerDeserializer(javax.wsdl.BindingOutput.class,
                    SOAPConstants.Q_ELEM_SOAP_HEADER, soapheaderserializer);
            mapExtensionTypes(javax.wsdl.BindingOutput.class,
                    SOAPConstants.Q_ELEM_SOAP_HEADER,
                    com.ibm.wsdl.extensions.soap.SOAPHeaderImpl.class);
            mapExtensionTypes(javax.wsdl.extensions.soap.SOAPHeader.class,
                    SOAPConstants.Q_ELEM_SOAP_HEADER_FAULT,
                    com.ibm.wsdl.extensions.soap.SOAPHeaderFaultImpl.class);
            SOAPBodySerializer soapbodyserializer = new SOAPBodySerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY, soapbodyserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY, soapbodyserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY,
                    com.ibm.wsdl.extensions.soap.SOAPBodyImpl.class);
            registerSerializer(javax.wsdl.BindingOutput.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY, soapbodyserializer);
            registerDeserializer(javax.wsdl.BindingOutput.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY, soapbodyserializer);
            mapExtensionTypes(javax.wsdl.BindingOutput.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY,
                    com.ibm.wsdl.extensions.soap.SOAPBodyImpl.class);
            registerSerializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY, soapbodyserializer);
            registerDeserializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY, soapbodyserializer);
            mapExtensionTypes(javax.wsdl.extensions.mime.MIMEPart.class,
                    SOAPConstants.Q_ELEM_SOAP_BODY,
                    com.ibm.wsdl.extensions.soap.SOAPBodyImpl.class);
            SOAPFaultSerializer soapfaultserializer = new SOAPFaultSerializer();
            registerSerializer(javax.wsdl.BindingFault.class,
                    SOAPConstants.Q_ELEM_SOAP_FAULT, soapfaultserializer);
            registerDeserializer(javax.wsdl.BindingFault.class,
                    SOAPConstants.Q_ELEM_SOAP_FAULT, soapfaultserializer);
            mapExtensionTypes(javax.wsdl.BindingFault.class,
                    SOAPConstants.Q_ELEM_SOAP_FAULT,
                    com.ibm.wsdl.extensions.soap.SOAPFaultImpl.class);
            SOAPOperationSerializer soapoperationserializer = new SOAPOperationSerializer();
            registerSerializer(javax.wsdl.BindingOperation.class,
                    SOAPConstants.Q_ELEM_SOAP_OPERATION, soapoperationserializer);
            registerDeserializer(javax.wsdl.BindingOperation.class,
                    SOAPConstants.Q_ELEM_SOAP_OPERATION, soapoperationserializer);
            mapExtensionTypes(javax.wsdl.BindingOperation.class,
                    SOAPConstants.Q_ELEM_SOAP_OPERATION,
                    com.ibm.wsdl.extensions.soap.SOAPOperationImpl.class);
            SOAP12AddressSerializer soap12addressserializer = new SOAP12AddressSerializer();
            registerSerializer(javax.wsdl.Port.class,
                    SOAP12Constants.Q_ELEM_SOAP_ADDRESS, soap12addressserializer);
            registerDeserializer(javax.wsdl.Port.class,
                    SOAP12Constants.Q_ELEM_SOAP_ADDRESS, soap12addressserializer);
            mapExtensionTypes(javax.wsdl.Port.class,
                    SOAP12Constants.Q_ELEM_SOAP_ADDRESS,
                    com.ibm.wsdl.extensions.soap12.SOAP12AddressImpl.class);
            SOAP12BindingSerializer soap12bindingserializer = new SOAP12BindingSerializer();
            registerSerializer(javax.wsdl.Binding.class,
                    SOAP12Constants.Q_ELEM_SOAP_BINDING, soap12bindingserializer);
            registerDeserializer(javax.wsdl.Binding.class,
                    SOAP12Constants.Q_ELEM_SOAP_BINDING, soap12bindingserializer);
            mapExtensionTypes(javax.wsdl.Binding.class,
                    SOAP12Constants.Q_ELEM_SOAP_BINDING,
                    com.ibm.wsdl.extensions.soap12.SOAP12BindingImpl.class);
            SOAP12HeaderSerializer soap12headerserializer = new SOAP12HeaderSerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    SOAP12Constants.Q_ELEM_SOAP_HEADER, soap12headerserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    SOAP12Constants.Q_ELEM_SOAP_HEADER, soap12headerserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    SOAP12Constants.Q_ELEM_SOAP_HEADER,
                    com.ibm.wsdl.extensions.soap12.SOAP12HeaderImpl.class);
            registerSerializer(javax.wsdl.BindingOutput.class,
                    SOAP12Constants.Q_ELEM_SOAP_HEADER, soap12headerserializer);
            registerDeserializer(javax.wsdl.BindingOutput.class,
                    SOAP12Constants.Q_ELEM_SOAP_HEADER, soap12headerserializer);
            mapExtensionTypes(javax.wsdl.BindingOutput.class,
                    SOAP12Constants.Q_ELEM_SOAP_HEADER,
                    com.ibm.wsdl.extensions.soap12.SOAP12HeaderImpl.class);
            mapExtensionTypes(javax.wsdl.extensions.soap12.SOAP12Header.class,
                    SOAP12Constants.Q_ELEM_SOAP_HEADER_FAULT,
                    com.ibm.wsdl.extensions.soap12.SOAP12HeaderFaultImpl.class);
            SOAP12BodySerializer soap12bodyserializer = new SOAP12BodySerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY, soap12bodyserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY, soap12bodyserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY,
                    com.ibm.wsdl.extensions.soap12.SOAP12BodyImpl.class);
            registerSerializer(javax.wsdl.BindingOutput.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY, soap12bodyserializer);
            registerDeserializer(javax.wsdl.BindingOutput.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY, soap12bodyserializer);
            mapExtensionTypes(javax.wsdl.BindingOutput.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY,
                    com.ibm.wsdl.extensions.soap12.SOAP12BodyImpl.class);
            registerSerializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY, soap12bodyserializer);
            registerDeserializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY, soap12bodyserializer);
            mapExtensionTypes(javax.wsdl.extensions.mime.MIMEPart.class,
                    SOAP12Constants.Q_ELEM_SOAP_BODY,
                    com.ibm.wsdl.extensions.soap12.SOAP12BodyImpl.class);
            SOAP12FaultSerializer soap12faultserializer = new SOAP12FaultSerializer();
            registerSerializer(javax.wsdl.BindingFault.class,
                    SOAP12Constants.Q_ELEM_SOAP_FAULT, soap12faultserializer);
            registerDeserializer(javax.wsdl.BindingFault.class,
                    SOAP12Constants.Q_ELEM_SOAP_FAULT, soap12faultserializer);
            mapExtensionTypes(javax.wsdl.BindingFault.class,
                    SOAP12Constants.Q_ELEM_SOAP_FAULT,
                    com.ibm.wsdl.extensions.soap12.SOAP12FaultImpl.class);
            SOAP12OperationSerializer soap12operationserializer = new SOAP12OperationSerializer();
            registerSerializer(javax.wsdl.BindingOperation.class,
                    SOAP12Constants.Q_ELEM_SOAP_OPERATION,
                    soap12operationserializer);
            registerDeserializer(javax.wsdl.BindingOperation.class,
                    SOAP12Constants.Q_ELEM_SOAP_OPERATION,
                    soap12operationserializer);
            mapExtensionTypes(javax.wsdl.BindingOperation.class,
                    SOAP12Constants.Q_ELEM_SOAP_OPERATION,
                    com.ibm.wsdl.extensions.soap12.SOAP12OperationImpl.class);
            HTTPAddressSerializer httpaddressserializer = new HTTPAddressSerializer();
            registerSerializer(javax.wsdl.Port.class,
                    HTTPConstants.Q_ELEM_HTTP_ADDRESS, httpaddressserializer);
            registerDeserializer(javax.wsdl.Port.class,
                    HTTPConstants.Q_ELEM_HTTP_ADDRESS, httpaddressserializer);
            mapExtensionTypes(javax.wsdl.Port.class,
                    HTTPConstants.Q_ELEM_HTTP_ADDRESS,
                    com.ibm.wsdl.extensions.http.HTTPAddressImpl.class);
            HTTPOperationSerializer httpoperationserializer = new HTTPOperationSerializer();
            registerSerializer(javax.wsdl.BindingOperation.class,
                    HTTPConstants.Q_ELEM_HTTP_OPERATION, httpoperationserializer);
            registerDeserializer(javax.wsdl.BindingOperation.class,
                    HTTPConstants.Q_ELEM_HTTP_OPERATION, httpoperationserializer);
            mapExtensionTypes(javax.wsdl.BindingOperation.class,
                    HTTPConstants.Q_ELEM_HTTP_OPERATION,
                    com.ibm.wsdl.extensions.http.HTTPOperationImpl.class);
            HTTPBindingSerializer httpbindingserializer = new HTTPBindingSerializer();
            registerSerializer(javax.wsdl.Binding.class,
                    HTTPConstants.Q_ELEM_HTTP_BINDING, httpbindingserializer);
            registerDeserializer(javax.wsdl.Binding.class,
                    HTTPConstants.Q_ELEM_HTTP_BINDING, httpbindingserializer);
            mapExtensionTypes(javax.wsdl.Binding.class,
                    HTTPConstants.Q_ELEM_HTTP_BINDING,
                    com.ibm.wsdl.extensions.http.HTTPBindingImpl.class);
            HTTPUrlEncodedSerializer httpurlencodedserializer = new HTTPUrlEncodedSerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    HTTPConstants.Q_ELEM_HTTP_URL_ENCODED, httpurlencodedserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    HTTPConstants.Q_ELEM_HTTP_URL_ENCODED, httpurlencodedserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    HTTPConstants.Q_ELEM_HTTP_URL_ENCODED,
                    com.ibm.wsdl.extensions.http.HTTPUrlEncodedImpl.class);
            HTTPUrlReplacementSerializer httpurlreplacementserializer = new HTTPUrlReplacementSerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    HTTPConstants.Q_ELEM_HTTP_URL_REPLACEMENT,
                    httpurlreplacementserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    HTTPConstants.Q_ELEM_HTTP_URL_REPLACEMENT,
                    httpurlreplacementserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    HTTPConstants.Q_ELEM_HTTP_URL_REPLACEMENT,
                    com.ibm.wsdl.extensions.http.HTTPUrlReplacementImpl.class);
            MIMEContentSerializer mimecontentserializer = new MIMEContentSerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT, mimecontentserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT, mimecontentserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT,
                    com.ibm.wsdl.extensions.mime.MIMEContentImpl.class);
            registerSerializer(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT, mimecontentserializer);
            registerDeserializer(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT, mimecontentserializer);
            mapExtensionTypes(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT,
                    com.ibm.wsdl.extensions.mime.MIMEContentImpl.class);
            registerSerializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT, mimecontentserializer);
            registerDeserializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT, mimecontentserializer);
            mapExtensionTypes(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_CONTENT,
                    com.ibm.wsdl.extensions.mime.MIMEContentImpl.class);
            MIMEMultipartRelatedSerializer mimemultipartrelatedserializer = new MIMEMultipartRelatedSerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    mimemultipartrelatedserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    mimemultipartrelatedserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    com.ibm.wsdl.extensions.mime.MIMEMultipartRelatedImpl.class);
            registerSerializer(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    mimemultipartrelatedserializer);
            registerDeserializer(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    mimemultipartrelatedserializer);
            mapExtensionTypes(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    com.ibm.wsdl.extensions.mime.MIMEMultipartRelatedImpl.class);
            registerSerializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    mimemultipartrelatedserializer);
            registerDeserializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    mimemultipartrelatedserializer);
            mapExtensionTypes(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_MULTIPART_RELATED,
                    com.ibm.wsdl.extensions.mime.MIMEMultipartRelatedImpl.class);
            mapExtensionTypes(
                    javax.wsdl.extensions.mime.MIMEMultipartRelated.class,
                    MIMEConstants.Q_ELEM_MIME_PART,
                    com.ibm.wsdl.extensions.mime.MIMEPartImpl.class);
            MIMEMimeXmlSerializer mimemimexmlserializer = new MIMEMimeXmlSerializer();
            registerSerializer(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML, mimemimexmlserializer);
            registerDeserializer(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML, mimemimexmlserializer);
            mapExtensionTypes(javax.wsdl.BindingInput.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML,
                    com.ibm.wsdl.extensions.mime.MIMEMimeXmlImpl.class);
            registerSerializer(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML, mimemimexmlserializer);
            registerDeserializer(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML, mimemimexmlserializer);
            mapExtensionTypes(javax.wsdl.BindingOutput.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML,
                    com.ibm.wsdl.extensions.mime.MIMEMimeXmlImpl.class);
            registerSerializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML, mimemimexmlserializer);
            registerDeserializer(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML, mimemimexmlserializer);
            mapExtensionTypes(javax.wsdl.extensions.mime.MIMEPart.class,
                    MIMEConstants.Q_ELEM_MIME_MIME_XML,
                    com.ibm.wsdl.extensions.mime.MIMEMimeXmlImpl.class);

        }
    }
}
