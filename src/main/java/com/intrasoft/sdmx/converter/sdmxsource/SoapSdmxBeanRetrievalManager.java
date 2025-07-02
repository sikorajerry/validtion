package com.intrasoft.sdmx.converter.sdmxsource;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.net.URL;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.soap.DetailEntry;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.MimeHeaders;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPConnection;
import javax.xml.soap.SOAPConnectionFactory;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPFault;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.sdmx.structureparser.SdmxBeansSchemaDecorator;
import org.sdmxsource.sdmx.api.constants.SDMX_ERROR_CODE;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.factory.ProxySettings;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationByProxyFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.query.RESTStructureQuery;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.util.sdmx.SdmxMessageUtil;
import org.sdmxsource.util.factory.CertificatesHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.intrasoft.sdmx.converter.services.exceptions.RegistryConnectionException;

/**
 * SoapSdmxBeanRetrievalManager abstract class
 * 
 * @author Mihaela Munteanu
 * @since 4th August 2017
 */
public abstract class SoapSdmxBeanRetrievalManager {

	private ReadableDataLocationFactory readableDataLocationFactory;

	private ReadableDataLocationByProxyFactory readableByProxyFactory;

	private StructureParsingManager structureParsingManager;

	private String soapURL;
	private String nsiNamespace;
	private String endpoint;
	private boolean isV21Type;

	private static final String NSI_PREFIX = "nsi";

	private static Logger logger = LogManager.getLogger(SoapSdmxBeanRetrievalManager.class);

	public SoapSdmxBeanRetrievalManager() {
		super();
	}

	public SoapSdmxBeanRetrievalManager(String endpoint, String namespace) {
		super();
		this.endpoint = endpoint;
		this.nsiNamespace = namespace;
	}

	public abstract SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail)
			throws Exception;

	public abstract SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail,
			String jksPath, String jksPassword) throws Exception;

	public abstract SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail,
			ProxySettings proxySet, String jksPath, String jksPassword) throws Exception;

	public abstract  SdmxBeans getSdmxBeans(RESTStructureQuery query, boolean resolveReferences, boolean detail, String jksPath, String jksPassword) throws Exception;

	public abstract SdmxBeans getSdmxBeans(RESTStructureQuery query, boolean resolveReferences, boolean detail, ProxySettings proxySet,
								  String jksPath, String jksPassword) throws Exception;

	/**
	 * Get the soap operation name depending on the type.
	 *
	 * @param operationName
	 * @return
	 */
	public abstract String getSoapAction(SDMX_STRUCTURE_TYPE operationName);

	/**
	 * Method delegated to transform the Document object in object that can be sent
	 * as SoapMessage. Obtain the SdmxBeans response.
	 * 
	 * @param wdoc
	 * @param structureType
	 * @return
	 * @throws Exception
	 */
	protected SdmxBeans transformAndSendDocumentRequest(Document wdoc, SDMX_STRUCTURE_TYPE structureType)
			throws Exception {
		// needed to make a transformation to get rid of "DOM Level 3 Not implemented" exception
		DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document wdocDOM3 = builder.newDocument();
		Element newRoot = (Element) wdocDOM3.importNode(wdoc.getDocumentElement(), true);
		wdocDOM3.appendChild(newRoot);

		final SOAPMessage soap = sendRequest(wdocDOM3, structureType);

		Node element = soap.getSOAPBody().getFirstChild();
		if (isV21Type) {
			element = element.getFirstChild();
			while (element.getNodeType() == Node.COMMENT_NODE) {
				element = element.getNextSibling();
			}
		}
		DOMSource source = new DOMSource(element);
		String message = null;
		try(StringWriter stringResult = new StringWriter();){
			//SDMXCONV-848
			Transformer transforming = TransformerFactory.newInstance().newTransformer();
			transforming.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transforming.transform(source, new StreamResult(stringResult));
			message = stringResult.toString();
		}
		StructureWorkspace structureWorkspace = null;
		SDMX_SCHEMA beansVersion = null;
		try(InputStream inStream = IOUtils.toInputStream(message, "UTF-8");){
			ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(inStream);
			structureWorkspace = structureParsingManager.parseStructures(dataLocation);
			//SDMXCONV-792
			beansVersion = SdmxMessageUtil.getSchemaVersion(dataLocation);
		}
		final SdmxBeans response = structureWorkspace.getStructureBeans(true);
		SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(response, beansVersion);
		return beansSchemaDecorator;
	}

	protected SdmxBeans transformAndSendDocumentRequest(Document wdoc, 
														SDMX_STRUCTURE_TYPE structureType,
														ProxySettings proxySet,
														String jksPath,
														String jksPassword) throws Exception {
		// needed to make a transformation to get rid of "DOM Level 3 Not implemented" exception
		DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document wdocDOM3 = builder.newDocument();
		Element newRoot = (Element) wdocDOM3.importNode(wdoc.getDocumentElement(), true);
		wdocDOM3.appendChild(newRoot);

		final SOAPMessage soap = sendRequest(wdocDOM3, structureType, proxySet, jksPath, jksPassword);

		Node element = soap.getSOAPBody().getFirstChild();
		if (isV21Type) {
			element = element.getFirstChild();
			while (element.getNodeType() == Node.COMMENT_NODE) {
				element = element.getNextSibling();
			}
		}
		DOMSource source = new DOMSource(element);
		String message = null;
		try(StringWriter stringResult = new StringWriter();){
			Transformer transforming = TransformerFactory.newInstance().newTransformer();
			//SDMXCONV-848
			transforming.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transforming.transform(source, new StreamResult(stringResult));
			message = stringResult.toString();
		}
		StructureWorkspace structureWorkspace = null;
		SDMX_SCHEMA beansVersion = null;
		try(InputStream inStream = IOUtils.toInputStream(message, "UTF-8");){
			ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(inStream);
			structureWorkspace = structureParsingManager.parseStructures(dataLocation);
			//SDMXCONV-792
            beansVersion = SdmxMessageUtil.getSchemaVersion(dataLocation);
		}
		final SdmxBeans response = structureWorkspace.getStructureBeans(true);
		SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(response, beansVersion);
		return beansSchemaDecorator;
	}

	protected SdmxBeans transformAndSendDocumentRequest(Document wdoc,
														SDMX_STRUCTURE_TYPE structureType,
														String jksPath,
														String jksPassword) throws Exception {
		// needed to make a transformation to get rid of "DOM Level 3 Not implemented exception"
		DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document wdocDOM3 = builder.newDocument();
		Element newRoot = (Element) wdocDOM3.importNode(wdoc.getDocumentElement(), true);
		wdocDOM3.appendChild(newRoot);

		final SOAPMessage soap = sendRequest(wdocDOM3, structureType, jksPath, jksPassword);

		Node element = soap.getSOAPBody().getFirstChild();
		if (isV21Type) {
			element = element.getFirstChild();
			while (element.getNodeType() == Node.COMMENT_NODE) {
				element = element.getNextSibling();
			}
		}
		DOMSource source = new DOMSource(element);
		String message = null;
		try(StringWriter stringResult = new StringWriter();){
			//SDMXCONV-848
			Transformer transforming = TransformerFactory.newInstance().newTransformer();
			transforming.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transforming.transform(source, new StreamResult(stringResult));
			//TransformerFactory.newInstance().newTransformer().transform(source, new StreamResult(stringResult));
			message = stringResult.toString();
		}
		StructureWorkspace structureWorkspace = null;
		
		SDMX_SCHEMA beansVersion = null;
		try(InputStream inStream = IOUtils.toInputStream(message, "UTF-8");){
			ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(inStream);
			structureWorkspace = structureParsingManager.parseStructures(dataLocation);
			//SDMXCONV-792
            beansVersion = SdmxMessageUtil.getSchemaVersion(dataLocation);
		}
		SdmxBeans response = structureWorkspace.getStructureBeans(true);
		return response;
	}

	/**
	 * Constructs a SOAP envelope request, with a body that includes the operation
	 * as element and the W3C Document and saves the SDMX Part of the response to
	 * the {@link #outBuffer}. The W3C Document contains either a SDMX-ML Query or a
	 * SDMX-ML Registry Interface
	 * 
	 * @param request - The W3C Document representation 
	 * 					of a SDMX-ML Query or QueryStructureRequest
	 * @param operationName - The operation name
	 * @return The response from the server or null in case of a SOAP fault
	 * @throws Exception
	 */
	public SOAPMessage sendRequest(final Document request, final SDMX_STRUCTURE_TYPE operationName) 
			throws Exception {
		// Create message
		MessageFactory mf;
		mf = MessageFactory.newInstance();
		final SOAPMessage msg = mf.createMessage();
		final MimeHeaders hd = msg.getMimeHeaders();

		hd.addHeader("SOAPAction", getSoapAction(operationName));
		// Object for message parts
		final SOAPPart part = msg.getSOAPPart();

		final SOAPEnvelope env = part.getEnvelope();
		env.addNamespaceDeclaration("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
		env.addNamespaceDeclaration(NSI_PREFIX, nsiNamespace);

		// Obtain the SOAPEnvelope and header and body elements.
		Node oldRoot = request.removeChild(request.getFirstChild());
		if (isV21Type) {
			final Element operation = request.createElementNS(nsiNamespace,
					String.format("%1$s:%2$s", NSI_PREFIX, getSoapAction(operationName)));
			Element parameter = operation;
			parameter.appendChild(oldRoot);
			request.appendChild(parameter);
		} else {
			request.appendChild(oldRoot);
		}

		final SOAPBody body = env.getBody();

		body.addDocument(request);
		msg.saveChanges();

		try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream();){
			msg.writeTo(outputStream);
			//SDMXCONV-848
			//logger.info(outputStream.toString("UTF-8"));
		}
		SOAPMessage response = null;
		final SOAPConnection connection = SOAPConnectionFactory.newInstance().createConnection();
		try {
			response = connection.call(msg, endpoint);
		} finally {
			connection.close();
		}
		if (response.getSOAPBody().hasFault()) {
			handleSoapFault(response.getSOAPBody().getFault());
			return null;
		}
		return response;
	}

	/**
	 * Method that constructs the soap request 
	 * and establish the connection when proxy is used
	 * @param request
	 * @param operationName
	 * @param proxySet
	 * @param jksPath
	 * @param jksPassword
	 * @return
	 * @throws Exception
	 */
	public SOAPMessage sendRequest( final Document request, 
									final SDMX_STRUCTURE_TYPE operationName, 
									ProxySettings proxySet, 
									final String jksPath,
									final String jksPassword) throws Exception {
		// Create message
		MessageFactory mf;
		mf = MessageFactory.newInstance();
		final SOAPMessage msg = mf.createMessage();
		final MimeHeaders hd = msg.getMimeHeaders();
		hd.addHeader("SOAPAction", getSoapAction(operationName));
		// Object for message parts
		final SOAPPart part = msg.getSOAPPart();
		final SOAPEnvelope env = part.getEnvelope();
		env.addNamespaceDeclaration("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
		env.addNamespaceDeclaration(NSI_PREFIX, nsiNamespace);

		// Obtain the SOAPEnvelope and header and body elements.
		Node oldRoot = request.removeChild(request.getFirstChild());
		if (isV21Type) {
			final Element operation = request.createElementNS(nsiNamespace,
					String.format("%1$s:%2$s", NSI_PREFIX, getSoapAction(operationName)));

			Element parameter = operation;
			parameter.appendChild(oldRoot);
			request.appendChild(parameter);
		} else {
			request.appendChild(oldRoot);
		}
		final SOAPBody body = env.getBody();
		body.addDocument(request);
		msg.saveChanges();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		msg.writeTo(outputStream);
		//logger.info(outputStream.toString("UTF-8"));
		
		// Default client initialization
		HttpClient infapiclient = HttpClients.createDefault();
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(100 * 1200) // 120s
				.setConnectionRequestTimeout(100 * 1200) // 100s
				.build();

		setSoapURL(soapURL.toString());
		URL soapURLnew = new URL(getSoapURL());

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
		
		HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
		httpClientBuilder = HttpClients.custom().setDefaultRequestConfig(requestConfig);
		httpClientBuilder.setProxy(proxy);
		Credentials credentials = new UsernamePasswordCredentials(proxySet.getUsername(), proxySet.getPassword());
		AuthScope authScope = new AuthScope(proxySet.getHost(), proxySet.getPort());

		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(authScope, credentials);
		httpClientBuilder.setDefaultCredentialsProvider(credsProvider);

		// Only if protocol is https load the cacerts file
		if (soapURLnew.getProtocol().toString().equalsIgnoreCase("https")) {
			infapiclient = httpClientBuilder
					.setRoutePlanner(routePlanner)
					.setSSLSocketFactory(CertificatesHandler.loadCertificates(jksPath, jksPassword)).build();
		} else {
			infapiclient = httpClientBuilder
					.setRoutePlanner(routePlanner)
					.build();
		}

		// We need the endpoint with the extension of ws
		HttpPost infapiclientpost = new HttpPost(getSoapURL());
		HttpResponse infresponse = null;		
		//SDMXCONV-848
		infapiclientpost.setHeader("SourceApplication", "application");
		infapiclientpost.setHeader("Content-Type", "text/xml; charset=UTF-8");
		infapiclientpost.setHeader("Accept-Encoding", "UTF-8");
		infapiclientpost.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;"); 
		infapiclientpost.setHeader("SOAPAction", getSoapAction(operationName));
		HttpEntity entity =null;
		try {
			entity = new ByteArrayEntity(outputStream.toString("UTF-8").getBytes("UTF-8"));
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
		
		infapiclientpost.setEntity(entity);
		try {
			infresponse = infapiclient.execute(infapiclientpost);
		} catch (Exception ex) {
			logger.error("Exception: ", ex);
		}

		int errorCode = infresponse.getStatusLine().getStatusCode();
		// Check the response code
		if (errorCode == 401 || errorCode == 407) {
			logger.error("Please provide valid authentication credentials for the target resource." + " Response status: " + errorCode);
			throw new SdmxException("Please provide valid authentication credentials for the target resource." + " Response status: " + errorCode, SDMX_ERROR_CODE.UNAUTHORISED);
		} else if (errorCode >= 400) {
			logger.error("Could not connect to URL: " + soapURL.toString() + ". Response status: " + errorCode);
			throw new SdmxException( "Could not connect to URL: " + soapURL.toString() + ". Response status: " + errorCode, SDMX_ERROR_CODE.INTERNAL_SERVER_ERROR);
		}
		// Create the SOAPMessage from the entity content that we get as response
		SOAPMessage response = MessageFactory.newInstance().createMessage(null, infresponse.getEntity().getContent());
		// Watch out you can consume it only once so if you print it above you can't read it again

		if (response.getSOAPBody().hasFault()) {
			handleSoapFault(response.getSOAPBody().getFault());
			return null;
		}
		return response;
	}

	/**
	 * Method that constructs the soap request and establish the connection 
	 * @param request
	 * @param operationName
	 * @param jksPath
	 * @param jksPassword
	 * @return
	 * @throws Exception
	 */
	public SOAPMessage sendRequest( final Document request,
									final SDMX_STRUCTURE_TYPE operationName,
									final String jksPath, 
									final String jksPassword) throws Exception {
		// Create message
		MessageFactory mf;
		mf = MessageFactory.newInstance();
		final SOAPMessage msg = mf.createMessage();
		final MimeHeaders hd = msg.getMimeHeaders();
		hd.addHeader("SOAPAction", getSoapAction(operationName));
		// Object for message parts
		final SOAPPart part = msg.getSOAPPart();
		final SOAPEnvelope env = part.getEnvelope();
		env.addNamespaceDeclaration("xsi", XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
		env.addNamespaceDeclaration(NSI_PREFIX, nsiNamespace);
		
		// Obtain the SOAPEnvelope and header and body elements.
		Node oldRoot = request.removeChild(request.getFirstChild());
		
		if (isV21Type) {
			final Element operation = request.createElementNS(nsiNamespace,
					String.format("%1$s:%2$s", NSI_PREFIX, getSoapAction(operationName)));

			Element parameter = operation;
			parameter.appendChild(oldRoot);
			request.appendChild(parameter);
		} else {
		/*	For testing with the local ws of registry
		 * if(soapURL.contains("localhost")) {
				final Element operation = request.createElement(String.format("QueryStructure"));
				Element parameter = operation;
				parameter.appendChild(oldRoot);
				request.appendChild(parameter);
			}else {*/
				request.appendChild(oldRoot);
		}			
		final SOAPBody body = env.getBody();
		body.addDocument(request);
		msg.saveChanges();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		msg.writeTo(outputStream);

		//logger.info(outputStream.toString("UTF-8"));

		// Default client initialization
		CloseableHttpClient infapiclient = HttpClients.createDefault();
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(100 * 1200) // 1200s
				.setConnectionRequestTimeout(100 * 1200) // 120s
				.build();

		setSoapURL(soapURL.toString());
		URL soapURLnew = new URL(getSoapURL());
		HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
		httpClientBuilder = HttpClients.custom().setDefaultRequestConfig(requestConfig);

		// Only if protocol is https load the cacerts file
		if (soapURLnew.getProtocol().toString().equalsIgnoreCase("https")) {
			infapiclient = httpClientBuilder.setSSLSocketFactory(CertificatesHandler.loadCertificates(jksPath, jksPassword)).build();
		} else {
			infapiclient = httpClientBuilder.build();
		}
		// We need the endpoint with the extension of ws
		HttpPost infapiclientpost = new HttpPost(getSoapURL());
		HttpResponse infresponse = null;
		//SDMXCONV-848
		infapiclientpost.setHeader("SourceApplication", "application");
		infapiclientpost.setHeader("Content-Type", "text/xml; charset=UTF-8");
		infapiclientpost.setHeader("Accept-Encoding", "UTF-8");
		infapiclientpost.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;"); 
		infapiclientpost.setHeader("SOAPAction", getSoapAction(operationName));
		HttpEntity entity = null;
		try {
			entity = new ByteArrayEntity(outputStream.toString("UTF-8").getBytes("UTF-8"));
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
		infapiclientpost.setEntity(entity);

		//SDMXCONV-1243
		infresponse = infapiclient.execute(infapiclientpost);

		int errorCode = infresponse.getStatusLine().getStatusCode();
		// Check the response code
		if (errorCode == 401 || errorCode == 407) {
			logger.error("Please provide valid authentication credentials for the target resource."
					+ " Response status: " + errorCode);
			throw new SdmxException("Please provide valid authentication credentials for the target resource."
					+ " Response status: " + errorCode, SDMX_ERROR_CODE.UNAUTHORISED);
		} else if (errorCode >= 400) {
			logger.error("Could not connect to URL: " + soapURL.toString() + ". Response status: " + errorCode);
			throw new SdmxException(
					"Could not connect to URL: " + soapURL.toString() + ". Response status: " + errorCode,
					SDMX_ERROR_CODE.INTERNAL_SERVER_ERROR);
		}
		// Create the SOAPMessage from the entity content that we get as response
		SOAPMessage response = MessageFactory.newInstance().createMessage(null, infresponse.getEntity().getContent());
		// Watch out you can consume it only once so if you print it above you can't
		// read it again

		if (response.getSOAPBody().hasFault()) {
			handleSoapFault(response.getSOAPBody().getFault());
			return null;
		}
		return response;
	}

	/**
	 * Handle a SOAP Fault from the WS It will parse the soap details and throw an
	 * NSIClientException
	 * 
	 * @param fault - The soap fault
	 * @throws RegistryConnectionException
	 * @throws RegistryConnectionException - Always
	 */
	protected void handleSoapFault(final SOAPFault fault) throws RegistryConnectionException {
		final StringBuilder error = new StringBuilder("SERVER RESPONSE ERROR: Received a SOAP FAULT:");

		error.append(String.format("%nActor: '%1$s'%nCode: '%2$s'%nFault: '%3$s'%n", fault.getFaultActor(),
				fault.getFaultCode(), fault.getFaultString()));
		if (fault.getDetail() != null) {
			final Iterator<?> entries = fault.getDetail().getDetailEntries();
			while (entries.hasNext()) {
				final DetailEntry entry = (DetailEntry) entries.next();
				error.append(String.format("%1$s : '%2$s'%n", entry.getLocalName(), entry.getValue()));
			}
		}
		logger.error(error.toString());
		throw new RegistryConnectionException(error.toString(), null);
	}

	/**
	 * @return the readableDataLocationFactory
	 */
	public ReadableDataLocationFactory getReadableDataLocationFactory() {
		return readableDataLocationFactory;
	}

	/**
	 * @param readableDataLocationFactory - the readableDataLocationFactory to set
	 */
	public void setReadableDataLocationFactory(ReadableDataLocationFactory readableDataLocationFactory) {
		this.readableDataLocationFactory = readableDataLocationFactory;
	}

	public void setReadableLocationByProxyFactory(ReadableDataLocationByProxyFactory locationFactory) {
		this.readableByProxyFactory = locationFactory;
	}

	/**
	 * @return the structureParsingManager
	 */
	public StructureParsingManager getStructureParsingManager() {
		return structureParsingManager;
	}

	/**
	 * @param structureParsingManager - the structureParsingManager to set
	 */
	public void setStructureParsingManager(StructureParsingManager structureParsingManager) {
		this.structureParsingManager = structureParsingManager;
	}

	/**
	 * @return the soapURL
	 */
	public String getSoapURL() {
		return soapURL;
	}

	/**
	 * @param soapURL - the soapURL to set
	 */
	public void setSoapURL(String soapURL) {	
		// Fix for SDMXCONV-695
		if(soapURL.contains("WSDL")) {
			this.soapURL = soapURL.replace("WSDL", "wsdl");
		}else {
			this.soapURL = soapURL;
		}
	}

	/**
	 * @return the nsiNamespace
	 */
	public String getNsiNamespace() {
		return nsiNamespace;
	}

	/**
	 * @param nsiNamespace - the nsiNamespace to set
	 */
	public void setNsiNamespace(String nsiNamespace) {
		this.nsiNamespace = nsiNamespace;
	}

	/**
	 * @return the endpoint
	 */
	public String getEndpoint() {
		return endpoint;
	}

	/**
	 * @param endpoint
	 *            the endpoint to set
	 */
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	/**
	 * @return the isV21Type
	 */
	public boolean isV21Type() {
		return isV21Type;
	}

	/**
	 * @param isV21Type - the isV21Type to set
	 */
	public void setV21Type(boolean isV21Type) {
		this.isV21Type = isV21Type;
	}
}