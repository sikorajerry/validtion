package com.intrasoft.sdmx.converter.ws;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.SDMX_ERROR_CODE;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.util.ObjectUtil;

import javax.xml.soap.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;

/**
 * Class to send a SOAPMessage request with HttpClient.
 * <p>We prefer the implementation of HttpClient because it can be extended to use authentication and security certificates.</p>
 */
public class WsSoapClient {

	private static Logger logger = LogManager.getLogger(WsSoapClient.class);

	/**
	 * Method that writes the SOAPMessage in a file,
	 * sets the entity in an HttpClient
	 * and sends a simple POST.
	 *
	 * @param message SOAPMessage
	 * @param url     Web Service URL
	 * @return SOAPMessage The response
	 * @throws Exception
	 */
	public static SOAPMessage sendRequest(final SOAPMessage message, String url) throws Exception {
		File outputFile = File.createTempFile("tempSoapMessage", null);
		FileOutputStream outputStream = null;
		CloseableHttpClient infapiclient = HttpClients.createDefault();
		SOAPMessage response = null;
		try {
			outputStream = new FileOutputStream(outputFile);
			message.writeTo(outputStream);
			// Default client initialization
			RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(100 * 1200) // 1200s
					.setConnectionRequestTimeout(100 * 1200) // 120s
					.build();

			HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
			httpClientBuilder = HttpClients.custom().addInterceptorFirst(new ContentLengthHeaderRemover()).setDefaultRequestConfig(requestConfig);
			infapiclient = httpClientBuilder.build();

			// We need the endpoint with the extension of ws
			HttpPost infapiclientpost = new HttpPost(url);
			HttpResponse infresponse;
			Iterator headers = message.getMimeHeaders().getAllHeaders();

			//Add all headers
			while (headers.hasNext()) {
				Object header = headers.next();
				MimeHeader mh = (MimeHeader) header;
				infapiclientpost.setHeader(mh.getName(), mh.getValue());
			}

			InputStreamEntity entity = new InputStreamEntity(new FileInputStream(outputFile));
			//We don't really know if this configs are required
			entity.setChunked(true); //to send in chunks
			entity.setContentType("multipart/related"); //to enforce multiparts as content
			infapiclientpost.setEntity(entity);
			infresponse = infapiclient.execute(infapiclientpost);
			//Status code
			int errorCode = infresponse.getStatusLine().getStatusCode();
			// Check the response code
			if (errorCode == 401 || errorCode == 407) {
				logger.error("Please provide valid authentication credentials for the target resource." + " Response status: " + errorCode);
				throw new SdmxException("Please provide valid authentication credentials for the target resource."
						+ " Response status: " + errorCode, SDMX_ERROR_CODE.UNAUTHORISED);
			} else if (errorCode >= 400) {
				logger.error("Could not connect to URL: " + url + ". Response status: " + errorCode);
				throw new SdmxException("Could not connect to URL: " + url + ". Response status: " + errorCode, SDMX_ERROR_CODE.INTERNAL_SERVER_ERROR);
			}
			// Create the SOAPMessage from the entity content that we get as response
			response = MessageFactory.newInstance().createMessage(null, infresponse.getEntity().getContent());
			response.saveChanges();
			try {
				SOAPBody body = response.getSOAPBody();
				if (body.hasFault()) {
					logger.error("Response contains Fault: " + response.getSOAPBody().getFault());
					response = null;
				}
			} catch (SOAPException ex) {
				if(ObjectUtil.validObject(ex.getMessage()) && ex.getMessage().contains("Unable to create envelope from given source")) {
					logger.error("Response is not what expected. Check the callback client and try again.");
				} else {
					throw ex;
				}
			}
			// Watch out you can consume it only once so if you print it above you can't read it again
			infapiclient.close();
		} catch (Exception ex) {
			logger.error("General Exception", ex);
		} finally {
			//Close everything really important
			IOUtils.closeQuietly(outputStream);
			FileUtils.deleteQuietly(outputFile);
			infapiclient.close();
		}
		return response;
	}

	/**
	 * Class that is used to avoid ProtocolException("Content-Length header already present").
	 * We intercept the header and remove attribute Content-Length, to avoid be added for the second time.
	 */
	public static class ContentLengthHeaderRemover implements HttpRequestInterceptor {
		@Override
		public void process(HttpRequest request, HttpContext context) {
			request.removeHeaders(HTTP.CONTENT_LEN);// fighting org.apache.http.protocol.RequestContent's ProtocolException("Content-Length header already present");
		}
	}

}
