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
package com.intrasoft.sdmx.converter.ws;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.util.files.TemporaryFilesUtil;
import org.sdmxsource.util.io.SharedApplicationFolder;

import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.File;
import java.io.IOException;

/**
 * One common RequestListener for HTI WS
 */
public class RequestListenerImpl implements ServletRequestListener {
	private static Logger logger = LogManager.getLogger(RequestListenerImpl.class);

	@Override
	public void requestDestroyed(ServletRequestEvent reqEvt) {
		HttpServletRequest request = (HttpServletRequest) reqEvt.getServletRequest();
		final HttpSession session = request.getSession();
		Boolean isListenerEnabled = (Boolean) session.getAttribute("enableListener");
		if (isListenerEnabled == null) {
			isListenerEnabled = false;  // Default value if not set
		}
		if(isListenerEnabled) {
			File tempFile = (File) session.getAttribute("responseFileName");
			deleteTempFile(tempFile);
			File headerFile = (File) session.getAttribute("headerResponseFile");
			deleteTempFile(headerFile);
			File excelResponseFile = (File) session.getAttribute("excelResponseFile");
			deleteTempFile(excelResponseFile);
			File msppingResponseFile = (File) session.getAttribute("mappingResponseFile");
			deleteTempFile(msppingResponseFile);
		}
	}

	@Override
	public void requestInitialized(ServletRequestEvent reqEvt) {

		HttpServletRequest request = (HttpServletRequest) reqEvt.getServletRequest();
		final HttpSession session = request.getSession();
		Boolean isListenerEnabled = (Boolean) session.getAttribute("enableListener");
		if (isListenerEnabled == null) {
			isListenerEnabled = false;  // Default value if not set
		}

		if(isListenerEnabled) {
			SharedApplicationFolder.setValidation(false);
			File tempDir = new File(SharedApplicationFolder.getLocalFileStorageConversion());
			File responseFile = null;
			File headerResponse = null;
			File excelResponse = null;
			File mappingResponse = null;
			try {
				responseFile = TemporaryFilesUtil.createTempFile("responseFileName", ".xml", tempDir);
				headerResponse = TemporaryFilesUtil.createTempFile("headerResponseFile", ".xml", tempDir);
				excelResponse = TemporaryFilesUtil.createTempFile("excelResponseFile", ".xls", tempDir);
				mappingResponse = TemporaryFilesUtil.createTempFile("mappingResponseFile", ".xml", tempDir);
			} catch (IOException e) {
				logger.error("I/O Exception", e);
			}
			session.setAttribute("responseFileName", responseFile);
			session.setAttribute("headerResponseFile", headerResponse);
			session.setAttribute("excelResponseFile", excelResponse);
			session.setAttribute("mappingResponseFile", mappingResponse);
		}
	}

	private void deleteTempFile(File input) {
		if (input != null && input.exists()) {
			try {
				input.delete();
			} catch (Exception e) {
				logger.error("I/O Exception during deletion", e);
			}
		}
	}

}
