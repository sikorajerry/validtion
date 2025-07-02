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

import com.intrasoft.commons.ui.services.JsonEndpoint;
import com.intrasoft.sdmx.converter.util.TypeOfVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
/**
 * Registry Service factory to decide depending on the URL string and 
 * the information form the wsdl which Registry Service to be used.
 * Rest, Rest v2, SOAP 2.0 or SOAP 2.1
 */
@Service
public class RegistryServiceFactory {

	private static Logger logger = LogManager.getLogger(RegistryServiceFactory.class);

	@Autowired
    private RestRegistryService restRegistryService;
	
    @Autowired
	private SoapRegistryService soapRegistryService;

    @Autowired
	private RestV2RegistryService restV2RegistryService;

	public RegistryService getRegistryService(JsonEndpoint endpoint) {
		if(!ObjectUtil.validObject(endpoint.getVersion())) {
			endpoint.setVersion(TypeOfVersion.EMPTY);
		}
		switch (endpoint.getVersion()) {
			case REST_V2:
				return restV2RegistryService;
			case REST_V1: {
				return restRegistryService;
			}
			case SOAP:
				return soapRegistryService;
            default: {
				if(isWsdlUrl(endpoint.getUrl())) {
					return soapRegistryService;
				} else if(isV2Url(endpoint.getUrl())) {
					return restV2RegistryService;
				} else {
					return restRegistryService;
				}
			}
        }
    }

	private boolean isWsdlUrl(String url) {
		//SDMXCONV-1520
		return (ObjectUtil.validString(url) && (url.toUpperCase().endsWith("?WSDL")));
	}

	private boolean isV2Url(String url) {
		//SDMXCONV-1520
		return (ObjectUtil.validString(url) && (url.toUpperCase().endsWith("/V2/") || url.toUpperCase().endsWith("/V2")));
	}
}
