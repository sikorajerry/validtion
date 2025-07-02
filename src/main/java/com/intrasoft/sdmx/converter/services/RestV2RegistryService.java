/**
 * Copyright (c) 2015 European Commission.
 * <p>
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl5
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.services;

import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.sdmx.converter.services.exceptions.RegistryConnectionException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.sdmx.structureretrieval.manager.RestV2CommonSdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.builder.common.CommonStructureQueryBuilder;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.constants.STRUCTURE_OUTPUT_FORMAT;
import org.sdmxsource.sdmx.api.factory.CertificateSettings;
import org.sdmxsource.sdmx.api.factory.ProxySettings;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.manager.query.common.StructureQueryWriterManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.reference.common.CommonStructureQuery;
import org.sdmxsource.sdmx.sdmxbeans.builder.common.structure.RestCommonStructureQueryBuilderV2;
import org.sdmxsource.sdmx.sdmxbeans.builder.common.structure.RestStructureQueryParamsV2;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This service class is responsible for interacting with the registry using REST V2.
 * It provides methods to retrieve structure stubs, data structure stubs, dataflow stubs, and full details for single DSD and dataflow.
 * It also provides methods to retrieve structure stubs and references for a single dataflow.
 * @see <a href="https://github.com/sdmx-twg/sdmx-rest/blob/master/doc/structures.md#syntax">Query Suntax</a>
 */
@Service
public class RestV2RegistryService implements RegistryService {

	private static final Logger logger = LogManager.getLogger(RestV2RegistryService.class);

	@Autowired
	private StructureParsingManager structureParsingManager;

	@Autowired
	private ReadableDataLocationFactory readableDataLocationFactory;

	@Autowired
	private StructureQueryWriterManager structureQueryCommonBuilderManager;

	@Autowired
	private ConfigService configService;

	/**
	 * Retrieves the structure stubs from the provided registry rest url.
	 *
	 * @param url           The rest endpoint of the registry
	 * @param structureType The type of structure to be retrieved from registry
	 * @return MaintainableBean
	 */
	@Override
	public <T extends MaintainableBean> Set<T> retrieveAllStructureStubs(String url, Class<T> structureType) throws RegistryConnectionException {
		Set<T> maintainables;
		try {
			RestV2CommonSdmxBeanRetrievalManager retrievalManager = configureRetrievalManager(url);
			SDMX_STRUCTURE_TYPE type = (ObjectUtil.validObject(structureType)) ? SDMX_STRUCTURE_TYPE.parseClass(structureType) : SDMX_STRUCTURE_TYPE.ANY;
			String structure = type.getUrnClass();
			String agencyID = "all";
			String resourceID = "all";
			String version = "all";
			String childRefs = null;
			String detail = "allstubs";
			String references = "none";
			maintainables = (Set<T>) retrievalManager.getMaintainables(createQuery(structure, agencyID, resourceID, version, childRefs, detail, references)).getMaintainables(type);
		} catch (RuntimeException | MalformedURLException runtimeExc) {
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}
		return maintainables;
	}

	/**
	 * This method is used to create a common structure query.
	 *
	 * @param structure The structure of the query.
	 * @param agencyID The ID of the agency.
	 * @param resourceID The ID of the resource.
	 * @param version The version of the query.
	 * @param childRefs The child references of the query.
	 * @param detail The detail of the query.
	 * @param references The references of the query.
	 * @return Returns a CommonStructureQuery object.
	 */
	private CommonStructureQuery createQuery (
			final String structure,
			final String agencyID,
			final String resourceID,
			final String version,
			final String childRefs,
			final String detail,
			final String references) {

		// Initialize the CommonStructureQuery object
		CommonStructureQuery commonStructureQuery;

		// Create a new instance of RestCommonStructureQueryBuilderV2
		CommonStructureQueryBuilder commonStructureQueryBuilderV2 = new RestCommonStructureQueryBuilderV2();

		// Create a new instance of RestStructureQueryParamsV2 with the provided parameters
		RestStructureQueryParamsV2 restStructureQueryParamsV2 = new RestStructureQueryParamsV2(STRUCTURE_OUTPUT_FORMAT.SDMX_V3_STRUCTURE_DOCUMENT, structure, agencyID, resourceID, version, detail, references, childRefs);

		// Build the CommonStructureQuery using the builder and the parameters
		commonStructureQuery = commonStructureQueryBuilderV2.buildCommonStructureQuery(restStructureQueryParamsV2);

		// Return the built CommonStructureQuery
		return commonStructureQuery;
	}


	/**
	 * Retrieves the stubs for all data Structures found at the given registry url.
	 *
	 * @param url The rest endpoint of the registry
	 * @return Set<DataStructureBean>
	 */
	@Override
	public Set<DataStructureBean> retrieveAllDataStructureStubs(String url) throws RegistryConnectionException {
		logger.info("retrieving all data structures from url {}", url);
		return retrieveAllStructureStubs(url, DataStructureBean.class);
	}

	/**
	 * Retrieves the stubs for all Dataflows found at the given registry url.
	 *
	 * @param url The rest endpoint of the registry
	 * @return Set<DataflowBean>
	 */
	@Override
	public Set<DataflowBean> retrieveAllDataflowStubs(String url) throws RegistryConnectionException {
		logger.info("retrieving all dataflows from url {}", url);
		return retrieveAllStructureStubs(url, DataflowBean.class);
	}

	@Override
	public Set<StructureIdentifier> retrieveStructureStubs(boolean isDataflow, String registryUrl) throws RegistryConnectionException {
		Set<? extends MaintainableBean> artefactStubs;
		if (isDataflow) {
			artefactStubs = retrieveAllDataflowStubs(registryUrl);
		} else {
			artefactStubs = retrieveAllDataStructureStubs(registryUrl);
		}
		Set<StructureIdentifier> structureIdentifiers = new HashSet<>(artefactStubs.size());
		for (MaintainableBean stub : artefactStubs) {
			logger.trace("parsing structure {} ", stub);
			structureIdentifiers.add(new StructureIdentifier(stub.getAgencyId(), stub.getId(), stub.getVersion()));
		}
		return structureIdentifiers;
	}

	@Override
	public SdmxBeans retrieveFullDetailsForSingleDSD(String url, StructureIdentifier structIdentifier) throws RegistryConnectionException {
		logger.info("retrieving data structure from registry {} for identifier = {}", url, structIdentifier);
		SdmxBeans maintainables;
		try {
			RestV2CommonSdmxBeanRetrievalManager retrievalManager = configureRetrievalManager(url);
			SDMX_STRUCTURE_TYPE type = SDMX_STRUCTURE_TYPE.DSD;
			String structure = type.getUrnClass();
			String agencyID = structIdentifier.getAgency();
			String resourceID = structIdentifier.getArtefactId();
			String version = structIdentifier.getArtefactVersion();
			String childRefs = null;
			String detail = "full";
			String references = "descendants";
			if(structIdentifier.getArtefactVersion().contains("+")){
				version = modifyVersion(version);
			}
			maintainables = retrievalManager.getMaintainables(createQuery(structure, agencyID, resourceID, version, childRefs, detail, references));
		} catch (RuntimeException | MalformedURLException runtimeExc) {
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}
		return maintainables;
	}

	@Override
	public SdmxBeans retrieveFullDetailsForSingleDataflow(String url, StructureIdentifier structIdentifier) throws RegistryConnectionException {
		logger.info("retrieving dataflow from registry {} for identifier = {}", url, structIdentifier);
		SdmxBeans maintainables;
		try {
			RestV2CommonSdmxBeanRetrievalManager retrievalManager = configureRetrievalManager(url);
			SDMX_STRUCTURE_TYPE type = SDMX_STRUCTURE_TYPE.DATAFLOW;
			String structure = type.getUrnClass();
			String agencyID = structIdentifier.getAgency();
			String resourceID = structIdentifier.getArtefactId();
			String version = structIdentifier.getArtefactVersion();
			String childRefs = null;
			String detail = "full";
			String references = "descendants";
			if(structIdentifier.getArtefactVersion().contains("+")){
				version = modifyVersion(version);
			}
			maintainables = retrievalManager.getMaintainables(createQuery(structure, agencyID, resourceID, version, childRefs, detail, references));
		} catch (RuntimeException | MalformedURLException runtimeExc) {
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}
		return maintainables;
	}

	/**
	 * This method retrieves Content Constraints only for a single dataflow from a registry.
	 * It configures the retrieval manager with the provided URL, and then fetches the maintainable artefacts.
	 * This is used for efficiency.
	 * @param url The URL of the registry to retrieve data from.
	 * @param structIdentifier The structure identifier of the dataflow.
	 * @return SdmxBeans The maintainable artefacts retrieved from the registry.
	 * @throws RegistryConnectionException If there is an error connecting to the registry.
	 */
	@Override
	public SdmxBeans retrieveReferencesForSingleDataflow(String url, StructureIdentifier structIdentifier) throws RegistryConnectionException {
		// Log the retrieval action
		logger.info("retrieving constraints from registry {} for identifier = {}", url, structIdentifier);
		SdmxBeans maintainables;
		try {
			// Configure the retrieval manager with the provided URL
			RestV2CommonSdmxBeanRetrievalManager retrievalManager = configureRetrievalManager(url);
			// Define the structure type as DATAFLOW
			SDMX_STRUCTURE_TYPE type = SDMX_STRUCTURE_TYPE.DATAFLOW;
			String structure = type.getUrnClass();
			String agencyID = structIdentifier.getAgency();
			String resourceID = structIdentifier.getArtefactId();
			String version = structIdentifier.getArtefactVersion();
			String childRefs = null;
			String detail = "full";
			// Fetch only the constraints
			String references = "dataconstraint"; //This is not referenced in documentation
			// Get the maintainable artefacts from the retrieval manager
			if(structIdentifier.getArtefactVersion().contains("+")){
				version = modifyVersion(version);
			}
			maintainables = retrievalManager.getMaintainables(createQuery(structure, agencyID, resourceID, version, childRefs, detail, references));
		} catch (RuntimeException | MalformedURLException runtimeExc) {
			// Throw a new exception if there is an error connecting to the registry
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}
		return maintainables;
	}


	private RestV2CommonSdmxBeanRetrievalManager configureRetrievalManager(String url) throws MalformedURLException {

		RestV2CommonSdmxBeanRetrievalManager retrievalManager = new RestV2CommonSdmxBeanRetrievalManager(
				url,
				structureParsingManager,
				readableDataLocationFactory,
				structureQueryCommonBuilderManager);
		//Set if certificate is needed
		CertificateSettings certificateSettings = null;
		if (ObjectUtil.validString(configService.getJksPassword())) {
			URL registryBaseUrl = new URL(url);
			boolean isSecure = registryBaseUrl.getProtocol().equalsIgnoreCase("https");
			certificateSettings = new CertificateSettings(configService.getJksPassword(), configService.getJksPath(), isSecure);
		}
		//Set if proxy is needed
		ProxySettings settings = null;
		if (configService.isProxyEnabled()) {
			if (ObjectUtil.validString(configService.getProxyUsername(), configService.getProxyPassword(), configService.getProxyHost())) {
				settings = new ProxySettings();
				settings.setUsername(configService.getProxyUsername());
				settings.setPassword(configService.getProxyPassword());
				settings.setHost(configService.getProxyHost());
				settings.setExclusions(configService.getProxyExclusions());
				settings.setPort(configService.getProxyPort());
			}
		}
		retrievalManager.setCertificateSettings(certificateSettings);
		retrievalManager.setProxySettings(settings);
		//If we don't have proxy but a login is required. We don't have a test scenario for the time being
		if (!ObjectUtil.validObject(settings) && ObjectUtil.validString(configService.getRegistryUsername(), configService.getRegistryPassword())) {
			retrievalManager.setUsername(configService.getRegistryUsername());
			retrievalManager.setPassword(configService.getRegistryPassword());
		}
		return retrievalManager;
	}

	public static String modifyVersion(String version) {
		// Split the 'version' string into parts based on the provided regular expression.
		// The regular expression splits the string at positions where there is a '+' symbol behind or a digit ahead.
		// SDMXCONV-1526
		String[] parts = version.split("(?<=\\+)|(?=\\d)");
		// Check if there are exactly two parts and in the last part exists +
		if (parts.length == 2 && parts[1].contains("+")) {
			// Modify the version by appending ".0" to the last part
			parts[1] += ".0";
			return String.join("", parts);
		} else {
			// Return the original version if it doesn't match the pattern
			return version;
		}
	}
}
