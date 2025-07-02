package com.intrasoft.sdmx.converter.services;

import com.intrasoft.commons.ui.services.ConfigService;
import com.intrasoft.sdmx.converter.sdmxsource.RestSdmxBeanRetrievalManager;
import com.intrasoft.sdmx.converter.services.exceptions.RegistryConnectionException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.constants.STRUCTURE_QUERY_DETAIL;
import org.sdmxsource.sdmx.api.constants.STRUCTURE_REFERENCE_DETAIL;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationByProxyFactory;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.query.RESTStructureQuery;
import org.sdmxsource.sdmx.querybuilder.builder.StructureQueryBuilderRest;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.reference.RESTStructureQueryImpl;
import org.sdmxsource.sdmx.util.beans.reference.MaintainableRefBeanImpl;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

/**
 * registry service
 *
 * @author dragos balan
 */
@Service
public class RestRegistryService implements RegistryService {

	private static Logger logger = LogManager.getLogger(RestRegistryService.class);

	@Autowired
	private StructureQueryBuilderRest restQueryBuilder;

	@Autowired
	private StructureParsingManager structureParsingManager;

	@Autowired
	@Qualifier("readableDataLocationFactory")
	private ReadableDataLocationFactory readableLocationFactory;

	@Autowired
	@Qualifier("readableDataLocationByProxyFactory")
	private ReadableDataLocationByProxyFactory readableDataLocationByProxyFactory;


	@Autowired
	private ConfigService configService;

	/**
	 * retrieves the structure stubs from the provided registry rest url
	 *
	 * @param restUrl       the rest endpoint of the registry
	 * @param structureType the type of structure to be retrieved from registry
	 * @return
	 */
	public <T extends MaintainableBean> Set<T> retrieveAllStructureStubs(String restUrl, Class<T> structureType)
			throws RegistryConnectionException {
		Set<T> setOfResults = null;
		try {
			RestSdmxBeanRetrievalManager restRetrievalManager = createRestRetrievalManagerInstance(restUrl);
			MaintainableRefBean maintainablRefBean = new MaintainableRefBeanImpl();

			if (configService.isProxyEnabled()) {
				setOfResults = restRetrievalManager.getMaintainableBeans(structureType,
						maintainablRefBean,
						false,
						true,
						configService.getProxyHost(),
						configService.getProxyPort(),
						configService.getProxyUsername(),
						configService.getProxyPassword(),
						configService.getProxyExclusions(),
						configService.getJksPath(),
						configService.getJksPassword());
			} else {
				setOfResults = restRetrievalManager.getMaintainableBeans(structureType,
						maintainablRefBean,
						false,
						true,
						configService.getJksPath(),
						configService.getJksPassword());
			}


		} catch (RuntimeException runtimeExc) {
			//f#ckin' sdmx source throws the generic RuntimeException for url connections
			//so this is a workaround
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}

		return setOfResults;
	}

	/**
	 * retrieves the stubs for all datastructures found at the given registry url
	 *
	 * @param url
	 * @return
	 */
	public Set<DataStructureBean> retrieveAllDataStructureStubs(String url) throws RegistryConnectionException {
		logger.info("retrieving all data structures from url {}", url);
		return retrieveAllStructureStubs(url, DataStructureBean.class);
	}

	/**
	 * retrieves the stubs for all dataflows found in the given registry
	 *
	 * @param url
	 * @return
	 */
	public Set<DataflowBean> retrieveAllDataflowStubs(String url) throws RegistryConnectionException {
		logger.info("retrieving all dataflows from url {}", url);
		return retrieveAllStructureStubs(url, DataflowBean.class);
	}

	/**
	 * @param isDataflow
	 * @param registryUrl
	 * @return
	 */
	public Set<StructureIdentifier> retrieveStructureStubs(boolean isDataflow, String registryUrl)
			throws RegistryConnectionException {
		Set<? extends MaintainableBean> artefactStubs = null;
		if (isDataflow) {
			artefactStubs = retrieveAllDataflowStubs(registryUrl);
		} else {
			//data structure
			artefactStubs = retrieveAllDataStructureStubs(registryUrl);
		}
		Set<StructureIdentifier> structureIdentifiers = new HashSet<>(artefactStubs.size());
		for (MaintainableBean stub : artefactStubs) {
			logger.trace("parsing structure {} ", stub);
			structureIdentifiers.add(new StructureIdentifier(stub.getAgencyId(), stub.getId(), stub.getVersion()));
		}
		return structureIdentifiers;
	}

	/**
	 * @param restUrl
	 * @param structIdentifier
	 * @return
	 */
	public SdmxBeans retrieveFullDetailsForSingleDSD(String restUrl, StructureIdentifier structIdentifier)
			throws RegistryConnectionException {
		logger.info("retrieving data structure from registry {} for identifier = {}", restUrl, structIdentifier);
		SdmxBeans result = null;
		try {
			RestSdmxBeanRetrievalManager restRetrievalManager = createRestRetrievalManagerInstance(restUrl);
			StructureReferenceBean uniqueIdentifier = new StructureReferenceBeanImpl(
					new MaintainableRefBeanImpl(structIdentifier.getAgency(),
							structIdentifier.getArtefactId(),
							structIdentifier.getArtefactVersion()),
					SDMX_STRUCTURE_TYPE.DSD);
			RESTStructureQuery query = new RESTStructureQueryImpl(STRUCTURE_QUERY_DETAIL.FULL,
					STRUCTURE_REFERENCE_DETAIL.DESCENDANTS,
					null,
					uniqueIdentifier,
					false);
			//compute the result
			if (configService.isProxyEnabled()) {
				result = restRetrievalManager.getMaintainables(
						query,
						configService.getProxyHost(),
						configService.getProxyPort(),
						configService.getProxyUsername(),
						configService.getProxyPassword(),
						configService.getProxyExclusions(),
						configService.getJksPath(),
						configService.getJksPassword());
			} else {
				result = restRetrievalManager.getMaintainables(
						query,
						configService.getJksPath(),
						configService.getJksPassword());
			}
			//result = restRetrievalManager.getMaintainables(query);
		} catch (RuntimeException runtimeExc) {
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}
		return result;
	}

	/**
	 * @param restUrl
	 * @param structIdentifier
	 * @return
	 */
	public SdmxBeans retrieveFullDetailsForSingleDataflow(String restUrl, StructureIdentifier structIdentifier)
			throws RegistryConnectionException {
		logger.info("retrieving dataflow from registry {} for identifier={}", restUrl, structIdentifier);
		SdmxBeans result = null;
		try {
			RestSdmxBeanRetrievalManager restRetrievalManager = createRestRetrievalManagerInstance(restUrl);
			StructureReferenceBean uniqueIdentifier = new StructureReferenceBeanImpl(
					new MaintainableRefBeanImpl(structIdentifier.getAgency(),
							structIdentifier.getArtefactId(),
							structIdentifier.getArtefactVersion()),
					SDMX_STRUCTURE_TYPE.DATAFLOW);
			RESTStructureQuery query = new RESTStructureQueryImpl(STRUCTURE_QUERY_DETAIL.FULL,
					STRUCTURE_REFERENCE_DETAIL.DESCENDANTS,
					null,
					uniqueIdentifier,
					false);
			//compute the result
			if (configService.isProxyEnabled()) {
				result = restRetrievalManager.getMaintainables(
						query,
						configService.getProxyHost(),
						configService.getProxyPort(),
						configService.getProxyUsername(),
						configService.getProxyPassword(),
						configService.getProxyExclusions(),
						configService.getJksPath(),
						configService.getJksPassword());
			} else {
				result = restRetrievalManager.getMaintainables(
						query,
						configService.getJksPath(),
						configService.getJksPassword());
			}
			//result = restRetrievalManager.getMaintainables(query);
		} catch (RuntimeException runtimeExc) {
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}
		return result;
	}

	public SdmxBeans retrieveReferencesForSingleDataflow(String restUrl, StructureIdentifier structIdentifier) throws RegistryConnectionException {
		logger.info("retrieving constraints from registry {} for identifier={}", restUrl, structIdentifier);
		SdmxBeans result = null;
		try {
			RestSdmxBeanRetrievalManager restRetrievalManager = createRestRetrievalManagerInstance(restUrl);
			StructureReferenceBean uniqueIdentifier = new StructureReferenceBeanImpl(
					new MaintainableRefBeanImpl(structIdentifier.getAgency(),
							structIdentifier.getArtefactId(),
							structIdentifier.getArtefactVersion()),
					SDMX_STRUCTURE_TYPE.DATAFLOW);
			RESTStructureQuery query = new RESTStructureQueryImpl(STRUCTURE_QUERY_DETAIL.FULL,
					STRUCTURE_REFERENCE_DETAIL.SPECIFIC,
					SDMX_STRUCTURE_TYPE.CONTENT_CONSTRAINT,
					uniqueIdentifier,
					false);
			//compute the result
			if (configService.isProxyEnabled()) {
				result = restRetrievalManager.getMaintainables(
						query,
						configService.getProxyHost(),
						configService.getProxyPort(),
						configService.getProxyUsername(),
						configService.getProxyPassword(),
						configService.getProxyExclusions(),
						configService.getJksPath(),
						configService.getJksPassword());
			} else {
				result = restRetrievalManager.getMaintainables(
						query,
						configService.getJksPath(),
						configService.getJksPassword());
			}
			//result = restRetrievalManager.getMaintainables(query);
		} catch (RuntimeException runtimeExc) {
			throw new RegistryConnectionException(runtimeExc.getMessage(), runtimeExc);
		}
		return result;
	}

	/**
	 * @param restEndpointUrl
	 * @return
	 */
	private RestSdmxBeanRetrievalManager createRestRetrievalManagerInstance(String restEndpointUrl) {
		RestSdmxBeanRetrievalManager restRetrievalManager = new RestSdmxBeanRetrievalManager();
		restRetrievalManager.setRestUrl(restEndpointUrl);
		restRetrievalManager.setStructureParsingManager(structureParsingManager);
		restRetrievalManager.setReadableLocationFactory(readableLocationFactory);
		restRetrievalManager.setReadableLocationByProxyFactory(readableDataLocationByProxyFactory);
		restRetrievalManager.setStructureQueryBuilder(restQueryBuilder);
		return restRetrievalManager;
	}
}
