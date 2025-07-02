package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.services.exceptions.RegistryConnectionException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Registry service
 *
 * @author Mihaela Munteanu
 * @since 7th August 2017
 */
@Service
public interface RegistryService {

	/**
	 * retrieves the structure stubs from the provided registry rest url
	 *
	 * @param url       the rest endpoint of the registry
	 * @param structureType the type of structure to be retrieved from registry
	 * @return
	 */
	<T extends MaintainableBean> Set<T> retrieveAllStructureStubs(String url, Class<T> structureType)
			throws RegistryConnectionException;

	/**
	 * retrieves the stubs for all datastructures found at the given registry url
	 *
	 * @param url
	 * @return
	 */
	Set<DataStructureBean> retrieveAllDataStructureStubs(String url) throws RegistryConnectionException;

	/**
	 * retrieves the stubs for all dataflows found in the given registry
	 *
	 * @param url
	 * @return
	 */
	Set<DataflowBean> retrieveAllDataflowStubs(String url) throws RegistryConnectionException;

	/**
	 * @param isDataflow
	 * @param registryUrl
	 * @return
	 */
	Set<StructureIdentifier> retrieveStructureStubs(boolean isDataflow, String registryUrl)
			throws RegistryConnectionException;

	/**
	 * @param restUrl
	 * @param structIdentifier
	 * @return
	 */
	SdmxBeans retrieveFullDetailsForSingleDSD(String restUrl, StructureIdentifier structIdentifier)
			throws RegistryConnectionException;

	/**
	 * @param restUrl
	 * @param structIdentifier
	 * @return
	 */
	SdmxBeans retrieveFullDetailsForSingleDataflow(String restUrl, StructureIdentifier structIdentifier)
			throws RegistryConnectionException;

	/**
	 * Retrieve content constraints for SDMXCONV-1226
	 * @param restUrl
	 * @param structIdentifier
	 * @return
	 */
	SdmxBeans retrieveReferencesForSingleDataflow(String restUrl, StructureIdentifier structIdentifier) throws RegistryConnectionException;

}
