/*******************************************************************************
 * Copyright (c) 2013 Metadata Technology Ltd.
 *  
 * All rights reserved. This program and the accompanying materials are made 
 * available under the terms of the GNU Lesser General Public License v 3.0 
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 * 
 * This file is part of the SDMX Component Library.
 * 
 * The SDMX Component Library is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 * 
 * The SDMX Component Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser 
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License 
 * along with The SDMX Component Library If not, see 
 * http://www.gnu.org/licenses/lgpl.
 * 
 * Contributors:
 * Metadata Technology - initial API and implementation
 ******************************************************************************/
package com.intrasoft.sdmx.converter.sdmxsource;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.sdmx.structureparser.SdmxBeansSchemaDecorator;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.constants.STRUCTURE_QUERY_DETAIL;
import org.sdmxsource.sdmx.api.constants.STRUCTURE_REFERENCE_DETAIL;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.factory.ProxySettings;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationByProxyFactory;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.ResolutionSettings.RESOLVE_CROSS_REFERENCES;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.query.RESTStructureQuery;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.querybuilder.builder.StructureQueryBuilderRest;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.reference.RESTStructureQueryImpl;
import org.sdmxsource.sdmx.structureretrieval.manager.BaseSdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;
import org.sdmxsource.sdmx.util.sdmx.SdmxMessageUtil;
import org.sdmxsource.util.ObjectUtil;

/**
 * this is a replacement for RESTSdmxBeanRetrievalManager which is unusable
 * 
 * @author dragos balan
 *
 */
public class RestSdmxBeanRetrievalManager extends BaseSdmxBeanRetrievalManager {

	private static Logger logger = LogManager.getLogger(RestSdmxBeanRetrievalManager.class);

	private String restURL;

	private StructureQueryBuilderRest restQueryBuilder;

	private StructureParsingManager spm;

	private ReadableDataLocationFactory rdlFactory;

	private ReadableDataLocationByProxyFactory rdlByProxyFactory;

	public RestSdmxBeanRetrievalManager() {
		super();
	}

	@Override
	public SdmxBeans getMaintainables(RESTStructureQuery sQuery) {
		String restQuery = restURL + "/" + restQueryBuilder.buildStructureQuery(sQuery);
		URL url;
		try {
			url = new URL(restQuery);
		} catch (MalformedURLException e) {
			throw new SdmxException(e, "Could not open a connexion to URL: " + restQuery);
		}
		ReadableDataLocation rdl = rdlFactory.getReadableDataLocation(url);
		return spm.parseStructures(rdl).getStructureBeans(false);
	}

	public SdmxBeans getMaintainables(RESTStructureQuery sQuery, String proxyHost, Integer proxyPort,
			String proxyUsername, String proxyPassword, List<String> proxyExclusions, String jksPath, String jksPassword) {
		String restQuery = restURL + "/" + restQueryBuilder.buildStructureQuery(sQuery);
		ProxySettings proxySet = new ProxySettings(proxyUsername, proxyPassword, proxyHost, proxyPort);
		if(!(proxyExclusions.isEmpty() || proxyExclusions==null) ) {
			List<String> excls;
			excls = proxyExclusions;
			proxySet.setExclusions(excls);
		}
		URL url;
		try {
			url = new URL(restQuery);
		} catch (MalformedURLException e) {
			throw new SdmxException(e, "Could not open a connexion to URL: " + restQuery);
		}
		ReadableDataLocation rdl;
		//SDMXCONV-1288
		if(ObjectUtil.validObject(proxySet)
				&& ObjectUtil.validCollection(proxySet.getExclusions())
					&& proxySet.getExclusions().stream().anyMatch(s -> url.toString().startsWith(s))) {
			//if the url we are trying to connect is in exclusions of proxy then connect without it
			rdl = rdlByProxyFactory.getReadableDataLocation(url, jksPath, jksPassword);
		} else {
			rdl = rdlByProxyFactory.getReadableDataLocation(url, proxySet, jksPath, jksPassword);
		}
		SdmxBeans bean = spm.parseStructures(rdl).getStructureBeans(false);
		//SDMXCONV-792
		SDMX_SCHEMA beansVersion = SdmxMessageUtil.getSchemaVersion(rdl);
		SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(bean, beansVersion);
		return beansSchemaDecorator;
	}

	public SdmxBeans getMaintainables(RESTStructureQuery sQuery, String jksPath, String jksPassword) {
		String restQuery = restURL + "/" + restQueryBuilder.buildStructureQuery(sQuery);
		URL url;
		try {
			url = new URL(restQuery);
		} catch (MalformedURLException e) {
			throw new SdmxException(e, "Could not open a connexion to URL: " + restQuery);
		}
		ReadableDataLocation rdl;
			rdl = rdlByProxyFactory.getReadableDataLocation(url, jksPath, jksPassword);
			SdmxBeans bean = spm.parseStructures(rdl).getStructureBeans(false);
			//SDMXCONV-792
			SDMX_SCHEMA beansVersion = SdmxMessageUtil.getSchemaVersion(rdl);
			SdmxBeansSchemaDecorator beansSchemaDecorator = new SdmxBeansSchemaDecorator(bean, beansVersion);

		return beansSchemaDecorator;
	}

	@Override
	public SdmxBeans getSdmxBeans(StructureReferenceBean sRef, RESOLVE_CROSS_REFERENCES resolveCrossReferences) {
		STRUCTURE_REFERENCE_DETAIL refDetail;
		switch (resolveCrossReferences) {
		case DO_NOT_RESOLVE:
			refDetail = STRUCTURE_REFERENCE_DETAIL.NONE;
		default:
			refDetail = STRUCTURE_REFERENCE_DETAIL.DESCENDANTS;
			break;
		}
		STRUCTURE_QUERY_DETAIL queryDetail = STRUCTURE_QUERY_DETAIL.FULL;
		RESTStructureQuery query = new RESTStructureQueryImpl(queryDetail, refDetail, null, sRef, false);
		return getMaintainables(query);
	}

	@SuppressWarnings("unchecked")
	public <T extends MaintainableBean> Set<T> getMaintainableBeans(Class<T> structureType, MaintainableRefBean ref,
			boolean returnLatest, boolean returnStub) {
		SDMX_STRUCTURE_TYPE type = SDMX_STRUCTURE_TYPE.ANY;
		if (structureType != null) {
			type = SDMX_STRUCTURE_TYPE.parseClass(structureType);
		}

		StructureReferenceBean sRef = new StructureReferenceBeanImpl(ref, type);
		STRUCTURE_REFERENCE_DETAIL refDetail = STRUCTURE_REFERENCE_DETAIL.NONE;
		STRUCTURE_QUERY_DETAIL queryDetail = returnStub ? STRUCTURE_QUERY_DETAIL.ALL_STUBS
				: STRUCTURE_QUERY_DETAIL.FULL;
		RESTStructureQuery query = new RESTStructureQueryImpl(queryDetail, refDetail, null, sRef, returnLatest);
		return (Set<T>) getMaintainables(query).getMaintainables(sRef.getMaintainableStructureType());
	}

	@SuppressWarnings("unchecked")
	public <T extends MaintainableBean> Set<T> getMaintainableBeans(Class<T> structureType, MaintainableRefBean ref,
			boolean returnLatest, boolean returnStub, String proxyHost, Integer proxyPort, String proxyUsername,
			String proxyPassword, List<String> proxyExclusions, String jksPath, String jksPassword) {
		SDMX_STRUCTURE_TYPE type = SDMX_STRUCTURE_TYPE.ANY;
		if (structureType != null) {
			type = SDMX_STRUCTURE_TYPE.parseClass(structureType);
		}

		StructureReferenceBean sRef = new StructureReferenceBeanImpl(ref, type);
		STRUCTURE_REFERENCE_DETAIL refDetail = STRUCTURE_REFERENCE_DETAIL.NONE;
		STRUCTURE_QUERY_DETAIL queryDetail = returnStub ? STRUCTURE_QUERY_DETAIL.ALL_STUBS
				: STRUCTURE_QUERY_DETAIL.FULL;
		RESTStructureQuery query = new RESTStructureQueryImpl(queryDetail, refDetail, null, sRef, returnLatest);
		return (Set<T>) getMaintainables(query, proxyHost, proxyPort, proxyUsername, proxyPassword, proxyExclusions, jksPath,
				jksPassword).getMaintainables(sRef.getMaintainableStructureType());
	}

	@SuppressWarnings("unchecked")
	public <T extends MaintainableBean> Set<T> getMaintainableBeans(Class<T> structureType, MaintainableRefBean ref,
			boolean returnLatest, boolean returnStub, String jksPath, String jksPassword) {
		SDMX_STRUCTURE_TYPE type = SDMX_STRUCTURE_TYPE.ANY;
		if (structureType != null) {
			type = SDMX_STRUCTURE_TYPE.parseClass(structureType);
		}

		StructureReferenceBean sRef = new StructureReferenceBeanImpl(ref, type);
		STRUCTURE_REFERENCE_DETAIL refDetail = STRUCTURE_REFERENCE_DETAIL.NONE;
		STRUCTURE_QUERY_DETAIL queryDetail = returnStub ? STRUCTURE_QUERY_DETAIL.ALL_STUBS
				: STRUCTURE_QUERY_DETAIL.FULL;
		RESTStructureQuery query = new RESTStructureQueryImpl(queryDetail, refDetail, null, sRef, returnLatest);
		return (Set<T>) getMaintainables(query, jksPath, jksPassword).getMaintainables(sRef.getMaintainableStructureType());
	}

	public String getRestUrl() {
		return restURL;
	}

	public void setRestUrl(String url) {
		this.restURL = url;
	}

	public void setStructureParsingManager(StructureParsingManager manager) {
		this.spm = manager;
	}

	public void setReadableLocationFactory(ReadableDataLocationFactory locationFactory) {
		this.rdlFactory = locationFactory;
	}

	public void setReadableLocationByProxyFactory(ReadableDataLocationByProxyFactory locationFactory) {
		this.rdlByProxyFactory = locationFactory;
	}

	public void setStructureQueryBuilder(StructureQueryBuilderRest queryBuilder) {
		this.restQueryBuilder = queryBuilder;
	}
}
