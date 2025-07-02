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

import org.estat.sdmxsource.sdmx.querybuilder.model.CustomQueryDocumentFormatV20;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.factory.ProxySettings;
import org.sdmxsource.sdmx.api.manager.query.QueryStructureRequestBuilderManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.format.StructureQueryFormat;
import org.sdmxsource.sdmx.api.model.query.RESTStructureQuery;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * SoapSdmxBeanRetrievalManager for SOAP SDMX 2.0
 * 
 * @author Mihaela Munteanu
 * @since 4th August 2017
 */
public class SoapSdmxBeanRetrievalManagerV20 extends SoapSdmxBeanRetrievalManager {
	
	private QueryStructureRequestBuilderManager queryStructureRequestBuilderManager;
	
	public SoapSdmxBeanRetrievalManagerV20() {
		super();		
	}
	
	public SoapSdmxBeanRetrievalManagerV20(String endpoint, String namespace) {
		super(endpoint, namespace);		
	}
	
	
	public SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail) throws Exception {
		StructureQueryFormat<Document> queryFormat = new CustomQueryDocumentFormatV20();
		List<StructureReferenceBean> references = null;
		if (sRef != null ) {
			references = new ArrayList<StructureReferenceBean>();
			references.add(sRef);
		}
		
		final Document wdoc = queryStructureRequestBuilderManager.buildStructureQuery(references, resolveReferences, detail, queryFormat);
		final SDMX_STRUCTURE_TYPE structureType = sRef.getMaintainableStructureType();
		SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType);
		//SDMXCONV-1204
		if(resolveReferences && structureType == SDMX_STRUCTURE_TYPE.DATAFLOW && response != null && response.getDataflows()!=null && response.getDataflows().size()>0){
		    response = retrieveCodelistsOnDataflow(response, queryFormat, sRef, resolveReferences, detail);
        }

		return response;
	}
	
	public SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail,
			ProxySettings proxySet, String jksPath, String jksPassword) throws Exception {
		StructureQueryFormat<Document> queryFormat = new CustomQueryDocumentFormatV20();
		List<StructureReferenceBean> references = null;
		if (sRef != null ) {
			references = new ArrayList<StructureReferenceBean>();
			references.add(sRef);
		}
		
		final Document wdoc = queryStructureRequestBuilderManager.buildStructureQuery(references, resolveReferences, detail, queryFormat);
		final SDMX_STRUCTURE_TYPE structureType = sRef.getMaintainableStructureType();
		SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType, proxySet, jksPath, jksPassword);
		//SDMXCONV-1204
		if(resolveReferences && structureType == SDMX_STRUCTURE_TYPE.DATAFLOW && response != null && response.getDataflows()!=null && response.getDataflows().size()>0){
		    response = retrieveCodelistsOnDataflow(response, queryFormat, sRef, resolveReferences, detail, proxySet, jksPath, jksPassword);
        }

		return response;
	}

	@Override
	public SdmxBeans getSdmxBeans(RESTStructureQuery query, boolean resolveReferences, boolean detail, String jksPath, String jksPassword) throws Exception {
		return null;
	}

	@Override
	public SdmxBeans getSdmxBeans(RESTStructureQuery query, boolean resolveReferences, boolean detail, ProxySettings proxySet, String jksPath, String jksPassword) throws Exception {
		return null;
	}

	public SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail, String jksPath, String jksPassword) throws Exception {
		StructureQueryFormat<Document> queryFormat = new CustomQueryDocumentFormatV20();
		List<StructureReferenceBean> references = null;
		if (sRef != null ) {
			references = new ArrayList<StructureReferenceBean>();
			references.add(sRef);
		}
		
		final Document wdoc = queryStructureRequestBuilderManager.buildStructureQuery(references, resolveReferences, detail, queryFormat);
		final SDMX_STRUCTURE_TYPE structureType = sRef.getMaintainableStructureType();
		SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType, jksPath, jksPassword);
		//SDMXCONV-1204
		if(resolveReferences && structureType == SDMX_STRUCTURE_TYPE.DATAFLOW && response != null && response.getDataflows()!=null && response.getDataflows().size()>0){
			response = retrieveCodelistsOnDataflow(response, queryFormat, sRef, resolveReferences, detail, jksPath, jksPassword);
		}
		return response;
	}

	/**
     * //SDMXCONV-1204
	 * When selecting v2.0 on registry the first time the registry gets queried it does not contain any codelist
	 * we need to rebuild structure query and merge the two sdmxbeans into one
	 * @param inputSdmxBean
	 * @param queryFormat
	 * @param sRef
	 * @param resolveReferences
	 * @param detail
	 * @param args
	 * @return
	 * @throws Exception
	 */
	private SdmxBeans retrieveCodelistsOnDataflow(SdmxBeans inputSdmxBean, StructureQueryFormat<Document> queryFormat,
												  StructureReferenceBean sRef, boolean resolveReferences, boolean detail,
												  Object... args ) throws Exception{

		ArrayList<StructureReferenceBean> dsds = new ArrayList<>();
		inputSdmxBean.getDataflows().forEach(dataflowBean -> dsds.add(dataflowBean.getDataStructureRef()));
		final SDMX_STRUCTURE_TYPE structureType = SDMX_STRUCTURE_TYPE.DSD;
		SdmxBeans newSdmxBeans;

		final Document wdoc = queryStructureRequestBuilderManager.buildStructureQuery(dsds, resolveReferences, detail, queryFormat);

		if(args.length==0){
			// case of first getSdmxBeans
			newSdmxBeans = transformAndSendDocumentRequest(wdoc, structureType);
		}
		else if(args.length==3){
			// case of second getSdmxBeans
			// where args[0], args[1], args[2] -> proxySet, jksPath, jksPassword
			newSdmxBeans = transformAndSendDocumentRequest(wdoc, structureType, (ProxySettings) args[0], (String)args[1], (String)args[2]);
		}
		else {
			// case of third getSdmxBeans and args.length==2
			// where args[0], args[1] -> jksPath, jksPassword
            newSdmxBeans = transformAndSendDocumentRequest(wdoc, structureType, (String) args[0], (String) args[1]);
		}

		inputSdmxBean.merge(newSdmxBeans);

		return inputSdmxBean;

	}

	/**
	 * Get the soap operation name depending on the type. 
	 * @param operationName
	 * @return
	 */
	public String getSoapAction(SDMX_STRUCTURE_TYPE operationName) {
		//not needed for V20
		return null;
	}

	/**
	 * @return the queryStructureRequestBuilderManager
	 */
	public QueryStructureRequestBuilderManager getQueryStructureRequestBuilderManager() {
		return queryStructureRequestBuilderManager;
	}

	/**
	 * @param queryStructureRequestBuilderManager the queryStructureRequestBuilderManager to set
	 */
	public void setQueryStructureRequestBuilderManager(QueryStructureRequestBuilderManager queryStructureRequestBuilderManager) {
		this.queryStructureRequestBuilderManager = queryStructureRequestBuilderManager;
	}
}
