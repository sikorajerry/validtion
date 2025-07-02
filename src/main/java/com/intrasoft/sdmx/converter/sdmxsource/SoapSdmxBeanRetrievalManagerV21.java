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

import java.util.ArrayList;
import java.util.List;

import org.estat.sdmxsource.sdmx.querybuilder.model.ComplexQueryDocumentFormatV21;
import org.sdmxsource.sdmx.api.builder.Builder;
import org.sdmxsource.sdmx.api.constants.COMPLEX_MAINTAINABLE_QUERY_DETAIL;
import org.sdmxsource.sdmx.api.constants.COMPLEX_STRUCTURE_QUERY_DETAIL;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.constants.STRUCTURE_REFERENCE_DETAIL;
import org.sdmxsource.sdmx.api.factory.ProxySettings;
import org.sdmxsource.sdmx.api.manager.query.ComplexStructureQueryBuilderManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.beans.reference.complex.ComplexStructureQuery;
import org.sdmxsource.sdmx.api.model.beans.reference.complex.ComplexStructureQueryMetadata;
import org.sdmxsource.sdmx.api.model.format.StructureQueryFormat;
import org.sdmxsource.sdmx.api.model.query.RESTStructureQuery;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.reference.RESTStructureQueryImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.reference.complex.ComplexStructureQueryImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.reference.complex.ComplexStructureQueryMetadataImpl;
import org.sdmxsource.sdmx.structureparser.builder.query.StructureQuery2ComplexQueryBuilder;
import org.w3c.dom.Document;

/**
 * SoapSdmxBeanREtrievalManager
 * 
 * @author Mihaela Munteanu
 * @since 4th August 2017
 */
public class SoapSdmxBeanRetrievalManagerV21 extends SoapSdmxBeanRetrievalManager {
	
	private ComplexStructureQueryBuilderManager complexStructureQueryBuilderManager;
	
	private static final String GET_DATAFLOW = "GetDataflow";
	private static final String GET_DSD = "GetDataStructure";
	
	public SoapSdmxBeanRetrievalManagerV21() {
		super();		
	}
	
	public SoapSdmxBeanRetrievalManagerV21(String endpoint, String namespace) {
		super(endpoint, namespace);		
	}
	
	
	public SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail) throws Exception {
			RESTStructureQuery structureQuery = new RESTStructureQueryImpl(sRef);
			Builder<ComplexStructureQuery, RESTStructureQuery> transformer = new StructureQuery2ComplexQueryBuilder();
			
			ComplexStructureQuery complexStructureQuery = transformer.build(structureQuery);
			StructureQueryFormat<Document> queryFormat = new ComplexQueryDocumentFormatV21();
			
			List<SDMX_STRUCTURE_TYPE> specificObjects = null;
			
			ComplexStructureQueryMetadata complexStructureQueryMetadataWithDSD =
						new ComplexStructureQueryMetadataImpl(false,
								getComplexStructureQuery(detail), 
								getReferenceDetailLevel(detail),
								getStructureReferenceDetail(resolveReferences),
								specificObjects);
				
			ComplexStructureQuery complexStructureQueryTemp = new ComplexStructureQueryImpl(
					complexStructureQuery.getStructureReference(),
					complexStructureQueryMetadataWithDSD);
			
			complexStructureQuery = complexStructureQueryTemp;
			
			final Document wdoc = complexStructureQueryBuilderManager.buildComplexStructureQuery(complexStructureQuery, queryFormat);
			final SDMX_STRUCTURE_TYPE structureType = sRef.getMaintainableStructureType();
			SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType);

		return response;
	}
	
	/**
	 * Get the soap operation name depending on the type. 
	 * @param operationName
	 * @return
	 */
	public String getSoapAction(SDMX_STRUCTURE_TYPE operationName) {
		switch (operationName) {
		case DATAFLOW:
			return GET_DATAFLOW;
		case DSD: 
			return GET_DSD;
		default:
			return null;
		}
	}
	
	public COMPLEX_MAINTAINABLE_QUERY_DETAIL getReferenceDetailLevel(boolean details) {
		if (details) {
			return COMPLEX_MAINTAINABLE_QUERY_DETAIL.FULL;
		} else {
			return COMPLEX_MAINTAINABLE_QUERY_DETAIL.STUB;
		}
	}
	
	public STRUCTURE_REFERENCE_DETAIL getStructureReferenceDetail(boolean resolveReferences) {
		if (resolveReferences) {
			return STRUCTURE_REFERENCE_DETAIL.DESCENDANTS;
		} else {
			return STRUCTURE_REFERENCE_DETAIL.NONE;
		}
	}
	
	public COMPLEX_STRUCTURE_QUERY_DETAIL getComplexStructureQuery(boolean details) {
		if (details) {
			return COMPLEX_STRUCTURE_QUERY_DETAIL.FULL;
		} else {
			return COMPLEX_STRUCTURE_QUERY_DETAIL.STUB;
		}
	}

	/**
	 * @return the complexStructureQueryBuilderManager
	 */
	public ComplexStructureQueryBuilderManager getComplexStructureQueryBuilderManager() {
		return complexStructureQueryBuilderManager;
	}

	/**
	 * @param complexStructureQueryBuilderManager the complexStructureQueryBuilderManager to set
	 */
	public void setComplexStructureQueryBuilderManager(ComplexStructureQueryBuilderManager complexStructureQueryBuilderManager) {
		this.complexStructureQueryBuilderManager = complexStructureQueryBuilderManager;
	}

	@Override
	public SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail, ProxySettings proxySet, 
			String jksPath, String jksPassword) throws Exception {
			RESTStructureQuery structureQuery = new RESTStructureQueryImpl(sRef);
			Builder<ComplexStructureQuery, RESTStructureQuery> transformer = new StructureQuery2ComplexQueryBuilder();
			
			ComplexStructureQuery complexStructureQuery = transformer.build(structureQuery);
			StructureQueryFormat<Document> queryFormat = new ComplexQueryDocumentFormatV21();

			List<SDMX_STRUCTURE_TYPE> specificObjects = new ArrayList<>();
			specificObjects.add(SDMX_STRUCTURE_TYPE.CONTENT_CONSTRAINT);
			
			ComplexStructureQueryMetadata complexStructureQueryMetadataWithDSD =
						new ComplexStructureQueryMetadataImpl(false,
								getComplexStructureQuery(detail),
								getReferenceDetailLevel(detail),
								getStructureReferenceDetail(resolveReferences),
								specificObjects);
				
			ComplexStructureQuery complexStructureQueryTemp = new ComplexStructureQueryImpl(
					complexStructureQuery.getStructureReference(),
					complexStructureQueryMetadataWithDSD);
			
			complexStructureQuery = complexStructureQueryTemp;
			
			final Document wdoc = complexStructureQueryBuilderManager.buildComplexStructureQuery(complexStructureQuery, queryFormat);
			final SDMX_STRUCTURE_TYPE structureType = sRef.getMaintainableStructureType();
			SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType, proxySet, jksPath, jksPassword);
	
		return response;
	}

	@Override
	public SdmxBeans getSdmxBeans(RESTStructureQuery query, boolean resolveReferences, boolean detail, ProxySettings proxySet,
								  String jksPath, String jksPassword) throws Exception {
		Builder<ComplexStructureQuery, RESTStructureQuery> transformer = new StructureQuery2ComplexQueryBuilder();
		ComplexStructureQuery complexStructureQuery = transformer.build(query);
		StructureQueryFormat<Document> queryFormat = new ComplexQueryDocumentFormatV21();

		List<SDMX_STRUCTURE_TYPE> specificObjects = new ArrayList<>();
		specificObjects.add(SDMX_STRUCTURE_TYPE.CONTENT_CONSTRAINT);

		ComplexStructureQueryMetadata complexStructureQueryMetadataWithDSD =
				new ComplexStructureQueryMetadataImpl(false,
						true,
						getComplexStructureQuery(detail),
						getReferenceDetailLevel(detail),
						STRUCTURE_REFERENCE_DETAIL.SPECIFIC,
						specificObjects);

		ComplexStructureQuery complexStructureQueryTemp = new ComplexStructureQueryImpl(
				complexStructureQuery.getStructureReference(),
				complexStructureQueryMetadataWithDSD);

		complexStructureQuery = complexStructureQueryTemp;

		final Document wdoc = complexStructureQueryBuilderManager.buildComplexStructureQuery(complexStructureQuery, queryFormat);
		final SDMX_STRUCTURE_TYPE structureType = SDMX_STRUCTURE_TYPE.CONTENT_CONSTRAINT;
		SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType, proxySet, jksPath, jksPassword);

		return response;
	}

	@Override
	public SdmxBeans getSdmxBeans(RESTStructureQuery query, boolean resolveReferences, boolean detail, String jksPath, String jksPassword) throws Exception {
			Builder<ComplexStructureQuery, RESTStructureQuery> transformer = new StructureQuery2ComplexQueryBuilder();
			ComplexStructureQuery complexStructureQuery = transformer.build(query);
			StructureQueryFormat<Document> queryFormat = new ComplexQueryDocumentFormatV21();

		List<SDMX_STRUCTURE_TYPE> specificObjects = new ArrayList<>();
		specificObjects.add(SDMX_STRUCTURE_TYPE.CONTENT_CONSTRAINT);
			
			ComplexStructureQueryMetadata complexStructureQueryMetadataWithDSD =
						new ComplexStructureQueryMetadataImpl(false,
								true,
								COMPLEX_STRUCTURE_QUERY_DETAIL.FULL,
								COMPLEX_MAINTAINABLE_QUERY_DETAIL.FULL,
								STRUCTURE_REFERENCE_DETAIL.SPECIFIC,
								specificObjects);
				
			ComplexStructureQuery complexStructureQueryTemp = new ComplexStructureQueryImpl(
					complexStructureQuery.getStructureReference(),
					complexStructureQueryMetadataWithDSD);
			
			complexStructureQuery = complexStructureQueryTemp;
			
			final Document wdoc = complexStructureQueryBuilderManager.buildComplexStructureQuery(complexStructureQuery, queryFormat);
			final SDMX_STRUCTURE_TYPE structureType = SDMX_STRUCTURE_TYPE.CONTENT_CONSTRAINT;
			SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType, jksPath, jksPassword);
	
		return response;
	}

	@Override
	public SdmxBeans getSdmxBeans(StructureReferenceBean sRef, boolean resolveReferences, boolean detail, String jksPath, String jksPassword) throws Exception {
		RESTStructureQuery structureQuery = new RESTStructureQueryImpl(sRef);
		Builder<ComplexStructureQuery, RESTStructureQuery> transformer = new StructureQuery2ComplexQueryBuilder();

		ComplexStructureQuery complexStructureQuery = transformer.build(structureQuery);
		StructureQueryFormat<Document> queryFormat = new ComplexQueryDocumentFormatV21();

		List<SDMX_STRUCTURE_TYPE> specificObjects = null;

		ComplexStructureQueryMetadata complexStructureQueryMetadataWithDSD =
				new ComplexStructureQueryMetadataImpl(false,
						getComplexStructureQuery(detail),
						getReferenceDetailLevel(detail),
						getStructureReferenceDetail(resolveReferences),
						specificObjects);

		ComplexStructureQuery complexStructureQueryTemp = new ComplexStructureQueryImpl(
				complexStructureQuery.getStructureReference(),
				complexStructureQueryMetadataWithDSD);

		complexStructureQuery = complexStructureQueryTemp;

		final Document wdoc = complexStructureQueryBuilderManager.buildComplexStructureQuery(complexStructureQuery, queryFormat);
		final SDMX_STRUCTURE_TYPE structureType = sRef.getMaintainableStructureType();
		SdmxBeans response = transformAndSendDocumentRequest(wdoc, structureType, jksPath, jksPassword);

		return response;
	}
}
