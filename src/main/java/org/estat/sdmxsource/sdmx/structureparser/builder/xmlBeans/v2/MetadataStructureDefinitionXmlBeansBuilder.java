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
package org.estat.sdmxsource.sdmx.structureparser.builder.xmlBeans.v2;

import org.estat.sdmxsource.util.BuilderUtil;
import org.estat.sdmxsource.util.XmlBeansUtil;
import org.sdmx.resources.sdmxml.schemas.v20.common.IDType;
import org.sdmx.resources.sdmxml.schemas.v20.common.TextType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.FullTargetIdentifierType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.IdentifierComponentType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.MetadataStructureDefinitionType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.PartialTargetIdentifierType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.RepresentationSchemeType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.TargetIdentifiersType;
import org.sdmxsource.sdmx.api.builder.Builder;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.exception.SdmxNotImplementedException;
import org.sdmxsource.sdmx.api.model.beans.metadatastructure.IdentifiableTargetBean;
import org.sdmxsource.sdmx.api.model.beans.metadatastructure.MetadataStructureDefinitionBean;
import org.sdmxsource.sdmx.api.model.beans.metadatastructure.MetadataTargetBean;
import org.sdmxsource.sdmx.api.model.beans.metadatastructure.ReportStructureBean;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
import org.sdmxsource.sdmx.structureparser.builder.xmlBeans.v2.ReportStructureXmlBeanBuilder;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service(value="MetadataStructureDefinitionXmlBeansBuilderV2")
public class MetadataStructureDefinitionXmlBeansBuilder extends org.sdmxsource.sdmx.structureparser.builder.xmlBeans.v2.MetadataStructureDefinitionXmlBeansBuilder implements Builder<MetadataStructureDefinitionType, MetadataStructureDefinitionBean> {
	@Autowired 
	private ReportStructureXmlBeanBuilder reportStructureXmlBeanBuilder;
	

	@Override
	public MetadataStructureDefinitionType build(MetadataStructureDefinitionBean buildFrom) throws SdmxException {
		MetadataStructureDefinitionType builtObj = (MetadataStructureDefinitionType) MetadataStructureDefinitionType.Factory.newInstance();
		if(BuilderUtil.validString(buildFrom.getAgencyId())){
			builtObj.setAgencyID(buildFrom.getAgencyId());
		}
		if(BuilderUtil.validString(buildFrom.getId())){
			builtObj.setId(buildFrom.getId());
		}
		if(buildFrom.getUri() != null){
			builtObj.setUri(buildFrom.getUri().toString());
		} else if(buildFrom.getStructureURL() != null){
			builtObj.setUri(buildFrom.getStructureURL().toString());
		} else if(buildFrom.getServiceURL() != null){
			builtObj.setUri(buildFrom.getStructureURL().toString());
		}
		if(BuilderUtil.validString(buildFrom.getUrn())){
			builtObj.setUrn(buildFrom.getUrn());
		}
		if(BuilderUtil.validString(buildFrom.getVersion())){
			builtObj.setVersion(buildFrom.getVersion());
		}
		if(buildFrom.getStartDate() != null) {
			builtObj.setValidFrom(buildFrom.getStartDate().getDate());
		}
		if(buildFrom.getEndDate() != null) {
			builtObj.setValidTo(buildFrom.getEndDate().getDate());
		}
		if(BuilderUtil.validCollection(buildFrom.getNames())) {
			builtObj.setNameArray(BuilderUtil.getTextType(buildFrom.getNames()));
		}
		if(BuilderUtil.validCollection(buildFrom.getDescriptions())) {
			builtObj.setDescriptionArray(BuilderUtil.getTextType(buildFrom.getDescriptions()));
		}
		if(BuilderUtil.hasAnnotations(buildFrom)) {
			builtObj.setAnnotations(BuilderUtil.getAnnotationsType(buildFrom));
		}
		if(buildFrom.isExternalReference().isSet()) {
			builtObj.setIsExternalReference(buildFrom.isExternalReference().isTrue());			
		}
		if(buildFrom.isFinal().isSet()) {
			builtObj.setIsFinal(buildFrom.isFinal().isTrue());
		}
		
		
		
		// extension: metadataTargets
		if(ObjectUtil.validCollection(buildFrom.getMetadataTargets())) {
			TargetIdentifiersType targetIdentifiersObj = builtObj.addNewTargetIdentifiers();
			int i = 0;
			// find the metadata target which has the superset of identifier components, it will act as FullTargetIdentifier
			int maxMetadataTargetSize = 0;
			int fullTargetPosition = 0;
			MetadataTargetBean candidateFullTarget = null;
			PartialTargetIdentifierType[] partialTargets = new PartialTargetIdentifierType[buildFrom.getMetadataTargets().size()];
			
			for(MetadataTargetBean currentMt : buildFrom.getMetadataTargets()) {
				if (currentMt.getKeyDescriptorValuesTargetBean() != null || 
						currentMt.getReportPeriodTargetBean() != null ||
						currentMt.getConstraintContentTargetBean() != null ||
						currentMt.getDataSetTargetBean() != null
						) {
					throw new SdmxNotImplementedException(ExceptionCode.UNSUPPORTED, "MSD contains Metadata Target content incompatible with SMDX v2.0 - please use SDMX v2.1");
				}
				 
				if (currentMt.getIdentifiableTargetBean().size() >= maxMetadataTargetSize) {
					maxMetadataTargetSize = currentMt.getIdentifiableTargetBean().size();
					fullTargetPosition = i;
					candidateFullTarget = currentMt;
				}
				partialTargets[i] = buildPartialTargetIdentifier(currentMt);
				i++;
			}
			targetIdentifiersObj.setPartialTargetIdentifierArray(partialTargets);
			targetIdentifiersObj.removePartialTargetIdentifier(fullTargetPosition);
			populateFullTargetIdentifier(candidateFullTarget, targetIdentifiersObj.addNewFullTargetIdentifier());
		}
		
		if(ObjectUtil.validCollection(buildFrom.getReportStructures())) {
			for(ReportStructureBean currentRs : buildFrom.getReportStructures()) {
				builtObj.getReportStructureList().add(reportStructureXmlBeanBuilder.build(currentRs));
			}
		}
		
		return builtObj;
	}
	
	private PartialTargetIdentifierType buildPartialTargetIdentifier(MetadataTargetBean buildFrom) {
		PartialTargetIdentifierType builtObj = (PartialTargetIdentifierType) PartialTargetIdentifierType.Factory.newInstance();
		if(BuilderUtil.validString(buildFrom.getId())){
			builtObj.setId(buildFrom.getId());
		}
		if(buildFrom.getUri() != null){
			builtObj.setUri(buildFrom.getUri().toString());
		}
		if(BuilderUtil.validString(buildFrom.getUrn())){
			builtObj.setUrn(buildFrom.getUrn());
		}
		TextType tt = builtObj.addNewName();
		BuilderUtil.setDefaultText(tt);
		
		if(BuilderUtil.hasAnnotations(buildFrom)) {
			builtObj.setAnnotations(BuilderUtil.getAnnotationsType(buildFrom));
		}
		
		if(ObjectUtil.validCollection(buildFrom.getIdentifiableTargetBean())) {
			for(IdentifiableTargetBean currentIdentifiableTarget : buildFrom.getIdentifiableTargetBean()) {
				IDType idType = builtObj.addNewIdentifierComponentRef();
				idType.setStringValue(currentIdentifiableTarget.getId());
			}
		}
		return builtObj;
	}
	
	private void populateFullTargetIdentifier(MetadataTargetBean buildFrom, FullTargetIdentifierType builtObj) {
		if(BuilderUtil.validString(buildFrom.getId())){
			builtObj.setId(buildFrom.getId());
		}
		if(buildFrom.getUri() != null){
			builtObj.setUri(buildFrom.getUri().toString());
		}
		if(BuilderUtil.validString(buildFrom.getUrn())){
			builtObj.setUrn(buildFrom.getUrn());
		}
		TextType tt = builtObj.addNewName();
		BuilderUtil.setDefaultText(tt);
		
		if(BuilderUtil.hasAnnotations(buildFrom)) {
			builtObj.setAnnotations(BuilderUtil.getAnnotationsType(buildFrom));
		}
		
		if(ObjectUtil.validCollection(buildFrom.getIdentifiableTargetBean())) {
			for(IdentifiableTargetBean currentIdentifiableTarget : buildFrom.getIdentifiableTargetBean()) {
				builtObj.getIdentifierComponentList().add(buildIdentifierComponent(currentIdentifiableTarget));
			}
		}
	}
	
	private IdentifierComponentType buildIdentifierComponent(IdentifiableTargetBean buildFrom) {
		IdentifierComponentType builtObj = (IdentifierComponentType) IdentifierComponentType.Factory.newInstance();
		
		if(BuilderUtil.validString(buildFrom.getId())){
			builtObj.setId(buildFrom.getId());
		}
		if(buildFrom.getUri() != null){
			builtObj.setUri(buildFrom.getUri().toString());
		}
		if(BuilderUtil.validString(buildFrom.getUrn())){
			builtObj.setUrn(buildFrom.getUrn());
		}
		TextType tt = builtObj.addNewName();
		BuilderUtil.setDefaultText(tt);
		
		if(BuilderUtil.hasAnnotations(buildFrom)) {
			builtObj.setAnnotations(BuilderUtil.getAnnotationsType(buildFrom));
		}
		
		if(buildFrom.getReferencedStructureType() != null) {
			SDMX_STRUCTURE_TYPE targetObjectClass = buildFrom.getReferencedStructureType();
			if (buildFrom.getReferencedStructureType() == SDMX_STRUCTURE_TYPE.TIME_DIMENSION) {
				targetObjectClass = SDMX_STRUCTURE_TYPE.DIMENSION;
			} 
			builtObj.setTargetObjectClass(XmlBeansUtil.getSdmxObjectIdType(targetObjectClass));
		}
		
		if(buildFrom.hasCodedRepresentation()) {
			RepresentationSchemeType representationSchemeObj = builtObj.addNewRepresentationScheme();
			
			CrossReferenceBean refBean = buildFrom.getRepresentation().getRepresentation();
			if (refBean != null) {
				MaintainableRefBean mRef = refBean.getMaintainableReference();
				if(BuilderUtil.validString(mRef.getMaintainableId())) {
					representationSchemeObj.setRepresentationScheme(mRef.getMaintainableId());
				}
				if(BuilderUtil.validString(mRef.getAgencyId())) {
					representationSchemeObj.setRepresentationSchemeAgency(mRef.getAgencyId());
				}
				//supports extended v2.0 xsds
				if(BuilderUtil.validString(mRef.getVersion())) {
					representationSchemeObj.setRepresentationSchemeVersion(mRef.getVersion());
				}
				representationSchemeObj.setRepresentationSchemeType(XmlBeansUtil.getSdmxRepresentationSchemeType(refBean.getTargetReference()));
			}
			
		}
		return builtObj;
	}
	

}
