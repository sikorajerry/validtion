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
import org.sdmx.resources.sdmxml.schemas.v20.structure.ConceptType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.TextFormatType;
import org.sdmxsource.sdmx.api.builder.Builder;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptBean;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
import org.springframework.stereotype.Service;


@Service(value="ConceptXmlBeanBuilderV2")
public class ConceptXmlBeanBuilder extends org.sdmxsource.sdmx.structureparser.builder.xmlBeans.v2.ConceptXmlBeanBuilder implements Builder<ConceptType, ConceptBean> {
	
	@Override
	public ConceptType build(ConceptBean buildFrom) throws SdmxException {
		ConceptType builtObj = (ConceptType) ConceptType.Factory.newInstance();
			
		if(BuilderUtil.validString(buildFrom.getId())){
			builtObj.setId(buildFrom.getId());
		}
		if(buildFrom.getUri() != null){
			builtObj.setUri(buildFrom.getUri().toString());
		}
		if(BuilderUtil.validString(buildFrom.getUrn())){
			builtObj.setUrn(buildFrom.getUrn());
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
		
		if(BuilderUtil.validString(buildFrom.getParentConcept())){
			builtObj.setParent(buildFrom.getParentConcept());
		}
		if(BuilderUtil.validString(buildFrom.getParentAgency())){
			builtObj.setParentAgency(buildFrom.getParentAgency());
		}
		if(buildFrom.getCoreRepresentation() != null){
			if(buildFrom.getCoreRepresentation().getRepresentation() != null) {
				MaintainableRefBean maintRef = buildFrom.getCoreRepresentation().getRepresentation().getMaintainableReference();
				builtObj.setCoreRepresentation(maintRef.getMaintainableId());
				builtObj.setCoreRepresentationAgency(maintRef.getAgencyId());
				//supports extended v2.0 xsds
				if(BuilderUtil.validString(maintRef.getVersion())){
					builtObj.setCoreRepresentationVersion(maintRef.getVersion());
				}
			}
			if(buildFrom.getCoreRepresentation().getTextFormat() != null) {
				TextFormatType textFormatType = (TextFormatType) TextFormatType.Factory.newInstance();
				BuilderUtil.populateTextFormatType(textFormatType, (buildFrom.getCoreRepresentation().getTextFormat()));
				builtObj.setTextFormat(textFormatType);
			}
		}
		return builtObj;
	}
	
	
}
