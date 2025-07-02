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
import org.sdmx.resources.sdmxml.schemas.v20.structure.MetadataAttributeType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.TextFormatType;
import org.sdmx.resources.sdmxml.schemas.v20.structure.UsageStatusType;
import org.sdmxsource.sdmx.api.builder.Builder;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.model.beans.metadatastructure.MetadataAttributeBean;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
//import org.sdmxsource.sdmx.structureparser.builder.xmlBeans.v2.AbstractBuilder;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.stereotype.Service;


@Service(value="MetadataAttributeXmlBeanBuilderV2")
public class MetadataAttributeXmlBeanBuilder extends org.sdmxsource.sdmx.structureparser.builder.xmlBeans.v2.MetadataAttributeXmlBeanBuilder implements Builder<MetadataAttributeType, MetadataAttributeBean> {
//	public static MetadataAttributeXmlBeanBuilder INSTANCE = new MetadataAttributeXmlBeanBuilder();
	
//	private MetadataAttributeXmlBeanBuilder() { }
	
	@Override
	public MetadataAttributeType build(MetadataAttributeBean buildFrom) throws SdmxException {
		MetadataAttributeType builtObj = (MetadataAttributeType) MetadataAttributeType.Factory.newInstance();
		
		if(BuilderUtil.hasAnnotations(buildFrom)) {
			builtObj.setAnnotations(BuilderUtil.getAnnotationsType(buildFrom));
		}
		
		// extension: set usageStatus
		if(buildFrom.getMinOccurs() != null) {
			if (buildFrom.getMinOccurs() == 0) {
				builtObj.setUsageStatus(UsageStatusType.CONDITIONAL);
			} else {
				builtObj.setUsageStatus(UsageStatusType.MANDATORY);
			}
		} else {
			builtObj.setUsageStatus(UsageStatusType.CONDITIONAL);
		}
		if(buildFrom.getConceptRef() != null) {
			MaintainableRefBean maintRef = buildFrom.getConceptRef().getMaintainableReference();
			if(BuilderUtil.validString(maintRef.getAgencyId())) {
				builtObj.setConceptSchemeAgency(maintRef.getAgencyId());
			}
			if(BuilderUtil.validString(maintRef.getMaintainableId())) {
				builtObj.setConceptSchemeRef(maintRef.getMaintainableId());
			}
			if(BuilderUtil.validString(buildFrom.getConceptRef().getChildReference().getId())) {
				builtObj.setConceptRef(buildFrom.getConceptRef().getChildReference().getId());
			}
			//copies version of ConceptScheme to Concept because there is no other place in standard schemas
			if(BuilderUtil.validString(maintRef.getVersion())) {
				builtObj.setConceptVersion(maintRef.getVersion());
			}
			//supports extended v2.0 xsds
			if(BuilderUtil.validString(maintRef.getVersion())) {
				builtObj.setConceptSchemeVersion(maintRef.getVersion());
			}
		}
		// extension: set coded representation
		if(buildFrom.hasCodedRepresentation()) {
			MaintainableRefBean maintRef = buildFrom.getRepresentation().getRepresentation().getMaintainableReference();
			if(BuilderUtil.validString(maintRef.getMaintainableId())) {
				builtObj.setRepresentationScheme(maintRef.getMaintainableId());
			}
			if(BuilderUtil.validString(maintRef.getAgencyId())) {
				builtObj.setRepresentationSchemeAgency(maintRef.getAgencyId());
			}
			//supports extended v2.0 xsds
			if(BuilderUtil.validString(maintRef.getVersion())) {
				builtObj.setRepresentationSchemeVersion(maintRef.getVersion());
			}
		}
		if(buildFrom.getRepresentation() != null && buildFrom.getRepresentation().getTextFormat() != null) {
			TextFormatType textFormatType = (TextFormatType) TextFormatType.Factory.newInstance();
			BuilderUtil.populateTextFormatType(textFormatType, buildFrom.getRepresentation().getTextFormat());
			builtObj.setTextFormat(textFormatType);
		}
		// extension: iterate recursively metadata attributes 
		if(ObjectUtil.validCollection(buildFrom.getMetadataAttributes())) {
			for(MetadataAttributeBean currentMat : buildFrom.getMetadataAttributes()) {
				builtObj.getMetadataAttributeList().add(this.build(currentMat));
			}
		}
		
		return builtObj;
	}

	
}
