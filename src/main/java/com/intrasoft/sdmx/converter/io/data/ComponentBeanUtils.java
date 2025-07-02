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
package com.intrasoft.sdmx.converter.io.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.estat.sdmxsource.sdmx.api.constants.COMPONENT_ROLE;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;

public class ComponentBeanUtils {

	/**
	 * Method getConceptSchemeRefVersionAgencyConceptRef parse the bean and extract infos
	 *
	 * @param    itemBean            a  org.sdmxsource.sdmx.api.model.beans.base.ComponentBean
	 *
	 * @return   a Map
	 * 
	 */
	public static Map<String, String> getValuesFromComponent (org.sdmxsource.sdmx.api.model.beans.base.ComponentBean itemBean){
		Map<String, String> result = new HashMap<String, String>();
	
		if (itemBean != null && itemBean.getConceptRef() != null && itemBean.getConceptRef().getMaintainableReference() != null)
		{
			org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean maintainableReference = itemBean.getConceptRef().getMaintainableReference();						
			result.put(Constants.CONCEPT_SCHEME_REF, maintainableReference.getMaintainableId());
			result.put(Constants.CONCEPT_SCHEME_VERSION, maintainableReference.getVersion());
			result.put(Constants.CONCEPT_SCHEME_AGENCY, maintainableReference.getAgencyId());
		}
	
		if (itemBean != null && itemBean.getConceptRef() != null & itemBean.getConceptRef().getChildReference() != null)
		{
			result.put(Constants.CONCEPT_REF, itemBean.getConceptRef().getChildReference().getId());
		}
	
		if (itemBean.hasCodedRepresentation())
		{
			MaintainableRefBean codelistRef = getCodelistRepresentation(itemBean); 
			
			if (codelistRef != null) {
				result.put(Constants.CODELIST_LOWER, codelistRef.getMaintainableId());
				result.put(Constants.CODELIST_VERSION, codelistRef.getVersion());
				result.put(Constants.CODELIST_AGENCY, codelistRef.getAgencyId());
			}
		}

		return result;	
	}
	
	/**
	 * Gets the Codelist {@link MaintainableRefBean} of this component
	 * @param itemBean The component.
	 * @return the Codelist {@link MaintainableRefBean} of this component; otherwise null
	 */
	public static MaintainableRefBean getCodelistRepresentation(
			org.sdmxsource.sdmx.api.model.beans.base.ComponentBean itemBean) {
		org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean ref = itemBean.getRepresentation().getRepresentation();
		MaintainableRefBean codelistRef = null;
		if (ref.getMaintainableStructureType().equals(SDMX_STRUCTURE_TYPE.CODE_LIST)) {
			codelistRef = ref.getMaintainableReference();
		}
		else {
			DataStructureBean dsd = (DataStructureBean) itemBean.getParent().getParent(); 
			if (itemBean instanceof org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean && 
		                 ((org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean)itemBean).isMeasureDimension() &&
		                 dsd instanceof CrossSectionalDataStructureBean)
			{
				org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean dim = (org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean)itemBean;
				org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean xsDataStructure = (org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) dsd;
				CrossReferenceBean referenceBean = xsDataStructure.getCodelistForMeasureDimension(dim.getId());
				if (referenceBean != null) {
					codelistRef = referenceBean.getMaintainableReference();
				}
			}	
		}
		return codelistRef;
	}
	
	
	/**
	 * Method getDimensionInfoAttributes returns some of dimension attributes like isAttachLevelDataset, isAttachLevelGroup,...
	 *
	 * @param    dimension           a  org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean
	 * @param    bean                a  DataStructureBean
	 *
	 * @return   a Map
	 * 
	 */
	public static Map<String, String> getDimensionInfo(org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean dimension, org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean bean)
	{
		Map<String, String> result = new HashMap<String, String>();
	
		String isAttachLevelDataset = Constants.FALSE_CONSTANT;
		String isAttachLevelGroup = Constants.FALSE_CONSTANT;
		String isAttachLevelSection = Constants.FALSE_CONSTANT;
		String isAttachLevelObservation = Constants.FALSE_CONSTANT;
		if (bean instanceof org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) {		
          	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachDataSetList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachDataSet(false, SDMX_STRUCTURE_TYPE.DIMENSION);
           	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachGroupList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachGroup(false, new SDMX_STRUCTURE_TYPE[]{SDMX_STRUCTURE_TYPE.DIMENSION, SDMX_STRUCTURE_TYPE.TIME_DIMENSION});
           	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachObservationList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachObservation(SDMX_STRUCTURE_TYPE.DIMENSION);
           	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachSectionList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachSection(false, SDMX_STRUCTURE_TYPE.DIMENSION);

            if (xsAttachDataSetList.contains(dimension)) {
                	isAttachLevelDataset = Constants.TRUE_CONSTANT;
            } 
                        
            if (xsAttachGroupList.contains(dimension)) {
                    	isAttachLevelGroup = Constants.TRUE_CONSTANT;
            }                
					
            if (xsAttachObservationList.contains(dimension)) {
                        isAttachLevelObservation = Constants.TRUE_CONSTANT;
            } 
				
            if (xsAttachSectionList.contains(dimension)) {
                        isAttachLevelSection = Constants.TRUE_CONSTANT;
            }		
			
			result.put(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET, isAttachLevelDataset);	
			result.put(Constants.CROSS_SECTIONAL_ATTACH_GROUP, isAttachLevelGroup);
			result.put(Constants.CROSS_SECTIONAL_ATTACH_SECTION, isAttachLevelSection);
			result.put(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION, isAttachLevelObservation);		
		}
	
		String isMeasureDimension = Constants.FALSE_CONSTANT;
        if (dimension.isMeasureDimension()){
            isMeasureDimension = Constants.TRUE_CONSTANT;
		}		
		result.put(Constants.IS_MEASURE_DIMENSION, isMeasureDimension); 	
	
		String isFrequencyDimension = Constants.FALSE_CONSTANT;
        if (dimension.isFrequencyDimension()){
            isFrequencyDimension = Constants.TRUE_CONSTANT;
		}
		result.put(Constants.IS_FREQUENCY_DIMENSION,isFrequencyDimension);

		String isEntityDimension = Constants.FALSE_CONSTANT;
		if (isEntityDimension(dimension)){
           isEntityDimension = Constants.TRUE_CONSTANT;
		}
		result.put(Constants.IS_ENTITY_DIMENSION, isEntityDimension); 	
	
		
		String isCountDimension = Constants.FALSE_CONSTANT;
        if (isCountDimension(dimension)){
             isCountDimension = Constants.TRUE_CONSTANT;
		}		
		result.put(Constants.IS_COUNT_DIMENSION, isCountDimension); 
		
		String isNonObservationTimeDimension = Constants.FALSE_CONSTANT;
        if (isNonObservationTimeDimension(dimension)){					
             isNonObservationTimeDimension = Constants.TRUE_CONSTANT;
		}	
		result.put(Constants.IS_NON_OBSERVATION_TIME_DIMENSION, isNonObservationTimeDimension);
	
	
		String isIdentityDimension = Constants.FALSE_CONSTANT;
        if (isIdentityDimension(dimension)){
            isIdentityDimension = Constants.TRUE_CONSTANT;
		}		
		result.put(Constants.IS_IDENTITY_DIMENSION, isIdentityDimension);
	
		return result;
	}

	/**
	 * Method isIdentityDimension
	 *
	 * @param    dimension           a  org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean
	 * @return   a boolean
	 */
	public static boolean isIdentityDimension(org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean dimension)
	{
		boolean result = false;
		result = isDimension(dimension, COMPONENT_ROLE.IDENTITY);
		return result;
	}
	/**
	 * Method isDimension
	 *
	 * @param    dimension           a  org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean
	 * @param    dimensionType       a  String
	 * @return   a boolean
	 */
	private static boolean isDimension(org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean dimension, COMPONENT_ROLE dimensionType)
	{
		boolean result = false;
		List<CrossReferenceBean> conceptRoleList = dimension.getConceptRole();
		for (CrossReferenceBean conceptRole : conceptRoleList)
		{
			COMPONENT_ROLE role = COMPONENT_ROLE.tryParse(conceptRole);
			if (role != null && role == dimensionType) {
				result = true;
				break;
			}
		}
		return result;
	}
	/**
	 * Method isNonObservationTimeDimension
	 *
	 * @param    dimension           a  org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean
	 * @return   a boolean
	 */
	public static boolean isNonObservationTimeDimension(org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean dimension)
	{	
		boolean result = false;
		result = isDimension(dimension, COMPONENT_ROLE.NON_OBS_TIME);
		return result;
	}
	
	/**
	 * Method isCountDimension
	 *
	 * @param    dimension           a  org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean
	 * @return   a boolean
	 */
	public static boolean isCountDimension(org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean dimension)
	{
		boolean result = false;
		result = isDimension(dimension, COMPONENT_ROLE.COUNT);
		return result;
	}
	
	public static boolean isEntityDimension(org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean dimension)
	{
		boolean result = false;
		result = isDimension(dimension, COMPONENT_ROLE.ENTITY);
		return result;
	}
	
	/**
	 * Method getDimensionInfoAttributes returns some of dimension attributes like isAttachLevelDataset, isAttachLevelGroup,...
	 * @param    bean                a  DataStructureBean
	 * @return   a Map
	 * 
	 */
	public static Map<String, String> getAttributeInfo(org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean attribute, org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean bean)
	{
		Map<String, String> result = new HashMap<String, String>();
	
		String isAttachLevelDataset = Constants.FALSE_CONSTANT;
		String isAttachLevelGroup = Constants.FALSE_CONSTANT;
		String isAttachLevelSection = Constants.FALSE_CONSTANT;
		String isAttachLevelObservation = Constants.FALSE_CONSTANT;
		if (bean instanceof org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) {		
          	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachDataSetList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachDataSet(false, SDMX_STRUCTURE_TYPE.DATA_ATTRIBUTE);
           	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachGroupList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachGroup(false, SDMX_STRUCTURE_TYPE.DATA_ATTRIBUTE);
           	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachObservationList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachObservation(SDMX_STRUCTURE_TYPE.DATA_ATTRIBUTE);
           	List<org.sdmxsource.sdmx.api.model.beans.base.ComponentBean> xsAttachSectionList = 
				((org.sdmxsource.sdmx.api.model.beans.datastructure.CrossSectionalDataStructureBean) bean).getCrossSectionalAttachSection(false, SDMX_STRUCTURE_TYPE.DATA_ATTRIBUTE);

            if (xsAttachDataSetList.contains(attribute)) {
                	isAttachLevelDataset = Constants.TRUE_CONSTANT;
            } 
                        
            if (xsAttachGroupList.contains(attribute)) {
                    	isAttachLevelGroup = Constants.TRUE_CONSTANT;
            }                
					
            if (xsAttachObservationList.contains(attribute)) {
                        isAttachLevelObservation = Constants.TRUE_CONSTANT;
            } 
				
            if (xsAttachSectionList.contains(attribute)) {
                        isAttachLevelSection = Constants.TRUE_CONSTANT;
            }		
			
			result.put(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET, isAttachLevelDataset);	
			result.put(Constants.CROSS_SECTIONAL_ATTACH_GROUP, isAttachLevelGroup);
			result.put(Constants.CROSS_SECTIONAL_ATTACH_SECTION, isAttachLevelSection);
			result.put(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION, isAttachLevelObservation);		
		}
	
		String isFrequencyAttribute = Constants.FALSE_CONSTANT;
        if (isFrequencyAttribute(attribute)){
            isFrequencyAttribute = Constants.TRUE_CONSTANT;
		}
		result.put(Constants.IS_FREQUENCY_ATTRIBUTE, isFrequencyAttribute);

		String isEntityAttribute = Constants.FALSE_CONSTANT;
		if (isEntityAttribute(attribute)){
           isEntityAttribute = Constants.TRUE_CONSTANT;
		}
		result.put(Constants.IS_ENTITY_ATTRIBUTE, isEntityAttribute); 		
		
		String isCountAttribute = Constants.FALSE_CONSTANT;
        if (isCountAttribute(attribute)){
             isCountAttribute = Constants.TRUE_CONSTANT;
		}		
		result.put(Constants.IS_COUNT_ATTRIBUTE, isCountAttribute); 
		
		String isNonObservationalTimeAttribute = Constants.FALSE_CONSTANT;
        if (isNonObservationalTimeAttribute(attribute)){					
             isNonObservationalTimeAttribute = Constants.TRUE_CONSTANT;
		}	
		result.put(Constants.IS_NON_OBSERVATIONAL_TIME_ATTRIBUTE, isNonObservationalTimeAttribute);	
	
		String isIdentityAttribute = Constants.FALSE_CONSTANT;
        if (isIdentityAttribute(attribute)){
            isIdentityAttribute = Constants.TRUE_CONSTANT;
		}		
		result.put(Constants.IS_IDENTITY_ATTRIBUTE, isIdentityAttribute);	
	
		return result;
	}
	
    private static boolean isAttribute(org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean attribute, COMPONENT_ROLE type)
	{
		boolean result = false;
		List<CrossReferenceBean> conceptRoleList = attribute.getConceptRoles();
		for (CrossReferenceBean conceptRole : conceptRoleList)
		{
            COMPONENT_ROLE role = COMPONENT_ROLE.tryParse(conceptRole);
            if (role != null && role == type) {
            	result = true;
            	break;
            }
		}
		return result;
	}
    
	/**
	 * Method isEntityAttribute
	 *
	 * @param    attribute           an org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean
	 *
	 * @return   a boolean
	 * 
	 */
	public static boolean isEntityAttribute(org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean attribute)
	{
		boolean result = false;
		result = isAttribute(attribute, COMPONENT_ROLE.ENTITY);
		return result;
	}
	
	/**
	 * Method isNonObservationalTimeAttribute
	 *
	 * @param    attribute           an org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean
	 *
	 * @return   a boolean
	 * 
	 */
	public static boolean isNonObservationalTimeAttribute(org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean attribute)
	{
		boolean result = false;
		result = isAttribute(attribute, COMPONENT_ROLE.NON_OBS_TIME);
		return result;
	}

	/**
	 * Method isCountAttribute
	 *
	 * @param    attribute           an org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean
	 *
	 * @return   a boolean
	 * 
	 */
	public static boolean isCountAttribute(org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean attribute)
	{
		boolean result = false;
		result = isAttribute(attribute, COMPONENT_ROLE.COUNT);
		return result;
	}

	/**
	 * Method isFrequencyAttribute
	 *
	 * @param    attribute           an org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean
	 *
	 * @return   a boolean
	 * 
	 */
	public static boolean isFrequencyAttribute(org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean attribute)
	{
		boolean result = false;
		result = isAttribute(attribute, COMPONENT_ROLE.FREQUENCY);
		return result;
	}

	public static boolean isIdentityAttribute(org.sdmxsource.sdmx.api.model.beans.datastructure.AttributeBean attribute)
	{
		boolean result = false;
		result = isAttribute(attribute, COMPONENT_ROLE.IDENTITY);
		return result;
	}
}
