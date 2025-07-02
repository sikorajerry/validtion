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
package org.estat.sdmxsource.sdmx.api.constants;

import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;

/**
 * Constant component roles based on ESTAT+COMPONENT_ROLES+1.0.xml.
 * They are used for SDMX v2.0 to/from CAPI translation.
 * @author tasos
 *
 */
public enum COMPONENT_ROLE {
	FREQUENCY("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).FREQUENCY"),
	COUNT("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).COUNT"),
	GEOGRAPHY("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).GEOGRAPHY"),
	IDENTITY("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).IDENTITY"),
	NON_OBS_TIME("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).NON_OBS_TIME"),
	TIME_FORMAT("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).TIME_FORMAT"),
	UNIT_OF_MEASURE("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).UNIT_OF_MEASURE"),
	ENTITY("urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=ESTAT:COMPONENT_ROLES(1.0).ENTITY");
	
	/**
	 * The urn.
	 */
	private final String urn;
	
	/**
	 * Returns the concept as a reference.
	 * @return the concept as a reference
	 */
	public StructureReferenceBean asReference() {
		return new StructureReferenceBeanImpl(this.urn);
	}
	
	/**
	 * Try to parse the specified {@code structureReference} 
	 * @param structureReference
	 * @return the {@link COMPONENT_ROLE} if {@code structureReference} is for a {@link SDMX_STRUCTURE_TYPE.CONCEPT} and exist a member of this enum with the concept id; othewise it returns null.
	 * 
	 */
	public static COMPONENT_ROLE tryParse(StructureReferenceBean structureReference) {
		
		if (structureReference == null) {
			throw new IllegalArgumentException("structureReference is null");
		}
		
		if (structureReference.getTargetReference() != SDMX_STRUCTURE_TYPE.CONCEPT) {
			throw new IllegalArgumentException("structureReference target reference is not a concept");
		}
		
		if (!structureReference.hasChildReference()) {
			throw new IllegalArgumentException("structureReference has not child reference");
		}
		
		String id = structureReference.getChildReference().getId();

		// valueOf throws an  IllegalArgumentException. We don't want that.
		for(COMPONENT_ROLE role : COMPONENT_ROLE.values())
		{
			if (role.name().equals(id)) {
				return role;
			}
		}
				
		return null;
	}
	
	/**
	 * Private constructor for {@link COMPONENT_ROLE}
	 * @param urn The SDMX concept URN.
	 */
	private COMPONENT_ROLE(String urn) {
		this.urn = urn;		
	}
	
}
