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
/**
 * 
 */
package org.estat.sdmxsource.sdmx.util.builder;

import org.sdmxsource.sdmx.api.builder.Builder;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.model.mutable.base.AnnotationMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.datastructure.CrossSectionalDataStructureMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.datastructure.DataStructureMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.datastructure.GroupMutableBean;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.datastructure.CrossSectionalDataStructureMutableBeanImpl;
import org.springframework.stereotype.Service;

/**
 * Build a {@link CrossSectionalDataStructureMutableBean} from a {@link DataStructureMutableBean}.
 * @author tasos
 * @date 28 11 2013
 */
@Service
public class CrossSectionalDataStructureBuilder
		implements
		Builder<CrossSectionalDataStructureMutableBean, DataStructureMutableBean> {

	/* (non-Javadoc)
	 * @see org.sdmxsource.sdmx.api.builder.Builder#build(java.lang.Object)
	 */
	public CrossSectionalDataStructureMutableBean build(
			DataStructureMutableBean buildFrom) throws SdmxException {
		if (buildFrom instanceof CrossSectionalDataStructureMutableBeanImpl) {
			return (CrossSectionalDataStructureMutableBean) buildFrom;
		}
		
		CrossSectionalDataStructureMutableBean crossBean = new CrossSectionalDataStructureMutableBeanImpl();
		if (buildFrom.getAnnotations() != null)
		{
			for(AnnotationMutableBean annotation : buildFrom.getAnnotations()) {
				crossBean.addAnnotation(annotation);
			}
		}
		
		crossBean.setDimensionList(buildFrom.getDimensionList());
		
		crossBean.setAttributeList(buildFrom.getAttributeList());
		
		crossBean.setMeasureList(buildFrom.getMeasureList());
		
		crossBean.setAgencyId(buildFrom.getAgencyId());
		crossBean.setId(buildFrom.getId());
		crossBean.setVersion(buildFrom.getVersion());
		crossBean.setNames(buildFrom.getNames());
		crossBean.setDescriptions(buildFrom.getDescriptions());
		crossBean.setEndDate(buildFrom.getEndDate());
		crossBean.setStartDate(buildFrom.getStartDate());
		crossBean.setExternalReference(buildFrom.getExternalReference());
		crossBean.setStub(buildFrom.isStub());
		crossBean.setFinalStructure(buildFrom.getFinalStructure());
		
		if (buildFrom.getGroups() != null) {
			for(GroupMutableBean group : buildFrom.getGroups()) {
				crossBean.addGroup(group);
			}
		}
		
		crossBean.setStructureURL(buildFrom.getStructureURL());
		crossBean.setServiceURL(buildFrom.getServiceURL());
		
		return crossBean;
	}

}
