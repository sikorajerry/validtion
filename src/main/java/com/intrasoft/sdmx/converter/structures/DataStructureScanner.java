package com.intrasoft.sdmx.converter.structures;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.ComponentBean;
import org.sdmxsource.sdmx.api.model.beans.base.RepresentationBean;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodeBean;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodelistBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptSchemeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.beans.reference.MaintainableRefBean;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.mutable.conceptscheme.ConceptMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.conceptscheme.ConceptSchemeMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.datastructure.DataStructureMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.datastructure.DimensionMutableBean;
import org.sdmxsource.util.ObjectUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by dbalan on 6/28/2017.
 */
public class DataStructureScanner implements SdmxBeansScanner, Serializable {

	private static Logger logger = LogManager.getLogger(DataStructureScanner.class);

	/**
	 * the sdmx beans object containing one or many structures
	 * one of which should be the one compatible with the dataStructIdCache
	 * (the other param in the constructor)
	 */
	private SdmxBeans sdmxBeans;

	/**
	 * lazy computed cache for data structure
	 */
	private DataStructureBean dataStructureBean;

	/**
	 *
	 */
	private StructureIdentifier dataStructureId;

	/**
	 * lazy computed cache for the names of available components
	 * (this one does not contain the cross x measures)
	 */
	private List<String> componentNames;

	/**
	 * lazy computed cache for the names of available components
	 * containing the cross x measures
	 */
	private List<String> componentNamesWithCrossXMeasures;

	private List<String> dimensions;

	private List<String> observationLevelAttributes;
	private List<String> datasetLevelAttributes;
	private List<String> seriesLevelAttributes;
	private List<String> groupLevelAttributes;

	private List<String> measures;

	private String measureDimension;
	private String dimensionAtObservation;

	/**
	 * lazy computed cache for the names of coded components
	 */
	private List<String> codedComponentNames;

	/**
	 * @param sdmxBeans
	 */
	public DataStructureScanner(SdmxBeans sdmxBeans) throws InvalidStructureException {
		this(sdmxBeans, null);
	}


	/**
	 *
	 * @return
	 */
	private DataStructureScanner getStructureScanner(){
		DataStructureScanner structureScanner = null;
		if(this == null){
			try {
				structureScanner = new DataStructureScanner(computeDataStructureById(sdmxBeans, dataStructureId));
			} catch (InvalidStructureException e) {
				throw new IllegalArgumentException(e);
			}
		}
		return structureScanner;
	}


    /**
     * the constructor
     * @param dsd
     */
    public DataStructureScanner(DataStructureBean dsd){
        this.dataStructureBean = dsd;
    }

	/**
	 * @param sdmxBeans
	 * @param dataStructureId
	 */
	public DataStructureScanner(SdmxBeans sdmxBeans, StructureIdentifier dataStructureId) throws InvalidStructureException {
		this.sdmxBeans = sdmxBeans;
		this.dataStructureId = dataStructureId;
		this.dataStructureBean = computeDataStructureById(sdmxBeans, dataStructureId);
		removeReferencesFromConcepts();
	}

	/**
	 * retrieves from the provided sdmx beans the data structure bean that matches the (agency, keyFamId, keyFamVersion) criteria
	 * or null if nothing is found
	 *
	 * @param structureBean the sdmxBeans which holds the data structure beans
	 * @param dataStructId  the id of the data structure
	 * @return the data structure that matches the criteria or null
	 */
	private DataStructureBean computeDataStructureById(SdmxBeans structureBean,
													   StructureIdentifier dataStructId) throws InvalidStructureException {
		DataStructureBean result = null;
		if (structureBean.hasDataStructures()) {
			if (dataStructId != null) {
				for (DataStructureBean dataStructure : structureBean.getDataStructures()) {
					if (dataStructure.getAgencyId().equalsIgnoreCase(dataStructId.getAgency())
							&& dataStructure.getId().equalsIgnoreCase(dataStructId.getArtefactId())
							&& isWildcardVersion(dataStructId)) {
						result = dataStructure;
						break;
					}
					if (dataStructure.getAgencyId().equalsIgnoreCase(dataStructId.getAgency())
							&& dataStructure.getId().equalsIgnoreCase(dataStructId.getArtefactId())
								&& dataStructure.getVersion().equalsIgnoreCase(dataStructId.getArtefactVersion())) {
						result = dataStructure;
						break;
					}
				}
			} else {
				//take the first data structure since the identifier was not specified
				result = structureBean.getDataStructures().iterator().next();
			}
		}
		if (result == null) {
			throw new InvalidStructureException("No datastructure found for identifier " + dataStructId);
		}
		return result;
	}

	@Override
	public DataStructureBean getDataStructure() {
		return this.dataStructureBean;
	}

	// delete references for concepts, this is a part of the solution for
	// SDMXCONV-567, 580, 589
	private void removeReferencesFromConcepts() {
		synchronized (this.sdmxBeans) {
			for (ConceptSchemeBean conceptSchemeBean : this.sdmxBeans.getConceptSchemes()) {
				ConceptSchemeMutableBean conceptSchemeMutableBean = conceptSchemeBean.getMutableInstance();
				for (ConceptMutableBean conceptMutableBean : conceptSchemeMutableBean.getItems()) {
					conceptMutableBean.setCoreRepresentation(null);
				}

				this.sdmxBeans.removeConceptScheme(conceptSchemeBean);
				this.sdmxBeans.addConceptScheme(conceptSchemeMutableBean.getImmutableInstance());
			}
			removeRoles();
		}
	}

	/**
	 * Remove Concept Role that contains "COMPONENT_ROLES"
	 * as MaintainableId, from dimension. See SDMXCONV-877.
	 */
	private void removeRoles() {
		synchronized (this.sdmxBeans) {
			for (DataStructureBean structureBean : sdmxBeans.getDataStructures()) {
				DataStructureMutableBean dsMutableBean = structureBean.getMutableInstance();
				if (ObjectUtil.validCollection(dsMutableBean.getDimensions())) {
					for (DimensionMutableBean dim : dsMutableBean.getDimensions()) {
						dim.getConceptRole().clear();
					}
					sdmxBeans.removeDataStructure(structureBean);
					sdmxBeans.addDataStructure(dsMutableBean.getImmutableInstance());
				}
			}
		}
	}

	/**
	 * returns true if the data structure version has wildcards
	 *SDMXCONV-1526
	 * @return
	 */
	private boolean isWildcardVersion (StructureIdentifier dataStructureId){
		if(dataStructureId.getArtefactVersion().contains("+")){
			return true;
		}
		return false;
	}

	@Override
	public boolean hasDataflow() {
		return false;
	}

	@Override
	public DataflowBean getDataflow() {
		return null;
	}

	@Override
	public SdmxBeans getSdmxBeans() {
		return sdmxBeans;
	}

	/**
	 * returns true is the data structure is cross sectional
	 *
	 * @return
	 */
	public boolean isCrossSectionalDataStructure() {
		return dataStructureBean instanceof CrossSectionalDataStructureBean;
	}

	/**
	 * returns true if the data structure has cross sectional measures
	 *
	 * @return
	 */
	public boolean hasCrossSectionalMeasures() {
		boolean result = false;
		if (dataStructureBean instanceof CrossSectionalDataStructureBean) {
			CrossSectionalDataStructureBean dataStructureAsCrossX = (CrossSectionalDataStructureBean) dataStructureBean;
			result = dataStructureAsCrossX.getCrossSectionalMeasures() != null
					&& dataStructureAsCrossX.getCrossSectionalMeasures().size() > 0;
		}
		return result;
	}

	/**
	 * lazily retrieves the measure dimension
	 *
	 * @return
	 */
	public String getMeasureDimension() {
		if (measureDimension == null) {
			measureDimension = computeMeasureDimension();
		}
		return measureDimension;
	}

	/**
	 * This method returns the concept name of the measure Dimension.
	 *
	 * @return measureDimension id
	 */
	private String computeMeasureDimension() {
		String result = null;
		final List<DimensionBean> dimensionsBean = dataStructureBean.getDimensions();
		for (DimensionBean dimBean : dimensionsBean) {
			if (dimBean.isMeasureDimension() && !dimBean.isTimeDimension()) {
				result = dimBean.getId();
				break;
			}
		}
		return result;
	}

	/**
	 * returns a list of names/id of all dimensions, crossX measures, obs attributes,
	 * dataset attributes, group attributes and dimension group attributes
	 * <p>
	 * Note: this is a helper method for the mapping dialog box, if you use this for any other purpose please consult the source code
	 *
	 * @param includeCrossXMeasures specifies whether the crossXMeasures should be included
	 * @return a list of dimensions, measures, attributes
	 */
	private List<String> computeDimensionsMeasuresAttributes(boolean includeCrossXMeasures) {
		List<String> result = new ArrayList<>();
		result.addAll(getDimensions(includeCrossXMeasures));
		result.addAll(getMeasures());
		result.addAll(getObservationLevelAttributes());
		result.addAll(getDatasetLevelAttributes());
		result.addAll(getGroupLevelAttributes());
		result.addAll(getSeriesLevelAttributes());
		return result;
	}

	public List<String> getDimensions(boolean includeCrossXMeasures) {
		dimensions = computeDimensions(includeCrossXMeasures);
		return dimensions;
	}

	/**
	 * This method is used to gather all the attributes from different levels of data structure.
	 * It first fetches attributes from Observation, Dataset, Group, and Series levels.
	 * Then, it iterates over the attributes with the order of structure file
	 * obtained from the data structure and adds them to a list.
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1544">SDMXCONV-1544</a>
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1254">SDMXCONV-1254</a>
	 *
	 * @return List of attribute IDs.
	 */
	private List<String> getAttributes() {
		// Fetch observation level attributes
		getObservationLevelAttributes();

		// Fetch dataset level attributes
		getDatasetLevelAttributes();

		// Fetch group level attributes
		getGroupLevelAttributes();

		// Fetch series level attributes
		getSeriesLevelAttributes();

		// Initialize a list to store attribute IDs
		List<String> attributes = new ArrayList<>();

		// Get the attributes from the data structure bean
		List<AttributeBean> attrs = dataStructureBean.getAttributes();

		// Iterate over the attributes and add their IDs to the list
		for(AttributeBean attr: attrs) {
			attributes.add(attr.getId());
		}

		// Return the list of attribute IDs
		return attributes;
	}

	private List<String> computeDimensions(boolean includeCrossXmeasures) {
		List<String> result = new ArrayList<>();
		// add dimensions
		List<DimensionBean> dimensions = dataStructureBean.getDimensions();

		// It is possible to have two component with the same getConceptRef() value!
		// We need to use getId() internally and expose getConceptRef() only to the writers that need that
		for (DimensionBean dimension : dimensions) {
			result.add(dimension.getId());
		}

		if (includeCrossXmeasures) {
			// if the value is to map the crossX measures then add the crossX measures as mapping components.
			//first remove the measureDimension from the list
			String concept = getMeasureDimension();
			logger.debug("removing the measure dimension {} from the list of dimensions, measures and attributes", concept);
			result.remove(concept);

			//add crossX measures
			List<CrossSectionalMeasureBean> crossXMeasures = ((CrossSectionalDataStructureBean) dataStructureBean).getCrossSectionalMeasures();
			for (CrossSectionalMeasureBean crossXMeasure : crossXMeasures) {
				result.add(crossXMeasure.getId());
			}
		} else {
			// add observation value
			if (ObjectUtil.validObject(dataStructureBean.getPrimaryMeasure())) {
				result.add(dataStructureBean.getPrimaryMeasure().getId());
			}
		}
		return result;
	}

	/**
	 * returns the component names (dimensions, measures, attributes) with or without the cross x measures included
	 *
	 * @return a list of component names
	 */
	public List<String> getComponents(boolean includeCrossXMeasures) {
		List<String> result = null;
		if (includeCrossXMeasures) {
			componentNamesWithCrossXMeasures = computeDimensionsMeasuresAttributes(includeCrossXMeasures);
			result = componentNamesWithCrossXMeasures;
		} else {
			componentNames = computeDimensionsMeasuresAttributes(includeCrossXMeasures);
			result = componentNames;
		}
		return result;
	}

	/**
	 * retrieves lazily the names of the coded components
	 *
	 * @return
	 */
	public List<String> getCodedComponentNames() {
		if (codedComponentNames == null) {
			codedComponentNames = computeCodedComponentNames();
		}
		return codedComponentNames;
	}

	/**
	 * retrieves the list of codes associated with the given dimension
	 *
	 * @param component one of dimension / attribute / measure
	 * @return list of codes for giving component; could be empty if the component is non-coded
	 */
	public List<String> getCodesForComponent(String component) {
		List<String> codes = new ArrayList<>();
		ComponentBean componentBean = getComponent(component);
		if (componentBean != null && componentBean.hasCodedRepresentation()) {
			RepresentationBean localRepresentation = componentBean.getRepresentation();
			StructureReferenceBean referenceBean = localRepresentation.getRepresentation();
			MaintainableRefBean codelistRef;
			if (referenceBean.getTargetReference() == SDMX_STRUCTURE_TYPE.CONCEPT_SCHEME && isCrossSectionalDataStructure()) {
				CrossReferenceBean crossReferenceBean = ((CrossSectionalDataStructureBean) dataStructureBean).getCodelistForMeasureDimension(component);
				codelistRef = crossReferenceBean.getMaintainableReference();
			} else {
				codelistRef = referenceBean.getMaintainableReference();
			}
			Set<CodelistBean> codelist = sdmxBeans.getCodelists(codelistRef);
			Iterator<CodelistBean> it = codelist.iterator();
			if (it.hasNext()) {
				CodelistBean codeLitsBean = codelist.iterator().next();
				for (CodeBean code : codeLitsBean.getItems()) {
					codes.add(code.getId());
				}
			}
		}
		return codes;
	}

	/**
	 * Return ComponentBean Object from component name
	 *
	 * @param componentName
	 * @return ComponentBean
	 */
	public ComponentBean getComponent(String componentName) {
		return dataStructureBean.getComponent(componentName);
	}

	public List<String> getObservationLevelAttributes() {
		if (observationLevelAttributes == null) {
			observationLevelAttributes = computeObservationLevelAttributes();
		}
		return observationLevelAttributes;
	}

	private List<String> computeObservationLevelAttributes() {
		List<String> result = new ArrayList<>();
		List<AttributeBean> attributes = dataStructureBean.getObservationAttributes(getDimensionAtObservation());
		for (AttributeBean attribute : attributes) {
			result.add(attribute.getId());
		}
		return result;
	}

	public List<String> getDatasetLevelAttributes() {
		if (datasetLevelAttributes == null) {
			datasetLevelAttributes = computeDatasetLevelAttributes();
		}
		return datasetLevelAttributes;
	}

	private List<String> computeDatasetLevelAttributes() {
		List<String> result = new ArrayList<>();
		List<AttributeBean> attributes = dataStructureBean.getDatasetAttributes();
		for (AttributeBean attribute : attributes) {
			result.add(attribute.getId());
		}
		return result;
	}

	public List<String> getSeriesLevelAttributes() {
		if (seriesLevelAttributes == null) {
			seriesLevelAttributes = computeSeriesLevelAttributes();
		}
		return seriesLevelAttributes;
	}

	private List<String> computeSeriesLevelAttributes() {
		List<String> result = new ArrayList<>();
		List<AttributeBean> attributes = dataStructureBean.getSeriesAttributes(getDimensionAtObservation());
		for (AttributeBean attribute : attributes) {
			result.add(attribute.getId());
		}
		return result;
	}

	public List<String> getGroupLevelAttributes() {
		if (groupLevelAttributes == null) {
			groupLevelAttributes = computeGroupLevelAttributes();
		}
		return groupLevelAttributes;
	}

	private List<String> computeGroupLevelAttributes() {
		List<String> result = new ArrayList<>();
		for (AttributeBean grpAttr : dataStructureBean.getGroupAttributes()) {
			result.add(grpAttr.getId());
		}
		return result;
	}

	private List<String> computeMeasures() {
		List<String> result = new ArrayList<>();
		String primaryMeasure = dataStructureBean.getPrimaryMeasure()!=null ? dataStructureBean.getPrimaryMeasure().getId() : null;
		for (MeasureBean measure : dataStructureBean.getMeasures()) {
			if(!measure.getId().equalsIgnoreCase(primaryMeasure)) {
				result.add(measure.getId());
			}
		}
		return result;
	}

	public List<String> getMeasures() {
		if (measures == null) {
			measures = computeMeasures();
		}
		return measures;
	}

	public String getDimensionAtObservation() {
		if (dimensionAtObservation == null) {
			dimensionAtObservation = computeDimensionAtObsevation().getId();
		}
		return dimensionAtObservation;
	}

	private DimensionBean computeDimensionAtObsevation() {
		DimensionBean result = null;
		if (dataStructureBean.getTimeDimension() != null) {
			result = dataStructureBean.getTimeDimension();
		} else {
			List<DimensionBean> listOfDimensionBeans = dataStructureBean.getDimensions(SDMX_STRUCTURE_TYPE.MEASURE_DIMENSION);
			if (!listOfDimensionBeans.isEmpty()) {
				result = listOfDimensionBeans.get(0);
			} else {
				List<DimensionBean> dimensions = dataStructureBean.getDimensionList().getDimensions();
				result = dimensions.get(dimensions.size() - 1);
			}
		}
		return result;
	}

	/**
	 * retrieves the list of dimensions and attributes for transcoding
	 *
	 * @return list of dimensions and attributes for transcoding
	 */
	private List<String> computeCodedComponentNames() {
		List<String> result = new ArrayList<String>();
		List<ComponentBean> dsdComponents = dataStructureBean.getComponents();
		//now we get all the attributes, measure dimensions and dimensions available and measures
		for (ComponentBean component : dsdComponents) {
			switch (component.getStructureType()) {
				case DIMENSION:
				case MEASURE_DIMENSION:
				case DATA_ATTRIBUTE:
				case MEASURE:
					result.add(component.getId());
					break;
			}
		}
		return result;
	}

	@Override
	public void clean() {
		sdmxBeans = null;
		dataStructureBean = null;
		dataStructureId = null;
		componentNames = null;
		componentNamesWithCrossXMeasures = null;
		dimensions = null;
		observationLevelAttributes = null;
		datasetLevelAttributes = null;
		seriesLevelAttributes = null;
		groupLevelAttributes = null;
		measures = null;
		measureDimension = null;
		dimensionAtObservation = null;
		codedComponentNames = null;
	}

}
