package com.intrasoft.sdmx.converter.structures;

import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.exception.SdmxNoResultsException;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.util.ObjectUtil;

import java.io.Serializable;
import java.util.List;

/**
 * a descriptor of a data structure based on SdmxBeans object and a data structure identifier. 
 * The power of this class lays in the utility methods 
 * 
 * @author dragos balan (by copy-pasting a lot of code from the ol' fuckin' converter gui)
 */
public final class DataflowScanner implements SdmxBeansScanner, Serializable {
    
    private static final long serialVersionUID = 8154533485424032248L;

    private static Logger logger = LogManager.getLogger(DataflowScanner.class);
    
    /**
     * the identifier of the structure ( could be a dataflow identifier or a datastructure identifier)
     */
    private StructureIdentifier dataflowIdentifier;

    /**
     * cached dataflow
     */
    private DataflowBean dataflow;

    private DataStructureScanner datastructScanner;

    /**
     * the only constructor available 
     * @param sdmxBeans
     * @param dataflowId
     */
    public DataflowScanner(SdmxBeans sdmxBeans,
                           StructureIdentifier dataflowId) throws InvalidStructureException {
        StructureIdentifier datastructId = computeStructureIdFromDataflow(sdmxBeans, dataflowId);
        this.datastructScanner = new DataStructureScanner(sdmxBeans, datastructId);
        this.dataflowIdentifier = dataflowId;
    }



    /**
     * retrieves the structure beans (set into the constructor)
     * @return
     */
    public SdmxBeans getSdmxBeans(){
        return datastructScanner.getSdmxBeans();
    }
    
    /**
     * lretrieves the data structure bean
     * @return
     */
    public DataStructureBean getDataStructure(){
        return datastructScanner.getDataStructure();
    }


    public boolean hasDataflow(){
        return true;
    }

    public DataflowBean getDataflow(){
        if(dataflow == null){
            dataflow = computeDataflowById(datastructScanner.getSdmxBeans(), dataflowIdentifier);
        }
        return dataflow;
    }

    public boolean hasCrossSectionalMeasures(){
        return datastructScanner.hasCrossSectionalMeasures(); 
    }

    /**
     * returns true is the data structure is cross sectional 
     * @return
     */
    public boolean isCrossSectionalDataStructure(){
        return datastructScanner.isCrossSectionalDataStructure();
    }
    
    /**
     * returns the component names (dimensions, measures, attributes) with or without the cross x measures included
     * @return  a list of component names
     */
    public List<String> getComponents(boolean includeCrossXMeasures){
        return datastructScanner.getComponents(includeCrossXMeasures);
    }
    
    /**
     * lazily retrieves the measure dimension
     * @return
     */
    public String getMeasureDimension() {
        return datastructScanner.getMeasureDimension();
    }
    
    /**
     * retrieves lazily the names of the coded components
     * @return
     */
    public List<String> getCodedComponentNames(){
        return datastructScanner.getCodedComponentNames();
    }

    /**
     * retrieves the list of codes associated with the given dimension
     *
     * @param component         one of dimension / attribute / measure
     * @return                  list of codes for giving component; could be empty if the component is non-coded
     */
    public List<String> getCodesForComponent(String component){
        return datastructScanner.getCodesForComponent(component);
    }

    /**
     * lazily retrieves the measure dimension
     * @return
     */
    public List<String> getDimensions(boolean includeCrossXMeasures) {
        return datastructScanner.getDimensions(includeCrossXMeasures);
    }
    /**
     * returns the datastructure referenced by the given dataflow
     *
     * @param dataflowId        the id of the dataflow
     * @return                  the id of the data structure identified by the dataflow
     */
    private StructureIdentifier computeStructureIdFromDataflow(SdmxBeans sdmxBeans, StructureIdentifier dataflowId) {
        StructureIdentifier result = null;
        if (sdmxBeans.hasDataflows()) {
        	if(dataflowId==null) {
        		//take the first data structure since the identifier was not specified
        		result = new StructureIdentifier(sdmxBeans.getDataflows().iterator().next().getDataStructureRef());
        		return result;
        	}
            DataflowBean dataflow = computeDataflowById(sdmxBeans, dataflowId);
            if (dataflow != null) {
                    result = new StructureIdentifier(dataflow.getDataStructureRef());
            } else {
                throw new SdmxNoResultsException(ExceptionCode.DATASET_REF_DATAFLOW_NOT_RESOLVED, dataflowId);
            }
        } else {
            throw new SdmxNoResultsException("The structure does not contain any dataflows. At least one dataflow expected!");
        }
        return result;
    }

    /**
     * returns the dataflow for the given agency, dataflowId and dataflowVersion or null if not found
     *
     * @param dataflowIdentifier    the unique identifier of the dataflow ( agency, artefact id, version)
     * @return
     */
    private DataflowBean computeDataflowById(SdmxBeans sdmxBeans, StructureIdentifier dataflowIdentifier) {
        DataflowBean result = null;
        if (sdmxBeans.hasDataflows()) {
            if (dataflowIdentifier != null) {
                for (DataflowBean dataflow : sdmxBeans.getDataflows()) {
                    if (ObjectUtil.validString(dataflow.getVersion())) {
                        if (dataflow.getVersion().equals("*")) {
                            result = null;
                        }
                        if (dataflow.getAgencyId().equalsIgnoreCase(dataflowIdentifier.getAgency())
                                && dataflow.getId().equalsIgnoreCase(dataflowIdentifier.getArtefactId())
                                && isWildcardVersion(dataflowIdentifier)) {
                            result = dataflow;
                            break;
                        }
                        if (dataflow.getAgencyId().equalsIgnoreCase(dataflowIdentifier.getAgency())
                                && dataflow.getId().equalsIgnoreCase(dataflowIdentifier.getArtefactId())
                                && (dataflow.getVersion().equalsIgnoreCase(dataflowIdentifier.getArtefactVersion()))) {
                            result = dataflow;
                            break;
                        }
                    }
                }
            } else {
                result = sdmxBeans.getDataflows().iterator().next();
            }
        }
        return result;
    }

    // SDMXCONV-1526
    private boolean isWildcardVersion (StructureIdentifier dataflowIdentifier){
        if(dataflowIdentifier.getArtefactVersion().contains("+")){
           return true;
        }
        return false;
    }

    @Override
    public void clean() {
        dataflowIdentifier = null;
        dataflow = null;
        if (datastructScanner != null) {
            datastructScanner.clean();
            datastructScanner = null;
        }
    }
}
