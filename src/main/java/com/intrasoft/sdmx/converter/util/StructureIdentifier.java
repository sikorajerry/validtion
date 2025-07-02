package com.intrasoft.sdmx.converter.util;


import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import org.estat.struval.builder.impl.SdmxCompactDataUrnParser;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.base.MaintainableBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.DatasetStructureReferenceBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.structureretrieval.manager.InMemoryRetrievalManager;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.exceptions.ParseXmlException;
import com.intrasoft.sdmx.converter.services.exceptions.WriteHeaderException;
import org.sdmxsource.sdmx.validation.exceptions.InvalidInputParamsException;

/**
 * Identifier of a structure. 
 * 
 * The only reason this class exists and is not replaced by MainatainableBeanImpl is that 
 * the latter is not Serializable ( really needed on the web, especially for spring webflow framework) 
 * 
 * @author dragos balan
 *
 */
public class StructureIdentifier implements Serializable{

    private static final long serialVersionUID = 37734955741469737L;
	
	private String agency;
    private String artefactId;
    private String artefactVersion;
    
    public StructureIdentifier(){
        
    }
    
    public StructureIdentifier(String agencyId, String artefactId, String artefactVersion){
    	this.agency = agencyId; 
    	this.artefactId = artefactId; 
    	this.artefactVersion = artefactVersion; 
    }
    
    public StructureIdentifier(MaintainableBean maintainable){
        this(maintainable.getAgencyId(), maintainable.getId(), maintainable.getVersion()); 
    }
    
    public StructureIdentifier(CrossReferenceBean crossRefBean){
        this(crossRefBean.getAgencyId(), crossRefBean.getMaintainableId(), crossRefBean.getVersion());
    }
    
    public String getAgency() {
		return agency;
	}
    
    public void setAgency(String agency){
        this.agency = agency; 
    }


	public String getArtefactId() {
		return artefactId;
	}
	
	public void setArtefactId(String artefactId){
	    this.artefactId = artefactId; 
	}

	public String getArtefactVersion() {
		return artefactVersion;
	}
	
	public void setArtefactVersion(String artefactVersion){
	    this.artefactVersion = artefactVersion; 
	}
	
	public boolean isEmptyAgency(){
	    return agency == null || agency.isEmpty(); 
	}
	
	public boolean isEmptyArtefactId(){
	    return artefactId == null || artefactId.isEmpty(); 
	}
	
	public boolean isEmptyArtefactVersion(){
	    return artefactVersion == null || artefactVersion.isEmpty();
	}
	
	public boolean isEmpty(){
	    return isEmptyAgency() || isEmptyArtefactId() || isEmptyArtefactVersion();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((agency == null) ? 0 : agency.hashCode());
		result = prime * result + ((artefactId == null) ? 0 : artefactId.hashCode());
		result = prime * result + ((artefactVersion == null) ? 0 : artefactVersion.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StructureIdentifier other = (StructureIdentifier) obj;
		if (agency == null) {
			if (other.agency != null)
				return false;
		} else if (!agency.equals(other.agency))
			return false;
		if (artefactId == null) {
			if (other.artefactId != null)
				return false;
		} else if (!artefactId.equals(other.artefactId))
			return false;
		if (artefactVersion == null) {
			if (other.artefactVersion != null)
				return false;
		} else if (!artefactVersion.equals(other.artefactVersion))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "StructureIdentifier [agency=" + agency + ", artefactId=" + artefactId + ", artefactVersion="
				+ artefactVersion + "]";
	}
	
	/**
	 * Returns the structure id composed from the parameter or null if no data structure was specified
	 * @param id
	 * @param agency
	 * @param version
	 * @return StructureIdentifer Object
	 * @throws InvalidInputParamsException
	 */
	public static StructureIdentifier computeStructureIdentifier(String id, String agency, String version)
			throws InvalidInputParamsException {
		StructureIdentifier structIdentifier = null;
        if ((agency != null && !"".equals(agency)) && (id != null && !"".equals(id)) && (version != null && !"".equals(version))) {
            structIdentifier = new StructureIdentifier(agency, id, version);
        } else if ((agency != null && !"".equals(agency)) && (id != null && !"".equals(id)) && (version != null && !"".equals(version))) { 
        	//if any identifier is null or empty but not all of them the information is incomplete and an error is thrown
            throw new InvalidInputParamsException(103, "DSD/Dataflow id or version or agency not specified !");
        }
		return structIdentifier;
	}
	

	
    /**
     * Read the header from the input file
     * in case it is Sdmx 2.1 we can identify
     * data structure from <message:Structure> node
     * @param format
     * @param headerBean
     * @return StructureIdentifier
     * @throws IOException
     * @throws WriteHeaderException
     */
	public static StructureIdentifier computeStructureIdentifier(Formats format, HeaderBean headerBean) throws IOException, ParseXmlException {
		StructureIdentifier structIdentifier = null;
		/*if(format.isSdmx21()) {*/
			structIdentifier = getStructureVersionFromHeader21(format, headerBean);
		/*}*/
		return structIdentifier;
	}
	
    /**
     * Read the structure id from second line of the sdmx_csv or urn
     * @param format
     * @param structureId string as appears inside the input file
     * @return StructureIdentifier
     * @throws IOException
     * @throws WriteHeaderException
     */
	public static StructureIdentifier computeStructureIdentifier(Formats format, String structureId) throws IOException, ParseXmlException {
		StructureIdentifier structIdentifier = null;
		if(structureId!=null) {
			if(format==Formats.SDMX_CSV || format == Formats.SDMX_CSV_2_0) {
				structIdentifier = getStructureVersionFromId(format, structureId);
			}
			if (Formats.COMPACT_SDMX.equals(format) ||
	    			Formats.UTILITY_SDMX.equals(format) ||
	    				Formats.CROSS_SDMX.equals(format)) {
				SdmxCompactDataUrnParser dataUrnParser = new SdmxCompactDataUrnParser();
				StructureReferenceBean structureReference = dataUrnParser.buildStructureReference(structureId);
				structIdentifier = getStructureIdentifierFromReference(structureReference);
			}
		}
		return structIdentifier;
	}
	
	/**
	 * This method is used when everything else has failed
	 * and structure Identifier cannot be computed from elsewhere.
	 * It fetches the latest structure bean (Dataflow/Datastructure)
	 * @param structureBeans
	 * @param isDataFlow
	 * @return
	 */
	public static StructureIdentifier computeStructureIdentifier(SdmxBeans structureBeans, String isDataFlow) {
		StructureIdentifier structIdentifier = null;
		InMemoryRetrievalManager retrievalManager = null;
		Set<DataStructureBean> latestDsd = null;
		Set<DataflowBean> latestDataflow = null;
		StructureReferenceBean reference = null;
		if(structureBeans!=null) {
			retrievalManager = new InMemoryRetrievalManager(structureBeans);
			if("true".equalsIgnoreCase(isDataFlow)){
				//get the latest dataflow
				latestDataflow = retrievalManager.getDataflowBeans(null, true, true);
	    		for(DataflowBean dfl:latestDataflow) {
	    			reference = dfl.asReference();
	    		}
			}else {
				//get the latest dsd
				latestDsd = retrievalManager.getDataStructureBeans(null, true, true);
	    		for(DataStructureBean dsd:latestDsd) {
	    			reference = dsd.asReference();
	    		}
			}
			structIdentifier = getStructureIdentifierFromReference(reference);
		}
		return structIdentifier;
	}

	private static StructureIdentifier getStructureVersionFromId(Formats format, String structureId) {
		StructureIdentifier structIdentifier = null;
		if(structureId!=null && !"".equals(structureId)) {
			StructureReferenceBean ref = new StructureReferenceBeanImpl(SDMX_STRUCTURE_TYPE.DATAFLOW.getUrnPrefix()+structureId);
			structIdentifier = getStructureIdentifierFromReference(ref);
		}
		return structIdentifier;
	}

	private static StructureIdentifier getStructureVersionFromHeader21(Formats format, HeaderBean headerBean) throws IOException {
		StructureIdentifier structIdentifier = null;
		if(headerBean!=null) {
			if(headerBean.getStructures()!=null && headerBean.getStructures().size()>0) {
				DatasetStructureReferenceBean dsd = headerBean.getStructures().get(0);
				StructureReferenceBean refs = dsd.getStructureReference();
				structIdentifier = getStructureIdentifierFromReference(refs);
			}
		}
		return structIdentifier;
	}
	
	private static StructureIdentifier getStructureIdentifierFromReference(StructureReferenceBean reference) {
		StructureIdentifier structIdentifier = null;
		if(reference!=null && reference.getMaintainableReference()!=null) {
			String agency = reference.getMaintainableReference().getAgencyId();
			String id = reference.getMaintainableReference().getMaintainableId();
			String version = reference.getMaintainableReference().getVersion();
			
			//System.out.println("agency: "+agency +", id: "+ id +", version: "+ version);
			if ((agency != null && !"".equals(agency)) && (id != null && !"".equals(id)) && (version != null && !"".equals(version))) {
	            structIdentifier = new StructureIdentifier(agency, id, version);
	        }
		}
		return structIdentifier;
	}
	
}