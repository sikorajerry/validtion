package com.intrasoft.sdmx.converter.structures;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.intrasoft.sdmx.converter.ui.Utils;
import com.intrasoft.sdmx.converter.util.StructureIdentifier;
import org.sdmxsource.util.ObjectUtil;

public final class StructureStubsScanner implements Serializable{
    
    private static final long serialVersionUID = 8083389471177005334L;

    private final Set<StructureIdentifier> structureStubs; 
    
    private Set<String> agencies; 
    
    public StructureStubsScanner(){
        this(Collections.<StructureIdentifier>emptySet());
    }
    
    public StructureStubsScanner(Set<StructureIdentifier> structureStubs){
        this.structureStubs = structureStubs; 
    }
    
    public int getNumberOfStubs(){
        return structureStubs.size(); 
    }
    
    public boolean hasStubs(){
        return !structureStubs.isEmpty(); 
    }
    
    public boolean hasOnlyOneStub(){
        return getNumberOfStubs() == 1; 
    }
    
    /**
     * warning: this should be called only when the number of stubs is 1. 
     * in all other cases, there's no guarantee that the set will return the same element 
     * @return
     */
    public StructureIdentifier getFirstStub(){
        return structureStubs.iterator().next(); 
    }
    
    public Set<String> getAgencies(){
        if(agencies == null){
            agencies = computeAgencies(); 
        }
        return agencies;
    }
    
    private Set<String> computeAgencies(){
        Set<String> result = new HashSet<>();
        for (StructureIdentifier structIdentifier: structureStubs){
            result.add(structIdentifier.getAgency());
        }
        //SDMXCONV-1156
        result = sortSet(result);
        return result;
    }
    
    public Set<String> getArtefactIdsForAgency(String agency){ //TODO: cache the values
        Set<String> result = new HashSet<>();
        for (StructureIdentifier structIdentifier: structureStubs){
            if(agency.equals(structIdentifier.getAgency())){
                result.add(structIdentifier.getArtefactId());
            }
        }
        //SDMXCONV-1156
        result = sortSet(result);
        return result;
    }

    /**
     * SDMXCONV-1156
     * Method which takes an unsortedSet as argument and sorts it. Then it returns it.
     * @param unsortedSet
     * @return
     */
    private Set<String> sortSet(Set<String> unsortedSet){
        Set<String> result = new HashSet<>();
        if(ObjectUtil.validCollection(unsortedSet)) {
            List<String> listOfArts = Utils.convertToList(unsortedSet);
            Collections.sort(listOfArts);
            result = Utils.convertToSet(listOfArts);
        }
        return  result;
    }

    public Set<String> getArtefactVersionsForAgencyAndArtefactId(String agency,
                                                                 String artefactId){//TODO: cache the values
        Set<String> result = new HashSet<>();
        for(StructureIdentifier structureIdentifier: structureStubs){
            if(agency.equals(structureIdentifier.getAgency()) && artefactId.equals(structureIdentifier.getArtefactId())){
                result.add(structureIdentifier.getArtefactVersion());
            }
        }
        //SDMXCONV-1156
        result = sortSet(result);
        return result;
    }
}
