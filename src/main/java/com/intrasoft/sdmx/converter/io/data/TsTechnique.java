package com.intrasoft.sdmx.converter.io.data;

public enum TsTechnique {

    TIME_RANGE("Time Range"), SINGLE_OBSERVATION("Single Observation");


    private String description;

    TsTechnique(String desc){
        this.description = desc;
    }

    public String getDescription(){
        return description;
    }
    
    /**
	 * returns a TsTechnique based on a string description 
	 * 
	 * @param description
	 * @return
	 */
	public static TsTechnique forDescription(String description){
		TsTechnique result = null; 
	    for(TsTechnique technique: TsTechnique.values()){
	        if(technique.getDescription().equals(description)){
	            result = technique; 
	            break; 
	        }
	    }	    
	    return result; 
	}
}
