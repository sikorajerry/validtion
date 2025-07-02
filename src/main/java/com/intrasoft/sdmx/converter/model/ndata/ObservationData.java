package com.intrasoft.sdmx.converter.model.ndata;

/**
 * Created by dbalan on 6/14/2017.
 */
public class ObservationData {

    private final String obsConceptId;
    private final String obsConceptValue;
    private final String obsValue;

    public ObservationData(String obsConceptId, String obsConceptValue, String obsValue){
        this.obsConceptId = obsConceptId;
        this.obsConceptValue = obsConceptValue;
        this.obsValue = obsValue;
    }

    public String getObsConceptId(){
        return obsConceptId;
    }

    public String getObsConceptValue(){
        return obsConceptValue;
    }

    public String getObsValue(){
        return obsValue;
    }
}
