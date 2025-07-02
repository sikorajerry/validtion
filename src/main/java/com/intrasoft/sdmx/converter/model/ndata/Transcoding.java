package com.intrasoft.sdmx.converter.model.ndata;


import lombok.Data;

import java.io.Serializable;

public @Data class Transcoding implements Serializable{

    private String sdmxComponentId;
    private String localCode;
    private String sdmxCode;
    
}
