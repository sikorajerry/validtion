package com.intrasoft.sdmx.converter.util;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Parameter for registry url (and rest api version) in webservice call for conversion
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1516">SDMXCONV-1516</a>
 */
public enum TypeOfVersion {
    EMPTY(""),
    SOAP("Soap"),
    REST_V1("Rest v1"),
    REST_V2("Rest v2");

    private String description;

    TypeOfVersion(String description) {
        this.description = description;
    }

    // Jackson uses this method to deserialize the JSON value into the enum
    @JsonCreator
    public static TypeOfVersion getTypeFromDescription(String description) {
        TypeOfVersion result = EMPTY;
        for (TypeOfVersion type : TypeOfVersion.values()) {
            if(type.getDescription().equalsIgnoreCase(description)){
                result = type;
                break;
            }
        }
        for (TypeOfVersion type : TypeOfVersion.values()) {
            if(type.name().equalsIgnoreCase(description)){
                result = type;
                break;
            }
        }
        return result;
    }

    public String getDescription() {
        return description;
    }
}
