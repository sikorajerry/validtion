package com.intrasoft.sdmx.converter.services.exceptions;

public class RegistryConnectionException extends Exception {
    
    public RegistryConnectionException(String message, Throwable cause){
        super(message, cause);
    }
    
    public RegistryConnectionException(Throwable cause){
        super(cause);
    }
}
