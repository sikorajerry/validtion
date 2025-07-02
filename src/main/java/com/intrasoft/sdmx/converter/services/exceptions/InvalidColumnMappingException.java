package com.intrasoft.sdmx.converter.services.exceptions;

public class InvalidColumnMappingException extends Exception{

    private static final long serialVersionUID = 8185751768563126891L;
    
    public InvalidColumnMappingException(String message){
        super(message);
    }
    
    public InvalidColumnMappingException(String message, Throwable source){
        super(message, source);
    }
    
}
