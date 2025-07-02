package com.intrasoft.sdmx.converter.services.exceptions;

public class InvalidStructureException extends Exception{
   
    private static final long serialVersionUID = -5129744828108088055L;

    public InvalidStructureException(String message, Throwable cause){
        super(message, cause);
    }
    
    public InvalidStructureException(Throwable cause){
        super(cause); 
    }
    
    public InvalidStructureException(String message){
        super(message);
    }
}
