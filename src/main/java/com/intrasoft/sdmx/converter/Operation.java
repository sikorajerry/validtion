package com.intrasoft.sdmx.converter;

public enum Operation {
    
    CONVERSION(true, false, "Convert"), VALIDATION(false, true, "Validation"), VALIDATION_AND_CONVERSION(true, true, "Validation and Conversion");
    
    private boolean isConversion; 
    private boolean isValidation;
    private String description;
    
    private Operation(boolean isConversion, boolean isValidation, String description){
        this.isConversion = isConversion; 
        this.isValidation = isValidation;
        this.description = description;
    }
    
    public static Operation getOperationByDescrption(String description) {
    	Operation result = null;
    	for (Operation operation: Operation.values()) {
    		if (operation.getDescription().equals(description)) {
    			result = operation;
    			break;
    		}
    	}
    	return result;
    }
    
    public boolean isConversion(){
        return isConversion; 
    }
    
    public boolean isValidation(){
        return isValidation; 
    }
    
    public String getDescription() {
    	return description;
    }
}
