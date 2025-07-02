package com.intrasoft.sdmx.converter;

public class ConverterException extends Exception {
	
	public ConverterException(Throwable cause) {
		super(cause);
	}
	
	public ConverterException(String message, Throwable cause){
		super(message, cause);
	}
	
	public ConverterException(String message) {
		super(message);
	}
	
}
