package com.intrasoft.sdmx.converter;

public class InvalidConversionParameters extends ConverterException {

	public InvalidConversionParameters(String message) {
		super(message);
	}
	
	public InvalidConversionParameters(String message, Throwable cause) {
		super(message, cause);
	}
}
