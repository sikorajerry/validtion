package com.intrasoft.sdmx.converter.services;

import java.util.ArrayList;
import java.util.List;
import org.estat.struval.ValidationError;

import lombok.Data;

public @Data class ValidationServiceReturn {

	private List<ValidationError> errors = new ArrayList<>();

	private int numberOfErrorsFound = 0;

	private boolean hasMoreErrors = false;
	
	private int obsCount = 0; 
}
