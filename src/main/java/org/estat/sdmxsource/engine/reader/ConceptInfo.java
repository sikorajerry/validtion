/**
 * Copyright (c) 2015 European Commission.
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl5
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package org.estat.sdmxsource.engine.reader;

public class ConceptInfo {
    /**
	 * the serial id
	 */
	@SuppressWarnings("unused")
	private static final long serialVersionUID = -76497946269676284L;

	/**
     * flag for fixed value
     */
    private boolean fixed = false;

    /**
     * the fixed value
     */
    private String fixedValue =  null;


    /**
     * the list of columns( zero based) mapped to the current dimension/attribute/measure
     */
    private Integer column;       
    
    
    /** 
     * Public constructor 
     * @param level
     * @param fixed
     * @param columns
     * @param fixedValue
     */
    public ConceptInfo(Integer column, boolean fixed, String fixedValue) {
    	this.fixed = fixed;
    	this.fixedValue = fixedValue;
    	this.column = column;
    }

    /*Getters and setters of class */
    
	public boolean isFixed() {
		return fixed;
	}

	public void setFixed(boolean fixed) {
		this.fixed = fixed;
	}

	public String getFixedValue() {
		return fixedValue;
	}

	public void setFixedValue(String fixedValue) {
		this.fixedValue = fixedValue;
	}

	public Integer getColumn() {
		return column;
	}

	public void setColumns(Integer column) {
		this.column = column;
	}    
}
