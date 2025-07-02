/**
 *
 * Copyright 2015 EUROSTAT
 *
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * 	https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */

package com.intrasoft.sdmx.converter.io.data;

import org.sdmxsource.sdmx.api.constants.DATA_TYPE;
import com.intrasoft.sdmx.converter.util.FormatFamily;

/**
 * Enumeration Class holding all formats supported by the SDMX Converter Application.
 */
public enum Formats {
    
    EMPTY("", true, true, FormatFamily.UNKNOWN, false, false, null, false, false, ""),
	CSV("CSV", true, true, FormatFamily.CSV, false, false, DATA_TYPE.CSV, true, false, ".csv"),
	MULTI_LEVEL_CSV("MULTI_LEVEL_CSV", true, true, FormatFamily.CSV, false, false, DATA_TYPE.CSV, true, false, ".csv"),
	SDMX_CSV("SDMX_CSV", true, true, FormatFamily.CSV, false, false, DATA_TYPE.CSV, true, false, ".csv"),
	SDMX_CSV_2_0("SDMX_CSV_2_0", true, true, FormatFamily.CSV, false, false, DATA_TYPE.CSV, true, false, ".csv"),
	iSDMX_CSV("iSDMX_CSV", false, true, FormatFamily.CSV, false, false, DATA_TYPE.CSV, false, false, ".csv"),//SDMXCONV-1141
	iSDMX_CSV_2_0("iSDMX_CSV_2_0", false, true, FormatFamily.CSV, false, false, DATA_TYPE.CSV, false, false, ".csv"),//SDMXCONV-1141
	FLR("FLR", true, true, FormatFamily.FLR, false, false, DATA_TYPE.OTHER, true, false, ".flr"),
	GESMES_TS("GESMES_TS",  true, true, FormatFamily.GESMES, false, false, DATA_TYPE.EDI_TS, false, true, ".ges"),
	GENERIC_SDMX("GENERIC_SDMX", true, true, FormatFamily.SDMX, false, false, DATA_TYPE.GENERIC_2_0, true, true, ".xml"),
	COMPACT_SDMX("COMPACT_SDMX", true, true, FormatFamily.SDMX, true, false, DATA_TYPE.COMPACT_2_0, true, true, ".xml"),
	UTILITY_SDMX("UTILITY_SDMX", true, true, FormatFamily.SDMX, true, false, DATA_TYPE.UTILITY_2_0, true, true, ".xml"),
	CROSS_SDMX("CROSS_SDMX", true, true, FormatFamily.SDMX, true, false, DATA_TYPE.CROSS_SECTIONAL_2_0, true, true, ".xml"),
	GENERIC_DATA_2_1("GENERIC_DATA_2_1", true, true, FormatFamily.SDMX, false, true, DATA_TYPE.GENERIC_2_1, true, true, ".xml"),
	GENERIC_TS_DATA_2_1("GENERIC_TS_DATA_2_1", true, true, FormatFamily.SDMX, false, true, DATA_TYPE.GENERIC_2_1, true, true, ".xml"),
	STRUCTURE_SPECIFIC_DATA_2_1("STRUCTURE_SPECIFIC_DATA_2_1", true, true, FormatFamily.SDMX, true, true, DATA_TYPE.COMPACT_2_1, true, true, ".xml"),
	STRUCTURE_SPECIFIC_TS_DATA_2_1("STRUCTURE_SPECIFIC_TS_DATA_2_1", true, true, FormatFamily.SDMX, true, true, DATA_TYPE.COMPACT_2_1, true, true, ".xml"),
	STRUCTURE_SPECIFIC_DATA_3_0("STRUCTURE_SPECIFIC_DATA_3_0", true, true, FormatFamily.SDMX, true, true, DATA_TYPE.STRUCTURE_SPECIFIC_3_0, true, true, ".xml"),
	EXCEL("EXCEL", true, true, FormatFamily.EXCEL, false, false, null, true, false, ".xlsx"),
	MESSAGE_GROUP("MESSAGE_GROUP", true, false, FormatFamily.SDMX, false, false, DATA_TYPE.MESSAGE_GROUP_2_0_GENERIC, true, true, ".xml");

	private boolean isInputFormat;
	private boolean isOutputFormat;
	private FormatFamily family;
	private String description;
	private boolean acceptCustomNameSpace;
	private boolean acceptReportingPeriod;
	private DATA_TYPE dataType;
	private boolean allowValidation;
	private boolean hasHeader;
	private String extension;

	/**
	 *
	 * @param description
	 * @param isInputFormat
	 * @param isOutputFormat
	 * @param family
	 * @param canHaveCustomNamespace
	 * @param canHaveReportingPeriod
	 * @param dataType
	 * @param allowValidation
	 * @param hasHeader
	 * @param extension
	 */
	private Formats(String description, 
	                boolean isInputFormat,
	                boolean isOutputFormat, 
	                FormatFamily family, 
	                boolean canHaveCustomNamespace, 
	                boolean canHaveReportingPeriod,
					DATA_TYPE dataType,
					boolean allowValidation,
					boolean hasHeader,
					String extension) {
		this.description = description;
		this.isInputFormat = isInputFormat;
		this.isOutputFormat = isOutputFormat;
		this.family = family; 
		this.acceptCustomNameSpace = canHaveCustomNamespace; 
		this.acceptReportingPeriod = canHaveReportingPeriod;
		this.dataType = dataType;
		this.allowValidation=allowValidation;
		this.hasHeader = hasHeader;
		this.extension = extension;
	}

	public boolean isInputFormat(){		
		return isInputFormat;		
	} 
	
	public boolean isOutputFormat(){		
		return isOutputFormat;		
	}
	
	public FormatFamily getFamily(){
		return family;
	}
	
	public String getDescription(){		
		return description;		
	}
	
	public boolean isSdmx(){
	    return FormatFamily.SDMX.equals(family); 
	}
	
	public boolean isAcceptCustomNamespace(){
	    return acceptCustomNameSpace; 
	}
			
	public boolean isAcceptReportingPeriod() {
		return acceptReportingPeriod;
	}

	public DATA_TYPE getDataType() {
		return dataType;
	}
		
	// returns true if the format can be validated
	public boolean isAllowValidation() {
		return allowValidation;
	}

	// returns true if the format has a header (eg.all SDMX formats)
	public boolean isHasHeader() {
		return hasHeader;
	}

	public String getExtension() {
		return extension;
	}

	public boolean isSdmx21() {
		if (Formats.STRUCTURE_SPECIFIC_DATA_2_1.equals(this) ||
			Formats.STRUCTURE_SPECIFIC_TS_DATA_2_1.equals(this) ||
			Formats.GENERIC_DATA_2_1.equals(this) ||
			Formats.GENERIC_TS_DATA_2_1.equals(this)) {
				return true;
			}
		return false;
	}

	public boolean isSdmx30() {
		if (Formats.SDMX_CSV_2_0.equals(this) ||
			Formats.iSDMX_CSV_2_0.equals(this) ||
			Formats.STRUCTURE_SPECIFIC_DATA_3_0.equals(this)) {
			return true;
		}
		return false;
	}

	/**
	 * returns a format based on a string description 
	 * 
	 * @param description
	 * @return
	 */
	public static Formats forDescription(String description){
	    Formats result = null; 
	    for(Formats frmt: Formats.values()){
	        if(frmt.getDescription().equals(description)){
	            result = frmt; 
	            break; 
	        }
	    }	    
	    return result; 
	}
	
	/**
	 * checks if the input/output format provided in the converter modules exists
	 * 
	 * @param description
	 * @return
	 */
	public static boolean isFormat(String description){
		for (Formats format: Formats.values()) {
			if (format.getDescription().equals(description)) {
				return true;
			}
		}
		return false;
	}
	
}
