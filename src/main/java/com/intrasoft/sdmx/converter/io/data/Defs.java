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

/**
 * This is a class is responsible for offering global information to the
 * application.
 * 
 * @author Markos Fragkakis
 */
public class Defs {

	// input / output formats
	// public static final String SDMX_GENERIC = "SDMX_GENERIC";
	// public static final String SDMX_UTILITY = "sdmxSDMX_UTILITY_utility";
	// public static final String SDMX_COMPACT = "SDMX_COMPACT";
	// public static final String GESMES_TS = "GESMES_TS";
	// public static final String GESMES_2_1 = "GESMES_2_1";
	// public static final String GESMES_DSIS = "GESMES_DSIS";
	// public static final String CSV = "CSV";
	// public static final String FLR = "FLR";

	// Gesmes Writing techniques
	public static final String GESMES_TIME_RANGE = "Time Range";
	public static final String GESMES_SINGLE_OBSERVATION = "Single Observation";

	// Csv/Flr Date Compatibility
	public static final String DateCompatibility_SDMX = "SDMX";
	public static final String DateCompatibility_GESMES = "GESMES";
	
	//CSV/Flr Header Row
	public static final String USE_COLUMN_HEADERS = "USE_COLUMN_HEADERS"; //"Use column headers";
	public static final String DISREGARD_COLUMN_HEADERS = "DISREGARD_COLUMN_HEADERS"; //"Disregard column headers";
	public static final String NO_COLUMN_HEADERS = "NO_COLUMN_HEADERS";  //No column headers"; 
	
	public static final String UNKNOWN = "Unknown";
	
	/** Indicate usage of temporary files for buffering XS information in XS reader and writer */
	public static final String XS_BUFFERING_FILE = "xs_buffering_file";
	/** Indicate usage of ByteArray streams in memory for buffering XS information in XS reader and writer */
	public static final String XS_BUFFERING_MEMORY = "xs_buffering_memory";
}
