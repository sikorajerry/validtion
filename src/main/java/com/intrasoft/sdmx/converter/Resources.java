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
package com.intrasoft.sdmx.converter;

import java.io.File;
import java.util.Properties;

/**
 * A general purpose class for things that must be accessible throughout the project
 * @author mfr
 */
public class Resources {
	
	// **************************************************************************
	// VERSIONING
	// **************************************************************************
	/** The GUI version */
	public static final String GUI_VERSION = "SDMX Converter v11.4.1";
	/** The release date */
	public static final String RELEASE_DATE = "04/15/2025";

//	// **************************************************************************
//	// LOGGING
//	// **************************************************************************
//	/** The logger used throughout the project */
//	public static Logger logger;
//	/** The file appender to be used for logging */
//	public static FileHandler appender;
//	/** The formatter to be used throughout logging */
//	public static SimpleFormatter layout;
	
	
	public static final String PARAMS_LOCATION = "params"; 
	
	public static final String REPOSITORY_CLASSPATH = "Converter_Data/"+PARAMS_LOCATION+"/";

	// **************************************************************************
	// SDMX REGISTRY
	// **************************************************************************
	/** The location of the parameters file */
	public static final String CONFIG_FILE_PATH = PARAMS_LOCATION + File.separator + "config.txt";
	/** The registry properties that will be parsed from the parameters file */
	public static Properties configProperties;		

	// **************************************************************************
	// ENCODING PARAMETERS FILE
	// **************************************************************************
	/** The location of the encoding file */
	public static final String ENCODING_FILE_PATH = PARAMS_LOCATION + File.separator + "encoding.txt";
	
	// **************************************************************************
	// ENCODING PARAMETERS FILE
	// **************************************************************************
	/** The location of the default output folder*/
	public static final String OUTPUT_FILE_PATH = "Output Files";
	
	/** The location of the repository path*/
	public static String repositoryPath ;
	
	// **************************************************************************
	// CONFIGURATION FILE FIELDS
	// **************************************************************************
	public static final String REGISTRY_URL = "registry.url";
	public static final String REGISTRY_ACTION = "registry.action";
	public static final String REGISTRY_USERNAME = "registry.username";
	public static final String REGISTRY_PASSWORD = "registry.password";
	public static final String REGISTRY_DOMAIN = "registry.domain";

	public static final String PROXY_HOST = "proxy.host";
	public static final String PROXY_PORT = "proxy.port";
	public static final String PROXY_USERNAME = "proxy.username";
	public static final String PROXY_PASSWORD = "proxy.password";
	public static final String NON_PROXY_HOSTS = "non.proxy.hosts";
	
	// **************************************************************************
	// DSPL CODE SOURCE REPOSITORY
	// **************************************************************************
	public static final String DSPL_REPO = "http://dspl.googlecode.com/hg/datasets/google/canonical";

}
