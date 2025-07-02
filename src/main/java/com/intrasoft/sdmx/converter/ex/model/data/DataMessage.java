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
package com.intrasoft.sdmx.converter.ex.model.data;

import com.intrasoft.sdmx.converter.ex.io.data.IoUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 
 * @author gko
 */
public class DataMessage extends MessageBean {
	private static Logger logger = LogManager.getLogger(DataMessage.class);
	private Properties properties;
	private DataSet dataSet;

	/**
	 * Creates a new instance of DataMessage. With this constructor, the default namespaces in the file
	 * params/sdmx_namespaces.properties will be used.
	 */
	public DataMessage() {
		super();
	}

	/**
	 * This constructor must be called if the namespaces that must be used are other than the default namespaces in the
	 * file params/sdmx_namespaces.properties.
	 * @param properties the namespace propertie to be loaded
	 */
	public DataMessage(final Properties properties) {
		super();
		this.properties = properties;
	}

	public void loadProperties() {	
		InputStream in = null;
		
		try{
			//try to load from classpath (in webservice deployment ConverterData/params folder should be added to classpath)
			in = IoUtils.getInputStreamFromClassPath("SDMX_namespaces.properties");
		    if(in ==null){
			    throw new IllegalStateException("The SDMX_namespaces.properties file could not be found in classpath ");
			}
			
			properties = new Properties();
	        properties.load(in);
	        
		}catch(IOException ioe){
			logger.error("Exception loading SDMX_namespaces.properties file ", ioe);
			throw new RuntimeException(ioe); 
		}
	}

	public DataSet getDataSet() {
		return dataSet;
	}

	public void setDataSet(final DataSet dataSet) {
		this.dataSet = dataSet;
	}

	public Properties getProperties() {
		if (properties == null) {
			loadProperties();
		}
		return properties;
	}
}
