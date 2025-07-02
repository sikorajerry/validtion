/**
 * Copyright 2015 EUROSTAT
 * <p>
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter;

import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;

/**
 * Class that groups converter input objects. 
 * 
 * @author Mihaela Munteanu
 * @since 10th of July 2017
 *
 */
public class ConverterStructure {
	
	private DataStructureBean dataStructure;
	
	private DataflowBean dataflow;
	
	private SdmxBeanRetrievalManager retrievalManager;
	
	private String defaultVersion;

	private SdmxBeans sdmxBeans;

	/** Default public constructor */
	public ConverterStructure() {

	}
	
	/** Full constructor */
	public ConverterStructure(DataStructureBean dataStructureBean, DataflowBean dataflow) {
		this.dataStructure = dataStructureBean;
		this.dataflow = dataflow;
	}
	
	/** Full constructor with retrieval manager */
	public ConverterStructure(DataStructureBean dataStructureBean, DataflowBean dataflow, SdmxBeanRetrievalManager retrievalManager) {
		this.dataStructure = dataStructureBean;
		this.dataflow = dataflow;
		this.retrievalManager = retrievalManager;
	}
	
	/** Full constructor with retrieval manager and default Structure version*/
	public ConverterStructure(DataStructureBean dataStructureBean, DataflowBean dataflow, SdmxBeanRetrievalManager retrievalManager, String defaultVersion) {
		this.dataStructure = dataStructureBean;
		this.dataflow = dataflow;
		this.retrievalManager = retrievalManager;
		this.defaultVersion = defaultVersion;
	}
	
	/** 
	 * Constructor only with retrieval Manager
	 * when we have multiple dsds
	 */
	public ConverterStructure(SdmxBeanRetrievalManager retrievalManager) {
		this.retrievalManager = retrievalManager;
	}
	
	/**
	 * @return the dataStructure
	 */
	public DataStructureBean getDataStructure() {
		return dataStructure;
	}

	/**
	 * @param dataStructure the dataStructure to set
	 */
	public void setDataStructure(DataStructureBean dataStructure) {
		this.dataStructure = dataStructure;
	}

	/**
	 * @return the dataflow
	 */
	public DataflowBean getDataflow() {
		return dataflow;
	}

	/**
	 * @param dataflow the dataflow to set
	 */
	public void setDataflow(DataflowBean dataflow) {
		this.dataflow = dataflow;
	}

	public SdmxBeanRetrievalManager getRetrievalManager() {
		return retrievalManager;
	}

	public void setRetrievalManager(SdmxBeanRetrievalManager retrievalManager) {
		this.retrievalManager = retrievalManager;
	}

	public String getDefaultVersion() {
		return defaultVersion;
	}

	public void setDefaultVersion(String defaultVersion) {
		this.defaultVersion = defaultVersion;
	}

	public SdmxBeans getSdmxBeans() {
		return sdmxBeans;
	}

	public void setSdmxBeans(SdmxBeans sdmxBeans) {
		this.sdmxBeans = sdmxBeans;
	}
}
