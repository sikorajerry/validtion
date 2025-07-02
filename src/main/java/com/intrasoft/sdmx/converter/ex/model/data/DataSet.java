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

import java.util.ArrayList;
import java.util.List;

/**
 * @created 2008 4:52:38
 * @version 1.0
 */
public class DataSet extends AttachableArtefact {

	private String keyFamilyRef;
	private String keyFamilyURI;
	private String datasetID;
	private String dataProviderSchemeAgencyId;
	private String dataProviderSchemeId;
	private String dataProviderID;
	private String dataflowAgencyID;
	private String dataflowID;
	private String action;
	private String reportingBeginDate;
	private String reportingEndDate;
	private String validFromDate;
	private String validToDate;
	private String publicationYear;
	private String publicationPeriod;

	private List<GroupKey> groups = new ArrayList<GroupKey>();
	private List<TimeseriesKey> series = new ArrayList<TimeseriesKey>();

	public DataSet() {
		super();
	}

	public String getKeyFamilyRef() {
		return keyFamilyRef;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setKeyFamilyRef(final String newVal) {
		keyFamilyRef = newVal;
	}

	public String getKeyFamilyURI() {
		return keyFamilyURI;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setKeyFamilyURI(final String newVal) {
		keyFamilyURI = newVal;
	}

	public String getDatasetID() {
		return datasetID;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setDatasetID(final String newVal) {
		datasetID = newVal;
	}

	public String getDataProviderSchemeAgencyId() {
		return dataProviderSchemeAgencyId;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setDataProviderSchemeAgencyId(final String newVal) {
		dataProviderSchemeAgencyId = newVal;
	}

	public String getDataProviderSchemeId() {
		return dataProviderSchemeId;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setDataProviderSchemeId(final String newVal) {
		dataProviderSchemeId = newVal;
	}

	public String getDataProviderID() {
		return dataProviderID;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setDataProviderID(final String newVal) {
		dataProviderID = newVal;
	}

	public String getDataflowAgencyID() {
		return dataflowAgencyID;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setDataflowAgencyID(final String newVal) {
		dataflowAgencyID = newVal;
	}

	public String getDataflowID() {
		return dataflowID;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setDataflowID(final String newVal) {
		dataflowID = newVal;
	}

	public String getAction() {
		return action;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setAction(final String newVal) {
		action = newVal;
	}

	public String getReportingBeginDate() {
		return reportingBeginDate;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setReportingBeginDate(final String newVal) {
		reportingBeginDate = newVal;
	}

	public String getReportingEndDate() {
		return reportingEndDate;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setReportingEndDate(final String newVal) {
		reportingEndDate = newVal;
	}

	public String getValidFromDate() {
		return validFromDate;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setValidFromDate(final String newVal) {
		validFromDate = newVal;
	}

	public String getValidToDate() {
		return validToDate;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setValidToDate(final String newVal) {
		validToDate = newVal;
	}

	public String getPublicationYear() {
		return publicationYear;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setPublicationYear(final String newVal) {
		publicationYear = newVal;
	}

	public String getPublicationPeriod() {
		return publicationPeriod;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setPublicationPeriod(final String newVal) {
		publicationPeriod = newVal;
	}

	public List<GroupKey> getGroups() {
		return groups;
	}

	public void setGroups(final List<GroupKey> groups) {
		this.groups = groups;
	}

	public List<TimeseriesKey> getSeries() {
		return series;
	}

	public void setSeries(final List<TimeseriesKey> series) {
		this.series = series;
	}
	
	/**
	 * partial implementation
	 */
	public String toString(){
		return new StringBuilder("DataSet[datasetID=").append(datasetID).append(" ,")
				.append("keyFamilyRef=").append(keyFamilyRef)
				.append("]")
				.toString(); 
	}

}