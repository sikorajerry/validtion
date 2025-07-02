package com.intrasoft.sdmx.converter.config;

import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;

public class SdmxOutputConfig implements OutputConfig {
   	
	private String namespaceprefix;
	
	private String namespaceuri;
	
	private String reportingStartYearDate="--01-01";
	
	private boolean useReportingPeriod = false;

	public String getNamespaceprefix() {
		return namespaceprefix;
	}

	public void setNamespaceprefix(String namespaceprefix) {
		this.namespaceprefix = namespaceprefix;
	}

	public String getNamespaceuri() {
		return namespaceuri;
	}

	public void setNamespaceuri(String namespaceuri) {
		this.namespaceuri = namespaceuri;
	}

	public String getReportingStartYearDate() {
		return reportingStartYearDate;
	}

	public void setReportingStartYearDate(String reportingStartYearDate) {
		this.reportingStartYearDate = reportingStartYearDate;
	}
	
	public boolean isUseReportingPeriod() {
		return useReportingPeriod;
	}

	public void setUseReportingPeriod(boolean useReportingPeriod) {
		this.useReportingPeriod = useReportingPeriod;
	}

}
