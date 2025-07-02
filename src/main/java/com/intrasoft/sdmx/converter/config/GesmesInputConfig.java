package com.intrasoft.sdmx.converter.config;

import org.estat.sdmxsource.config.InputConfig;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;

public class GesmesInputConfig  implements InputConfig {
	//codelists are needed for Gesmes input in wildcard situation
	private SdmxBeans sdmxBeans;

	//SDMXCONV-1060
	private boolean inlineReportFormat;
	//SDMXCONV-1205
	private boolean errorIfEmpty;

	private String callbackUrl;

	private SDMX_SCHEMA sdmxSchema;

	@Override
	public SDMX_SCHEMA getStructureSchemaVersion() {
		return this.sdmxSchema;
	}

	@Override
	public void setStructureSchemaVersion(SDMX_SCHEMA sdmxSchema) {
		this.sdmxSchema = sdmxSchema;
	}

	@Override
	public String getCallbackUrl() {
		return callbackUrl;
	}

	@Override
	public void setCallbackUrl(String callbackUrl) {
		this.callbackUrl = callbackUrl;
	}

	/**
	 * @return the sdmxBeans
	 */
	public SdmxBeans getSdmxBeans() {
		return sdmxBeans;
	}

	/**
	 * @param sdmxBeans the sdmxBeans to set
	 */
	public void setSdmxBeans(SdmxBeans sdmxBeans) {
		this.sdmxBeans = sdmxBeans;
	}

	@Override
	public boolean isInlineReportFormat() {
		return inlineReportFormat;
	}

	@Override
	public void setInlineReportFormat(boolean inlineReportFormat) {
		this.inlineReportFormat=inlineReportFormat;
	}

	@Override
	public boolean isErrorIfEmpty() {
		return errorIfEmpty;
	}

	@Override
	public void setErrorIfEmpty(boolean errorIfEmpty) {
		this.errorIfEmpty = errorIfEmpty;
	}
}
