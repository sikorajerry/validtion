package com.intrasoft.sdmx.converter.config;

import org.estat.sdmxsource.config.InputConfig;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;

public class SdmxInputConfig implements InputConfig {
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
