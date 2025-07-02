package com.intrasoft.sdmx.converter.config;

import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvDateFormat;
import org.estat.sdmxsource.util.csv.CsvInputColumnHeader;
import org.estat.sdmxsource.util.csv.FlrInColumnMapping;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.util.LinkedHashMap;
import java.util.Map;

public class FlrOutputConfig  implements OutputConfig {
    private CsvInputColumnHeader outputColumnHeader = CsvInputColumnHeader.NO_HEADER;

    private HeaderBean header;

    private CsvDateFormat dateFormat = CsvDateFormat.SDMX;

    private Boolean inputOrdered = true;

    private LinkedHashMap<String, FlrInColumnMapping> mapping;

    private LinkedHashMap<String, FlrInColumnMapping> finalMapping;

    private LinkedHashMap<String, LinkedHashMap<String,String>> transcoding = new LinkedHashMap<String, LinkedHashMap<String,String>>();

    private boolean mapTranscoding = false;

    private SdmxBeanRetrievalManager beanRetrieval;

    private String padding = " ";

    private Map<String, Integer> lengthsCounting = null;

    /**
     * This is used for SDMX 3.0 to separate names and multiple values inside a field
     */
    private String subFieldSeparationChar;

    private SdmxBeanRetrievalManager retrievalManager;

    public SdmxBeanRetrievalManager getBeanRetrieval() {
        return beanRetrieval;
    }

    public void setBeanRetrieval(SdmxBeanRetrievalManager beanRetrieval) {
        this.beanRetrieval = beanRetrieval;
    }

    public FlrOutputConfig(){}

    public CsvInputColumnHeader getInputColumnHeader() {
        return outputColumnHeader;
    }

    public void setOutputColumnHeader(CsvInputColumnHeader outputColumnHeader) {
        this.outputColumnHeader = outputColumnHeader;
    }

    public HeaderBean getHeader() {
        return header;
    }

    public void setHeader(HeaderBean header) {
        this.header = header;
    }

    public CsvDateFormat getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(CsvDateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }

    public Boolean getInputOrdered() {
        return inputOrdered;
    }

    public void setInputOrdered(Boolean inputOrdered) {
        this.inputOrdered = inputOrdered;
    }

    public LinkedHashMap<String, FlrInColumnMapping> getMapping() {
        return mapping;
    }

    public void setMapping(LinkedHashMap<String, FlrInColumnMapping> mapping) {
        this.mapping = mapping;
    }

    public LinkedHashMap<String, LinkedHashMap<String, String>> getTranscoding() {
        return transcoding;
    }

    public void setTranscoding(LinkedHashMap<String, LinkedHashMap<String, String>> transcoding) {
        this.transcoding = transcoding;
    }

    public boolean isMapTranscoding() {
        return mapTranscoding;
    }

    public void setMapTranscoding(boolean mapTranscoding) {
        this.mapTranscoding = mapTranscoding;
    }

    public String getPadding() {
        return padding;
    }

    public Map<String, Integer> getLengthsCounting() {
        return lengthsCounting;
    }

    public void setLengthsCounting(Map<String, Integer> lengthsCounting) {
        this.lengthsCounting = lengthsCounting;
    }

    public void setPadding(String padding) {
        this.padding = padding;
    }

    public LinkedHashMap<String, FlrInColumnMapping> getFinalMapping() {
        return finalMapping;
    }

    public void setFinalMapping(LinkedHashMap<String, FlrInColumnMapping> finalMapping) {
        this.finalMapping = finalMapping;
    }

    public String getSubFieldSeparationChar() {
        return subFieldSeparationChar;
    }

    public void setSubFieldSeparationChar(String subFieldSeparationChar) {
        this.subFieldSeparationChar = subFieldSeparationChar;
    }

    public static boolean checkIfAutoExists(LinkedHashMap<String, FlrInColumnMapping> mapping) {
        boolean exists = false;
        if(mapping==null || mapping.isEmpty()) {
            exists = true;
        } else {
            for (Map.Entry entry : mapping.entrySet()) {
                FlrInColumnMapping flrMapping = (FlrInColumnMapping) entry.getValue();
                if (flrMapping.getFixedValue() != null && !flrMapping.getFixedValue().isEmpty() && flrMapping.getFixedValue().equalsIgnoreCase("AUTO")) {
                    exists = true;
                    return exists;
                }
            }
        }
        return exists;
    }
}
