package com.intrasoft.sdmx.converter.io.data;

import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.exception.ErrorLimitException;
import org.sdmxsource.sdmx.api.exception.ExceptionHandler;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.manager.retrieval.SdmxSuperBeanRetrievalManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.registry.ProvisionAgreementBean;
import org.sdmxsource.sdmx.api.model.data.Keyable;
import org.sdmxsource.sdmx.api.model.data.Observation;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.dataparser.model.DatasetInformation;
import org.springframework.beans.factory.annotation.Required;

public class MetadataManager {

    private SdmxSuperBeanRetrievalManager superRetrievalManager;

    private HeaderBean header;

    private int observationsCount;

    public void readData(DataReaderEngine dataReaderEngine, ExceptionHandler exceptionHandler) {
        int obsCount = 0;
        if (dataReaderEngine == null) {
            throw new IllegalArgumentException("AbstractDataValidationEngine can not be constructucted: DataReaderEngine can not be null");
        }
        dataReaderEngine.reset();
        setHeader(dataReaderEngine.getHeader());
        try {
            while (dataReaderEngine.moveNextDataset()) {
                // Construct a DataSetInformation
                DataStructureBean dataStructure = dataReaderEngine.getDataStructure();
                DataflowBean dataFlow = dataReaderEngine.getDataFlow();
                ProvisionAgreementBean provisionAgreement = dataReaderEngine.getProvisionAgreement();
                DatasetHeaderBean currentDatasetHeaderBean = dataReaderEngine.getCurrentDatasetHeaderBean();
                DatasetInformation dsi = new DatasetInformation(currentDatasetHeaderBean, dataStructure, dataFlow, provisionAgreement, superRetrievalManager);
                // Analyse the data file asking each DataValidationEngine to perform validation
                while (moveNextKeyable(dataReaderEngine, exceptionHandler)) {
                    Keyable key = null;
                    try {
                        key = dataReaderEngine.getCurrentKey();
                    } catch (SdmxException e) {
                        exceptionHandler.handleException(e);
                    }

                    if (key != null) {
                        while (moveNextObservation(dataReaderEngine, exceptionHandler)) {
                            Observation obs = dataReaderEngine.getCurrentObservation();
                            obsCount++;
                        }
                    }
                }
            }
        } catch (ErrorLimitException ex) {
            throw ex;
        } catch (Throwable th) {
            exceptionHandler.handleException(th);
        }
        setObservationsCount(obsCount);
    }

    private boolean moveNextKeyable(DataReaderEngine dre, ExceptionHandler exceptionHandler) {
        boolean hasNext = false;
        try {
            hasNext = dre.moveNextKeyable();
        } catch (SdmxException e) {
            exceptionHandler.handleException(e);
        }
        return hasNext;
    }

    private boolean moveNextObservation(DataReaderEngine dre, ExceptionHandler exceptionHandler) {
        boolean hasNext = false;
        try {
            hasNext = dre.moveNextObservation();
        } catch (SdmxException e) {
            exceptionHandler.handleException(e);
        }
        return hasNext;
    }

    @Required
    public void setSuperBeanRetrievalManager(SdmxSuperBeanRetrievalManager superBeanRetrievalManager) {
        this.superRetrievalManager = superBeanRetrievalManager;
    }

    public HeaderBean getHeader() {
        return header;
    }

    public void setHeader(HeaderBean header) {
        this.header = header;
    }

    public int getObservationsCount() {
        return observationsCount;
    }

    public void setObservationsCount(int observationsCount) {
        this.observationsCount = observationsCount;
    }
}
