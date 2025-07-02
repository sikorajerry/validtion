package com.intrasoft.sdmx.converter.io.data;

import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;
import com.intrasoft.sdmx.converter.model.ndata.ObservationData;

/**
 * Created by dragos balan
 */
public interface BufferedDataWriterEngine {

    /**
     * Typically used to open IO resources
     */
    void openWriter();

    /**
     * Typically used to close IO resources
     */
    void closeWriter();

    /**
     * header writing
     */
    void openHeader();
    void doWriteHeader(HeaderBean header);
    void closeHeader();

    /**
     *	dataset writing
     */
    void openDataset(DatasetHeaderBean header, AnnotationBean...annotations);
    void doWriteDatasetAttributes(Attrs datasetAttributes);
    void closeDataset();

    /**
     *	series writing
     */
    void openSeries(AnnotationBean... annotations);
    void doWriteSeriesKeysAndAttributes(Keys seriesKeys, Attrs seriesAttributes);
    void closeSeries();

    /**
     * group writing
     */
    void openGroup(String groupId, AnnotationBean... annotations);
    void doWriteGroupKeysAndAttributes(Keys groupKeys, Attrs groupAttributes);
    void closeGroup();

    /**
     * observation writing
     */
    void openObservation(AnnotationBean... annotations);
    void doWriteObservation(ObservationData observation, Attrs observationAttributes);
    void closeObservation();

    /**
     * accessor for the data structure.
     * By contract this method returns null during openWriter() and closeWriter() and a
     * not null data structure when called inside all other methods.
     *
     * @return  the data structure behind this writer
     */
    DataStructureBean getDataStructure();
}
