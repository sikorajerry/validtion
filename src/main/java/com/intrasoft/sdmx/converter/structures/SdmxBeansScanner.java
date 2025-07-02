package com.intrasoft.sdmx.converter.structures;

import org.sdmxsource.sdmx.api.model.beans.SdmxBeans;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;

import java.util.List;

/**
 * Created by dbalan on 6/27/2017.
 */
public interface SdmxBeansScanner {

    DataStructureBean getDataStructure();

    boolean hasDataflow();

    DataflowBean getDataflow();

    SdmxBeans getSdmxBeans();

    List<String> getCodedComponentNames();

    List<String> getCodesForComponent(String component);

    boolean hasCrossSectionalMeasures();

    boolean isCrossSectionalDataStructure();

    List<String> getComponents(boolean includeCrossXMeasures);
    
    List<String> getDimensions(boolean includeCrossXMeasures);

    void clean();
}
