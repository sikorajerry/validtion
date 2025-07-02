package com.intrasoft.sdmx.converter.io.data;

import java.io.OutputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.SdmxConstants;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;

import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;
import com.intrasoft.sdmx.converter.model.ndata.ObservationData;
import com.intrasoft.sdmx.converter.util.GroupsCache;

/**
 * Created by dragos balan
 */
public class UtilityDataWriterEngine extends XmlBufferedDataWriterEngine {

    private static Logger logger = LogManager.getLogger(UtilityDataWriterEngine.class);

    //NAMESPACES
    private String structSpecificPrefix;
    private String structSpecificUri;

    /**
     * the cache for group keys and values
     */
    private GroupsCache groupsCache = new GroupsCache();
    private String currentGroup;
    private boolean isGroupTagOpen = false;
    
    private String comment;
    
    /**
     *
     * @param out
     */
    public UtilityDataWriterEngine(OutputStream out) {
        super(out);
    }

    @Override
    public void startXmlDocument(){
		// If there is a comment insert it at the top of the XML Document.
		if (this.comment != null) {
			writeComment(this.comment);
		}
        structSpecificPrefix = getDataStructure().getId().toLowerCase().substring(0, 3);
        structSpecificUri = "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure="+getDataStructure().getAgencyId()+
                                    ":"+getDataStructure().getId()+
                                    "("+getDataStructure().getVersion()+")"+
                                    ":utility";

        setDefaultNamespace(SdmxConstants.MESSAGE_NS_2_0);
        startXmlElement(SdmxConstants.UTILITY_DATA_ROOT_NODE);
        writeDefaultNamespace(SdmxConstants.MESSAGE_NS_2_0);
        writeXmlNamespace(structSpecificPrefix, structSpecificUri);
        //writeXmlNamespace("utility", SdmxConstants.UTILITY_NS_2_0);
        writeXmlNamespace(XSI_NS_PREFIX, XSI_NS_URI);
        writeXmlNamespace(XML_NS_PREFIX, XML_NS_URI);
        writeXmlAttribute(XSI_NS_PREFIX,
                 XSI_NS_URI,
                "schemaLocation",
                SdmxConstants.MESSAGE_NS_2_0+" SDMXMessage.xsd "+structSpecificUri+" "+getDataStructure().getAgencyId()+"_"+getDataStructure().getId()+"_Utility.xsd");
    }

    @Override
    public void endXmlDocument(){
        endXmlElement();//UtilityData
    }

    @Override
    public void openDataset(DatasetHeaderBean header, AnnotationBean... annotations) {
        startXmlElement(structSpecificPrefix, structSpecificUri, "DataSet");
    }

    @Override
    public void doWriteDatasetAttributes(Attrs datasetAttributes) {
        for (String attrName : datasetAttributes.getAttributeNames()) {
            writeXmlAttribute(attrName, datasetAttributes.getAttributeValue(attrName));
        }
    }

    @Override
    public void closeDataset() {
        //close the xml dataset element
        endXmlElement();
    }

    @Override
    public void openSeries(AnnotationBean... annotations) {
    }

    @Override
    public void doWriteSeriesKeysAndAttributes(Keys seriesKeys, Attrs seriesAttributes) {
        if(groupsCache.hasGroupForKey(seriesKeys)){
            Pair<String, Attrs> groupInfo = groupsCache.getGroupForSeries(seriesKeys);
            startXmlElement(structSpecificPrefix, structSpecificUri, groupInfo.getLeft());//start group element
            isGroupTagOpen = true;
        }
        startXmlElement(structSpecificPrefix, structSpecificUri, "Series");
        for (String attrName : seriesAttributes.getAttributeNames()) {
            writeXmlAttribute(attrName, seriesAttributes.getAttributeValue(attrName));
        }
        startXmlElement(structSpecificPrefix, structSpecificUri, "Key");
        for (String seriesKey : seriesKeys.getKeyNames()) {
            startXmlElement(structSpecificPrefix, structSpecificUri, seriesKey);
            writeXmlCharacters(seriesKeys.getKeyValue(seriesKey));
            endXmlElement();
        }
        endXmlElement(); //key
    }

    @Override
    public void closeSeries() {
        endXmlElement();//Series
        if(isGroupTagOpen){
            endXmlElement();//Group
            isGroupTagOpen = false;
        }
    }

    @Override
    public void openGroup(String groupId, AnnotationBean... annotations) {
        currentGroup = groupId;
    }

    @Override
    public void doWriteGroupKeysAndAttributes(Keys groupKeys, Attrs groupAttributes) {
        groupsCache.addGroup(currentGroup, groupKeys, groupAttributes);
    }

    @Override
    public void closeGroup() {
        currentGroup = null;
    }

    @Override
    public void openObservation(AnnotationBean... annotations) {}

    @Override
    public void doWriteObservation(ObservationData obs, Attrs observationAttributes) {
        startXmlElement(structSpecificPrefix, structSpecificUri,"Obs");
        for (String attrName:observationAttributes.getAttributeNames()) {
            writeXmlAttribute(attrName, observationAttributes.getAttributeValue(attrName));
        }
        startXmlElement(structSpecificPrefix, structSpecificUri, obs.getObsConceptId());
        writeXmlCharacters(obs.getObsConceptValue());
        endXmlElement();

        String primaryMeasureConcept = getComponentId(getStructureScanner().getDataStructure().getPrimaryMeasure());
        startXmlElement(structSpecificPrefix, structSpecificUri, primaryMeasureConcept);
        writeXmlCharacters(obs.getObsValue());
        endXmlElement();//this.isCrossSectional  = !crossSectionConcept.equals(DimensionBean.TIME_DIMENSION_FIXED_ID);
    }

    @Override
    public void closeObservation() {
        endXmlElement();//Obs
    }

	/** 
	 * Sets the appropriate comment
	 * 
	 * @return String: Comment
	 */
	public void setGeneratedComment(String comment) {
		this.comment = comment;
	}

    @Override
    public void writeComplexAttributeValue(KeyValue keyValue) {

    }

    @Override
    public void writeComplexMeasureValue(KeyValue keyValue) {

    }

    @Override
    public void writeMeasureValue(String id, String value) {

    }

    @Override
    public void writeObservation(String obsConceptValue, AnnotationBean... annotations) {

    }
}



