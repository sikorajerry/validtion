package com.intrasoft.sdmx.converter.io.data;

import java.util.ArrayList;
import java.util.List;

import com.intrasoft.sdmx.converter.io.data.BufferedDataWriterEngineAdaptor;
import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;
import com.intrasoft.sdmx.converter.model.ndata.ObservationData;

import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.header.DatasetHeaderBean;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

public class MockBufferedDataWriterEngine extends BufferedDataWriterEngineAdaptor {
	
	private Attrs datasetAttrs;
	
	private Attrs seriesAttrs; 
	private Keys seriesKeys;
	
	private Keys groupKeys;
	private Attrs groupAttrs; 
	
	private Attrs obsAttrs;


	public MockBufferedDataWriterEngine() {
	}

	private List<String> performedOperations = new ArrayList<>();
	
	public Attrs getDatasetAttributes(){
		return datasetAttrs; 
	}
	
	public Attrs getSeriesAttributes(){
		return seriesAttrs; 
	}
	
	public Keys getSeriesKeys(){
		return seriesKeys; 
	}
	
	public Keys getGroupKeys(){
		return groupKeys; 
	}
	
	public Attrs getGroupAttributes(){
		return groupAttrs; 
	}
	
	public Attrs getObsAttributes(){
		return obsAttrs; 
	}
	
	public List<String> getPerformedOperations(){
		return performedOperations; 
	}
	
	public void openWriter(){
		performedOperations.add("openWriter"); 
	}
	public void closeWriter(){
		performedOperations.add("closeWriter");
	} 
	
	public void openHeader(){
		performedOperations.add("openHeader");
	}
	public void doWriteHeader(HeaderBean header){
		performedOperations.add("doWriteHeader");
	} 
	public void closeHeader(){
		performedOperations.add("closeHeader");
	}
	
	public void openDataset(DatasetHeaderBean header, AnnotationBean ...annotations){
		performedOperations.add("openDataset");
	}
	
	@Override
	public void doWriteDatasetAttributes(Attrs datasetAttributes) {
		this.datasetAttrs = datasetAttributes;
		performedOperations.add("doWriteDatasetAttributes");
	}

	public void closeDataset(){
		performedOperations.add("closeDataset");
	} 
	
	public void openSeries(AnnotationBean... annotations){
		performedOperations.add("openSeries");
	} 
	
	@Override
	public void doWriteSeriesKeysAndAttributes(Keys seriesKeys, Attrs seriesAttributes) {
		this.seriesKeys = seriesKeys; 
		this.seriesAttrs = seriesAttributes;
		performedOperations.add("doWriteSeriesKeysAndAttributes");
	}
	
	public void closeSeries(){
		performedOperations.add("closeSeries"); 
	}
	
	public void openGroup(String groupId, AnnotationBean... annotations){
		performedOperations.add("openGroup"); 
	} 
	
	@Override
	public void doWriteGroupKeysAndAttributes(Keys groupKeys, Attrs groupAttributes) {
		this.groupKeys = groupKeys; 
		this.groupAttrs = groupAttributes; 
		performedOperations.add("doWriteGroupKeysAndAttributes"); 
	}
	
	public void closeGroup(){
		performedOperations.add("closeGroup"); 
	}
	
	public void openObservation(AnnotationBean... annotations){
		performedOperations.add("openObservation");
	} 
	
	public void doWriteObservation(ObservationData obs, Attrs obsAttrs){
		this.obsAttrs = obsAttrs;
		performedOperations.add("doWriteObservation");
	}
	
	public void closeObservation(){
		performedOperations.add("closeObservation"); 
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
