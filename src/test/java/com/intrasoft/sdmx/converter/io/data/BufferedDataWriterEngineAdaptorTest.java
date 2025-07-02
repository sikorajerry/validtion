package com.intrasoft.sdmx.converter.io.data;

import com.intrasoft.sdmx.converter.io.data.BufferedDataWriterEngineAdaptor.Position;
import com.intrasoft.sdmx.converter.model.ndata.Attrs;
import com.intrasoft.sdmx.converter.model.ndata.Keys;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;

import java.util.Arrays;
import java.util.logging.Handler;
import java.util.logging.LogManager;

public class BufferedDataWriterEngineAdaptorTest {
	
	private MockBufferedDataWriterEngine classUnderTest = null;
	
	@Rule
    public ExpectedException expectedException = ExpectedException.none();
	
	@Before
	public void builClassUnderTest(){
		classUnderTest = new MockBufferedDataWriterEngine();
	}

	@BeforeClass
	public static void testSetup() {
		Configurator.setAllLevels("", org.apache.logging.log4j.Level.OFF);
		java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
		rootLogger.setLevel(java.util.logging.Level.OFF);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(java.util.logging.Level.OFF);
		}
	}
	
	@Ignore
	public void correctStatusIsComputed() {
		Assert.assertEquals(Position.WRITER_CREATED, classUnderTest.getCurrentPosition()); 
		classUnderTest.startDataset(null, null, null, null);
		Assert.assertEquals(Position.DATASET, classUnderTest.getCurrentPosition()); 
		classUnderTest.startGroup(null, null);
		Assert.assertEquals(Position.GROUP, classUnderTest.getCurrentPosition());
		classUnderTest.close(null);
		Assert.assertEquals(Position.WRITER_CLOSED, classUnderTest.getCurrentPosition());
	}
	
	@Ignore
	public void ensureCorrectPositionWhenStartingDatasetInsideDataset() {
		//we setup a dataset start and a group start
		classUnderTest.startDataset(null, null, null, null);
		
		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage("Illegal position: expected one of : [WRITER_CREATED, HEADER] but found DATASET");
		//there should be an exception thrown here because a 
		//dataset cannot be started inside a group
		classUnderTest.startDataset(null, null, null, null);
	}
	
	@Ignore
	public void ensureCorrectPositionWhenStartingDatasetInsideGroup() {
		//we setup a dataset start and a group start
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.startGroup(null, null);
		
		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage("Illegal position: expected one of : [WRITER_CREATED, HEADER] but found GROUP");
		//there should be an exception thrown here because a 
		//dataset cannot be started inside a group
		classUnderTest.startDataset(null, null, null, null);
	}
	
	@Ignore
	public void ensureCorrectPositionWhenStartingDatasetInsideSeries() {
		//we setup a dataset start and a group start
		classUnderTest.startDataset(null, null, null, new AnnotationBean[]{});
		classUnderTest.startGroup(null, null);
		classUnderTest.startSeries(null);
		
		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage("Illegal position: expected one of : [WRITER_CREATED, HEADER] but found SERIES");
		//there should be an exception thrown here because a 
		//dataset cannot be started inside a group
		classUnderTest.startDataset(null, null, null, new AnnotationBean[]{});
	}
	
	@Ignore
	public void ensureCorrectPositionWhenStartingDatasetAfterClose() {
		//we setup a data set start and a group start
		classUnderTest.startDataset(null, null, null, new AnnotationBean[]{});
		classUnderTest.close(null); 
		
		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage("Illegal position: expected one of : [WRITER_CREATED, HEADER] but found WRITER_CLOSED");
		//there should be an exception thrown here because a 
		//dataset cannot be started inside a group
		classUnderTest.startDataset(null, null, null, null);
	}

	@Ignore
	public void correctComputationOfDatasetAttributesAfterStartSeries() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		classUnderTest.startSeries(null, null);
		classUnderTest.close(null);
		
		Attrs expectedDatasetAttrs = new Attrs(); 
		expectedDatasetAttrs.add("attr1", "attrValue1"); 
		expectedDatasetAttrs.add("attr2", "attrValue2"); 
		
		//check correct computation of dataset attributes
		Assert.assertEquals(expectedDatasetAttrs, classUnderTest.getDatasetAttributes());
	}
	
	@Ignore
	public void correctComputationOfDatasetAttributesAfterStartingGroup() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		classUnderTest.startGroup(null, null);
		
		Attrs expectedDatasetAttrs = new Attrs(); 
		expectedDatasetAttrs.add("attr1", "attrValue1"); 
		expectedDatasetAttrs.add("attr2", "attrValue2"); 
		
		Assert.assertEquals(expectedDatasetAttrs, classUnderTest.getDatasetAttributes());
	}
	
	@Ignore
	public void correctComputationOfDatasetAttribAfterClose() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		classUnderTest.close(null);
		
		Attrs expectedDatasetAttrs = new Attrs(); 
		expectedDatasetAttrs.add("attr1", "attrValue1"); 
		expectedDatasetAttrs.add("attr2", "attrValue2"); 
		
		Assert.assertEquals(expectedDatasetAttrs, classUnderTest.getDatasetAttributes());
	}
	
	@Ignore
	public void correctUnderlyingCallsWhenClosingWriterAfterStartingSeries() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		classUnderTest.startSeries(null, null);
		classUnderTest.close(null);
		
		//check correct computation of dataset attributes
		Assert.assertEquals(
				Arrays.asList(	"openWriter", 
								"openDataset", 
								"doWriteDatasetAttributes", 
								"openSeries",
								"doWriteSeriesKeysAndAttributes", 
								"closeSeries", 
								"closeDataset", 
								"closeWriter"), 
				classUnderTest.getPerformedOperations());
		
	}
	
	@Ignore
	public void correctUnderlyingCallsWhenClosingWriterAfterStartingGroup() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr2", "attrValue2");
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "value1");
		classUnderTest.writeAttributeValue("groupAttr1", "value1");
		classUnderTest.close(null);
		
		//check correct computation of dataset attributes
		Assert.assertEquals(
						Arrays.asList(	"openWriter", 
										"openDataset", 
										"doWriteDatasetAttributes", 
										"openGroup", 
										"doWriteGroupKeysAndAttributes",
										"closeGroup",
										"closeDataset", 
										"closeWriter"), 
						classUnderTest.getPerformedOperations());
		
	}
	
	@Ignore
	public void correctUnderlyingCallsInANormalWriterFlow() {
		classUnderTest.writeHeader(null);
		classUnderTest.startDataset(null, null, null, null);		
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr2", "attrValue2");
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "value1");
		classUnderTest.writeAttributeValue("groupAttr1", "value1");
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeAttributeValue("seriesAttr1", "value1");
		classUnderTest.writeObservation("1", "1", "1", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("obsAttr1", "obs1");
		classUnderTest.writeObservation("2", "2", "2", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("obsAttr2", "obs1");
		classUnderTest.close(null);
		
		//check correct computation of dataset attributes
		Assert.assertEquals(
						Arrays.asList(	"openWriter",
										"openHeader", 
										"doWriteHeader", 
										"closeHeader",
										"openDataset", 
										"doWriteDatasetAttributes", 
										"openGroup", 
										"doWriteGroupKeysAndAttributes",
										"closeGroup",
										"openSeries",
										"doWriteSeriesKeysAndAttributes",
										"openObservation", 
										"doWriteObservation", 
										"closeObservation",
										"openObservation", 
										"doWriteObservation", 
										"closeObservation",
										"closeSeries", 
										"closeDataset", 
										"closeWriter"), 
						classUnderTest.getPerformedOperations());
	}
	
	@Ignore
	public void correctUnderlyingCallsWhenNoGroupsArePresent() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr2", "attrValue2");
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeAttributeValue("seriesAttr1", "value1");
		classUnderTest.writeObservation("1", "1", "1", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("obsAttr1", "obs1");
		classUnderTest.writeObservation("2", "2", "2", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("obsAttr2", "obs1");
		classUnderTest.close(null);
		
		//check correct computation of dataset attributes
		Assert.assertEquals(
						Arrays.asList(	"openWriter", 
										"openDataset", 
										"doWriteDatasetAttributes", 
										"openSeries",
										"doWriteSeriesKeysAndAttributes",
										"openObservation", 
										"doWriteObservation", 
										"closeObservation",
										"openObservation", 
										"doWriteObservation", 
										"closeObservation",
										"closeSeries", 
										"closeDataset", 
										"closeWriter"), 
						classUnderTest.getPerformedOperations());
	}
	
	@Ignore
	public void correctUnderlyingCallsWhenMultipleSeriesInDataset() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr2", "attrValue2");
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "value1");
		classUnderTest.writeAttributeValue("groupAttr1", "value1");
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeAttributeValue("seriesAttr1", "value1");
		classUnderTest.writeObservation("1", "1", "1", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("obsAttr1", "obs1");
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey2", "value1");
		classUnderTest.writeAttributeValue("seriesAttr2", "value1");
		classUnderTest.writeObservation("2", "2", "2", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("obsAttr1", "obs1");
		classUnderTest.close(null);
		
		//check correct computation of dataset attributes
		Assert.assertEquals(
						Arrays.asList(	"openWriter", 
										"openDataset", 
										"doWriteDatasetAttributes", 
										"openGroup", 
										"doWriteGroupKeysAndAttributes",
										"closeGroup",
										"openSeries",
										"doWriteSeriesKeysAndAttributes",
										"openObservation", 
										"doWriteObservation", 
										"closeObservation",
										"closeSeries", 
										"openSeries",
										"doWriteSeriesKeysAndAttributes",
										"openObservation", 
										"doWriteObservation", 
										"closeObservation",
										"closeSeries",
										"closeDataset", 
										"closeWriter"), 
						classUnderTest.getPerformedOperations());
	}
	
	
	@Ignore
	public void correctUnderlyingCallsWhenNoGroupIsPresent() {
		classUnderTest.startDataset(null, null, null, null);
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr2", "attrValue2");
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeAttributeValue("seriesAttr1", "value1");
		classUnderTest.writeObservation("", "", "", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("obsAttr1", "obs1");
		classUnderTest.close(null);
		
		//check correct computation of dataset attributes
		Assert.assertEquals(
						Arrays.asList(	"openWriter", 
										"openDataset", 
										"doWriteDatasetAttributes", 
										"openSeries",
										"doWriteSeriesKeysAndAttributes",
										"openObservation", 
										"doWriteObservation", 
										"closeObservation",
										"closeSeries", 
										"closeDataset", 
										"closeWriter"), 
						classUnderTest.getPerformedOperations());
	}
	
	@Ignore
	public void correctComputationOfGroupKeysAndAttributesAfterStartGroup() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		Attrs expectedGroupAttrs = new Attrs(); 
		expectedGroupAttrs.add("attr1", "attrValue1"); 
		expectedGroupAttrs.add("attr2", "attrValue2"); 
		
		Keys expectedGroupKeys = new Keys(); 
		expectedGroupKeys.add("groupKey1", "keyValue1");
	
		Assert.assertEquals(expectedGroupKeys, classUnderTest.getGroupKeys());
		Assert.assertEquals(expectedGroupAttrs, classUnderTest.getGroupAttributes());
	}
	
	@Ignore
	public void correctComputationOfGroupKeysAndAttributesAfterStartSeries() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.startSeries(null, null);
		Attrs expectedGroupAttrs = new Attrs(); 
		expectedGroupAttrs.add("attr1", "attrValue1"); 
		expectedGroupAttrs.add("attr2", "attrValue2"); 
		
		Keys expectedGroupKeys = new Keys(); 
		expectedGroupKeys.add("groupKey1", "keyValue1");
	
		Assert.assertEquals(expectedGroupKeys, classUnderTest.getGroupKeys());
		Assert.assertEquals(expectedGroupAttrs, classUnderTest.getGroupAttributes());
	}
	
	@Ignore
	public void correctComputationOfGroupKeysAndAttributesAfterClose() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.startSeries(null, null);
		Attrs expectedGroupAttrs = new Attrs(); 
		expectedGroupAttrs.add("attr1", "attrValue1"); 
		expectedGroupAttrs.add("attr2", "attrValue2"); 
		
		Keys expectedGroupKeys = new Keys(); 
		expectedGroupKeys.add("groupKey1", "keyValue1"); 
	
		Assert.assertEquals(expectedGroupKeys, classUnderTest.getGroupKeys());
		Assert.assertEquals(expectedGroupAttrs, classUnderTest.getGroupAttributes());
	}
	
	@Ignore
	public void correctComputationOfSeriesKeysAndAttributesAfterStartSeries() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("groupAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("groupAttr2", "attrValue2");
		
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeSeriesKeyValue("seriesKey2", "value2");
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.startSeries(null);
		Attrs expectedSeriesAttrs = new Attrs(); 
		expectedSeriesAttrs.add("attr1", "attrValue1"); 
		expectedSeriesAttrs.add("attr2", "attrValue2"); 
		
		Keys expectedSeriesKeys = new Keys(); 
		expectedSeriesKeys.add("seriesKey1", "value1"); 
		expectedSeriesKeys.add("seriesKey2", "value2");
	
		Assert.assertEquals(expectedSeriesKeys, classUnderTest.getSeriesKeys());
		Assert.assertEquals(expectedSeriesAttrs, classUnderTest.getSeriesAttributes());
	}
	
	@Ignore
	public void correctComputationOfSeriesKeysAndAttributesAfterWriteObservation() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("groupAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("groupAttr2", "attrValue2");
		
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeSeriesKeyValue("seriesKey2", "value2");
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.writeObservation("", "", "", new AnnotationBean[]{});
		Attrs expectedSeriesAttrs = new Attrs(); 
		expectedSeriesAttrs.add("attr1", "attrValue1"); 
		expectedSeriesAttrs.add("attr2", "attrValue2"); 
		
		Keys expectedSeriesKeys = new Keys(); 
		expectedSeriesKeys.add("seriesKey1", "value1"); 
		expectedSeriesKeys.add("seriesKey2", "value2"); 
	
		Assert.assertEquals(expectedSeriesKeys, classUnderTest.getSeriesKeys());
		Assert.assertEquals(expectedSeriesAttrs, classUnderTest.getSeriesAttributes());
	}
	
	@Ignore
	public void correctComputationOfSeriesKeysAndAttributesAfterClose() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("groupAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("groupAttr2", "attrValue2");
		
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeSeriesKeyValue("seriesKey2", "value2");
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.startSeries(null);
		Attrs expectedSeriesAttrs = new Attrs(); 
		expectedSeriesAttrs.add("attr1", "attrValue1"); 
		expectedSeriesAttrs.add("attr2", "attrValue2"); 
		
		Keys expectedSeriesKeys = new Keys(); 
		expectedSeriesKeys.add("seriesKey1", "value1"); 
		expectedSeriesKeys.add("seriesKey2", "value2");
	
		Assert.assertEquals(expectedSeriesKeys, classUnderTest.getSeriesKeys());
		Assert.assertEquals(expectedSeriesAttrs, classUnderTest.getSeriesAttributes());
	}
	
	@Ignore
	public void correctComputationOfObservationAttributesAfterStartSeries() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue2");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("groupAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("groupAttr2", "attrValue2");
		
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeSeriesKeyValue("seriesKey2", "value2");
		classUnderTest.writeAttributeValue("seriesAttr1", "attrValue1");
		classUnderTest.writeAttributeValue("seriesAttr2", "attrValue2");
		
		classUnderTest.writeObservation("", "", "", new AnnotationBean[]{});
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.startSeries(null);
		
		Attrs expectedObsAttrs = new Attrs(); 
		expectedObsAttrs.add("attr1", "attrValue1"); 
		expectedObsAttrs.add("attr2", "attrValue2"); 
	
		Assert.assertEquals(expectedObsAttrs, classUnderTest.getObsAttributes());
	}
	
	@Ignore
	public void correctComputationOfObservationAttributesAfterWriteObservation() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("groupAttr1", "attrValue1");
		
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeAttributeValue("seriesAttr1", "attrValue1");
		
		classUnderTest.writeObservation("", "", "", null);
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.writeObservation("", "", "", null);
		
		Attrs expectedObsAttrs = new Attrs(); 
		expectedObsAttrs.add("attr1", "attrValue1"); 
		expectedObsAttrs.add("attr2", "attrValue2");		 
	
		Assert.assertEquals(expectedObsAttrs, classUnderTest.getObsAttributes());
	}
	
	
	@Ignore
	public void correctComputationOfObservationAttributesAfterClose() {
		classUnderTest.startDataset(null, null, null, null);
		//dataset attributes
		classUnderTest.writeAttributeValue("datasetAttr1", "attrValue1");
		
		classUnderTest.startGroup(null, null);
		classUnderTest.writeGroupKeyValue("groupKey1", "keyValue1");
		classUnderTest.writeAttributeValue("groupAttr1", "attrValue1");
		
		classUnderTest.startSeries(null);
		classUnderTest.writeSeriesKeyValue("seriesKey1", "value1");
		classUnderTest.writeAttributeValue("seriesAttr1", "attrValue1");
		
		classUnderTest.writeObservation("", "", "", null);
		classUnderTest.writeAttributeValue("attr1", "attrValue1");
		classUnderTest.writeAttributeValue("attr2", "attrValue2");
		
		classUnderTest.close();
		
		Attrs expectedObsAttrs = new Attrs(); 
		expectedObsAttrs.add("attr1", "attrValue1"); 
		expectedObsAttrs.add("attr2", "attrValue2");		 
	
		Assert.assertEquals(expectedObsAttrs, classUnderTest.getObsAttributes());
	}
}
