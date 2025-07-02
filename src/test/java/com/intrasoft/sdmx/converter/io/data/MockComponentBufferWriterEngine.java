package com.intrasoft.sdmx.converter.io.data;

import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import com.intrasoft.sdmx.converter.ComponentValuesBuffer;
import com.intrasoft.sdmx.converter.io.data.ComponentBufferWriterEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dbalan on 6/13/2017.
 */
public class MockComponentBufferWriterEngine extends ComponentBufferWriterEngine {

    private List<Map<String, String>> valuesWritten = new ArrayList<>();

    public MockComponentBufferWriterEngine(){
    }

    @Override
    protected void doWriteComponentsValues(ComponentValuesBuffer componentValues) {
        valuesWritten.add(componentValues.toMap());
    }

    @Override
    public void openWriter() {

    }

    @Override
    public void closeWriter() {

    }

    @Override
    public void openHeader() {

    }

    @Override
    public void doWriteHeader(HeaderBean header) {

    }

    @Override
    public void closeHeader() {

    }

    public List<Map<String, String>> getValuesWritten(){
        return valuesWritten;
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
