package com.intrasoft.sdmx.converter.io.data.csv;

import com.intrasoft.sdmx.converter.ComponentValuesBuffer;
import com.intrasoft.sdmx.converter.io.data.ComponentBufferWriterEngine;
import org.estat.sdmxsource.util.csv.MultiLevelCsvOutputConfig;
import org.sdmxsource.sdmx.api.model.beans.base.AnnotationBean;
import org.sdmxsource.sdmx.api.model.data.KeyValue;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;

import java.io.*;
import java.util.List;

public class MultilevelCsvDataWriterEngine extends ComponentBufferWriterEngine {

    /**
     * the writer
     */
    private BufferedWriter outputWriter;

    /**
     * the output config
     */
    private MultiLevelCsvOutputConfig outputConfig;

	/**
	 *
	 * @param outputStream
	 * @param csvOutputConfig
	 */
	public MultilevelCsvDataWriterEngine(   OutputStream outputStream,
								            MultiLevelCsvOutputConfig csvOutputConfig){
        super(csvOutputConfig.getTranscoding());
        this.outputConfig = csvOutputConfig;
        try {
            this.outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Typically used to open IO resources
     */
    @Override
    public void openWriter() {

    }

    /**
     * Typically used to close IO resources
     */
    @Override
    public void closeWriter() {
        try {
            outputWriter.flush();
            outputWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * start for header writing
     */
    @Override
    public void openHeader() {

    }

    @Override
    public void doWriteHeader(HeaderBean header) {

    }

    @Override
    public void closeHeader() {

    }



	@Override
	protected void doWriteComponentsValues(ComponentValuesBuffer componentValues) {
        try {
            StringBuilder row ;
        	for (int i=1; i <= outputConfig.getLevels() ; i++) {
        	    row = new StringBuilder();
        		List<String> dimensionsFromLevel = outputConfig.getColumnMapping().getMappedComponentsForLevel(i);
        		//first element of the csv line is the level number
        		row.append(i).append(outputConfig.getDelimiter());
        		for (String dimension: dimensionsFromLevel) {
					String componentFromMap = componentValues.getValueFor(dimension);
        			if(componentFromMap == null){
						dimension = findComponentFromId(dimension);
					}
                    row.append(componentValues.getValueFor(dimension));
                    row.append(outputConfig.getDelimiter());
        		}
                row.deleteCharAt(row.length()-1);//delete the last delimiter
        		outputWriter.write(row.toString());
        		outputWriter.newLine();
        	}
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

	//SDMXRI-1166
    @Override
    public void close() {
        close(new FooterMessage[]{});
        //closeWriter();
    }
}