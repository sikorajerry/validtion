package com.intrasoft.sdmx.converter.tranformation;

import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;

public class ReadWriteContext {

	IReadWriteStrategy strategy;

	DataReaderEngine dataReaderEngine;

	DataWriterEngine dataWriterEngine;

	public ReadWriteContext(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine) {
		this.dataReaderEngine = dataReaderEngine;
		this.dataWriterEngine = dataWriterEngine;
	}
	public ReadWriteContext(IReadWriteStrategy strategy, DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine) {
		this.strategy = strategy;
		this.dataReaderEngine = dataReaderEngine;
		this.dataWriterEngine = dataWriterEngine;
	}

	void setStrategy(IReadWriteStrategy strategy) {
		this.strategy = strategy;
	}

	void reset() {
		strategy.reset(dataReaderEngine);
	}

	void writeHeader() {
		strategy.writeHeader(dataReaderEngine, dataWriterEngine);
	}

	String computeDimensionAtObservation() {
		return strategy.computeDimensionAtObservation(dataReaderEngine);
	}

	void writeDataset() {
		strategy.writeDataset(dataReaderEngine, dataWriterEngine);
	}

	/**
	 * <b>This is the main different method for every strategy.</b>
	 * @param dimensionAtObservation Dimension At Observation
	 * @param sdmxSchema Version of Structure
	 */
	void readWrite(String dimensionAtObservation, SDMX_SCHEMA sdmxSchema) {
		this.strategy.readWrite(dataReaderEngine, dataWriterEngine, sdmxSchema, dimensionAtObservation);
	}
}
