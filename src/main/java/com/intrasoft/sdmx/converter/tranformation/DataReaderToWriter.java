package com.intrasoft.sdmx.converter.tranformation;

import com.intrasoft.sdmx.converter.io.data.Formats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.config.InputConfig;
import org.estat.sdmxsource.engine.decorators.ObservationCounterDecorator;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.engine.DataReaderEngine;
import org.sdmxsource.sdmx.api.engine.DataWriterEngine;
import org.sdmxsource.sdmx.api.exception.SdmxSemmanticException;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.sdmxbeans.model.beans.datastructure.CrossSectionalDataStructureBeanImpl;
import org.sdmxsource.util.ObjectUtil;

/**
 * <b>Class that synchronize reading and writing.</b>
 * <p>There are many different cases about the order we write the series, groups and observations that depends on the type of data.</p>
 * <p>This class implements the Strategy Design Pattern to reduce complexity and decouple read and write.</p>
 *
 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1324">SDMXCONV-1324</a>
 * @see <a href="https://howtodoinjava.com/design-patterns/behavioral/strategy-design-pattern/">Strategy Pattern</a>
 */
public class DataReaderToWriter {

	private static Logger logger = LogManager.getLogger(DataReaderToWriter.class);

	public static void copyToWriter(DataReaderEngine dataReaderEngine, DataWriterEngine dataWriterEngine, DataStructureBean dataStructureBean, InputConfig inputConfig, Formats outputFormat) {

		//No reader Found error
		if(!ObjectUtil.validObject(dataReaderEngine))
			throw new SdmxSemmanticException("Could not read the input File, no compatible Reader found!");

		ReadWriteContext context = new ReadWriteContext(dataReaderEngine, dataWriterEngine);
		if (outputFormat == Formats.CROSS_SDMX) {
			context.setStrategy(new CrossSectionalWriterStrategy());
			if(!(dataStructureBean instanceof CrossSectionalDataStructureBeanImpl)) {
				if (dataReaderEngine instanceof ObservationCounterDecorator) {
					((ObservationCounterDecorator) dataReaderEngine).setFatalErrorHappened(true);
				}
				throw new SdmxSemmanticException(ExceptionCode.UNSUPPORTED,
						"The Structure is not CrossSectional compatible, and the "+ outputFormat + " format is not supported");
			}
		} else if(outputFormat == Formats.STRUCTURE_SPECIFIC_DATA_2_1
				|| outputFormat == Formats.STRUCTURE_SPECIFIC_TS_DATA_2_1
				|| outputFormat == Formats.GENERIC_DATA_2_1
				|| outputFormat == Formats.GENERIC_TS_DATA_2_1
				|| outputFormat == Formats.GESMES_TS) {
			context.setStrategy(new TwoPointOneWriterStrategy());
		} else if(outputFormat == Formats.COMPACT_SDMX
				|| outputFormat == Formats.GENERIC_SDMX
				|| outputFormat == Formats.UTILITY_SDMX
				|| outputFormat == Formats.MESSAGE_GROUP) {
			context.setStrategy(new TwoPointZeroWriterStrategy());
		} else if( outputFormat == Formats.CSV
				|| outputFormat == Formats.MULTI_LEVEL_CSV
				|| outputFormat == Formats.SDMX_CSV
				|| outputFormat == Formats.iSDMX_CSV
				|| outputFormat == Formats.EXCEL
				|| outputFormat == Formats.FLR) {
			context.setStrategy(new FlatOutputWriterStrategy());
		} else if(outputFormat == Formats.STRUCTURE_SPECIFIC_DATA_3_0
				|| outputFormat == Formats.SDMX_CSV_2_0
				|| outputFormat == Formats.iSDMX_CSV_2_0) {
			context.setStrategy(new ThreePointZeroWriterStrategy());
		}
		context.reset();
		context.writeHeader();
		try {
			//Reading and writing starts here
			while (dataReaderEngine.moveNextDataset()) {
				//No compatible DSD when output is 2.0 version time must exist in the DSD
				if(!ObjectUtil.validObject(dataReaderEngine.getDataStructure().getTimeDimension())
						&& (outputFormat == Formats.COMPACT_SDMX || outputFormat == Formats.GENERIC_SDMX || outputFormat == Formats.UTILITY_SDMX)) {
					if (dataReaderEngine instanceof ObservationCounterDecorator) {
						((ObservationCounterDecorator) dataReaderEngine).setFatalErrorHappened(true);
					}
					throw new SdmxSemmanticException(ExceptionCode.UNSUPPORTED,
							"The DSD is not SDMX v2.0 compatible, It does not have a Time Dimension which is needed for time series 2.0 formats Compact, Generic and Utility");
				}

				//No compatible DSD when output is not 3.0 and the structure has inside either complex, multiple measures or multilingual.
				if(!dataReaderEngine.getDataStructure().isCompatible(SDMX_SCHEMA.VERSION_TWO_POINT_ONE)
						&& (outputFormat != Formats.iSDMX_CSV_2_0 && outputFormat != Formats.SDMX_CSV_2_0
							&& outputFormat != Formats.STRUCTURE_SPECIFIC_DATA_3_0 && outputFormat != Formats.CSV
								&& outputFormat != Formats.FLR)) {
					if (dataReaderEngine instanceof ObservationCounterDecorator) {
						((ObservationCounterDecorator) dataReaderEngine).setFatalErrorHappened(true);
					}
					throw new SdmxSemmanticException(ExceptionCode.UNSUPPORTED,
							"The DSD is SDMX v3.0 compatible, and the " + outputFormat + " format is not supported");
				}
				//First we write the dataSet and its attributes for every reader and writer this is the same
				context.writeDataset();
				String dimensionAtObservation = context.computeDimensionAtObservation();
				//Then we write series and observations every strategy implements this differently
				while (dataReaderEngine.moveNextKeyable()) {
					context.readWrite(dimensionAtObservation, inputConfig.getStructureSchemaVersion());
				}
			}
		} catch (Exception ex) {
			logger.debug("Error when transferring data from reader to writer", ex);
			throw ex;
		} finally {
			if(dataReaderEngine != null)
				dataReaderEngine.close();
			if(dataWriterEngine != null)
				dataWriterEngine.close();
		}

	}
}
