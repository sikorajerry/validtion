package com.intrasoft.sdmx.converter.util;

import com.intrasoft.sdmx.converter.ConverterInput;
import com.intrasoft.sdmx.converter.ConverterOutput;
import com.intrasoft.sdmx.converter.ConverterStructure;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.ConverterDelegatorService;
import org.estat.sdmxsource.config.InputConfig;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.engine.reader.ThreadLocalOutputReporter;
import java.io.OutputStream;

/**
 * Utility class to accept all paramaters for already existent tests.
 * 
 * @author Mihaela Munteanu
 * @since 10th of July 2017
 *
 */

public class TestConverterUtil {
	
	private static int ignoredCount = 0;
	public static int getIgnoredCount() {
		return ignoredCount;
	}
	private static int proccessedCount = 0;
	public static int getProccessedCount() {
		return proccessedCount;
	}
	
	public static void convert(Formats inputType, 
			   Formats outputType,
			   ReadableDataLocation readableDataLocation,
			   OutputStream outputStream, 
			   InputConfig inputConfig, 
			   OutputConfig outputConfig, 
			   DataStructureBean dataStructure, 
			   DataflowBean dataflow, 
			   ConverterDelegatorService converterDelegatorService) throws Exception {
		ConverterInput converterInput = new ConverterInput(inputType, readableDataLocation, inputConfig);
		ConverterOutput converterOutput = new ConverterOutput(outputType, outputStream, outputConfig);
		ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow);
		converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
		ignoredCount = converterDelegatorService.getMetrics().getObservationsIgnored();
		proccessedCount = converterDelegatorService.getMetrics().getObservationsProcessed();
		// Clean all thread variables used
		ThreadLocalOutputReporter.unset();
	}
	public static void convert(Formats inputType, 
			   Formats outputType,
			   ReadableDataLocation readableDataLocation,
			   OutputStream outputStream, 
			   InputConfig inputConfig, 
			   OutputConfig outputConfig, 
			   DataStructureBean dataStructure, 
			   DataflowBean dataflow, 
			   ConverterDelegatorService converterDelegatorService, 
			   String defaultVersion) throws Exception {
		ConverterInput converterInput = new ConverterInput(inputType, readableDataLocation, inputConfig);
		ConverterOutput converterOutput = new ConverterOutput(outputType, outputStream, outputConfig);
		ConverterStructure converterStructure = new ConverterStructure(dataStructure, dataflow);
		converterStructure.setDefaultVersion(defaultVersion);
		converterDelegatorService.converterDelegate(converterInput, converterOutput, converterStructure);
		ignoredCount = converterDelegatorService.getMetrics().getObservationsIgnored();
		proccessedCount = converterDelegatorService.getMetrics().getObservationsProcessed();
		// Clean all thread variables used
		ThreadLocalOutputReporter.unset();
	}
}
