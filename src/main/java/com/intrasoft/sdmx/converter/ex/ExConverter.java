/**
 *
 * Copyright 2015 EUROSTAT
 *
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * 	https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.ex;

import com.intrasoft.sdmx.converter.ConverterMetrics;
import com.intrasoft.sdmx.converter.Resources;
import com.intrasoft.sdmx.converter.ex.io.data.Reader;
import com.intrasoft.sdmx.converter.ex.io.data.Writer;
import com.intrasoft.sdmx.converter.ex.io.data.*;
import com.intrasoft.sdmx.converter.io.data.ComponentBeanUtils;
import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.ui.exceptions.CSVWriterValidationException;
import com.intrasoft.sdmx.converter.ui.exceptions.ReaderException;
import com.intrasoft.sdmx.converter.ui.exceptions.ReaderValidationException;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.weaver.patterns.ParserException;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.dataparser.engine.reader.RecordReaderCounter;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

/**
 * Main Class of the SDMX Converter Application. This Class accepts the input format, parses it according to the given
 * definitions, invokes the appropriate conversion writer classes and returns the resulting dataset.
 */
public class ExConverter {

	private String lineEnd = System.getProperty("line.separator");
	private Formats from;
	private Formats to;
	private InputStream is;
	private OutputStream os;
	private InputParams params;

	private static Logger logger = LogManager.getLogger(ExConverter.class);

	public static ConverterMetrics getMetrics() { return metrics; }

	private static ConverterMetrics metrics;

	/**
	 * This is a field to contain potential exceptions that may make the thread crash. It is used to pass the exception
	 * to the main application
	 */
	private static Throwable crashThrowable;

	/**
	 * Getter for the crashTrowable. Typically used by the outer GUI to know that an exception occured
	 * @return crashTrowable
	 */
	public Throwable getCrashThrowable() {
		return crashThrowable;
	}

	/**
	 * This constructor sets the parameters for the converter to be able to run. The converter can now run as a thread
	 * with the run method
	 * @param fromFormat the format from which we will convert
	 * @param toFormat the format to wich we will convert
	 * @param inStr the input stream of the input file
	 * @param outStr the output stream of the output file
	 * @param inputParams the input parameters
	 */
	public ExConverter(Formats fromFormat, Formats toFormat, InputStream inStr, OutputStream outStr, InputParams inputParams) {
		from = fromFormat;
		to = toFormat;
		is = inStr;
		os = outStr;
		params = inputParams;
	}

	/**
	 * No argument constructor
	 */
	public ExConverter() {

	}

	/**
	 * The core method of the Corverter Class that handles the flow of the conversions. It accepts the input and ouput
	 * format and files, the parameters needed for the conversion and the definition of the Dataset, and invokes the
	 * appropriate readers and writers.
	 * @param from the input format
	 * @param to the output format
	 * @param is the input dataset
	 * @param os the output dataset
	 * @param params the conversion parameters
	 * @throws Exception
	 */
	public static void convert(Formats from, Formats to, InputStream is, OutputStream os, InputParams params) throws Exception {

		Object[] sliceFileNames =null;
		boolean fromEqualsDSXML = false;
		try {
			long before = System.currentTimeMillis();

			Validate.notNull(from, "You must select an input type");
			Validate.notNull(to, "You must select an output type");
			Validate.notNull(is, "You must provide an input file");
			Validate.notNull(os, "You must provide an output file");
			if (from == Formats.CSV) {
				Validate.notNull(params.getHeaderBean(), "You must provide a header file");
			}
			// check if there exist unmapped dimensions
			String cref;
			DataStructureBean tmp = params.getKeyFamilyBean();
			Map<String, String[]> map = params.getMappingMap();
			if (map != null) {
				for (int i = 0; i < tmp.getDimensionList().getDimensions().size(); i++) {
					if (tmp.getDimensionList().getDimensions().get(i).isTimeDimension()) {
						continue;
					}
					cref = tmp.getDimensionList().getDimensions().get(i).getId();
					String data[] = map.get(cref);
					String position="";
					if (data!=null) {
						position = data[0];
					}
				}
			}						
			
			/* if the output format is CrossX then the DataStructureBean should be examined...if it is a flat DSD file then a customXS writer is utilized
			 * in order to avoid memory exceptions
			 */
			boolean customCross = true;						
			if (to == Formats.CROSS_SDMX){		
				//check the crossX attachment level of the dimensions	
				if (tmp != null) {// && !(tmp instanceof CrossSectionalDataStructureBean)) {									
					final ArrayList<DimensionBean> dimensions = (ArrayList<DimensionBean>) tmp.getDimensions();					
					for (final DimensionBean dim : dimensions) {
						Map<String, String> valuesDimension = ComponentBeanUtils.getDimensionInfo(dim, tmp);						
						if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION) == null || 
								!valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION).equalsIgnoreCase("true")) {							
							if (!dim.isFrequencyDimension()){
								//if it is not the FREQ dimension then do not use the custom crossX writer
								customCross = false;
								break;
							}															
						}																	
					}					
				}
				
			}
			
			int metr = 1;

			int ingoredObsCount = 0;
			int obsCount = 0;
			for (int i = 0; i<metr; i++){
				Reader reader = getReader(from);
				Writer writer = getWriter(to);
				writer.setOutputStream(os);
				if (writer instanceof CompactDataWriter || writer instanceof CrossDataWriter) {
					params.setGeneratedFileComment("Created with "+ Resources.GUI_VERSION);
				}
				writer.setInputParams(params);
				reader.setInputStream(is);
				reader.setInputParams(params);				
				reader.setWriter(writer);

				try {
					reader.readData();
					if (reader instanceof RecordReaderCounter) {
					    ingoredObsCount = ((RecordReaderCounter) reader).getIngoredObsCount();
					    obsCount = ((RecordReaderCounter) reader).getObsCount();
					}else {
						ingoredObsCount=0;
						obsCount = 0;
					}
				}catch (IndexOutOfBoundsException iofbe) {
					throw new ReaderException(107, iofbe.getMessage(), iofbe.getCause());
				} catch (OutOfMemoryError ofme) {
					throw new ReaderException(107, ofme.getMessage(), ofme.getCause());
				} catch (SAXParseException spe) {
					throw new ReaderException(107, spe.getMessage(), spe.getCause());
				} catch (SAXException se) {
					throw new ReaderException(107, se.getMessage(), se.getCause());
				} catch (FileNotFoundException ffe) {
					throw new ReaderException(107, ffe.getMessage(), ffe.getCause());
				} catch (UnsupportedEncodingException uee) {
					throw new ReaderException(107, uee.getMessage(), uee.getCause());
				} catch (IOException ioe) {
					throw new ReaderException(107, ioe.getMessage(), ioe.getCause());
				/*} catch (JDOMException jde) {
					throw new ReaderException(107, jde.getMessage(), jde.getCause());*/
				} catch (ParserException pe) {
					throw new ReaderException(107, pe.getMessage(), pe.getCause());
				} catch (ReaderValidationException rve) {
					throw rve;
				} catch(CSVWriterValidationException csve) {
					throw csve;
				} catch (Exception e) {
					if (i == metr-1){
						throw new ReaderException(107, e.getMessage(), e.getCause());
					}else{
						logger.warn(e.getMessage() + e.getCause());
						continue;
					}					
				}
			}
			long after = System.currentTimeMillis();
			metrics = new ConverterMetrics(before, after, ingoredObsCount, obsCount);
			logger.info("Conversion time: " + (after - before) + " msecs");
		} catch (ReaderValidationException rve) {
			crashThrowable = rve;
			throw new ReaderValidationException(rve.getCode(), rve.getMessage(), rve.getMessage(), rve);
		} catch (ReaderException re) {
			String errorMessage = "Converter exception:";
			crashThrowable = re;
			if (re.getMessage() != null && re.getMessage().startsWith("INPUT Parsing Exception")) {
				throw new ReaderException(re.getCode(), re.getMessage(), re.getMessage(), re);
			} else {
				throw new ReaderException(re.getCode(), errorMessage + re.getMessage(), re);
			}
			
		} catch (Exception e) {
			String errorMessage = "Converter exception:";
			crashThrowable = e;
			if (e.getMessage() != null && e.getMessage().startsWith("INPUT Parsing Exception")) {
				throw new Exception(e.getMessage(), e);
			} else {
				throw new Exception(errorMessage + e.getMessage(), e);
			}
			
		}
		finally{		
			if (fromEqualsDSXML){
				os.close();
				//delete all the slice csv files
				for (int j = 0; j<sliceFileNames.length;j++){
					File sliceFile = new File(sliceFileNames[j].toString());
					sliceFile.delete();
				}
			}		
		}
	}
	
	private static Reader getReader(Formats from) throws Exception {
		switch (from) {
		case CROSS_SDMX:
			return (Reader) Class.forName("com.intrasoft.sdmx.converter.ex.io.data.CrossDataReader").newInstance();
		case CSV:
			return (Reader) Class.forName("com.intrasoft.sdmx.converter.ex.io.data.CsvDataReader").newInstance();
		case COMPACT_SDMX:
			return (Reader) Class.forName("com.intrasoft.sdmx.converter.ex.io.data.CompactDataReader").newInstance();
		default:
			throw new Exception("Input type " + from.getDescription() + " was not expected for this non-sdmxsource implementation.");
		}
	}
	
	private static Writer getWriter(Formats to) throws Exception {
		switch (to) {
		case CROSS_SDMX:
			return (Writer) Class.forName("com.intrasoft.sdmx.converter.ex.io.data.CrossDataWriter").newInstance();
		case CSV:
			return (Writer) Class.forName("com.intrasoft.sdmx.converter.ex.io.data.CsvDataWriter").newInstance();
		case COMPACT_SDMX:
			return (Writer) Class.forName("com.intrasoft.sdmx.converter.ex.io.data.CompactDataWriter").newInstance();
		default:
			throw new Exception("Input type " + to.getDescription() + " was not expected for this non-sdmxsource implementation.");
		}
	}
}
