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
package com.intrasoft.sdmx.converter.ex.io.data;

import au.com.bytecode.opencsv.CSVReader;
import com.intrasoft.sdmx.converter.ex.model.data.Observation;
import com.intrasoft.sdmx.converter.ex.model.data.TimeseriesKey;
import com.intrasoft.sdmx.converter.io.data.ComponentBeanUtils;
import com.intrasoft.sdmx.converter.services.exceptions.InvalidStructureException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.TIME_FORMAT;
import org.sdmxsource.sdmx.api.model.beans.base.ContactBean;
import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.beans.datastructure.*;
import org.sdmxsource.sdmx.api.model.header.HeaderBean;
import org.sdmxsource.sdmx.api.model.header.PartyBean;
import org.sdmxsource.sdmx.util.date.DateUtil;
import org.sdmxsource.util.ObjectUtil;
import org.w3c.dom.Document;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a utility class that contains utility static methods for all readers/writers
 * 
 * 
 */
public class IoUtils {
	public final static SimpleDateFormat HEADER_DATE_FROMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
	private static Logger logger = LogManager.getLogger(IoUtils.class);
	private static HashMap<String, String> specialCharsReplacements;
	private static Pattern specialCharsPattern;
	/* a map which links a canonical concept with an array of two values i)first value : the dataset name ii)second value  its url iii) third value its type iv)third value: its enumeration csv file*/
	public static LinkedHashMap<String,String[]> dsplConceptsMap;
	
	static {
		specialCharsReplacements = new HashMap<String, String>();

		// populate the map with all the special chars and their replacements.
		// NOTE: these are not the java string values, but the REGEX values
		specialCharsReplacements.put("&", "&amp;");
		specialCharsReplacements.put("\n", "&#xA;");
		specialCharsReplacements.put("\t", "&#x9;");
		specialCharsReplacements.put("'", "&#039;");
		specialCharsReplacements.put("!", "&#033;");
		specialCharsReplacements.put("=", "&#061;");
		specialCharsReplacements.put(">", "&gt;");
		specialCharsReplacements.put("<", "&lt;");
		specialCharsReplacements.put("?", "&#063;");
		specialCharsReplacements.put("\"", "&quot;");
		specialCharsReplacements.put("/", "&#047;");

		// create a pattern like this: "|/|\?|>|!|&|<|=|'
		StringBuilder patternStringSb = new StringBuilder("");
		boolean expressionStarted = false;
		for (String specialChar : specialCharsReplacements.keySet()) {
			if (expressionStarted) {
				patternStringSb.append("|");
			}
			patternStringSb.append(specialChar);
			expressionStarted = true;
		}
		// ? is a metacharacter. It it is as it is, it will change the meaning of the REGEX. It must be replaced with
		// \\?
		// So, the REGEX will understand "\?" (java eats one backslash)
		specialCharsPattern = Pattern.compile(patternStringSb.toString().replace("?", "\\?"));
		//System.out.println("lalakosWTF");

	}

	public ParseMonitor getParseMonitor(final int keySetLimit, final boolean log) {
		return new ParseMonitor(keySetLimit, log);
	}

	public ParseMonitor getParseMonitor(final int keySetLimit) {
		return new ParseMonitor(keySetLimit, false);
	}

	public class ParseMonitor {

		private long td = 0;// total duration
		private float avt;// average loop time
		private long lt = 0;
		private int fl = 0;
		private int ooml = 0;
		private int keySetLimit;
		public boolean log = false;
		public int timedRecs = 1;
		// public int timedRecs = 5000;
		// public int loopsSkiped = 1;
		public int ratio = 2;
		public int maxRatio = 4;
		public int extremeRatio = 6;
		public int maxSlowLoops = 2;
		private boolean settingWindow = true;
		private boolean started = false;

		public ParseMonitor(final int _keySetLimit, final boolean _log) {
			keySetLimit = _keySetLimit;
			log = _log;
		}

		public void monitor(final int row) throws OutOfMemoryError {
			if (row % timedRecs == 0) {
				long t = System.currentTimeMillis();

				if (settingWindow) {
					if (!started) {
						lt = t;// start counting loop time
						started = true;
					}
					long ld = t - lt;// loop duration
					if (ld > 1000) {
						timedRecs = row;
						if (log) {
							logger.info("row " + row);
						}
						if (log) {
							logger.info("timedRecs " + timedRecs);
						}
						settingWindow = false;
						lt = t;// start counting loop time
					}
					return;
				}

				long ld = t - lt;// loop duration
				td += ld;
				float lavt = (float) ld / timedRecs;// loop time
				// avt = (float)td / (float)(row - timedRecs);//average loop
				// time
				if (avt == 0) {
					avt = lavt;
				}
				float w = lavt / avt;// ration of loop average time to average
				// loop time
				if (log) {
					logger.info("row " + row + "\tld " + ld + "\ttd " + td + "\tlavt " + lavt + "\tavt " + avt + "\tw " + w);
				}
				// consider a 2-time increase of the loop duration as a
				// significant slow down in the parsing speed
				if (w > ratio) {
					if (w > extremeRatio) {
						ooml = maxSlowLoops;
					} else if (w > maxRatio) {
						ooml++;
					} else {
						ooml = 0;
					}
					td = td - ld;
					// avt = (float)td / (float)(row - timedRecs * 2);
					// if (log) System.out.println("row " + row + "\tld " + ld +
					// "\ttd " + td + "\tlavt " + lavt + "\tavt " + avt + "\tw "
					// + w);
					// if the loop time has increased more than 'maxRation'
					// times in 'maxSlowLoops' consecutive loops then reduce
					// keySetLimit
					if (ooml == maxSlowLoops) {
						throw (new OutOfMemoryError("forced out of memory error, current keySetLimit:" + keySetLimit));
					}
				} else {
					avt = (float) td / (float) (row - timedRecs);// average loop
					// time
				}
				lt = t;
			}
		}
	}//end monitor class

	// private static final Namespace rootNs =
	// Namespace.getNamespace("http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message");
	/**
	 * No argument constructor
	 * 
	 */
	public IoUtils() {
	}

	// /**
	// * Method that sorts the series
	// * @List<TimeseriesKey> entries
	// * @String[] groupKeys
	// */
	// public void sortSeriesEntries(List<TimeseriesKey> entries, String[]
	// groupKeys) {
	//
	// ComparatorChain compChain = new ComparatorChain();
	//
	// for (int i = 0; i < groupKeys.length; i++) {
	// compChain.addComparator(new SeriesComparator());
	// }
	// Collections.sort(entries, compChain);
	// }
	/**
	 * A class that represents a customized serie comparator
	 * 
	 */
	class SeriesComparator implements Comparator {

		/**
		 * Comparation method
		 * 
		 * @Object o1
		 * @Object o2
		 * @return int result
		 */
		public int compare(final Object o1, final Object o2) {
			if (o1 instanceof TimeseriesKey && o2 instanceof TimeseriesKey) {
				String s1 = "";
				String s2 = "";
				final Iterator<String> iter1 = ((TimeseriesKey) o1).getKeyValues().keySet().iterator();
				while (iter1.hasNext()) {
					s1 = s1 + ((TimeseriesKey) o1).getKeyValues().get(iter1.next());
				}

				final Iterator<String> iter2 = ((TimeseriesKey) o2).getKeyValues().keySet().iterator();
				while (iter2.hasNext()) {
					s2 = s2 + ((TimeseriesKey) o2).getKeyValues().get(iter2.next());
				}

				return s1.compareTo(s2);
			}
			return 0;
		}
	}

	public static void writeDOMDocumentToFile(Document document, File file){
        	try {
        	    Source source = new DOMSource(document);
        	    Result result = new StreamResult(file);
        	    Transformer xformer= TransformerFactory.newInstance().newTransformer();
        	    xformer.transform(source, result);       
            } catch (TransformerException ex) {
				logger.error("Transformer Exception", ex);
            }	    	    
	}
	
	/**
	 * Method that parses a CSV input flat file with column headers and returns a map of dimensions-values
	 * The first row contains the name of the columns as they are found in the DSD.  
	 * 
	 * @File input CSV
	 * @return Map<String, String> result
	 * @throws IOException
	 * @throws JDOMException
	 */
	public static Map<String, String[]> getInputWithMappingMap(	final InputStream inputIs, 
																String fieldDelimiter, 
																DataStructureBean dsd) throws Exception {
		final Map<String, String[]> map = new LinkedHashMap<String, String[]>();
		
		//first read the column headers from the file
		BufferedReader brdo = new BufferedReader(new InputStreamReader(inputIs));
		String line = brdo.readLine();
		//String[] names = line.split(fieldDelimiter, -1);		
		String[] names = null;
		CSVReader csvReader = new CSVReader(new StringReader(line), fieldDelimiter.charAt(0));			
		try {
			names = csvReader.readNext();
		} catch (IOException e) {				
			logger.error("I/O error ", e);
		}
		
		//then read the dsd items (dimensions, measures, attributes)
		List<String> dsdItems = new ArrayList<String>();
		for (DimensionBean dimensionBean : dsd.getDimensions()) {
			if (dimensionBean.isTimeDimension()) {
				dsdItems.add(dimensionBean.getConceptRef().getFullId());
			} else {
				dsdItems.add(dimensionBean.getId());
			}
		}
		for (AttributeBean attributeBean : dsd.getAttributes()) {
			dsdItems.add(attributeBean.getId());
		}
		
		if (dsd.getPrimaryMeasure() != null) {
			dsdItems.add(dsd.getPrimaryMeasure().getId());
		}
		
		//for the columns that match a dsd item, add a default value
		List<String> nameListFromFile = new ArrayList<String>();
		int valueIncrementor = 1;
		for (String name : names) {
			nameListFromFile.add(name);
			if (dsdItems.contains(name)) {
				final String[] data = new String[4];				
				data[0] = String.valueOf(valueIncrementor++);
				data[1] = "false";
				data[2] = "";
				data[3] = "";

				map.put(name, data);
			} else {
				throw new Exception("INPUT Parsing Exception: " + name + " is not in the DSD");
			}
		}
		
		//for the dsd items that didn't match any column header propose empty values
		for (String dsdItem : dsdItems) {
			if (!nameListFromFile.contains(dsdItem)) {
				final String[] data = new String[4];				
				data[0] = "";
				data[1] = "false";
				data[2] = "";
				data[3] = "";

				map.put(dsdItem, data);
			}
		}
		
		inputIs.close();
		return map;
	}

	/**
	 * Method that check dataset period according to a list of observations and a frequency
	 * 
	 * @List<ObservationDataBean> observations
	 * @String frequency
	 * @return String period
	 */		
	public static List<Calendar> checkObservationTimePeriod(final List<Observation> observations, final String frequency) throws Exception {
		if (observations.size() != 0) {
			String period = "";
			String dateFormatCode = "";
			String allDates = "";
			String startTime = observations.get(0).getTimeValue();
			startTime = startTime.replaceAll("-", "");
			startTime = startTime.replaceAll("Q", "");// for quarter date format
			startTime = startTime.replaceAll("W", "");// for week date format
			String endTime = observations.get(observations.size() - 1).getTimeValue();
			endTime = endTime.replaceAll("-", "");
			endTime = endTime.replaceAll("Q", "");// for quarter date format
			endTime = endTime.replaceAll("W", "");// for week date format
			if (frequency.startsWith("M")) {
				if (startTime.equalsIgnoreCase(endTime)) {
					dateFormatCode = "610";
				} else {
					dateFormatCode = "710";
				}
			} else if (frequency.startsWith("Q")) {
				if (startTime.equalsIgnoreCase(endTime)) {
					dateFormatCode = "608";
				} else {
					dateFormatCode = "708";
				}
			} else if (frequency.startsWith("H")) {
				if (startTime.equalsIgnoreCase(endTime)) {
					dateFormatCode = "604";
				} else {
					dateFormatCode = "704";
				}
			} else if (frequency.startsWith("T")) {
				// trimester?
			} else if (frequency.startsWith("A")) {
				if (startTime.equalsIgnoreCase(endTime)) {
					dateFormatCode = "602";
				} else {
					dateFormatCode = "702";
				}
			} else if (frequency.startsWith("D") || frequency.startsWith("B")) {
				if (startTime.equalsIgnoreCase(endTime)) {
					dateFormatCode = "102";
				} else {
					dateFormatCode = "711";
				}
			} else if (frequency.startsWith("W")) {
				if (startTime.equalsIgnoreCase(endTime)) {
					dateFormatCode = "616";
				} else {
					dateFormatCode = "716";
				}
			}
			if (!startTime.equalsIgnoreCase(endTime)) {
				allDates = period + startTime + endTime ;
				period = period + startTime + endTime + ":" + dateFormatCode + ":";
			} else {
				allDates = period + startTime  ;
				period = period + startTime + ":" + dateFormatCode + ":";
			}
			return getEncodedDates(allDates,dateFormatCode);
		} else {
			return null;
		}
	}
	/**
	 * Method that returns the start and the end date of the dataset
	 * 
	 * @String s1
	 * @String s2
	 * @return List<Calendar> result
	 * @throws Exception 
	 */
	public static List<Calendar> getEncodedDates(final String s1, final String s2) throws Exception {
		// initialize calendars (initialize day at 5, so that the Calendars are valid even if the month is later set to
		// february). This is because normally the getInstance returns a calendar with today's date. If the month has 30
		// and the month is changed to February, nasty things happen.
		final List<Calendar> list = new ArrayList<Calendar>();
		// if it is not a delete time series messge
		if ((s1 != null) && (s2 != null)) {
			final Calendar startDate = Calendar.getInstance();
			startDate.set(Calendar.YEAR, 1555);
			startDate.set(Calendar.MONTH, 5);
			startDate.set(Calendar.DAY_OF_MONTH, 5);
			startDate.set(Calendar.HOUR_OF_DAY, 0);
			startDate.set(Calendar.MINUTE, 0);
			startDate.set(Calendar.SECOND, 0);

			final Calendar endDate = Calendar.getInstance();
			endDate.set(Calendar.YEAR, 1555);
			endDate.set(Calendar.MONTH, 5);
			endDate.set(Calendar.DAY_OF_MONTH, 5);
			endDate.set(Calendar.HOUR_OF_DAY, 23);
			endDate.set(Calendar.MINUTE, 59);
			endDate.set(Calendar.SECOND, 59);

			if (s2.startsWith("101")) {
				// YYMMDD
				if (!s1.matches("[0-9]{2}[0-1]{1}[0-9]{1}[0-3]{1}[0-9]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format YYMMDD");
				}
				startDate.set(Calendar.YEAR, new Integer("20" + s1.substring(0, 2)).intValue());
				startDate.set(Calendar.MONTH, new Integer(s1.substring(2, 4)).intValue() - 1);
				startDate.set(Calendar.DAY_OF_MONTH, new Integer(s1.substring(4, 6)).intValue());

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, startDate.get(Calendar.MONTH));
				endDate.set(Calendar.DAY_OF_MONTH, startDate.get(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("102")) {
				// CCYYMMDD
				if (!s1.matches("[1-9]{1}[0-9]{3}[0-1]{1}[0-9]{1}[0-3]{1}[0-9]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYMMDD");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.MONTH, new Integer(s1.substring(4, 6)).intValue() - 1);
				startDate.set(Calendar.DAY_OF_MONTH, new Integer(s1.substring(6, 8)).intValue());

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, startDate.get(Calendar.MONTH));
				endDate.set(Calendar.DAY_OF_MONTH, startDate.get(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("201")) {
				// YYMMDDHHMM
				if (!s1.matches("[0-9]{2}[0-1]{1}[0-9]{1}[0-3]{1}[0-9]{1}[0-9]{4}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format YYMMDDHHMM");
				}
				startDate.set(Calendar.YEAR, new Integer("20" + s1.substring(0, 2)).intValue());
				startDate.set(Calendar.MONTH, new Integer(s1.substring(2, 4)).intValue() - 1);
				startDate.set(Calendar.DAY_OF_MONTH, new Integer(s1.substring(4, 6)).intValue());
				startDate.set(Calendar.HOUR_OF_DAY, new Integer(s1.substring(6, 8)).intValue());
				startDate.set(Calendar.MINUTE, new Integer(s1.substring(8, 10)).intValue());

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, startDate.get(Calendar.MONTH));
				endDate.set(Calendar.DAY_OF_MONTH, startDate.get(Calendar.DAY_OF_MONTH));
				endDate.set(Calendar.HOUR_OF_DAY, startDate.get(Calendar.HOUR_OF_DAY));
				endDate.set(Calendar.MINUTE, startDate.get(Calendar.MINUTE));

			} else if (s2.startsWith("203")) {
				// CCYYMMDDHHMM
				if (!s1.matches("[1-9]{1}[0-9]{3}[0-1]{1}[0-9]{1}[0-3]{1}[0-9]{1}[0-9]{4}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYMMDDHHMM");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.MONTH, new Integer(s1.substring(4, 6)).intValue() - 1);
				startDate.set(Calendar.DAY_OF_MONTH, new Integer(s1.substring(6, 8)).intValue());
				startDate.set(Calendar.HOUR_OF_DAY, new Integer(s1.substring(8, 10)).intValue());
				startDate.set(Calendar.MINUTE, new Integer(s1.substring(10, 12)).intValue());

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, startDate.get(Calendar.MONTH));
				endDate.set(Calendar.DAY_OF_MONTH, startDate.get(Calendar.DAY_OF_MONTH));
				endDate.set(Calendar.HOUR_OF_DAY, startDate.get(Calendar.HOUR_OF_DAY));
				endDate.set(Calendar.MINUTE, startDate.get(Calendar.MINUTE));

			} else if (s2.startsWith("602")) {
				// CCYY
				if (!s1.matches("[1-9]{1}[0-9]{3}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYY");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.MONTH, Calendar.JANUARY);
				startDate.set(Calendar.DAY_OF_MONTH, 1);

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, Calendar.DECEMBER);
				endDate.set(Calendar.DAY_OF_MONTH, 31);

			} else if (s2.startsWith("604")) {
				// CCYYS
				if (!s1.matches("[1-9]{1}[0-9]{3}[1-2]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYS");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				if (s1.substring(4, 5).equals("1")) {
					startDate.set(Calendar.MONTH, Calendar.JANUARY);
				} else {
					startDate.set(Calendar.MONTH, Calendar.JULY);
				}
				startDate.set(Calendar.DAY_OF_MONTH, 1);

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, startDate.get(Calendar.MONTH) + 5);
				endDate.set(Calendar.DAY_OF_MONTH, endDate.getActualMaximum(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("608")) {
				// CCYYQ
				if (!s1.matches("[1-9]{1}[0-9]{3}[1-4]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYQ");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				if (s1.substring(4, 5).equals("1")) {
					startDate.set(Calendar.MONTH, Calendar.JANUARY);
				} else if (s1.substring(4, 5).equals("2")) {
					startDate.set(Calendar.MONTH, Calendar.APRIL);
				} else if (s1.substring(4, 5).equals("3")) {
					startDate.set(Calendar.MONTH, Calendar.JULY);
				} else if (s1.substring(4, 5).equals("4")) {
					startDate.set(Calendar.MONTH, Calendar.OCTOBER);
				}
				startDate.set(Calendar.DAY_OF_MONTH, 1);

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, startDate.get(Calendar.MONTH) + 2);
				endDate.set(Calendar.DAY_OF_MONTH, endDate.getActualMaximum(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("610")) {
				// CCYYMM
				if (!s1.matches("[1-9]{1}[0-9]{3}[0-1]{1}[0-9]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYMM");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.MONTH, new Integer(s1.substring(4, 6)).intValue() - 1);
				startDate.set(Calendar.DAY_OF_MONTH, 1);

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.MONTH, startDate.get(Calendar.MONTH));
				endDate.set(Calendar.DAY_OF_MONTH, endDate.getActualMaximum(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("616")) {
				// CCYYWW
				if (!s1.matches("[1-9]{1}[0-9]{3}[0-9]{1}[0-9]{0,1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYWW");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.WEEK_OF_YEAR, new Integer(s1.substring(4, 6)).intValue());
				startDate.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

				endDate.set(Calendar.YEAR, startDate.get(Calendar.YEAR));
				endDate.set(Calendar.WEEK_OF_YEAR, startDate.get(Calendar.WEEK_OF_YEAR));
				endDate.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

			} else if (s2.startsWith("711")) {
				// CCYYMMDDCCYYMMDD
				if (!s1.matches("[1-9]{1}[0-9]{3}[0-9]{1}[0-9]{1}[0-3]{1}[0-9]{1}[-]{0,1}[1-9]{1}[0-9]{3}[0-9]{1}[0-9]{1}[0-3]{1}[0-9]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYMMDDCCYYMMDD");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.MONTH, new Integer(s1.substring(4, 6)).intValue() - 1);
				startDate.set(Calendar.DAY_OF_MONTH, new Integer(s1.substring(6, 8)).intValue());

				endDate.set(Calendar.YEAR, new Integer(s1.substring(8, 12)).intValue());
				endDate.set(Calendar.MONTH, new Integer(s1.substring(12, 14)).intValue() - 1);
				endDate.set(Calendar.DAY_OF_MONTH, new Integer(s1.substring(14, 16)).intValue());

			} else if (s2.startsWith("702")) {
				// CCYYCCYY
				if (!s1.matches("[1-9]{1}[0-9]{3}[-]{0,1}[1-9]{1}[0-9]{3}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYCCYY");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.MONTH, Calendar.JANUARY);
				startDate.set(Calendar.DAY_OF_MONTH, 1);
				// System.out.println(s1.substring(0, 4));

				endDate.set(Calendar.YEAR, new Integer(s1.substring(4, 8)).intValue());
				endDate.set(Calendar.MONTH, Calendar.DECEMBER);
				endDate.set(Calendar.DAY_OF_MONTH, 31);
				// System.out.println(s2.substring(4,8));

			} else if (s2.startsWith("704")) {
				// CCYYSCCYYS
				if (!s1.matches("[1-9]{1}[0-9]{3}[1-2]{1}[-]{0,1}[1-9]{1}[0-9]{3}[1-2]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYSCCYYS");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				if (s1.substring(4, 5).equals("1")) {
					startDate.set(Calendar.MONTH, Calendar.JANUARY);
				} else {
					startDate.set(Calendar.MONTH, Calendar.JULY);
				}
				startDate.set(Calendar.DAY_OF_MONTH, 1);

				endDate.set(Calendar.YEAR, new Integer(s1.substring(5, 9)).intValue());
				if (s1.substring(9, 10).equals("1")) {
					endDate.set(Calendar.MONTH, Calendar.JUNE);
				} else {
					endDate.set(Calendar.MONTH, Calendar.DECEMBER);
				}
				endDate.set(Calendar.DAY_OF_MONTH, endDate.getActualMaximum(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("708")) {
				// CCYYQCCYYQ
				if (!s1.matches("[1-9]{1}[0-9]{3}[1-4]{1}[-]{0,1}[1-9]{1}[0-9]{3}[1-4]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYQCCYYQ");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				if (s1.substring(4, 5).equals("1")) {
					startDate.set(Calendar.MONTH, Calendar.JANUARY);
				} else if (s1.substring(4, 5).equals("2")) {
					startDate.set(Calendar.MONTH, Calendar.APRIL);
				} else if (s1.substring(4, 5).equals("3")) {
					startDate.set(Calendar.MONTH, Calendar.JULY);
				} else if (s1.substring(4, 5).equals("4")) {
					startDate.set(Calendar.MONTH, Calendar.OCTOBER);
				}
				startDate.set(Calendar.DAY_OF_MONTH, 1);

				endDate.set(Calendar.YEAR, new Integer(s1.substring(5, 9)).intValue());
				if (s1.substring(9, 10).equals("1")) {
					endDate.set(Calendar.MONTH, Calendar.MARCH);
				} else if (s1.substring(9, 10).equals("2")) {
					endDate.set(Calendar.MONTH, Calendar.JUNE);
				} else if (s1.substring(9, 10).equals("3")) {
					endDate.set(Calendar.MONTH, Calendar.SEPTEMBER);
				} else if (s1.substring(9, 10).equals("4")) {
					endDate.set(Calendar.MONTH, Calendar.DECEMBER);
				}
				endDate.set(Calendar.DAY_OF_MONTH, endDate.getActualMaximum(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("710")) {
				// CCYYMMCCYYMM
				if (!s1.matches("[1-9]{1}[0-9]{3}[0-1]{1}[0-9]{1}[-]{0,1}[1-9]{1}[0-9]{3}[0-1]{1}[0-9]{1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYMMCCYYMM");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.MONTH, new Integer(s1.substring(4, 6)).intValue() - 1);
				startDate.set(Calendar.DAY_OF_MONTH, 1);

				endDate.set(Calendar.YEAR, new Integer(s1.substring(6, 10)).intValue());
				endDate.set(Calendar.MONTH, new Integer(s1.substring(10, 12)).intValue() - 1);
				endDate.set(Calendar.DAY_OF_MONTH, endDate.getActualMaximum(Calendar.DAY_OF_MONTH));

			} else if (s2.startsWith("716")) {
				// CCYYWWCCYYWW
				if (!s1.matches("[1-9]{1}[0-9]{3}[0-9]{1}[0-9]{0,1}[-]{0,1}[1-9]{1}[0-9]{3}[0-9]{1}[0-9]{0,1}")) {
					throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is expected to be of format CCYYWWCCYYWW");
				}
				startDate.set(Calendar.YEAR, new Integer(s1.substring(0, 4)).intValue());
				startDate.set(Calendar.WEEK_OF_YEAR, new Integer(s1.substring(4, 6)).intValue());
				startDate.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

				endDate.set(Calendar.YEAR, new Integer(s1.substring(6, 10)).intValue());
				endDate.set(Calendar.WEEK_OF_YEAR, new Integer(s1.substring(10, 12)).intValue());
				endDate.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			}

			// List<Calendar> list = new ArrayList<Calendar>();
			list.add(startDate);
			list.add(endDate);
			return list;
		} else {
			return list;
		}
	}
	/**
	 * Method that returns the date format for single-observation technique, based on the frequency
	 * 
	 * @List<ObservationDataBean> observations
	 * @String frequency
	 * @return String period
	 */
	public static String getSingleObservationDateFormat(final String frequency) {
		String dateFormat = null;

		if (frequency.startsWith("M")) {
			dateFormat = "610";
		} else if (frequency.startsWith("Q")) {
			dateFormat = "608";
		} else if (frequency.startsWith("H")) {
			dateFormat = "604";
		} else if (frequency.startsWith("T")) {
			// trimester?
		} else if (frequency.startsWith("A")) {
			dateFormat = "602";
		} else if (frequency.startsWith("D") || frequency.startsWith("B")) {
			dateFormat = "102";
		} else if (frequency.startsWith("W")) {
			dateFormat = "616";
		}

		return dateFormat;
	}

	/**
	 * Method to replace special characters with their codes for xml output files
	 * 
	 * @param string the string where special characters are replaced
	 * @return the string without special characters
	 */
	// public static String handleSpecialCharacters(String string) {
	//
	// string = string.replace("&", "&amp;");
	// string = string.replace("'", "&#039;");
	// string = string.replace("!", "&#033;");
	// string = string.replace("=", "&#061;");
	// string = string.replace(">", "&gt;");
	// string = string.replace("<", "&lt;");
	// string = string.replace("?", "&#063;");
	// string = string.replace("\"", "&quot;");
	// string = string.replace("/", "&#047;");
	//
	// return string;
	//
	// }
	// /**
	// * Method to replace special characters with their codes for xml output files
	// *
	// * @param string the string where special characters are replaced
	// * @return the string without special characters
	// */
	public static String handleSpecialCharacters(final String string) {

		final Matcher matcher = specialCharsPattern.matcher(string);
		final StringBuffer sb = new StringBuffer("");
		while (matcher.find()) {

			matcher.appendReplacement(sb, specialCharsReplacements.get(matcher.group()));
		}
		matcher.appendTail(sb);

		return sb.toString();

	}

	/**
	 * Method for writing a Properties file with header information if requested from the user
	 * @param header, the HeaderBean to be writen
	 * @param hos, the path for the output file to be written
	 * @throws Exception
	 */
	public static void printHeader(final HeaderBean header, final String hos) throws Exception {

		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(hos);
			printHeader(header, fos);
		} catch (IOException e) {
			throw (new Exception(e.getMessage()));
		}

	}

	/**
	 * Method for writing a Properties file with header information if requested from the user
	 * @param header, the HeaderBean to be written
	 * @param os, the OutputStream
	 * @throws Exception
	 */
	public static void printHeader(final HeaderBean header, final OutputStream os) throws Exception {
		final Properties properties = new Properties();
		// ID
		if (header.getId() != null) {
			properties.setProperty("header.id", header.getId());
		} else {
			properties.setProperty("header.id", "");
		}
		// Test
		if (String.valueOf(header.isTest()) != null) {
			properties.setProperty("header.test", String.valueOf(header.isTest()));
		} else {
			properties.setProperty("header.test", "");
		}
		// Truncated
		if(header.hasAdditionalAttribute("Truncated")) {
			properties.setProperty("header.truncated", String.valueOf(header.getAdditionalAttribute("Truncated")));
		}
		// Name
		if (header.getName().size() != 0) {
			for (final TextTypeWrapper name : header.getName()) {
				if (name.getValue() != null) {
					properties.setProperty("header.name", name.getValue());
				} else {
					properties.setProperty("header.name", "");

				}
				if (name.getLocale() != null) {
					properties.setProperty("header.lang", name.getLocale());
				} else {
					properties.setProperty("header.lang", "");
				}
			}
		} else {
			properties.setProperty("header.name", "");
			properties.setProperty("header.lang", "");
		}
		// Prepared Date
		if (header.getPrepared() != null) {
			properties.setProperty("header.prepared", HEADER_DATE_FROMATTER.format(header.getPrepared()));
		} else {
			properties.setProperty("header.prepared", "");
		}
		// Sender
		if (header.getSender() != null) {
			PartyBean sender = header.getSender();
			 {
				// Sender Id
				if (sender.getId() != null) {
					properties.setProperty("header.senderid", sender.getId());
				} else {
					properties.setProperty("header.senderid", "");
				}
				// Sender Name
				if (sender.getName() != null && sender.getName().size() != 0) {
					for (TextTypeWrapper name : sender.getName()) {
						if (name.getValue() != null) {
							properties.setProperty("header.sendername", name.getValue());
						} else {
							properties.setProperty("header.sendername", "");
						}
						if (name.getLocale() != null) {
							properties.setProperty("header.lang", name.getLocale());
						}
					}
				} else {
					properties.setProperty("header.sendername", "");
				}
				// Sender Contact Type
				if (sender.getContacts().size() != 0) {
					for (final ContactBean name : sender.getContacts()) {
						// Contact Name
						if (name.getName().size() != 0) {
							for (final TextTypeWrapper contactname : name.getName()) {
								if (contactname.getValue() != null) {
									properties.setProperty("header.sendercontactname", contactname.getValue());
								} else {
									properties.setProperty("header.sendercontactname", "");

								}
								if (contactname.getLocale() != null) {
									properties.setProperty("header.lang", contactname.getLocale());
								}
							}
						} else {
							properties.setProperty("header.sendercontactname", "");
						}
						// Contact Department
						if (name.getDepartments().size() != 0) {
							for (final TextTypeWrapper departname : name.getDepartments()) {
								if (departname.getValue() != null) {
									properties.setProperty("header.sendercontactdepartment", departname.getValue());
								} else {
									properties.setProperty("header.sendercontactdepartment", "");
								}
								if (departname.getLocale() != null) {
									properties.setProperty("header.lang", departname.getLocale());
								}
							}
						} else {
							properties.setProperty("header.sendercontactdepartment", "");
						}
						// Contact Telephone,Fax,X400,Uri,Email
						if (name.getTelephone() != null && name.getTelephone().size() != 0) {
							properties.setProperty("header.sendercontacttelephone", name.getTelephone().get(0));
						} else {
							properties.setProperty("header.sendercontacttelephone", "");
						}
						if (name.getFax() != null && name.getFax().size() != 0) {
							properties.setProperty("header.sendercontactfax", name.getFax().get(0));
						} else {
							properties.setProperty("header.sendercontactfax", "");
						}
						if (name.getX400() != null && name.getX400().size() != 0) {
							properties.setProperty("header.sendercontactx400", name.getX400().get(0));
						} else {
							properties.setProperty("header.sendercontactx400", "");
						}
						if (name.getUri() != null && name.getUri().size() != 0) {
							properties.setProperty("header.sendercontacturi", name.getUri().get(0));
						} else {
							properties.setProperty("header.sendercontacturi", "");
						}
						if (name.getEmail() != null && name.getEmail().size() != 0) {
							properties.setProperty("header.sendercontactemail", name.getEmail().get(0));
						} else {
							properties.setProperty("header.sendercontactemail", "");
						}

						// Contact Role
						if (name.getRole().size() != 0) {
							for (final TextTypeWrapper rolename : name.getRole()) {
								if (rolename.getValue() != null) {
									properties.setProperty("header.sendercontactrole", rolename.getValue());
								} else {
									properties.setProperty("header.sendercontactrole", "");
								}
								if (rolename.getLocale() != null) {
									properties.setProperty("header.lang", rolename.getLocale());
								}
							}
						} else {
							properties.setProperty("header.sendercontactrole", "");
						}
					}
				} else {
					properties.setProperty("header.sendercontactname", "");
					properties.setProperty("header.sendercontactdepartment", "");
					properties.setProperty("header.sendercontactrole", "");
					properties.setProperty("header.sendercontacttelephone", "");
					properties.setProperty("header.sendercontactfax", "");
					properties.setProperty("header.sendercontactx400", "");
					properties.setProperty("header.sendercontacturi", "");
					properties.setProperty("header.sendercontactemail", "");
				}
			}
		} else {
			properties.setProperty("header.senderid", "");
			properties.setProperty("header.sendername", "");
			properties.setProperty("header.sendercontactname", "");
			properties.setProperty("header.sendercontactdepartment", "");
			properties.setProperty("header.sendercontactrole", "");
			properties.setProperty("header.sendercontacttelephone", "");
			properties.setProperty("header.sendercontactfax", "");
			properties.setProperty("header.sendercontactx400", "");
			properties.setProperty("header.sendercontacturi", "");
			properties.setProperty("header.sendercontactemail", "");
		}
		// Receiver
		if (header.getReceiver() != null && header.getReceiver().size() != 0) {
			for (final PartyBean receiver : header.getReceiver()) {
				// Receiver Id
				if (receiver.getId() != null) {
					properties.setProperty("header.receiverid", receiver.getId());
				} else {
					properties.setProperty("header.receiverid", "");
				}
				// Receiver Name
				if (receiver.getName().size() != 0) {
					for (final TextTypeWrapper name : receiver.getName()) {
						if (name.getValue() != null) {
							properties.setProperty("header.receivername", name.getValue());
						} else {
							properties.setProperty("header.receivername", "");
						}
						if (name.getLocale() != null) {
							properties.setProperty("header.lang", name.getLocale());
						}
					}
				} else {
					properties.setProperty("header.receivername", "");

				}
				// Receiver Contact Type
				if (receiver.getContacts().size() != 0) {
					for (final ContactBean name : receiver.getContacts()) {
						if (name.getName().size() != 0) {
							// Contact Name
							for (final TextTypeWrapper contactname : name.getName()) {
								if (contactname.getValue() != null) {
									properties.setProperty("header.receivercontactname", contactname.getValue());
								} else {
									properties.setProperty("header.receivercontactname", "");
								}
								if (contactname.getLocale() != null) {
									properties.setProperty("header.lang", contactname.getLocale());
								}
							}
						} else {
							properties.setProperty("header.receivercontactname", "");
						}
						// Contact Department
						if (name.getDepartments().size() != 0) {
							for (final TextTypeWrapper departname : name.getDepartments()) {
								if (departname.getValue() != null) {
									properties.setProperty("header.receivercontactdepartment", departname.getValue());
								} else {
									properties.setProperty("header.receivercontactdepartment", "");
								}
								if (departname.getLocale() != null) {
									properties.setProperty("header.lang", departname.getLocale());

								}
							}
						} else {
							properties.setProperty("header.receivercontactdepartment", "");
						}
						// Contact Role
						if (name.getRole().size() != 0) {
							for (final TextTypeWrapper rolename : name.getRole()) {
								if (rolename.getValue() != null) {
									properties.setProperty("header.receivercontactrole", rolename.getValue());
								} else {
									properties.setProperty("header.receivercontactrole", "");
								}
								if (rolename.getLocale() != null) {
									properties.setProperty("header.lang", rolename.getLocale());
								}
							}
						} else {
							properties.setProperty("header.receivercontactrole", "");
						}
						// Contact Telephone, Fax,X400,Uri,Email
						if (name.getTelephone() != null && name.getTelephone().size() != 0) {
							properties.setProperty("header.receivercontacttelephone", name.getTelephone().get(0));
						} else {
							properties.setProperty("header.receivercontacttelephone", "");
						}
						if (name.getFax() != null && name.getFax().size() != 0) {
							properties.setProperty("header.receivercontactfax", name.getFax().get(0));
						} else {
							properties.setProperty("header.receivercontactfax", "");
						}
						if (name.getX400() != null && name.getX400().size() != 0) {
							properties.setProperty("header.receivercontactx400", name.getX400().get(0));
						} else {
							properties.setProperty("header.receivercontactx400", "");
						}
						if (name.getUri() != null && name.getUri().size() != 0) {
							properties.setProperty("header.receivercontacturi", name.getUri().get(0));
						} else {
							properties.setProperty("header.receivercontacturi", "");
						}
						if (name.getEmail() != null && name.getEmail().size() != 0) {
							properties.setProperty("header.receivercontactemail", name.getEmail().get(0));
						} else {
							properties.setProperty("header.receivercontactemail", "");
						}
					}
				} else {
					properties.setProperty("header.receivercontactname", "");
					properties.setProperty("header.receivercontactdepartment", "");
					properties.setProperty("header.receivercontactrole", "");
					properties.setProperty("header.receivercontacttelephone", "");
					properties.setProperty("header.receivercontactfax", "");
					properties.setProperty("header.receivercontactx400", "");
					properties.setProperty("header.receivercontacturi", "");
					properties.setProperty("header.receivercontactemail", "");
				}

			}
		} else {
			properties.setProperty("header.receiverid", "");
			properties.setProperty("header.receivername", "");
			properties.setProperty("header.receivercontactname", "");
			properties.setProperty("header.receivercontactdepartment", "");
			properties.setProperty("header.receivercontactrole", "");
			properties.setProperty("header.receivercontacttelephone", "");
			properties.setProperty("header.receivercontactfax", "");
			properties.setProperty("header.receivercontactx400", "");
			properties.setProperty("header.receivercontacturi", "");
			properties.setProperty("header.receivercontactemail", "");
		}
		
		
//		// KeyFamilyRef
//		if (header.getKeyFamilyRef() != null) {
//			properties.setProperty("header.keyfamilyref", header.getKeyFamilyRef());
//		} else {
//			properties.setProperty("header.keyfamilyref", "");
//		}
//		// KeyFamilyAgency
//		if (header.getKeyFamilyAgency() != null) {
//			properties.setProperty("header.keyfamilyagency", header.getKeyFamilyAgency());
//		} else {
//			properties.setProperty("header.keyfamilyagency", "");
//		}
		
		properties.setProperty("header.keyfamilyref", "");
		properties.setProperty("header.keyfamilyagency", "");
		
		// DataSetAgency
		if (header.getDataProviderReference() != null && header.getDataProviderReference().getAgencyId() !=null) {
			properties.setProperty("header.datasetagency", header.getDataProviderReference().getAgencyId());
		} else {
			properties.setProperty("header.datasetagency", "");
		}
		// DataSetId
		if (header.getDatasetId() != null) {
			properties.setProperty("header.datasetid", header.getDatasetId());
		} else {
			properties.setProperty("header.datasetid", "");
		}
		// DataSetAction
		if (header.getAction() != null) {
			properties.setProperty("header.datasetaction", header.getAction().getAction());
		} else {
			properties.setProperty("header.datasetaction", "");
		}
		
		// Extracted
		if (header.getExtracted() != null) {
			properties.setProperty("header.extracted", HEADER_DATE_FROMATTER.format(header.getExtracted()));
		} else {
			properties.setProperty("header.extracted", "");
		}
		// Reporting Begin Date
		if (header.getReportingBegin() != null) {
			properties.setProperty("header.reportingbegin", HEADER_DATE_FROMATTER.format(header.getReportingBegin()));
		} else {
			properties.setProperty("header.reportingbegin", "");
		}
		// Reporting End Date
		if (header.getReportingEnd() != null) {
			properties.setProperty("header.reportingend", HEADER_DATE_FROMATTER.format(header.getReportingEnd()));
		} else {
			properties.setProperty("header.reportingend", "");
		}
		// Source
		if (header.getSource().size() != 0) {
			for (TextTypeWrapper source : header.getSource()) {
				if (source.getValue() != null) {
					properties.setProperty("header.source", source.getValue());
				} else {
					properties.setProperty("header.source", "");
				}
				if (source.getLocale() != null) {
					properties.setProperty("header.lang", source.getLocale());
				}
			}
		} else {
			properties.setProperty("header.source", "");
		}
		try {
			properties.store(os, null);
		} catch (IOException e) {
			throw (new Exception(e.getMessage()));
		} finally {
			os.close();
		}
	}

	/**
	 * This method creates the new mapping for the new flat records that are taken from the multilevel input CSV file
	 * @param component
	 * @param columnsPerLevel
	 * @param newMapping
	 * @param initialLevel
	 * 
	 */
	public static void createNewMapping(final String component, final LinkedHashMap<String, String> columnsPerLevel, final Map<String, String[]> newMapping,
			final String initialLevel) {
		final String info[] = newMapping.get(component);
		final String level = info[2];
		final String isFixed = info[1];
		final String order = info[0];
		// find the new position of the component
		int newOrder;
		final String newInfo[] = new String[3];
		// if the level is the top level then the component will have the same order minus the position of the
		// level.
		if (!order.equals("") && (!level.equals(""))) {
			if (level.equals(initialLevel)) {
				//newOrder = Integer.parseInt(order) - level.length();
				newOrder = Integer.parseInt(order) - Integer.parseInt(level);
			} else {// the new order is plus the offset of the previous level
				int parentLevel = Integer.parseInt(level) - 1;
				// get the new order for the component
				newOrder = (Integer.parseInt(order) - level.length()) + getOffset(parentLevel, order, columnsPerLevel);
			}
			newInfo[0] = String.valueOf(newOrder);
		} else {
			newInfo[0] = "";
		}

		newInfo[1] = isFixed;
		newInfo[2] = level;
		newMapping.put(component, newInfo);
	}

	/**
	 * This method creates the new mapping for the new flat records that are taken from the multilevel input FLR file
	 * @param component
	 * @param columnsPerLevel
	 * @param newMapping
	 * @param initialLevel
	 * 
	 */
	public static void createNewMappingforFLR(final String component, final LinkedHashMap<String, String> columnsPerLevel, final Map<String, String[]> newMapping,
			final String initialLevel) {
		String info[] = newMapping.get(component);
		String level = info[2];
		String isFixed = info[1];
		String order = info[0];
		// find the new position of the component
		String newOrder;
		String newInfo[] = new String[3];
		String start_pos[];
		String end_pos[];

		// get the new order for the component
		if (!order.equals("") && (!level.equals(""))) {
			if (order.contains("+")) {
				// eg.15-16+17-19+20-21
				String data[] = order.split("\\+");
				// eg. last part will be 20-21
				String last_part = data[data.length - 1];
				end_pos = last_part.split("\\-");
				// eg. last part will be 15-16
				String first_part = data[0];
				start_pos = first_part.split("\\-");
			} else {
				start_pos = order.split("\\-");
				end_pos = order.split("\\-");
			}
			// if the level is the top level then the component will have the same order minus the position of the
			// level.
			if (level.equals(initialLevel)) {
				int start = Integer.parseInt(start_pos[0]) - level.length();
				int end = Integer.parseInt(end_pos[end_pos.length - 1]) - level.length();
				newOrder = String.valueOf(start) + '-' + String.valueOf(end);
			} else {// the new order is plus the offset of the previous level
				int parentLevel = Integer.parseInt(level) - 1;
				// Find Start point
				int start = Integer.parseInt(start_pos[0]) - level.length() + getOffset(parentLevel, order, columnsPerLevel);
				int end = Integer.parseInt(end_pos[end_pos.length - 1]) - level.length() + getOffset(parentLevel, order, columnsPerLevel);
				// newOrder = (Integer.parseInt(order) - level.length()) + getOffset(parentLevel, order,
				// columnsPerLevel);
				newOrder = String.valueOf(start) + '-' + String.valueOf(end);
			}
			newInfo[0] = newOrder;
		} else {
			newInfo[0] = "";
		}

		newInfo[1] = isFixed;
		newInfo[2] = level;
		newMapping.put(component, newInfo);
	}

	/**
	 * This recursive method returns the offset of the line start for a component
	 * @param lineparentLevel
	 * @param order
	 * @param columnsPerLevel
	 * @return newOrder
	 * 
	 */
	public static int getOffset(final int parentLevel, final String order, final LinkedHashMap<String, String> columnsPerLevel) {
		final int newOrder = 0;
		if (parentLevel == 0) {
			return newOrder;
		} else {
			return Integer.parseInt(columnsPerLevel.get(String.valueOf(parentLevel))) + getOffset(parentLevel - 1, order, columnsPerLevel);
		}
	}

	/**
	 * This method parses the fieldDelimiter and substitutes any special regex character with the \\character. For
	 * example the + becomes \\+ in order to be treated as literal character. All the regex characters with special
	 * meaning are the following: [,\,^,$,.,|,?,*,+,(,)
	 * 
	 * @param fieldDelimiter
	 * @return delimiter
	 * 
	 */
	public static String parseDelimiter(final String fieldDelimiter) {
		String delimiter = "";
		final StringBuilder delimiterBuilder = new StringBuilder();
		// parse the delimiter
		for (int index = 0; index < fieldDelimiter.length(); index++) {
			char ch = fieldDelimiter.charAt(index);
			if (ch == '[' || ch == '^' || ch == '$' || ch == '.' || ch == '|' || ch == '?' || ch == '*' || ch == '+' || ch == '(' || ch == ')'
				|| ch == 92) {

				delimiterBuilder.append('\\');// regex compilation
				delimiterBuilder.append(ch);
			} else {
				delimiterBuilder.append(ch);
			}
		}
		delimiter = delimiterBuilder.toString();
		return delimiter;
	}
	
	/**
	 * This method returns true if in the mapping provided the crossX measures have been mapped and not the measure dimension.
	 * @param keyFamilyBean
	 * @param mappingMap
	 * @return mapsCrossXM
	 */
	public static boolean mapCrossXMeasures(final DataStructureBean keyFamilyBean , Map<String, String[]> mappingMap) {
		//the concept of the MeasureDimension
		final String concept = getMeasureDimension(keyFamilyBean);
		if (concept.equals("")) {
			return false;
		} else {
			if (!mappingMap.containsKey(concept)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * This method returns the concept name of the measure Dimension.
	 * @param keyFamilyBean
	 * @return measureDimension
	 * @deprecated please try to use the StructureService#getMeasureDimension instead
	 */
	public static String getMeasureDimension(final DataStructureBean keyFamilyBean) {
		String measureDimension="";
		final List<DimensionBean> dimensionsBean = keyFamilyBean.getDimensions();
		for (final DimensionBean dimBean: dimensionsBean){
			if (dimBean.isMeasureDimension() && !dimBean.isTimeDimension()){
				measureDimension = dimBean.getId();
				break;
			}
		}	
		return measureDimension;
	}

	/**
	 * This method checks a DSD for compliance with producing Cross-Sectional Data. In detail, it checks all Dimensions
	 * and all Attributes for having or not having cross-sectional attachment level. If there are components with no
	 * attachment level, it throws an exception listing them in its message.
	 * @param dsd the DSD to be checked
	 */
	public static String checkXsCompliance(final DataStructureBean dsd) throws InvalidStructureException {

		// this is a boolean to determine if the dimension is the frequency dimension
		boolean isFreqDimension = false;
		// this is a boolean to determine if the dimension is the measure dimension
		boolean isMeasureDimension = false;

		// StringBuilder, where the message for a possible exception will be gradually built up
		final StringBuilder exceptionMessage = new StringBuilder("");

		// do dimensions have a cross-sectional attachment level?
		final ArrayList<DimensionBean> dimensions = (ArrayList<DimensionBean>) dsd.getDimensions();
		if(!ObjectUtil.validCollection(dimensions)) {
			throw new InvalidStructureException("Structure retrieval was not possible, could not retrieve dimension list from this dsd. Not compatible DSD with Cross-Sectional");
		}
		for (final DimensionBean dim : dimensions) {
			// if the dimension is not the frequency dimension then make the appropriate checks
			if (dim.isFrequencyDimension()) {
				isFreqDimension = true;
			} else {
				isFreqDimension = false;
			}

			// if the dimension is not the measure dimension then make the appropriate checks
			if (dim.isMeasureDimension()) {
				isMeasureDimension = true;
			} else {
				isMeasureDimension = false;
			}
			
			Map<String, String> valuesDimension = ComponentBeanUtils.getDimensionInfo(dim, dsd); 

			if (!isFreqDimension && !isMeasureDimension) {
				// this is a boolean to determine if the dimension has crossXattachlevel
				boolean hasCrossXAttachment = false;
				// if crossXattacement information is null then the dimension does not have a crossX attach information.
				if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET) == null && valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP) == null
						&& valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION) == null && valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION) == null) {
					exceptionMessage.append("Dimension " + dim.getConceptRef() +" "+" does not have a Cross-Sectional attachment level"+System.lineSeparator());
					
				} else {// it has a crossXattach information. It must be checked if it has true or false value
					if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET) != null) {
						if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET).equalsIgnoreCase("true")) {
							hasCrossXAttachment = true;
						}
					}
					if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP) != null) {
						if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP).equalsIgnoreCase("true")) {
							hasCrossXAttachment = true;
						}
					}
					if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION) != null) {
						if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION).equalsIgnoreCase("true")) {
							hasCrossXAttachment = true;
						}
					}
					if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION) != null) {
						if (valuesDimension.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION).equalsIgnoreCase("true")) {
							hasCrossXAttachment = true;
						}
					}

					if (!dim.isTimeDimension() && !hasCrossXAttachment) {
						exceptionMessage.append("Dimension " + dim.getConceptRef() +" "+" does not have a Cross-Sectional attachment level"+System.lineSeparator());
					}
				}
			}
		}

		// do attributes have a cross-sectional attachment level?
		final ArrayList<AttributeBean> attributes = (ArrayList<AttributeBean>) dsd.getAttributes();
		for (final AttributeBean att : attributes) {
			// this is a boolean to determine if the dimension has crossXattachlevel
			boolean hasCrossXAttachment = false;
			Map<String, String> valuesAttribute = ComponentBeanUtils.getAttributeInfo(att, dsd); 
			// if crossXattacement information is null then the dimension does not have a crossX attach information.
			if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET) == null && valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP) == null
					&& valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION) == null && valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION) == null) {
				exceptionMessage.append("Dimension " + att.getConceptRef() +" "+" does not have a Cross-Sectional attachment level"+System.lineSeparator());
			} else {// it has a crossXattach information. It must be checked if it has true or false value
				if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET) != null) {
					if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_DATA_SET).equalsIgnoreCase("true"))
						hasCrossXAttachment = true;
				}

				if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP) != null) {
					if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_GROUP).equalsIgnoreCase("true"))
						hasCrossXAttachment = true;
				}

				if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION) != null) {
					if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_SECTION).equalsIgnoreCase("true"))
						hasCrossXAttachment = true;
				}
				if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION) != null) {
					if (valuesAttribute.get(Constants.CROSS_SECTIONAL_ATTACH_OBSERVATION).equalsIgnoreCase("true"))
						hasCrossXAttachment = true;
				}
				if (!hasCrossXAttachment) {
					exceptionMessage.append("Dimension " + att.getConceptRef() +" "+" does not have a Cross-Sectional attachment level"+System.lineSeparator());
				}
			}
		}
		// throw exception?
		return exceptionMessage.toString();
	}
	
    /**
     * This method returns the number of components that belong to a specific level
     * @param mappingMap
     * @param level
	 * @return compNo
	 */

    public static int getComponentNo(final Map<String, String[]> mappingMap, final String level){
		int compNo=0;
		final Iterator<Entry<String, String[]>> iter = mappingMap.entrySet().iterator();
        while (iter.hasNext()) {
        	final Entry<String, String[]> entry = iter.next();
             if (entry.getValue()[2].equals(level))
                 compNo++;
        }
        return compNo;
    }
    
    /**
	 * This method finds for the crossSectional measure concept for the code given
	 * @param code
	 * @param keyFamilyBean
	 * @return measureConcept
	 */
    public  static String getCrossXConceptRef(final String code, final DataStructureBean keyFamilyBean) {
            String measureConcept="";
            if (keyFamilyBean instanceof CrossSectionalDataStructureBean) {
	            final List<CrossSectionalMeasureBean> crossXMeasures = ((CrossSectionalDataStructureBean)keyFamilyBean).getCrossSectionalMeasures();
				for (final CrossSectionalMeasureBean crossXMeasure : crossXMeasures) {
	                if (crossXMeasure.getCode().equals(code)){
	                    measureConcept=crossXMeasure.getId();
	                    break;
	                }                    
	            }
            }
            return measureConcept;
    }
    
    /**
     * This method returns the maximum column that appears for the specific level.
     * @param mappingMap
     * @param level
	 * @return compNo
	 */
	public static int getMaxColumn(final String level, Map<String, String[]> mappingMap) {
		int maxColumn=0;
		final Iterator<Entry<String, String[]>> iter = mappingMap.entrySet().iterator();
		 while (iter.hasNext()) {
			 final Entry<String, String[]> entry = iter.next();
			 if (entry.getValue()[2].equals(level)){
				 final String pos=entry.getValue()[0];
				 if (!"".equals(pos)) {
					 //data will contain for pos 3-7, the values 3,7
					 final String data[]=pos.split("-");
					 //the last value might be the maximum column
					 int column =Integer.parseInt(data[data.length-1]);
					 if (maxColumn < column)
						 maxColumn=column;
				 }
			 }
		 }		 
		 return maxColumn;
	}
	
	/**
	 * Method for validating a string according to its compliance with the SDMX Id Type.
	 * Valid characters include A-Z, a-z, @, 0-9, _, -, $ for IDType. Each invalid character is replaced by '_' char
	 * @param id, the string to be validated
	 */
	public static String validateSDMXId(String id){
		//the id should contain only the following chars (A-Z, a-z, @, 0-9, _, -, $)		
		String idValue = id;
		for (int i = 0; i< id.length(); i++){
			char ch = id.charAt(i);
			if (ch !=36 && ch !=45 && !(ch >47 && ch<58) && !(ch>63 && ch<91) && ch !=95 && !(ch>96 && ch < 123)){			
				
				idValue = id.substring(0, i) + "_" + id.substring(i+1,id.length());
				id = idValue;
			}
		}		
		return idValue;
	}
	
	/**
	 * a setter for the Hash Map with the canonical concepts
	 * @param dsplConceptsMap
	 */
	public static void setDSPLConceptsMap (final LinkedHashMap<String,String[]> dsplConceptsHashMap){
		dsplConceptsMap = dsplConceptsHashMap;
	}
	
	/**
	 * a getter for the Hash Map with the canonical concepts
	 * @param dsplConceptsMap
	 */
	public static LinkedHashMap<String,String[]> getDSPLConceptsMap (){
		return dsplConceptsMap ;
	}
	
	/**
	 * If the dimensions contain the time dimension then return the size - 1
	 * 
	 * @param keyFamilyBean
	 * @return the size of the dimension beans list without the time dimension
	 */
	public static int dataStructureDimensionSize(DataStructureBean keyFamilyBean) {
		int dimensionSize = keyFamilyBean.getDimensions().size();
		for (DimensionBean dimensionBean : keyFamilyBean.getDimensions()) {
			if (!dimensionBean.isTimeDimension()) {
				return (dimensionSize - 1);
			}
		}
		return dimensionSize;
	}
	
	/**
	 * A KeyFamily Structure Response is processed: all the occurrences of "conceptSchemeVersion" are replaced with "conceptVersion"
	 * 
	 * @param inBuffer
	 * @return
	 */
	public static String replaceConceptVersionInStructureResponse(InputStream inBuffer, String encoding) {
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
 
		String line;
		try {
			br = new BufferedReader(new InputStreamReader(inBuffer, encoding)); 
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
 		} catch (IOException e) {
			logger.error("I/O error ", e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("I/O error ", e);
				}
			}
		}
		
		String response = sb.toString();		
		String processedResponse = response.replaceAll("conceptSchemeVersion", "conceptVersion");
		return processedResponse;
	}
	
	/**
	 * Method to get an ByteArrayOutputStream so that it can be used as many times as needed. 
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static ByteArrayOutputStream getByteArrayOutputStream(FileInputStream input) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buffer = new byte[1024];
		int len;
		while ((len = input.read(buffer)) > -1 ) {
		    baos.write(buffer, 0, len);
		}
		baos.flush();
		input.close();
		
		return baos;
	}
	
	/**
	 * Method to get an ByteArrayOutputStream so that it can be used as many times as needed. 
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static ByteArrayOutputStream getByteArrayOutputStream(byte[] inputBytes) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		baos.write(inputBytes);
		baos.flush();
		
		return baos;
	}
	
	/**
	 * Gets the encoding of the xml stream from the encoding attribute
	 * 
	 * @param isForEncoding
	 * @return
	 */
	public static String getEncodingFromStream(InputStream isForEncoding) {
		String encoding  = "UTF-8"; 
		Scanner scan = new Scanner(isForEncoding);
		
		if (scan.hasNextLine()) {
			String encodingLine =  scan.nextLine();
			if (encodingLine.contains("encoding=\"")) {
				String partialEncoding = encodingLine.substring(encodingLine.indexOf("encoding=\"") + 10);
				encoding = partialEncoding.substring(0 , partialEncoding.indexOf("\""));
			}
		}
		return encoding;
	}
	
	/**
	 * Processes the input to get the encoding and to replace the attributes not supported by sdmxsource.
	 *  
	 * @param inputBytes
	 * @return the stream to be used in the application
	 * @throws IOException
	 */
	public static ByteArrayInputStream getProcessedInputStream(byte[] inputBytes) throws IOException {
		//needed to be able to have more input streams, first one only for reading the encoding
		ByteArrayOutputStream baos = IoUtils.getByteArrayOutputStream(inputBytes);
		
		InputStream isForEncoding = new ByteArrayInputStream(baos.toByteArray()); 
		String encoding = IoUtils.getEncodingFromStream(isForEncoding);
		isForEncoding.close();
		
		InputStream is = new ByteArrayInputStream(baos.toByteArray()); 
		String processedResponse = IoUtils.replaceConceptVersionInStructureResponse(is, encoding);
		is.close();
		
		ByteArrayInputStream processedInputStream = new ByteArrayInputStream(processedResponse.getBytes(encoding));
		
		return processedInputStream;
	}
	
	/**
	 * Processes the input to get the encoding and to replace the attributes not supported by sdmxsource.
	 *  
	 * @param fileInputStream
	 * @return the stream to be used in the application
	 * @throws IOException
	 */
	public static InputStream getProcessedInputStream(FileInputStream input) throws IOException {
		//needed to be able to have more input streams, first one only for reading the encoding
		ByteArrayOutputStream baos = IoUtils.getByteArrayOutputStream(input);
		
		InputStream isForEncoding = new ByteArrayInputStream(baos.toByteArray()); 
		String encoding = IoUtils.getEncodingFromStream(isForEncoding);
		isForEncoding.close();
		
		InputStream is = new ByteArrayInputStream(baos.toByteArray()); 
		String processedResponse = IoUtils.replaceConceptVersionInStructureResponse(is, encoding);
		is.close();
		
		InputStream processedInputStream = new ByteArrayInputStream(processedResponse.getBytes(encoding));

//		BufferedReader br = new BufferedReader(new InputStreamReader(processedInputStream));
//		String readLine; 
//		while (((readLine = br.readLine()) != null)) {
//		System.out.println(readLine);
//		}
		
		return processedInputStream;
	}
	
	public static Set<String> checkTimeSeriesDimensions(final DataStructureBean keyFamilyBean, final TimeseriesKey tsKey) {
		
		Set<String> missingDimensions = new HashSet<String>();
		List<DimensionBean> dimensionBeans = keyFamilyBean.getDimensions();
	    Set<String> dimensionsNames = new HashSet<String>();
	    for (DimensionBean dimensionBean : dimensionBeans) {
	    	if (dimensionBean.isTimeDimension()) {
	    		continue;
	    	}
	    	dimensionsNames.add(dimensionBean.getId());
	    }
	    
	    Set<String> tsKeys = tsKey.getKeyValues().keySet();
	    
	    for (String dimName:dimensionsNames) {
	    	if(!tsKeys.contains(dimName)) {
	    		missingDimensions.add(dimName);
	    	}
	    }
	    
	    return missingDimensions;		
	}
	
	public static void checkFrequencyAndTimeFormatFromObservations(final List<Observation> observations, final String frequency) throws Exception {
		// depending on the frequency it checks TIME from all observations to see if it matches pattern
		TIME_FORMAT frequecyFormat = TIME_FORMAT.getTimeFormatFromCodeId(frequency);
		for (Observation observation : observations) {
			if (observation.getTimeValue() != "" && !DateUtil.getTimeFormatOfDate(observation.getTimeValue()).equals(frequecyFormat)) {
				throw new Exception("INPUT Parsing Exception: According to the FREQ value the time value is incorrect.");
			}
		}
	}
	
	/**
	 * returns an input stream from a class path resource
	 * 
	 * @param classPath
	 * @return
	 */
	public static InputStream getInputStreamFromClassPath(String classPath){
        return IoUtils.class.getClassLoader().getResourceAsStream(classPath); 
    }
	
}