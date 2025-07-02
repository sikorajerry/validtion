package com.intrasoft.sdmx.converter.io.data;

public interface TranscodingEngineInterface {

	/**
	 * Method that checks if transcoding rules exist for the specific dimension.
	 * @param key
	 * @return
	 */
	boolean hasTranscodingRules(final String key);

	/**
	 * Method that finds the code that the 'value' will be transcoded to.
	 * <p>This method is called only in case there is a rule for that key. (Always we check with {@link #hasTranscodingRules(String)}).</p>
	 * <ul>
	 *     <li>Firstly we check id the rules in the transcoding contains our value, return the mapped value.</li>'
	 *     <li>If the value is null and the rules contain an empty string as key, then te default value for this dim will be used.
	 *     (see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1164">SDMXCONV-1164</a>)</li>
	 *     <li>If there is no corresponding rule just return the original value.</li>
	 * </ul>
	 * @String key
	 * @String value
	 * @return String returnedValue
	 */
	String getValueFromTranscoding(final String key, final String value);
}
