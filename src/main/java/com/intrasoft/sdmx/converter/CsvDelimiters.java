package com.intrasoft.sdmx.converter;

import org.sdmxsource.util.ObjectUtil;

import java.util.ArrayList;
import java.util.List;

public enum CsvDelimiters {

    EMPTY("", "",""),
    SEMICOLON(";", ";", ";"),
    TAB("TAB", "\\t", "	"),
    COMMA(",", ",", ","),
    COLON(":", ":", ":"),
    SPACE("SPACE", " ", " "),
    OTHER("OTHER", "", "");
    
    /**
     * the user friendly description (i.e. TAB, SPACE, etc)
     */
    private String description;
    
    /**
     * the real character to be used during the conversion
     */
    private String delimiter;

    private String delimiterChar;

    CsvDelimiters(String description, String delimiterChar, String delimiter) {
        this.description = description;
        this.delimiterChar = delimiterChar;
        this.delimiter = delimiter;
    }

    public String getDescription(){
        return description;
    }
    
    public String getDelimiterString(){
        return delimiter; 
    }

    public String getDelimiterChar(){
        return delimiterChar;
    }
    
    public static String[] descriptions(){
        List<String> result = new ArrayList<String>();
        for (CsvDelimiters delimiter : values()) {
            result.add(delimiter.getDescription()); 
        }
        return result.toArray(new String[result.size()]);
    }
    
    public static String[] descriptionsNoOther () {
    	List<String> result = new ArrayList<String>();
        for (CsvDelimiters delimiter : values()) {
        	if (delimiter != CsvDelimiters.OTHER) {
        		result.add(delimiter.getDescription());
        	}
        }
        return result.toArray(new String[result.size()]);
    }

    public static CsvDelimiters fromDelimiter(String delimiter) {
        CsvDelimiters result = null;
        //System.out.println("[" + delimiter + "]");
        for (CsvDelimiters delim : values()) {
            //System.out.println("[" + delim.getDelimiterString() + "]");
            //System.out.println("[" + delim.getDescription() + "]");
            //System.out.println("[" + delim.getDelimiterChar()+ "]");
            if (delim.getDelimiterString().equalsIgnoreCase(delimiter)
                    || delim.getDescription().equalsIgnoreCase(delimiter)
                        || delim.getDelimiterChar().equalsIgnoreCase(delimiter)) {
                result = delim;
                break;
            }
        }
        return result;
    }

    public static String delimiterNormalized(String delimiter, String defaultDelimiter) {
        String result = ObjectUtil.validObject(delimiter) ? delimiter : defaultDelimiter;
        CsvDelimiters normalizedDelimiter = fromDelimiter(delimiter);
        if(ObjectUtil.validObject(normalizedDelimiter)){
            result = normalizedDelimiter.getDelimiterString();
        }
        return result;
    }
}
