package com.intrasoft.sdmx.converter.io.data.excel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * manager for row / column headers
 * 
 * @author dragos balan
 *
 */
public class ExcelRowColumnHeader {
	
	private int currentIndex = -1;
	
	/**
	 * map (colIndex, headerValues) sorted by column index
	 */
	private Map<Integer, List<String>> headerValues = new TreeMap<Integer, List<String>>();
	
	/**
	 * the list of dimensions which will be managed by this header. 
	 * WARNING: the order in the list is very important
	 */
	private List<String> dimensions = null;
	
	
	/**
	 * constructor for this header manager
	 * 
	 * @param dimensions	the list of dimensions to be managed by this header
	 */
	public ExcelRowColumnHeader(List<String> dimensions){
		this.dimensions = dimensions; 
	}
	
	/**
	 * 
	 * @param values
	 * @return	true if the values are already in the header values
	 */
	public boolean containsValues(List<String> values){
		return headerValues.containsValue(values); 
	}
	
	/**
	 * adds the provided values in the list of managed values (on a different index)
	 * 
	 * @param values	a list of new values to be added in the list of values managed by this header
	 * @return			the index of the values in the header
	 */
	public int addValues(List<String> values){
		if(dimensions.size() != values.size()){
			throw new IllegalArgumentException("Number of values added in header ("+
						values.size()+") different than expected "+dimensions.size()+" components in dsd");
		}
		currentIndex++;
		headerValues.put(Integer.valueOf(currentIndex), values);
		return currentIndex; 
	}
	
	/**
	 * retrieves the index of the values in the list 
	 * 
	 * @param values	the list of values to be looked up
	 * @return			the index of the values in the header
	 */
	public int getIndexForValues(List<String> values){
		Integer result = null; 
		for(Integer idx: headerValues.keySet()){
			if(headerValues.get(idx).equals(values)){
				result = idx; 
				break; 
			}
		}
		return result; 
	}
	
	/**
	 * returns the values for the given dimension
	 * 
	 * @param dimension	the dimension
	 * @return a list of values for the given dimension
	 */
	public List<String> getValuesForDimension(String dimension){
		List<String> result = new ArrayList<String>(); 
		int index = dimensions.indexOf(dimension);
		if(index >=0){
			for(Integer key: headerValues.keySet()){
				result.add(headerValues.get(key).get(index)); 
			}			
		}
		return result; 
	}
}
