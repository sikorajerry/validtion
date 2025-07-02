package com.intrasoft.sdmx.converter.ex.ui;

import org.estat.sdmxsource.util.csv.CsvInColumnMapping;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class OldImplementationUtils {
//
//	public static HeaderBean translateHeaderBean(org.sdmxsource.sdmx.api.model.header.HeaderBean currentHeader) {
//		HeaderBean headerBean = new HeaderBean();
//		headerBean.addName(currentHeader.getName().get(0));
//		headerBean.addReceiver(currentHeader.getReceiver().get(0));
//		headerBean.addSender(currentHeader.getSender());
//		
//		headerBean.addSource(currentHeader.getSource().get(0));
//		headerBean.setDataSetID(currentHeader.getDatasetId());
//		headerBean.setExtracted(currentHeader.getExtracted().toString());
//		headerBean.setId(currentHeader.getId());
//		headerBean.setPrepared(currentHeader.getPrepared().toString());
//		headerBean.setNames(currentHeader.getName());
//		headerBean.setReceivers(currentHeader.getReceiver());
//		
//		return headerBean;
//	}
//	
//	public static HeaderBean defaultHeaderBean(String senderName, String senderId) {
//		HeaderBean headerBean = new HeaderBean();
//		
//		List<TextTypeWrapper> senderNames = null;
//		if (senderName != null) {
//			senderNames = new ArrayList<TextTypeWrapper>();
//			senderNames.add(new TextTypeWrapperImpl("en", senderName, null));
//		}
//		final PartyBean sender = new PartyBeanImpl(senderNames, senderId, null, null);	
//		headerBean.getSenders().add(sender);
//		
//		return headerBean;
//	}
	
	/**
	 * Method that parses a mapping.xml file and returns a map of dimensions-values
	 * 
	 * @File mapping
	 * @return Map<String, String> result
	 * @throws IOException
	 */
	public static Map<String, String[]> getMappingMap(Map<String, CsvInColumnMapping> mapping) throws IOException {
		final Map<String, String[]> map = new LinkedHashMap<String, String[]>();

		for (final String elementName : mapping.keySet()) {
			final String name = elementName;
			String value = String.valueOf(mapping.get(elementName).getColumns().get(0).getIndex());
			for (int i=1; i<mapping.get(elementName).getColumns().size(); i++) {
				value = value + "+" + mapping.get(elementName).getColumns().get(i);
			}
			final String isFixed = mapping.get(elementName).getFixedValue();
			final String level = String.valueOf(mapping.get(elementName).getLevel());
			final String[] data = new String[4];
			
			if (value == null) {
				data[0] = "";
			} else {
				data[0] = value;
			}
			if (isFixed == null) {
				data[1] = "false";
			} else {
				data[1] = isFixed;
			}
			if (level == null) {
				data[2] = "";
			} else {
				data[2] = level;
			}
			data[3] = "";
			map.put(name, data);
		}
		return map;
	}
	
	/**
	 * Method that takes MultiLevelCsvOutColMapping output csv mapping 
	 * and transforms it to Map Map<String, String[]> to be compatible with ex InputParams
	 * 
	 * MultiLevelCsvOutColMapping mapping
	 * @return Map<String, String> result
	 */
	public static Map<String, String[]> getMappingMapOutput(MultiLevelCsvOutColMapping mapping) {
		final Map<String, String[]> map = new LinkedHashMap<String, String[]>();

		Map<Integer, TreeMap<Integer, String>> mapped = mapping.getDimensionsMappedOnLevel();
		for (final int elementLevel : mapped.keySet()) {
			TreeMap<Integer, String> trMap = mapped.get(elementLevel);
			for(int elementIndx : trMap.keySet()) {
				final String[] data = new String[4];
				String elementMapped = trMap.get(elementIndx);
					data[0] = String.valueOf(elementIndx);
					data[1] = "false";
					data[2] = String.valueOf(elementLevel);
				data[3] = "";
				map.put(elementMapped, data);
			}
		}

		return map;
	}
	
}
