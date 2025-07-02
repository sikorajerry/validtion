package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.services.exceptions.WriteTranscodingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.constants.STRUCTURE_OUTPUT_FORMAT;
import org.sdmxsource.sdmx.api.manager.output.StructureWriterManager;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.mapping.StructureSetBean;
import org.sdmxsource.sdmx.api.model.format.StructureFormat;
import org.sdmxsource.sdmx.api.model.mutable.base.StructureMapMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.mapping.ComponentMapMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.mapping.RepresentationMapRefMutableBean;
import org.sdmxsource.sdmx.api.model.mutable.mapping.StructureSetMutableBean;
import org.sdmxsource.sdmx.sdmxbeans.model.SdmxStructureFormat;
import org.sdmxsource.sdmx.sdmxbeans.model.header.HeaderBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.mapping.ComponentMapMutableBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.mapping.RepresentationMapRefMutableBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.mapping.StructureMapMutableBeanImpl;
import org.sdmxsource.sdmx.sdmxbeans.model.mutable.mapping.StructureSetMutableBeanImpl;
import org.sdmxsource.sdmx.util.beans.reference.StructureReferenceBeanImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;


@Service
public class TranscodingService {

	private static Logger logger = LogManager.getLogger(TranscodingService.class);
	
	private final static StructureFormat outputFormat = new SdmxStructureFormat(STRUCTURE_OUTPUT_FORMAT.SDMX_V21_STRUCTURE_DOCUMENT);
	
	@Autowired
	private StructureWriterManager structureWritingManager;
	
	
	/** Method for save the Transcoding into a file
	 * creates StructureMapMutableBean bean and trancoding values
	 * and writes xml using structureWritingManager from sdmxsource.sdmx.api
	 * 
	 * @param transcodingMap - the transcoding values
	 * @param bean - the dsd
	 * @param transOutputFile - the output xml file
	 * @throws WriteTranscodingException
	 */
	public void saveTranscoding(LinkedHashMap<String, LinkedHashMap<String, String>> transcodingMap, DataStructureBean bean, OutputStream outputStream) throws WriteTranscodingException  {
			StructureSetMutableBean structureSet = new StructureSetMutableBeanImpl();
			structureSet.setAgencyId(bean.getAgencyId());
			structureSet.setId("CONVERTER_TRANSCODING");
			structureSet.addName("en", bean.getName());
			
			StructureMapMutableBean map = new StructureMapMutableBeanImpl();
			map.addName("en", "Transcoding for Converter");
			map.setId(bean.getId());
			map.setSourceRef(new StructureReferenceBeanImpl("CONVERTER", "SINGLE_LEVEL_CSV", "1.0", SDMX_STRUCTURE_TYPE.DSD));
			map.setTargetRef(bean.asReference());
			int i=0;
			for (Map.Entry<String, LinkedHashMap<String, String>> transcoding : transcodingMap.entrySet()) {
				ComponentMapMutableBean componentMap = new ComponentMapMutableBeanImpl();
				componentMap.setMapConceptRef("CSV_COLUMN" + i);
				componentMap.setMapTargetConceptRef(transcoding.getKey());
				RepresentationMapRefMutableBean representationRef = new RepresentationMapRefMutableBeanImpl();
				componentMap.setRepMapRef(representationRef);
				for (Map.Entry<String, String> rule : transcoding.getValue().entrySet()) {
					representationRef.addMapping(rule.getKey(), rule.getValue());
				}
				map.addComponent(componentMap);
			}			
			structureSet.addStructureMap(map);		
			StructureSetBean immutable = structureSet.getImmutableInstance();
		try {
			structureWritingManager.writeStructure(immutable, new HeaderBeanImpl("IREF000001", "ZZ9"), outputFormat, outputStream);
			outputStream.flush();
		} catch (FileNotFoundException ex) {
			throw new WriteTranscodingException("Transcoding file not found!", ex);
		} catch (IOException e) {
			throw new WriteTranscodingException("Error while writting the transcoding File!", e);
		}
	}
}
