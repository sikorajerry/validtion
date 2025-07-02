package com.intrasoft.sdmx.converter;

import com.intrasoft.sdmx.converter.io.data.Formats;
import lombok.Data;
import org.estat.sdmxsource.util.csv.*;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;

import java.io.Serializable;

@Data
public class HtiData implements Serializable {

	//SDMXCONV-1215
	private Formats inputFormat;
	private Formats outputFormat;
	private boolean structureInRegistry = false;
	private String structureAgency;
	private String structureId;
	private String structureVersion;
	private String dataflowagency;
	private String dataflowid;
	private String dataflowversion;
	private String keyfamilyagency;
	private String keyfamilyid;
	private String keyfamilyversion;
	private boolean specifydataflow;
	private boolean useregistry;
	private String registryUrl;
	private String outputDelimiter;
	private String inputDelimiter;
	private boolean csvInputOrdered;
	private boolean errorIfEmpty;
	private String csvInputDateFormat;
	private int csvInputLevels;
	private int csvOutputLevels;
	private boolean unescapecsvinput;
	private CsvInputColumnHeader csvInputColumnHeader;
	private CsvOutputColumnHeader csvOutputColumnHeader;
	//FlrOutput
	private String padding;
	private boolean useDefaultNamespace = true;
	private String namespaceUri;
	private String namespacePrefix;
	private String day;
	private String month;
	private String reportingPeriod;
	private boolean useXSMeasures;
	private boolean useExplicitMeasures;
	private CsvLabel writeLabels = CsvLabel.ID;
	private EscapeCsvValues escapeCsvValues;
	private String csvOutputDateFormat;
	private String gesmeswritingtechnique;
	private String sdmxHeaderValue;
	private String fieldSeparationCharacter;
	private String outputFieldSeparationCharacter;
}
