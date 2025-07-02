package com.intrasoft.sdmx.converter;

import com.intrasoft.sdmx.converter.io.data.Formats;
import lombok.Data;
import org.estat.sdmxsource.util.csv.*;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;

import java.io.Serializable;
import java.util.*;

@Data
public class CliData implements Serializable {

    /**
     * default serial key
     */
    private static final long serialVersionUID = 1L;

    // InputSelection panel
    //Operation
    private Operation operationType;

    //Input_Output
    private Formats inputFormat;
    private Formats outputFormat;
    private Set<String> inputFilePaths;
    private String outputFilePath;
    private int maximumErrors = 3;

    //Structure
    private boolean structureInRegistry=false;
    private boolean structureDataflow;
    private String registryUrl;
    private String structureAgency;
    private String structureId;
    private String structureVersion;
    private String structureFilePath;

    //InputConfig
    // Useful before generating CLI command in result panel of webapp
    private boolean isManualHeader;
    private String headerFilePath;
    private boolean errorIfEmpty;
    //CSVInput
    private boolean csvInputOrdered;
    //SDMXCONV-800
    private boolean inputMapMeasures;
    private boolean inputTranscodingMeasures;
    private boolean hasDoubleQuotes;
    private boolean isSDMXCsvInput=false;
    private int csvInputLevels;
    private String csvInputDateFormat;
    private String csvInputDelimiter;
    private boolean csvInputMapCrossSectional=false;
    private CsvInputColumnHeader csvInputColumnHeader;
    private boolean manualInputTranscoding;
    private String inputTranscodingFilename;
    private String csvInputMappingFilename;
    private boolean manualCsvInputMapping;
    private String flrMappingFilename;
    private boolean manualFlrInputMapping;
    //ExcelInput
    private String excelParamsFilename;

    //OutputConfig
    //CSVOutput
    private boolean useDoubleQuotes;
    //SDMXCONV-1095
    private EscapeCsvValues escapeCsvValue=EscapeCsvValues.DEFAULT;
    private boolean isSDMXCsvOutput=false;
    private int csvOutputLevels;
    private String csvOutputDateFormat;
    private String csvOutputDelimiter;
    //SDMXCONV-800
    private boolean outputMapMeasures;
    private boolean outputTranscodingMeasures;
    private boolean csvOutputMapCrossSectional=false;
    private CsvOutputColumnHeader csvOutputColumnHeader;
    private boolean manualOutputTranscoding;
    private String outputTranscodingFilename;
    private String csvOutputMappingFilename;
    private boolean isManualCsvOutputMapping;
    //ExcelOutput
    private String excelTemplate;
    //SDMXOutputConfig
    private boolean useDefaultNamespace = true;
    private String namespaceUri;
    private String namespacePrefix;
    private String day;
    private String month;
    private String reportingPeriod;
    //GesmesOutput
    private String gesmesTechnique;
    //FlrOutput
    private String padding;
    private boolean isManualFlrOutputMapping;
    //other properties, this is used in the all template
    private boolean writeHeader;
    private boolean useTemplateData = false;
    private boolean allowAdditionalColumn;
    private boolean allowOnlyOrderedColumn = false;
    private CsvLabel writeLabels = CsvLabel.ID;
    private String sdmxHeaderValue;
    private String fieldSeparationCharacter;
    private String outputFieldSeparationCharacter;
}
