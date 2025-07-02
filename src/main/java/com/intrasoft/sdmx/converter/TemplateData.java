package com.intrasoft.sdmx.converter;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.model.ndata.Transcoding;
import lombok.Data;
import org.estat.sdmxsource.util.csv.*;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvLabel;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.CsvOutputColumnHeader;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.EscapeCsvValues;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.MultiLevelCsvOutColMapping;

import java.io.Serializable;
import java.util.*;

/**
 * Created by dbalan on 7/12/2017.
 */
public @Data class TemplateData implements Serializable {

	/**
	 * default serial key
	 */
	private static final long serialVersionUID = 1L;

	public static final String MAP_MEASURE_DIMENSION = "Map measure Dimension";
	public static final String MAP_CROSSX_MEASURES = "Map CrossX measures";
	
	//Operation
	private Operation operationType;

	//Input_Output
	private Formats inputFormat;
	private Formats outputFormat;
	private Set<String> inputFilePaths;
	private String outputFilePath;

    //Structure
    private boolean structureInRegistry=false;
    private boolean structureDataflow;
    private String registryUrl;
    private String structureAgency;
    private String structureId;
    private String structureVersion;
    private String structureFilePath;
            
    //InputConfig    
    //HeaderInformation
    private Properties header; 
    private String headerFilePath;
    private boolean errorIfEmpty;
    //CSVInput    
    private boolean csvInputOrdered;
    //SDMXCONV-800
    private boolean inputMapMeasures;
    private boolean inputTranscodingMeasures;
    private boolean hasDoubleQuotes;
    private boolean isSDMXCsvInput=false;
    private int csvInputLevels=1;
    private String csvInputDateFormat;
    private String csvInputDelimiter;
    private boolean csvInputMapCrossSectional=false;
    private CsvInputColumnHeader csvInputColumnHeader;
    private LinkedHashMap<String, LinkedHashMap<String, String>> csvInputStructureSet;
    private Map<String, CsvInColumnMapping> csvInputMapping;    
    private Map<String, FlrInColumnMapping> flrInputMapping;
    private boolean isColumnMappingEnabled;
    private String columnMappingFileName;
    //ExcelInput
    private String parametersInExternalFile;
    private Map<String, String> parameterSheetMapping;
    private LinkedHashMap<String, ArrayList<String>> excelParameterMultipleMap = new LinkedHashMap<String, ArrayList<String>>();

    //OutputConfig
    //CSVOutput
    private boolean useDoubleQuotes;
    //SDMXCONV-1095
    private EscapeCsvValues escapeCsvValue=EscapeCsvValues.DEFAULT;
    private boolean isSDMXCsvOutput=false;
    private int csvOutputLevels=1;
    private String csvOutputDateFormat;
    private String csvOutputDelimiter;
    //SDMXCONV-800
    private boolean outputMapMeasures;
    private boolean outputTranscodingMeasures;
    private boolean csvOutputMapCrossSectional=false;
    private CsvOutputColumnHeader csvOutputColumnHeader;
    private LinkedHashMap<String, LinkedHashMap<String, String>> csvOutputStructureSet;
    private MultiLevelCsvOutColMapping csvOutputMapping;
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
    
    //other properties, this is used in the all template
    private boolean writeHeader;        
    private boolean useTemplateData = false;

    private boolean allowAdditionalColumn;
    private CsvLabel writeLabels = CsvLabel.ID;
    private String fieldSeparationCharacter;
    private String outputFieldSeparationCharacter;

    public List<Transcoding> getInputTranscodings(LinkedHashMap<String, LinkedHashMap<String, String>> input){
        List<Transcoding> result = new ArrayList<>();
        if(input != null){
            for(String sdmxComponentId: input.keySet()){
                Map<String, String> targetAndSourceCodesMap = input.get(sdmxComponentId);
                for(Map.Entry<String, String> sourceAndTargetPair: targetAndSourceCodesMap.entrySet()){
                    Transcoding transcoding = new Transcoding();
                    transcoding.setSdmxComponentId(sdmxComponentId);
                    transcoding.setSdmxCode(sourceAndTargetPair.getKey());
                    transcoding.setLocalCode(sourceAndTargetPair.getValue());
                    result.add(transcoding);
                }
            }
        }
        return result;
    }

    public void resetAll() {
        // Reset Operation
        operationType = null;

        // Reset Input_Output
        inputFormat = null;
        outputFormat = null;
        inputFilePaths = new HashSet<>();
        outputFilePath = null;

        // Reset Structure
        structureInRegistry = false;
        structureDataflow = false;
        registryUrl = null;
        structureAgency = null;
        structureId = null;
        structureVersion = null;
        structureFilePath = null;

        // Reset InputConfig and HeaderInformation
        header = new Properties();
        headerFilePath = null;
        errorIfEmpty = false;

        // Reset CSVInput
        csvInputOrdered = false;
        inputMapMeasures = false;
        inputTranscodingMeasures = false;
        hasDoubleQuotes = false;
        isSDMXCsvInput = false;
        csvInputLevels = 1;
        csvInputDateFormat = null;
        csvInputDelimiter = null;
        csvInputMapCrossSectional = false;
        csvInputColumnHeader = null;
        csvInputStructureSet = new LinkedHashMap<>();
        csvInputMapping = new HashMap<>();
        flrInputMapping = new HashMap<>();
        isColumnMappingEnabled = false;
        columnMappingFileName = null;

        // Reset ExcelInput
        parametersInExternalFile = null;
        parameterSheetMapping = new HashMap<>();
        excelParameterMultipleMap = new LinkedHashMap<>();

        // Reset OutputConfig
        useDoubleQuotes = false;
        escapeCsvValue = EscapeCsvValues.DEFAULT;
        isSDMXCsvOutput = false;
        csvOutputLevels = 1;
        csvOutputDateFormat = null;
        csvOutputDelimiter = null;
        outputMapMeasures = false;
        outputTranscodingMeasures = false;
        csvOutputMapCrossSectional = false;
        csvOutputColumnHeader = null;
        csvOutputStructureSet = new LinkedHashMap<>();
        csvOutputMapping = null;

        // Reset ExcelOutput
        excelTemplate = null;

        // Reset SDMXOutputConfig
        useDefaultNamespace = true;
        namespaceUri = null;
        namespacePrefix = null;
        day = null;
        month = null;
        reportingPeriod = null;

        // Reset GesmesOutput
        gesmesTechnique = null;

        // Reset FlrOutput
        padding = null;

        // Reset other properties
        writeHeader = false;
        useTemplateData = false;
        allowAdditionalColumn = true;
        writeLabels = CsvLabel.ID;
        fieldSeparationCharacter = null;
        outputFieldSeparationCharacter = null;
    }
}
