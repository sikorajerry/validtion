package com.intrasoft.sdmx.converter.services;

import com.intrasoft.sdmx.converter.io.data.Formats;
import com.intrasoft.sdmx.converter.services.exceptions.ParseXmlException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.util.csv.SdmxCsvInputConfig;
import org.sdmxsource.sdmx.api.constants.ExceptionCode;
import org.sdmxsource.sdmx.api.constants.SDMX_ERROR_CODE;
import org.sdmxsource.sdmx.api.constants.SDMX_SCHEMA;
import org.sdmxsource.sdmx.api.constants.SdmxConstants;
import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.validation.exceptions.SdmxDataFormatException;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.stereotype.Service;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * helper service for format detection
 *
 * @author a bad combo from agilis and intrasoft
 */
@Service
public class InputService {

    /**
     * the one and only logger
     */
    private static Logger logger = LogManager.getLogger(InputService.class);

    /**
     * detects the format of the given file by checking the extension first and the (only if needed) by checking the contents
     *
     * @param file the file which format should be detected
     * @return the file format or null if the file format could not be detected
     * @throws ParseXmlException
     * @throws XMLStreamException
     */
    public Formats detectFormat(File file) throws IOException {
        Formats result = detectFormatFromFileExtension(file);
        if (result == null) {
            result = detectFormatFromFileContent(file);
        }
        //SDMXCONV-76 If file has .csv extension then we will try to recognize the type of csv from its contents
        if (result == Formats.CSV) {
            result = detectCsvTypeFromFileContent(file);
        }
        return result;
    }

    /**
     * detects the format of the given file only based on its extension.
     * This method is only detecting the Excel formats
     *
     * @param file the file which format should be detected
     * @return the file format or null if the format could not be detected
     */
    public Formats detectFormatFromFileExtension(File file) {
        logger.info("detecting format based on file extension for file {}", file.getAbsolutePath());
        Formats result = null;
        if (file.isFile()) {
            //for the moment only excel is detected based on the file extension
            if (file.getName().endsWith(".xls") ||
                    file.getName().endsWith(".xlsx") ||
                    file.getName().endsWith(".xlsm") ||
                    file.getName().endsWith(".XLS") ||
                    file.getName().endsWith(".XLSX") ||
                    file.getName().endsWith(".XLSM") ||
                    file.getName().endsWith(".dat")) {
                result = Formats.EXCEL;
            }
            if (file.getName().endsWith(".ges") ||
                    file.getName().endsWith(".dat")) {
                result = Formats.GESMES_TS;
            }
            if (file.getName().endsWith(".flr") ||
                    file.getName().endsWith(".txt") ||
                    file.getName().endsWith(".FLR") ||
                    file.getName().endsWith(".TXT") ||
                    file.getName().endsWith(".dat")) {
                result = Formats.FLR;
            }
            if (file.getName().endsWith(".csv")
                    || file.getName().endsWith(".CSV")
                    || file.getName().endsWith(".dat")) {
                result = Formats.CSV;
            }
        }
        return result;
    }

    public Formats detectFormatFromFileNames(Set<String> inputFileNames) {
        Formats result = null;
        List<Formats> resultList = new ArrayList<>();
        for (String inputFileName : inputFileNames) {
            resultList.add(detectFormatFromFileName(inputFileName));
        }
        if (ObjectUtil.validCollection(resultList)) {
            if (resultList != null && !ObjectUtil.isAllNulls(resultList)) {
                if (verifyAllEqualUsingALoop(resultList)) {
                    result = resultList.get(0);
                } else {
                    throw new IllegalArgumentException("Multiple files must all be the same Format! Can not detect one Format!");
                }
            }
        }
        return result;
    }

    public boolean verifyAllEqualUsingALoop(List<Formats> list) {
        for (Formats s : list) {
            if (s != null && !s.equals(list.get(0)))
                return false;
        }
        return true;
    }

    /**
     * detects the Excel file format based on the filename provided
     *
     * @param inputFileName the name of the file
     * @return the format detected or null if a format could not be detected
     */
    public Formats detectFormatFromFileName(String inputFileName) {
        Formats result = null;
        if (inputFileName.endsWith(".xls") ||
                inputFileName.endsWith(".xlsx") ||
                inputFileName.endsWith(".xlsm") ||
                inputFileName.endsWith(".XLS") ||
                inputFileName.endsWith(".XLSX") ||
                inputFileName.endsWith(".XLSM")) {
            result = Formats.EXCEL;
        }
        if (inputFileName.endsWith(".ges") ||
                inputFileName.endsWith(".GES")) {
            result = Formats.GESMES_TS;
        }
        if (inputFileName.endsWith(".flr") ||
                inputFileName.endsWith(".txt") ||
                inputFileName.endsWith(".FLR") ||
                inputFileName.endsWith(".TXT")) {
            result = Formats.FLR;
        }
        if (inputFileName.endsWith(".csv") ||
                inputFileName.endsWith(".CSV")) {
            result = Formats.CSV;
        }
        return result;
    }

    public Formats detectFormatFromFileContent(ReadableDataLocation dataLocation) throws IOException {
        Formats result = null;
        try (InputStream inputStream = dataLocation.getInputStream()) {
            result = detectFormatFromFileContent(inputStream);
        }
        return result;
    }

    /**
     * Detects the input format from the file contents.
     * For efficiency reasons open and read only the first 650 characters.
     *
     * @param stream Input stream
     * @return Formats
     * @throws IOException
     */
    public Formats detectFormatFromFileContent(InputStream stream) throws IOException {
        BOMInputStream bomInStream = new BOMInputStream(stream);
        Formats result = null;
        char[] firstPortion = null;
        String str = null;
        try (InputStreamReader strReader = new InputStreamReader(bomInStream);
             BufferedReader br = new BufferedReader(strReader)) {
            firstPortion = new char[650];
            br.read(firstPortion, 0, 650);
            str = new String(firstPortion);
            str = str.toUpperCase();
            for (int i = 0; i < SdmxConstants.getNamespacesV2().size(); i++) {
                if (str.contains(SdmxConstants.getNamespacesV2().get(i).toUpperCase())) {
                    if (str.contains(SdmxConstants.GENERIC_DATA_ROOT_NODE.toUpperCase())) {
                        result = Formats.GENERIC_SDMX;
                    } else if (str.contains(SdmxConstants.UTILITY_DATA_ROOT_NODE.toUpperCase())) {
                        result = Formats.UTILITY_SDMX;
                    } else if (str.contains(SdmxConstants.COMPACT_DATA_ROOT_NODE.toUpperCase())) {
                        result = Formats.COMPACT_SDMX;
                    } else if (str.contains(SdmxConstants.CROSS_SECTIONAL_DATA_ROOT_NODE.toUpperCase())) {
                        result = Formats.CROSS_SDMX;
                    } else if (str.contains(SdmxConstants.MESSAGE_GROUP_ROOT_NODE.toUpperCase())) {
                        result = Formats.MESSAGE_GROUP;
                    }
                }
            }
            for (int i = 0; i < SdmxConstants.getNamespacesV2_1().size(); i++) {
                if (str.contains(SdmxConstants.getNamespacesV2_1().get(i).toUpperCase())) {
                    if (str.contains(SdmxConstants.STRUCTURE_SPECIFIC_TIME_SERIES_DATA.toUpperCase())) {
                        result = Formats.STRUCTURE_SPECIFIC_TS_DATA_2_1;
                    } else if (str.contains(SdmxConstants.STRUCTURE_SPECIFIC_DATA.toUpperCase())) {
                        result = Formats.STRUCTURE_SPECIFIC_DATA_2_1;
                    } else if (str.contains(SdmxConstants.GENERIC_TIME_SERIES_DATA_ROOT_NODE.toUpperCase())) {
                        result = Formats.GENERIC_TS_DATA_2_1;
                    } else if (str.contains(SdmxConstants.GENERIC_DATA_ROOT_NODE.toUpperCase())) {
                        result = Formats.GENERIC_DATA_2_1;
                    }
                }
            }
            for (int i= 0; i < SdmxConstants.getNamespacesV3_0_0().size(); i++){
                if (str.contains((SdmxConstants.getNamespacesV3_0_0().get(i).toUpperCase()))){
                    if (str.contains(SdmxConstants.STRUCTURE_SPECIFIC_DATA.toUpperCase())){
                        result = Formats.STRUCTURE_SPECIFIC_DATA_3_0;
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Error parsing the file! Can not detect the Format!", e);
        } finally {
            str = null;
            firstPortion = null;
            if (bomInStream != null) {
                try {
                    bomInStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return result;
    }

    /**
     * detects the format based on the file contents.
     * This method is only detecting the SDMX contents
     *
     * @param file the file
     * @return the format if the format can be detected or null otherwise
     * @throws ParseXmlException
     * @throws XMLStreamException
     * @throws FileNotFoundException if the provided file does not exist
     */
    public Formats detectFormatFromFileContent(File file) throws IOException {
        Formats format = null;
        try (FileInputStream inStream = new FileInputStream(file)) {
            BOMInputStream bomInStream = new BOMInputStream(inStream);
            format = detectFormatFromFileContent(bomInStream);
        }
        return format;
    }

    /**
     * Detects only the type of csv files based on the first line file contents.
     *
     * @param file the input csv file
     * @return the format if the format can be detected as SDMX_CSV or MULTILEVEL_CSV, returns CSV otherwise
     * @throws FileNotFoundException if the provided file does not exist
     */
    public Formats detectCsvTypeFromFileContent(File file) throws IOException {
        Formats format = null;
        SdmxCsvInputConfig sdmxCsvInputConfig = new SdmxCsvInputConfig();
        try (FileReader input = new FileReader(file)) {
            format = detectCsvTypeFromFileContent(input);
        }
        if(format.equals(Formats.SDMX_CSV_2_0)){
            sdmxCsvInputConfig.setSdmxCsv20(true);
        }
        return format;
    }

    private Formats detectCsvTypeFromFileContent(Reader input) throws IOException {
        Formats result = Formats.CSV;
        BufferedReader bufRead = new BufferedReader(input, 500);
        try {
            //Read only the first line
            String lineFromFile = bufRead.readLine();
            if (lineFromFile != null) {
                //SDMX_CSV files always have a header that starts with DATAFLOW column
                if (lineFromFile.startsWith("DATAFLOW") || lineFromFile.startsWith("Dataflow")
                || lineFromFile.startsWith("\"DATAFLOW\"") || lineFromFile.startsWith("\"Dataflow\"")) {
                    result = Formats.SDMX_CSV;
                }
                if (lineFromFile.startsWith("STRUCTURE") || lineFromFile.startsWith("Structure") ||
                        lineFromFile.startsWith("\"STRUCTURE\"") || lineFromFile.startsWith("\"Structure\"")) {
                    result = Formats.SDMX_CSV_2_0;
                }
                //MULTI_LEVEL_CSV files always have a number for level as first column
                if (Character.isDigit(lineFromFile.charAt(0))) {
                    result = Formats.MULTI_LEVEL_CSV;
                }
            }
        } finally {
            IOUtils.closeQuietly(bufRead);
            IOUtils.closeQuietly(input);
        }
        return result;
    }

    /**
     * <strong>Check for BOM encoding, without processing the stream</strong>
     * <p>Used in cases we don't autodetect format. Avoid to skip the error.</p>
     *
     * @param dataLocation    Input data
     * @param isCheckBomError Configuration on component level
     */
    public static void checkBomError(ReadableDataLocation dataLocation, boolean isCheckBomError) {
        try (InputStream inputStream = dataLocation.getInputStream();
             BOMInputStream bomInStream = new BOMInputStream(inputStream)) {
            if (isCheckBomError && bomInStream.hasBOM()) {
                final String errorString = "Unexpected character encoding: 'UTF-8 BOM'. Please make sure the input file is encoded in 'UTF-8'.";
                throw new SdmxDataFormatException(ExceptionCode.XML_PARSE_EXCEPTION, null, 1, 1, errorString);
            }
        } catch (IOException ex) {
            throw new SdmxException(ex, SDMX_ERROR_CODE.SEMANTIC_ERROR, ExceptionCode.JAVA_IO_EXCEPTION, ex.getMessage());
        }
    }

    /**
     * Check if input format is compatible with the structure given.
     * @return boolean
     */
    public static boolean compatibilityCheck(Formats inputFormat, SDMX_SCHEMA sdmxSchema) {
        boolean isCompatible = true;
        if(inputFormat == Formats.CROSS_SDMX && sdmxSchema == SDMX_SCHEMA.VERSION_THREE) {
            isCompatible = false;
            throw new SdmxException("Cross sectional format is not compatible with SDMX 3.0 DSD. Please provide Cross sectional DSD in SDMX 2.0 format.", SDMX_ERROR_CODE.SEMANTIC_ERROR);
        }
        return isCompatible;
    }

}
