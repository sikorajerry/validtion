package com.intrasoft.sdmx.converter.services;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.util.IOUtils;
import org.sdmxsource.sdmx.api.factory.ReadableDataLocationFactory;
import org.sdmxsource.sdmx.api.model.data.DataFormat;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.sdmx.dataparser.manager.DataInformationManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.intrasoft.sdmx.converter.io.data.Formats;

@Service
public class MessageGroupService {

    private static Logger logger = LogManager.getLogger(MessageGroupService.class);
    
    @Autowired
    private ReadableDataLocationFactory readableDataLocationFactory;
    
    @Autowired
    private DataInformationManager dataInformationManager;
    
    
    private Formats detectFormatForDataFormat(DataFormat inputDataFormat){
        Formats result = Formats.COMPACT_SDMX;  
        switch (inputDataFormat.getSdmxDataFormat()) {
            case MESSAGE_GROUP_2_0_COMPACT:
                result = Formats.COMPACT_SDMX;
                break;
            case MESSAGE_GROUP_2_0_UTILITY:
                result = Formats.UTILITY_SDMX;
                break;
            case MESSAGE_GROUP_2_0_GENERIC:
                result = Formats.GENERIC_SDMX;
                break;
            default:
                logger.warn("could not detect the replacement format for {} defaulting to compact", inputDataFormat);
                break;
        }
        return result; 
    }
    
    /**
     * detects the real format of the given message group file
     * (you're probably aware that MESSAGE_GROUP is not a real format)
     * 
     * @param inputFile the input file 
     * @return  the real format of the given file
     */
    public Formats detectFormatForMessageGroupFile(File inputFile){
        ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(inputFile);
        DataFormat inputDataFormat =  dataInformationManager.getDataType(dataLocation);
        return detectFormatForDataFormat(inputDataFormat);
    }
    
    /**
     * detects the real format of the given message group file
     * 
     * @param inputFileName the name of the input file
     * @return  the format of the file
     */
    public Formats detectFormatForMessageGroupFile(String inputFileName){
        File inputFile = new File(inputFileName); 
        return detectFormatForMessageGroupFile(inputFile); 
    }
    
    /**
     * detects the real format of the given message group byte array
     * 
     * @param inputFileName the name of the input file
     * @return  the format of the file
     */
    public Formats detectFormatForMessageGroupByteArray(byte[] input){
        ReadableDataLocation dataLocation = readableDataLocationFactory.getReadableDataLocation(input);
        DataFormat inputDataFormat =  dataInformationManager.getDataType(dataLocation);
        return detectFormatForDataFormat(inputDataFormat); 
    }
    
    /**
     * 
     * @param format
     * @return
     */
    public String getXmlDiscriminatorFor(Formats format){
        //depending on the inner type make the replacement text in the file to process
        String replacement = "CompactData"; 
        switch (format) {
            case COMPACT_SDMX:
                replacement = "CompactData";
                break;
            case UTILITY_SDMX:
                replacement = "UtilityData";
                break;
            case GENERIC_SDMX:
                replacement = "GenericData";
                break;
            default:
                logger.warn("could not detect the xml discriminator for {} defaulting to CompactData", format);
                break;
        }
        return replacement; 
    }
    
    /**
     * Method to process the input file in case it is of type MessageGroup
     * 
     * SDMXCONV-206 stage 1 implementation - 
     * if input format is MESSAGE_GROUP then it will be checked the type contained inside the message group
     * and the input file will be modified accordingly 
     * 
     * @param inputFileName - the path to file and file name
     * @return the name of the new modified file to be used
     * @throws IOException 
     */
    public MsgGroupReplacement processMessageGroupFile(String inputFileName) throws IOException {
        File messageGroupInputFile = new File(inputFileName); 
        return processMessageGroupFile(messageGroupInputFile);
    }
    
    /**
     * 
     * @param messageGroupInputFile
     * @return
     * @throws IOException
     */
    public MsgGroupReplacement processMessageGroupFile(File messageGroupInputFile) throws IOException {
        File tempFile = createTemporaryReplacementFile(messageGroupInputFile.getAbsolutePath()); 
        Formats realFormat = processMessageGroupFile(messageGroupInputFile, tempFile); 
        return new MsgGroupReplacement(realFormat, tempFile);
    }
    
    /**
     * replaces the message group tag in the input file with the real format 
     * and stores the replaced content into the destination file 
     *  
     * The real format is detected by looking into the file 
     * 
     * @param msgGroupInputFile     the file containing the message group 
     * @param destFile       the file where the replaced content will be stored
     * 
     * @return  the real format of the file
     * @throws IOException
     */
    public Formats processMessageGroupFile(File msgGroupInputFile, File destFile) throws IOException {
        Formats realFormat = detectFormatForMessageGroupFile(msgGroupInputFile); 
        String replacement = getXmlDiscriminatorFor(realFormat); 
        replaceMessageGroupTag(msgGroupInputFile, replacement, destFile);
        return realFormat;
    }
    
    /**
     * replaces the MessageGroup tag with the given provided replacement tag
     * 
     * @param messageGroupFile  the message group file
     * @param replacementTag    the tag which replaces the MessageGroup tag
     * @param destination       the destination file 
     * @throws IOException      
     */
    private void replaceMessageGroupTag(Reader messageGroupInputReader, 
                                        String replacementTag, 
                                        File destination) throws IOException {
        BufferedReader reader = null;
        PrintWriter writer = null; 
        try{
            reader = new BufferedReader(messageGroupInputReader);  //new FileReader(messageGroupFile));  
            writer = new PrintWriter(destination);  
            String line = null;  
            String matchRegex = "MessageGroup";  
            while ((line = reader.readLine()) != null){  
                line = line.replaceAll(matchRegex, replacementTag);  
                writer.println(line);  
            }
        }finally{
            IOUtils.closeQuietly(writer);
            IOUtils.closeQuietly(reader);
        }        
    }
    
    /**
     * 
     * @param messageGroupFile
     * @param replacementTag
     * @param destination
     * @throws IOException
     */
    private void replaceMessageGroupTag(File messageGroupFile, 
                                        String replacementTag, 
                                        File destination) throws IOException {
        Reader messageGroupInputReader = new FileReader(messageGroupFile); //error - system dependent charset (average developers to blame)
        replaceMessageGroupTag(messageGroupInputReader, replacementTag, destination); 
    }
    
    /**
     * 
     * @param messageGroupByteArray
     * @param replacementTag
     * @param destination
     * @throws IOException
     */
    private void replaceMessageGroupTag(byte[] messageGroupByteArray, 
                                        String replacementTag, 
                                        File destination) throws IOException {
        Reader messageGroupInputReader = new InputStreamReader(new ByteArrayInputStream(messageGroupByteArray)); //error - charset not specified (average developers to blame)
        replaceMessageGroupTag(messageGroupInputReader, replacementTag, destination); 
    }
    
    /**
     * creates a temporary file in the same folder as the inputFile provided 
     * The temporary file will be used as auxiliary storage for the new file containing the 
     * replacement xml tag for the MessageGroup
     * 
     * @param inputFileName
     * @return
     * @throws IOException
     */
    public File createTemporaryReplacementFile(String inputFileName) throws IOException{
        //make a temporary modified file for processing, this temporary file will be used in the conversion
        String fileSeparator = System.getProperty("file.separator");
        //String inputPath = inputFileName.substring(0, inputFileName.lastIndexOf("\\"));
        String inputPath = inputFileName.substring(0, inputFileName.lastIndexOf(fileSeparator));
        
        File inputModifiedFile = File.createTempFile("sdmx_message_temp", ".xml", new File(inputPath));
        inputModifiedFile.deleteOnExit();
        
        return inputModifiedFile; 
    }
    
    /**
     * Method to process the input file in case it is of type MessageGroup
     * 
     * SDMXCONV-206 stage 1 implementation - 
     * if input format is MESSAGE_GROUP then it will be checked the type contained inside the message group
     * and the input file will be modified accordingly 
     * 
     * @param inputFileName - the path to file and file name
     * @return the new modified file to be used and the format type 
     * @throws IOException 
     */
    public Object[] processMessageGroupFileforWebService(byte[] input) throws IOException {
        Formats realFormat = detectFormatForMessageGroupByteArray(input); 
        String replacement = getXmlDiscriminatorFor(realFormat); 
        
        //make a temporary modified file for processing, this temporary file will be used in the conversion
        File inputModifiedFile = File.createTempFile("sdmx_message_temp", ".xml", null);
        inputModifiedFile.deleteOnExit();
        
        replaceMessageGroupTag(input, replacement, inputModifiedFile);  
        
        InputStream streamFromModifiedFile = new FileInputStream(inputModifiedFile);
        Object [] inputData = {streamFromModifiedFile, realFormat.name()};
        
        return inputData;
    }
    
}
