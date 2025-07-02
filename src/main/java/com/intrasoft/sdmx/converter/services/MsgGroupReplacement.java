package com.intrasoft.sdmx.converter.services;

import java.io.File;

import com.intrasoft.sdmx.converter.io.data.Formats;

/**
 * replacement for a message group file
 * 
 * @author dragos balan
 *
 */
public final class MsgGroupReplacement {
    
    /**
     * the real format of the message group file
     */
    private final Formats realFormat; 
    
    /**
     * the file with the MessageGroup replaced; 
     */
    private final File replacementFile; 
    
    /**
     * 
     * @param realFormat
     * @param replacementFile
     */
    public MsgGroupReplacement(Formats realFormat, File replacementFile){
        this.realFormat = realFormat; 
        this.replacementFile = replacementFile; 
    }
    
    public Formats getRealFormat(){
        return realFormat; 
    }
    
    public File getReplacementFile(){
        return replacementFile; 
    }
    
    public String toString(){
        return "MsgGroupReplacement[realFormat="+ (realFormat != null ? realFormat : "null")
                +", replacementFile="+(replacementFile != null ? replacementFile.getAbsolutePath() : "null")+"]"; 
    }
}
