package com.intrasoft.commons.ui.services;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.poi.util.IOUtils;
import org.springframework.stereotype.Service;

import com.intrasoft.sdmx.converter.gui.ConverterWizardSections;
import com.intrasoft.sdmx.converter.io.data.IoUtils;

@Service
public class HelpService {
    
    public static final String HELP_FILE_CLASSPATH = "help.properties"; 
    
    private Properties helpCache = new Properties(); 
    
    @PostConstruct
    public void loadProperties() throws IOException{
        InputStream helpPropertiesIS = null; 
        try{
            helpPropertiesIS = IoUtils.getInputStreamFromClassPath(HELP_FILE_CLASSPATH); 
            helpCache.load(helpPropertiesIS);
        }finally{
            IOUtils.closeQuietly(helpPropertiesIS);
        }
    }
    
    public String getHelp(ConverterWizardSections section){
        String valueInCache = helpCache.getProperty(section.name()); 
        return valueInCache != null ? valueInCache : "No help available"; 
    }
}
