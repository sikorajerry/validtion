package com.intrasoft.sdmx.converter.io.data.excel;

import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.sdmxsource.sdmx.dataparser.engine.writer.utils.OutputConfig;

import java.io.InputStream;

/**
 * Created by dbalan on 6/8/2017.
 */
public class ExcelOutputConfig implements OutputConfig {

    private InputStream excelOutputTemplate;

    private ExcelConfiguration excelConfig;

    public ExcelOutputConfig(InputStream outputTemplate, ExcelConfiguration excelConfig){
        this.excelOutputTemplate = outputTemplate;
        this.excelConfig = excelConfig;
    }

    public InputStream getExcelOutputTemplate(){
        return excelOutputTemplate;
    }

    public ExcelConfiguration getExcelConfig(){
        return excelConfig;
    }

}
