package com.intrasoft.sdmx.converter.io.data.excel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.estat.sdmxsource.util.excel.ExcelConfigParser;
import org.estat.sdmxsource.util.excel.ExcelConfiguration;
import org.estat.sdmxsource.util.excel.ExcelDimAttrConfig;

import java.util.List;

/**
 * Created by dbalan on 6/22/2017.
 */
public class ExcelOutputConfigParser extends ExcelConfigParser {

    private static Logger logger = LogManager.getLogger(ExcelOutputConfigParser.class);

    /**
     * @param excelConfig
     */
    public ExcelOutputConfigParser(ExcelConfiguration excelConfig) {
        super(excelConfig, null, null);
    }

    public ExcelOutputConfigParser(ExcelConfiguration excelConfig, List<String> concepts) {
        super(excelConfig, null, concepts);
    }

    @Override
    public void validateConfig(ExcelDimAttrConfig dimensionConfig){
        switch(dimensionConfig.getPositionType()){
            case MIXED:
                logger.error("MIXED position types unsupported when writing excel");
                break;
            case FIX:
            case SKIP:
                logger.warn("FIX position types do not affect the final excel output");
                break;
            default:
                logger.info("excel dimension {} configured", dimensionConfig);
        }
    }
}
