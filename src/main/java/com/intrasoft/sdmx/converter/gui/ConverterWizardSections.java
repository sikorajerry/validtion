package com.intrasoft.sdmx.converter.gui;

import static com.intrasoft.sdmx.converter.gui.ConverterWizardSteps.*;

public enum ConverterWizardSections {
    
    //input screen sections 
    INPUT_OPERATION(INPUT), INPUT_PARAMETERS(INPUT), INPUT_OUTPUT(INPUT), INPUT_LOAD_TEMPLATE(INPUT),
    
    //structure screen sections
    STRUCTURE_TYPE(STRUCTURE_SELECTION), STRUCTURE_FILE(STRUCTURE_SELECTION), STRUCTURE_ID(STRUCTURE_SELECTION), 
    
    //csv input screen sections
    CSV_INPUT_SCREEN_HEADER(CSV_INPUT_DETAILS), CSV_INPUT_SCREEN_PARAMETERS(CSV_INPUT_DETAILS), CSV_INPUT_SCREEN_COLMAPPING(CSV_INPUT_DETAILS), CSV_INPUT_SCREEN_TRANSC(CSV_INPUT_DETAILS),
    
    //excel input screen sections
    EXCEL_INPUT_HEADER(EXCEL_INPUT_DETAILS), EXCEL_PARAMETERS(EXCEL_INPUT_DETAILS), EXCEL_PARAMSHEETS_MAPPPING(EXCEL_INPUT_DETAILS),
    
    //excel output config
    EXCEL_OUTPUT_CONFIG_SCREEN(EXCEL_OUTPUT_CONFIG),
    
    //csv and excel header screen sections, SDMXCONV-1133
    HEADER_ID(EXCEL_HEADER), HEADER_SENDER_RECEIVER(EXCEL_HEADER), HEADER_OTHER(EXCEL_HEADER), HEADER_ID_CONSTRAINTS(EXCEL_HEADER),
    ID_CONSTRAINTS(EXCEL_HEADER), CONTACT_URI_CONSTRAINTS(EXCEL_HEADER), DATE_CONSTRAINTS(EXCEL_HEADER), DATASET_ACTIONS_CONSTRAINTS(EXCEL_HEADER),

    //input column mapping sections
    CSV_IN_COLMAPPING_UNUSED_COLUMNS(CSV_INPUT_COLUMN_MAPPING), 
    CSV_IN_COLMAPPING_MAPPINGS(CSV_INPUT_COLUMN_MAPPING),
    CSV_IN_COLMAPPING_MAPPINGS_ORDER(CSV_INPUT_COLUMN_MAPPING),
    CSV_IN_COLMAPPING_AVAILABLE_COLUMN_ORDER(CSV_INPUT_COLUMN_MAPPING),
    
    //input column mapping sections
    FLR_IN_COLMAPPING_UNUSED_COLUMNS(FLR_INPUT_COLUMN_MAPPING), 
    FLR_IN_COLMAPPING_MAPPINGS(FLR_INPUT_COLUMN_MAPPING),
    FLR_IN_COLMAPPING_MAPPINGS_ORDER(FLR_INPUT_COLUMN_MAPPING),
    FLR_IN_COLMAPPING_AVAILABLE_COLUMN_ORDER(FLR_INPUT_COLUMN_MAPPING),
    
    //excel param sheet mapping screen
    CUSTOM_EXCEL_SHEET_MAPPING(EXCEL_PARAM_SHEET_MAPPING), 
    
    //csv input transcoding screen
    INPUT_EXISTING_TRANSCODINGS(CSV_INPUT_TRANSCODING), INPUT_TRANSCODING_PARAMS(CSV_INPUT_TRANSCODING), 
    
    //csv output screen
    CSV_OUTPUT_SCREEN_PARAMS(CSV_OUTPUT_DETAILS), CSV_OUTPUT_SCREEN_COLMAPPING(CSV_OUTPUT_DETAILS), CSV_OUTPUT_SCREEN_TRANSC(CSV_OUTPUT_DETAILS),

    //flr output screen
    FLR_OUTPUT_SCREEN_PARAMS(FLR_OUTPUT_DETAILS), FLR_OUTPUT_SCREEN_COLMAPPING(FLR_OUTPUT_DETAILS), FLR_OUTPUT_SCREEN_TRANSC(FLR_OUTPUT_DETAILS),

    //gesmes output screen
    GESMES_OUTPUT_PARAMS(GESMES_OUTPUT_DETAILS), 
    
    //sdmx output parameters screen
    SDMX_OUTPUT_DETAILS_PARAMS(SDMX_OUTPUT_DETAILS), 
    SDMX_OUTPUT_REPORTING_PERIOD(SDMX_OUTPUT_DETAILS),
    
    //csv output column mapping screen
    CSV_OUT_COLMAPPING_UNUSED_COLS(CSV_OUTPUT_COLUMN_MAPPING), 
    CSV_OUT_COLMAPPING_MAPPING(CSV_OUTPUT_COLUMN_MAPPING), 
    CSV_OUT_COLMAPPING_COLUMN_ORDER(CSV_OUTPUT_COLUMN_MAPPING), 
    CSV_OUT_COLMAPPING_AVAILABLE_COLUMN_ORDER(CSV_OUTPUT_COLUMN_MAPPING),

    //flr output column mapping sections
    FLR_OUT_COLMAPPING_UNUSED_COLUMNS(FLR_OUTPUT_COLUMN_MAPPING),
    FLR_OUT_COLMAPPING_MAPPINGS(FLR_OUTPUT_COLUMN_MAPPING),
    FLR_OUT_COLMAPPING_MAPPINGS_ORDER(FLR_OUTPUT_COLUMN_MAPPING),
    FLR_OUT_COLMAPPING_AVAILABLE_COLUMN_ORDER(FLR_OUTPUT_COLUMN_MAPPING),
    
    //csv output transcoding screen 
    OUT_EXISTING_TRANSCODINGS(CSV_OUTPUT_TRANSCODING), OUTPUT_TRANSCODING_PARAMS(CSV_OUTPUT_TRANSCODING),
    
    //result screen
    RESULT_SCREEN_CONVERSION(RESULT), RESULT_SCREEN_VALIDATION(RESULT), RESULT_SCREEN_SAVE_TEMPLATE(RESULT);
    
    
    private ConverterWizardSteps parent; 
    
    ConverterWizardSections(ConverterWizardSteps parent){
        this.parent = parent; 
    }
    
    ConverterWizardSteps getParent(){
        return parent; 
    }
}
