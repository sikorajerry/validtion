package com.intrasoft.sdmx.converter.gui;

public enum ConverterWizardSteps {

    //position 0
    INPUT("Input", 0),

    //position 1
    STRUCTURE_SELECTION("Structure selection", 1),

    //position 2 (any change at this position triggers a delete in positions 3, 4, 5)
    CSV_INPUT_DETAILS("Csv/Flr input details", 2),
    EXCEL_INPUT_DETAILS("Excel input details", 2),
   

    //position 3
    CSV_INPUT_HEADER("SDMX header", 3),
    EXCEL_HEADER("Excel header", 3),

    //position 4
    CSV_INPUT_COLUMN_MAPPING("Csv input column mapping", 4),
    FLR_INPUT_COLUMN_MAPPING("Flr input column mapping", 4),
    EXCEL_PARAM_SHEET_MAPPING("Parameter sheet mapping", 4),
    CSV_MULTI_INPUT_COLUMN_MAPPING("Multilevel Csv input column mapping", 4),

    //position 5
    CSV_INPUT_TRANSCODING("Csv input transcoding", 5),

    //position 6
    CSV_OUTPUT_DETAILS("Csv output details", 6),
    FLR_OUTPUT_DETAILS("Flr output details", 6),
    GESMES_OUTPUT_DETAILS("Gesmes output details", 6),
    SDMX_OUTPUT_DETAILS("Sdmx output details", 6),
    EXCEL_OUTPUT_CONFIG("Excel output config",6),

    //position 7
    CSV_OUTPUT_COLUMN_MAPPING("Csv output column mapping", 7),
    FLR_OUTPUT_COLUMN_MAPPING("Flr output column mapping", 7),

    //position 8
    CSV_OUTPUT_TRANSCODING("Csv output transcoding", 8),

    //positiono 9
    RESULT("Result", 9);

    private String description;
    private int position;


    ConverterWizardSteps(String stepDescription, int position){
        this.description = stepDescription;
        this.position = position;
    }

    public String getDescription(){
        return description;
    }

    public int getPosition(){return position;}

}
