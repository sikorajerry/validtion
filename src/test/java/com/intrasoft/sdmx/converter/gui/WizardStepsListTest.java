package com.intrasoft.sdmx.converter.gui;

import org.junit.Test;

import java.util.List;

import static com.intrasoft.sdmx.converter.gui.ConverterWizardSteps.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class WizardStepsListTest {

    @Test
    public void testDefaultValues() throws Exception {
        WizardStepsList defaultList = new WizardStepsList();
        List<ConverterWizardSteps> result = defaultList.getVisibleSteps();

        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals(INPUT, result.get(0));
        assertEquals(STRUCTURE_SELECTION,result.get(1));
        assertEquals(RESULT, result.get(2));
    }

    @Test
    public void testAddInputDetails() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList();
        listUnderTest.add(CSV_INPUT_DETAILS);
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps();

        assertNotNull(result);
        assertEquals(4, result.size());
        assertEquals(result.get(0), INPUT);
        assertEquals(result.get(1), STRUCTURE_SELECTION);
        assertEquals(result.get(2), CSV_INPUT_DETAILS);
        assertEquals(result.get(3), RESULT);
    }

    @Test
    public void testAddTwoCsvInputOptions() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList();
        listUnderTest.add(CSV_INPUT_DETAILS);
        listUnderTest.add(CSV_INPUT_COLUMN_MAPPING);
        listUnderTest.add(CSV_INPUT_TRANSCODING);
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps();

        assertNotNull(result);
        assertEquals(6, result.size());
        assertEquals(result.get(0), INPUT);
        assertEquals(result.get(1), STRUCTURE_SELECTION);
        assertEquals(result.get(2), CSV_INPUT_DETAILS);
        assertEquals(result.get(3), CSV_INPUT_COLUMN_MAPPING);
        assertEquals(result.get(4), CSV_INPUT_TRANSCODING);
        assertEquals(result.get(5), RESULT);
    }

    @Test
    public void testAddExcelInputParamsAfterCsvInputOptions() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList();
        listUnderTest.add(CSV_INPUT_DETAILS);
        listUnderTest.add(CSV_INPUT_COLUMN_MAPPING);
        listUnderTest.add(CSV_INPUT_TRANSCODING);

        listUnderTest.add(EXCEL_INPUT_DETAILS); //this should invalidate the column mapping and transcoding
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps();

        assertNotNull(result);
        assertEquals(4, result.size());
        assertEquals(result.get(0), INPUT);
        assertEquals(result.get(1), STRUCTURE_SELECTION);
        assertEquals(result.get(2), EXCEL_INPUT_DETAILS);
        assertEquals(result.get(3), RESULT);
    }

    @Test
    public void testAddCsvOutputOptions() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList();
        listUnderTest.add(CSV_OUTPUT_DETAILS);//this should invalidate the default sdmx output
        listUnderTest.add(CSV_OUTPUT_COLUMN_MAPPING);
        listUnderTest.add(CSV_OUTPUT_TRANSCODING);

        listUnderTest.add(EXCEL_INPUT_DETAILS);
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps();

        assertNotNull(result);
        assertEquals(7, result.size());
        assertEquals(result.get(0), INPUT);
        assertEquals(result.get(1), STRUCTURE_SELECTION);
        assertEquals(result.get(2), EXCEL_INPUT_DETAILS);
        assertEquals(result.get(3), CSV_OUTPUT_DETAILS);
        assertEquals(result.get(4), CSV_OUTPUT_COLUMN_MAPPING);
        assertEquals(result.get(5), CSV_OUTPUT_TRANSCODING);
        assertEquals(result.get(6), RESULT);
    }

    @Test
    public void testTheBiggestNumberOfWizardSteps() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList();
        listUnderTest.add(CSV_OUTPUT_DETAILS);//this should invalidate the default sdmx output
        listUnderTest.add(CSV_OUTPUT_COLUMN_MAPPING);
        listUnderTest.add(CSV_OUTPUT_TRANSCODING);

        listUnderTest.add(EXCEL_INPUT_DETAILS);
        listUnderTest.add(EXCEL_PARAM_SHEET_MAPPING);
        listUnderTest.add(EXCEL_HEADER);
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps();

        assertNotNull(result);
        assertEquals(9, result.size());
        assertEquals(result.get(0), INPUT);
        assertEquals(result.get(1), STRUCTURE_SELECTION);
        assertEquals(result.get(2), EXCEL_INPUT_DETAILS);
        assertEquals(result.get(3), EXCEL_HEADER);
        assertEquals(result.get(4), EXCEL_PARAM_SHEET_MAPPING);
        //assertEquals(result.get(5), OUTPUT);
        assertEquals(result.get(5), CSV_OUTPUT_DETAILS);
        assertEquals(result.get(6), CSV_OUTPUT_COLUMN_MAPPING);
        assertEquals(result.get(7), CSV_OUTPUT_TRANSCODING);
        assertEquals(result.get(8), RESULT);
    }
    
    @Test
    public void testResetAll() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList();
        listUnderTest.add(CSV_INPUT_DETAILS);
        listUnderTest.add(CSV_INPUT_COLUMN_MAPPING);
        listUnderTest.add(CSV_INPUT_TRANSCODING);
        listUnderTest.resetAll();
        
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps();

        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals(result.get(0), INPUT);
        assertEquals(result.get(1), STRUCTURE_SELECTION);
        assertEquals(result.get(2), RESULT);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testResetStepAtPositionZero() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList(); 
        listUnderTest.add(CSV_INPUT_DETAILS);
        listUnderTest.add(CSV_INPUT_COLUMN_MAPPING);
        
        listUnderTest.resetStepData(INPUT);
    }
    
    @Test
    public void testResetStepAtPositionTwo() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList(); 
        listUnderTest.add(CSV_INPUT_DETAILS);
        listUnderTest.add(CSV_INPUT_COLUMN_MAPPING);
        
        listUnderTest.resetStepData(CSV_INPUT_DETAILS);
        
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps(); 
        assertNotNull(result);
        assertEquals(3, result.size()); 
        assertEquals(result.get(0), INPUT); 
        assertEquals(result.get(1), STRUCTURE_SELECTION); 
        assertEquals(result.get(2), RESULT); 
    }
    
    @Test
    public void testResetStepAtPositionSix() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList(); 
        listUnderTest.add(EXCEL_INPUT_DETAILS);
        listUnderTest.add(EXCEL_HEADER);
        listUnderTest.add(CSV_OUTPUT_DETAILS);
        listUnderTest.add(CSV_OUTPUT_COLUMN_MAPPING);
        
        listUnderTest.resetStepData(CSV_OUTPUT_DETAILS);
        
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps(); 
        assertNotNull(result);
        assertEquals(5, result.size()); 
        assertEquals(result.get(0), INPUT); 
        assertEquals(result.get(1), STRUCTURE_SELECTION); 
        assertEquals(result.get(2), EXCEL_INPUT_DETAILS);
        assertEquals(result.get(3), EXCEL_HEADER); 
        assertEquals(result.get(4), RESULT);
    }
    
    @Test
    public void testResetStepAtPositionFour() throws Exception {
        WizardStepsList listUnderTest = new WizardStepsList(); 
        listUnderTest.add(EXCEL_INPUT_DETAILS);
        listUnderTest.add(EXCEL_HEADER);
        listUnderTest.add(CSV_OUTPUT_DETAILS);
        listUnderTest.add(CSV_OUTPUT_COLUMN_MAPPING);
        
        listUnderTest.resetStepData(EXCEL_HEADER);
        
        List<ConverterWizardSteps> result = listUnderTest.getVisibleSteps(); 
        assertNotNull(result);
        assertEquals(6, result.size()); 
        assertEquals(result.get(0), INPUT); 
        assertEquals(result.get(1), STRUCTURE_SELECTION); 
        assertEquals(result.get(2), EXCEL_INPUT_DETAILS);
        assertEquals(result.get(3), CSV_OUTPUT_DETAILS);
        assertEquals(result.get(4), CSV_OUTPUT_COLUMN_MAPPING);
        assertEquals(result.get(5), RESULT);
    }
}