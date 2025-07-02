package com.intrasoft.sdmx.converter.gui;


import static com.intrasoft.sdmx.converter.gui.ConverterWizardSteps.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * a structure to manage the wizard steps.
 * This is not a generic structure but a converter-specific one.
 * The main idea is to keep in memory (as a list) the most steps possible as in the table below:
 *
 * <table>
 *     <tr>
 *      <th>Position 0</th>
 *      <th>Position 1</th>
 *      <th>Position 2</th>
 *      <th>Position 3</th>
 *      <th>Position 4</th>
 *      <th>Position 5</th>
 *      <th>Position 6</th>
 *      <th>Position 7</th>
 *      <th>Position 8</th>
 *      <th>Position 9</th>
 *     </tr>
 *     <tr>
 *      <td>INPUT</td>
 *      <td>STRUCTURE</td>
 *      <td>INPUT DETAILS</td>
 *      <td>INPUT HEADER</td>
 *      <td>INPUT MAPPING</td>
 *      <td>INPUT TRANSCODING</td>
 *      <td>OUTPUT DETAILS</td>
 *      <td>OUTPUT MAPPING</td>
 *      <td>OUTPUT TRANSCODING</td>
 *      <td>RESULT</td>
 *     </tr>
 * </table>
 *
 * When a step is null, it won't be shown to the screen (see #getVisibleSteps() method).
 * When a new step is added to the position 2 or 7, it invalidates the children of that position (if any present).
 * Example:
 *      if the list of steps contains CSV_INPUT_DETAILS and CSV_INPUT_TRANSCODING
 * for more details please see the unit tests
 *
 */
public class WizardStepsList implements Serializable{
	
	public final static Integer INPUT_STEP_POSITION =               Integer.valueOf(0);
	public final static Integer STRUCTURE_SELECTION_STEP_POSITION = Integer.valueOf(1);
	public final static Integer INPUT_DETAILS_STEPS_POSITION =      Integer.valueOf(2);
	public final static Integer INPUT_HEADER_STEP_POSITION =        Integer.valueOf(3);
	public final static Integer INPUT_MAPPING_STEP_POSITION =       Integer.valueOf(4);
	public final static Integer INPUT_TRANSCODING_STEP_POSITION =   Integer.valueOf(5);
	public final static Integer OUTPUT_DETAILS_POSITION =           Integer.valueOf(6);
	public final static Integer OUTPUT_MAPPING_STEP_POSITION =      Integer.valueOf(7);
	public final static Integer OUTPUT_TRANSCODING_STEP_POSITION =  Integer.valueOf(8);
	public final static Integer RESULT_STEP_POSITION =              Integer.valueOf(9);

    /**
     * the list of steps
     */
    private List<ConverterWizardSteps> steps;

    /**
     * builds the initial list of steps
     */
    public WizardStepsList(){
    	buildDefaultStepList();
    }


    /**
     * adds the new step only if it's not the same as the previous.
     * If it's not the same, for positions 3 and 6 it invalidates the dependent positions
     *
     * @param newStep   the new step
     */
    public void add(ConverterWizardSteps newStep){
        ConverterWizardSteps existingStep = steps.get(newStep.getPosition());
        if(!newStep.equals(existingStep)){
            if(newStep.getPosition() == 2){
                steps.set(3, null);
                steps.set(4, null);
                steps.set(5, null);
            }else{
                if(newStep.getPosition() == 6){
                    steps.set(7, null);
                    steps.set(8, null);
                }
            }
            steps.set(newStep.getPosition(), newStep);
        }
    }
    
    /**
     * resets the all information regarding the provided step by taking into account the dependencies 
     * (i.e. when resetting data for step 2 also the dependent steps 3,4,5 will also be deleted)
     * 
     * @param step the step to be reset
     */
    public void resetStepData(ConverterWizardSteps step) {
    	if(step.getPosition() == 0 || step.getPosition() == 1 || step.getPosition() == 9) {
    		throw new IllegalArgumentException("The INPUT, STRUCTURE and RESULT steps should not be reset from the WizardStepsList");
    	} else if (step.getPosition() == 2) {
    		steps.set(2, null);
    		steps.set(3, null);
            steps.set(4, null);
            steps.set(5, null);
    	} else if (step.getPosition() == 6) {
    		steps.set(6, null);
    		steps.set(7, null);
            steps.set(8, null);
    	} else {
    		steps.set(step.getPosition(), null);
    	}
    }
    //SDMXCONV-968
    /**
     * hides the header panel
     * useful when changing output file from eg:sdmx-ml to csv coming from previous conversion to sdmx-ml format
     */
    public void hideExcelHeader(){
        steps.set(3, null);
    }

    /**
     * returns the list of visible steps.
     * Useful to display the left hand side menu items
     *
     * @return
     */
    public List<ConverterWizardSteps> getVisibleSteps(){
        // TODO: 3/27/16 optimize here
        List<ConverterWizardSteps> result = new ArrayList<ConverterWizardSteps>();
        for (ConverterWizardSteps step:steps) {
            if(step != null){
                result.add(step);
            }
        }
        return result;
    }

    /**
     * returns the percent completion of the current flow based on the existing steps and the current step
     *
     * @param step  current step
     * @return
     */
    public int getPercentComplete(ConverterWizardSteps step){
        int result = 0;
        List<ConverterWizardSteps> existingSteps = getVisibleSteps();
        int index = existingSteps.indexOf(step);
        if(index >= 0){
            //result = ((index+1) * 100) / existingSteps.size();
            result = index * 100 /existingSteps.size();
        }
        return result;
    }

    /**
     * builds the list of steps as needed at the beginning of the flow
     */
    private void buildDefaultStepList(){    	 
    	 steps = Arrays.asList(
    	            INPUT,                  //position 0 - input
    	            STRUCTURE_SELECTION,    //position 1 - structure selection
    	            null,                   //position 2 - input details
    	            null,                   //position 3 - input header
    	            null,                   //position 4 - input mapping
    	            null,                   //position 5 - input transcoding
    	            null,    				//position 6 - output details
    	            null,                   //position 7 - output mapping
    	            null,                   //position 8 - output transcoding
    	            RESULT                  //position 9 - result
    	    );	
    }

    /**
     * resets the list of steps to its initial status
     */
    public void resetAll(){    	 
    	 buildDefaultStepList();   	 
    };
}
