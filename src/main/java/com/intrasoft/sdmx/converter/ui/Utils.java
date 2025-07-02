/**
 *
 * Copyright 2015 EUROSTAT
 *
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * 	https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.ui;

import com.intrasoft.sdmx.converter.io.data.Formats;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Logger;

/**
 * 
 * This is a utility class that filters a file chooser according to a specific file extension
 * and String utility methods 
 * 
 */
public class Utils {
	
	public final static String jpeg = "jpeg";
	public final static String jpg = "jpg";
	public final static String gif = "gif";
	public final static String tiff = "tiff";
	public final static String tif = "tif";
	public final static String png = "png";
	public final static String prop = "prop";

	public static Logger logger;
	private static FileHandler fh;
	private static Formatter frm;
	/**
	 * Get the extension of a file.
	 * @File f
	 * @return String result
	 */
	public static String getExtension(File f) {
		String ext = null;
		String s = f.getName();
		int i = s.lastIndexOf('.');

		if (i > 0 && i < s.length() - 1) {
			ext = s.substring(i + 1).toLowerCase();
		}
		return ext;
	}

	/**
	 * Returns an ImageIcon, or null if the path was invalid.
	 * @String path
	 * @return ImageIcon result
	 * */
	protected static ImageIcon createImageIcon(String path) {
		java.net.URL imgURL = Utils.class.getResource(path);
		if (imgURL != null) {
			return new ImageIcon(imgURL);
		} else {
			System.err.println("Couldn't find file: " + path);
			return null;
		}
	}
/**
 * Method that creates a pop up Window that warns the user for overwriting files
 * @param parent, JPanel object
 * @param msg
 * @return choice
 */
	public static int createWarningMsgOverwrittingFiles(Container parent , String msg){
		Object[] options = {"Yes, please",
		"No"};
		int choice = JOptionPane.showOptionDialog(parent,
				"The " + msg + " file already exists and will be overwritten"  + "\n" + "with this new " + msg + " file. Do you want to proceed?",
				"Warning Message",
				JOptionPane.YES_NO_OPTION,
				JOptionPane.WARNING_MESSAGE,
				null,
				options,
				null);
		return choice;
	}

	protected static int createWarning2MsgOverwrittingFiles(Container parent , String msg){
		Object[] options = {"Use the default mapping",
		"Propose a mapping"};
		int choice = JOptionPane.showOptionDialog(parent,
			    "Would you like to present the default mapping or propose one based on the header row?",
			    "Mapping to be used",
			    JOptionPane.YES_NO_OPTION,
			    JOptionPane.QUESTION_MESSAGE,
			    null,     
			    options,  
			    options[0]);
		return choice;
	}

	// /**
	// * This method creates an AxisFault exception based on the exception passed and thrown by the Web service. The
	// * AxisFault exception normally should be caught by the entry point main method of the WS transforming it into a
	// * SOAP Fault exception.
	// * @param Exception
	// * @return an AxisFault
	// */
	// public static AxisFault createAxisFaultMessage(BaseException exception) {
	// AxisFault fault = new AxisFault();
	//
	// String generic_message = IoUtils.getGenericErrorMessage(exception.getCode());
	// String result = "Error " + exception.getCode() + ":" + generic_message + "\n" + exception.getMessage();
	//
	// if (result != null) {
	// fault.setFaultString(result);
	// }
	// return fault;
	// }
	
	/**
	 * Returns the extension according to the specified output format
	 * @param outputFormat
	 * @return
	 */
	protected static String getFormatType (Formats outputFormat){
		if (outputFormat.toString().startsWith("GESMES")) {

			return ".ges";

		} else if (outputFormat.toString().startsWith("CSV") || outputFormat.toString().startsWith("DSXML")) {

			return ".csv";

		} else if (outputFormat.toString().startsWith("FLR")) {

			return ".txt";

		} else {

			return ".xml";

		}
	}
		
	/**
	 * Method for determining if a String is empty (null, or empty String)
	 * 
	 * @param item - the String to be checked
	 * @return true if the String is null or Empty String
	 */
	protected static boolean isEmpty(String item) {
		if (item == null || "".equals(item)) {
			return true;
		} else {
			return false;
		}
	}

	public static List<String> convertToList(Set<String> set) {
		List<String> list = new LinkedList<>();
		for(String element: set) {
			list.add(element);
		}
		return list;
	}
	public static Set<String> convertToSet(List<String> list) {
		Set<String> set = new LinkedHashSet<>();
		for(String element: list) {
			set.add(element);
		}
		return set;
	}
}