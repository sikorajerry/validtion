/**
 * Copyright 2015 EUROSTAT
 * <p>
 * Licensed under the EUPL, Version 1.1 or � as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 * <p>
 * https://joinup.ec.europa.eu/software/page/eupl
 * <p>
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.io.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sdmxsource.util.ObjectUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This is a utility class that contains utility static methods for all readers/writers
 */
public class IoUtils {

    private IoUtils() {
        throw new IllegalStateException("Utility class");
    }

    private static final Logger logger = LogManager.getLogger(IoUtils.class);
    private static final HashMap<String, String> specialCharsReplacements;
    private static final Pattern specialCharsPattern;

    static {
        specialCharsReplacements = new HashMap<>();

        // populate the map with all the special chars and their replacements.
        // NOTE: these are not the java string values, but the REGEX values
        specialCharsReplacements.put("&", "&amp;");
        specialCharsReplacements.put("\n", "&#xA;");
        specialCharsReplacements.put("\t", "&#x9;");
        specialCharsReplacements.put("'", "&#039;");
        specialCharsReplacements.put("!", "&#033;");
        specialCharsReplacements.put("=", "&#061;");
        specialCharsReplacements.put(">", "&gt;");
        specialCharsReplacements.put("<", "&lt;");
        specialCharsReplacements.put("?", "&#063;");
        specialCharsReplacements.put("\"", "&quot;");
        specialCharsReplacements.put("/", "&#047;");

        // create a pattern like this: "|/|\?|>|!|&|<|=|'
        StringBuilder patternStringSb = new StringBuilder();
        boolean expressionStarted = false;
        for (String specialChar : specialCharsReplacements.keySet()) {
            if (expressionStarted) {
                patternStringSb.append("|");
            }
            patternStringSb.append(specialChar);
            expressionStarted = true;
        }
        // ? is a metacharacter. It it is as it is, it will change the meaning of the REGEX. It must be replaced with
        // \\?
        // So, the REGEX will understand "\?" (java eats one backslash)
        specialCharsPattern = Pattern.compile(patternStringSb.toString().replace("?", "\\?"));
    }

    /**
     * Method to replace special characters with their codes for xml output files
     *
     * @param string the string where special characters are replaced
     * @return the string without special characters
     */
    public static String handleSpecialCharacters(final String string) {

        final Matcher matcher = specialCharsPattern.matcher(string);
        final StringBuffer sb = new StringBuffer();
        while (matcher.find()) {

            matcher.appendReplacement(sb, specialCharsReplacements.get(matcher.group()));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * returns an input stream from a class path resource
     *
     * @param classPath
     * @return
     */
    public static InputStream getInputStreamFromClassPath(String classPath) {
        return IoUtils.class.getClassLoader().getResourceAsStream(classPath);
    }

    /**
     * This method is used to create a zip file from the provided file.
     * It uses the Java's built-in ZipOutputStream to write the contents of the file to a zip file.
     *
     * @param fileToZip      The file that needs to be zipped.
     * @throws IOException   If an I/O error occurs while reading or writing.
     */
    public static File createZip(File fileToZip) throws IOException {
        String outputFileName = "temp_zip";
        if(ObjectUtil.validObject(fileToZip) && fileToZip.exists()) {
            outputFileName = fileToZip.getAbsolutePath() + ".zip";
        }
        File outputFile = new File(outputFileName);
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             ZipOutputStream zipOut = new ZipOutputStream(fos)) {

            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zipOut.putNextEntry(zipEntry);
            Files.copy(fileToZip.toPath(), zipOut);
        }
        return outputFile;
    }

}