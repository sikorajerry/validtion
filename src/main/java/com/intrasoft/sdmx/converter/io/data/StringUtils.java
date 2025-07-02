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
package com.intrasoft.sdmx.converter.io.data;

import org.estat.struval.ValidationError;
import org.sdmxsource.util.ObjectUtil;

import java.util.List;

/**
 * Utility class for String processing.
 * 
 * @author Mihaela Munteanu
 *
 */
public class StringUtils {

	/**
	 * <h2>Clean the urn of an artifact</h2>
	 * <p>Trim the path to structure before the artifact.</p>
	 * <p>e.g. the artifact's urn is "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESTAT:CL_AGE(3.0)".
	 * The referenced code list is only "ESTAT:CL_AGE(3.0)".</p>
	 *
	 * @param artifactUrn String
	 * @return String Trimmed urn
	 * @see <a href="https://citnet.tech.ec.europa.eu/CITnet/jira/browse/SDMXCONV-1114">SDMXCONV-1114</a>
	 */
	public static String artifactsUrnTrim(String artifactUrn) {
		String cleanedUrn = null;
		if (ObjectUtil.validString(artifactUrn)) {
			//regex to replace strings that start with urn:sdmx:org.sdmx.infomodel. until the first = character is found
			cleanedUrn = artifactUrn.replaceAll("urn:sdmx:org\\.sdmx\\.infomodel\\..*?=", "");
		}
		return cleanedUrn;
	}

	/**
	 * SDMXCONV-1114
	 * Method that takes validationResult as input
	 * loops over it and check each error message whether it contains Codelist or not
	 * If yes it uses StringUtils' artifactsUrnTrim method to trim it and store it back to each error object
	 * @param errors
	 */
	public static List<ValidationError> trimCodelist(List<ValidationError> errors){
		if(ObjectUtil.validCollection(errors)) {
			for(ValidationError error: errors){
				if(ObjectUtil.validObject(error) && ObjectUtil.validString(error.getMessage()) && error.getMessage().contains("urn")){
					String trimedCodelist = StringUtils.artifactsUrnTrim(error.getMessage());
					if(ObjectUtil.validString(trimedCodelist))
						error.setMessage(trimedCodelist);
				}
			}
		}
		return errors;
	}

	public void close(){}
}
