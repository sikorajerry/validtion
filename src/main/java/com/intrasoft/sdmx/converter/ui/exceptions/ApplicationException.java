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
package com.intrasoft.sdmx.converter.ui.exceptions;


import org.sdmxsource.sdmx.validation.exceptions.BaseException;

/**
 * An exception for problems when IO, SAX prasing exceptions occur.
 * 
 */

public class ApplicationException extends BaseException {

	/**
	 * Creates a new instance of ApplicationException with the provided message.
	 * @param code The code of the error
	 * @param details A message explaining the circumstances that created the exception.
	 */
	public ApplicationException(int code, String details) {
		super(code, details);
	}

	/**
	 * Constructs a new exception with the specified detail message and cause.
	 * @param code The code of the error
	 * @param details A message about the exception.
	 * @param cause The cause of the Exception.
	 */
	public ApplicationException(int code, String details, Throwable cause) {
		super(code, details, cause);
	}

}
