/*******************************************************************************
 * Copyright (c) 2013 Metadata Technology Ltd.
 *  
 * All rights reserved. This program and the accompanying materials are made 
 * available under the terms of the GNU Lesser General Public License v 3.0 
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 * 
 * This file is part of the SDMX Component Library.
 * 
 * The SDMX Component Library is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 * 
 * The SDMX Component Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser 
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License 
 * along with The SDMX Component Library If not, see 
 * http://www.gnu.org/licenses/lgpl.
 * 
 * Contributors:
 * Metadata Technology - initial API and implementation
 ******************************************************************************/
package com.intrasoft.sdmx.converter.sdmxsource;

import java.io.File;
import java.util.Locale;

import org.sdmxsource.sdmx.api.exception.SdmxException;
import org.sdmxsource.sdmx.api.util.MessageResolver;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.stereotype.Service;

import com.intrasoft.commons.ui.services.ConfigService;

@Service
public class MessageDecoderConverter implements MessageResolver {
	
	public static final String EXPLANATION_FILE_NAME = "explanation"; 
	public static final String EXCEPTION_FILE_NAME = "exception";
	
	private static ReloadableResourceBundleMessageSource messageSource;
	private static Locale loc = new Locale("en");
	
	public MessageDecoderConverter(ConfigService configService) {
		messageSource = new ReloadableResourceBundleMessageSource();
		if(new File(configService.getErrorMessagePath() + EXCEPTION_FILE_NAME + "_" + loc.getLanguage() + ".properties").exists())
			messageSource.addBasenames("file:" + configService.getErrorMessagePath() + EXCEPTION_FILE_NAME);				// CUSTOM Location
		messageSource.addBasenames("default_exception_explanation");															// DEFAULT Location
		SdmxException.setMessageResolver(this);
	}

	@Override
	public String resolveMessage(String messageCode, Locale locale, Object... args) {
		try {
			return messageSource.getMessage(messageCode, args, locale);
		} catch(Throwable th) {
			return messageCode;
		}
	}

	public static String decodeMessage(String id, Object... args) {
		if(messageSource == null) {
			return id;
		}
		return messageSource.getMessage(id, args, loc);
	}
	
	public static String decodeMessageDefaultLocale(String id, Object... args) {
		return messageSource.getMessage(id, args, loc);
	}
	
	public static String decodeMessageGivenLocale(String id, String lang, Object... args) {
		
		if(messageSource == null) {
			return id;
		}
		return messageSource.getMessage(id, args, loc);
	}
	
	public static ReloadableResourceBundleMessageSource getMessageSource() {
		return messageSource;
	}

	public void clear() {
		if(ObjectUtil.validObject(messageSource))
			messageSource.clearCache();
	}
}
