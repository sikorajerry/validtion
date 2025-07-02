/**
 * Copyright (c) 2015 European Commission.
 *
 * Licensed under the EUPL, Version 1.1 or – as soon they
 * will be approved by the European Commission - subsequent
 * versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the
 * Licence.
 * You may obtain a copy of the Licence at:
 *
 * https://joinup.ec.europa.eu/software/page/eupl5
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the Licence is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the Licence for the specific language governing
 * permissions and limitations under the Licence.
 */
package com.intrasoft.sdmx.converter.io.data.excel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.estat.sdmxsource.util.excel.ExcelInputConfig;
import java.util.LinkedHashMap;

/**
 * Created by dbalan on 6/8/2017.
 */
public class ExcelInputConfigImpl extends ExcelInputConfig {

	@JsonIgnore
	private LinkedHashMap<String, ExcelSheetUtils> codesSheetUtils;

	public LinkedHashMap<String, ExcelSheetUtils> getCodesSheetUtils() {
		return codesSheetUtils;
	}

	public void setCodesSheetUtils(LinkedHashMap<String, ExcelSheetUtils> codesSheetUtils) {
		this.codesSheetUtils = codesSheetUtils;
	}
}
