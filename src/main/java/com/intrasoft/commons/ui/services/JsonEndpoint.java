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
package com.intrasoft.commons.ui.services;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.intrasoft.sdmx.converter.util.TypeOfVersion;
import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)

/**
 * Endpoint Json Object to read config file endpoints.json
 */
public class JsonEndpoint implements Serializable {
    @JsonProperty("url")
    private String url;
    @JsonProperty("type")
    private TypeOfVersion version;
    @JsonProperty("label")
    private String label;

    public JsonEndpoint(String url) {
        this.url = url;
        this.version = TypeOfVersion.EMPTY;
    }
}
