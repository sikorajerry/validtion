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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sdmxsource.util.ObjectUtil;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class is responsible for parsing JSON endpoints.
 * It uses the Jackson library to map JSON data to Java objects.
 */
@Service
public class JsonEndpointsParserService {

    private JsonEndpoints endpoints;

    /**
     * Parses the JSON endpoints from a file named "endpoints.json" located in the classpath.
     *
     * @return A JsonEndpoints object that represents the parsed JSON data.
     * @throws RuntimeException if an IOException occurs during the parsing process.
     */
    @PostConstruct
    private void parse() {
        // Create a new ObjectMapper instance for JSON parsing
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream jsonStream = JsonEndpointsParserService.class.getClassLoader().getResourceAsStream("endpoints.json")) {
            // Map the JSON data from the input stream to a JsonEndpoints object
            this.endpoints = mapper.readValue(jsonStream, JsonEndpoints.class);
            if(ObjectUtil.validObject(this.endpoints)) {
                this.endpoints.getEndpoints().add( new JsonEndpoint("Other"));
            }
        } catch (IOException e) {
            // If an IOException occurs, wrap it in a RuntimeException and throw it
            throw new RuntimeException(e);
        }
    }

    public JsonEndpoints getEndpoints() {
        return endpoints;
    }
}
