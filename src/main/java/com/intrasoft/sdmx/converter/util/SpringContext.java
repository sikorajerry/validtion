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
package com.intrasoft.sdmx.converter.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * The spring application context singleton.
 * Eurostat (c) 2014 EUPL
 * User: tasos
 * Date: 11/1/2014
 */
public enum SpringContext
{
    /**
     * The main instance
     */
    INSTANCE("/spring/converter-spring-beans.xml");
    /**
     * The spring application context
     */
    private final ApplicationContext context;

    /**
     *
     * @param path the class path;
     */
    private SpringContext(String path)
    {
        this.context = new ClassPathXmlApplicationContext(path);
    }

    /**
     * Returns the bean of the specified class.
     * @param clazz The class object of the bean.
     * @param <T> The type of the bean.
     * @return The bean mapped to the specified class.
     */
    public <T> T getBean(Class<T> clazz)
    {
        return this.context.getBean(clazz);
    }
    
    public Object getBean(String beanName) {
    	return this.context.getBean(beanName);
    }
}
