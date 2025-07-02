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
package com.intrasoft.sdmx.converter.cache;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.util.LinkedHashMap;

/**
 * Singleton class for managing Cache. 
 * ehcache is used with the JSR107(jcache) api 
 * 
 * Currently the cache has a basic configuration configured programmatically. 
 * Depending on the needs configuration files can be added. 
 * The expiration time can be changed. 
 * Always have in mind the expiration of elements. 
 * An element can expire:
 * 	- when the configured time passed since it was saved in cache
 *  - when the element is accessed
 *  
 * @author Mihaela Munteanu
 * @since  7th of June 2017
 *
 */
public class ConverterCacheManager {
	CachingProvider cachingProvider;
	CacheManager cacheManager;
	
	public static final String CROSS_SECTIONAL_CACHE="xsCache";
	public static final String CSV_XS_CACHE="CsvXsCache";
	
	public static final ConverterCacheManager INSTANCE =  new ConverterCacheManager();
	
	private ConverterCacheManager() {
        //resolve a cache manager
        cachingProvider = Caching.getCachingProvider();
        cacheManager = cachingProvider.getCacheManager();
	}
	
	public void createConverterCache(String cacheName){
		
		if (cacheManager.getCache(cacheName) == null) {
	        //configure the cache
	        MutableConfiguration<LinkedHashMap<String, String>, LinkedHashMap<String, String>> config =
	           new MutableConfiguration<LinkedHashMap<String, String>, LinkedHashMap<String, String>>()
	//           .setTypes(HashMap.class, HashMap.class)
	           .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(Duration.THIRTY_MINUTES))
	           .setStatisticsEnabled(true);
	
	        //create the cache
	        cacheManager.createCache(cacheName, config);
		}
	}
	
    public Cache<LinkedHashMap<String, String>, LinkedHashMap<String, String>> getConverterCache(String cacheName){
    		return cacheManager.getCache(cacheName);
    }
    
    public void putKeyAndValue(Cache<LinkedHashMap<String, String>, LinkedHashMap<String, String>> cache, LinkedHashMap<String, String> key, LinkedHashMap<String, String> value) {
    	cache.put(key, value);
    }
    
    public LinkedHashMap<String, String> getValueFromKey(Cache<LinkedHashMap<String, String>, LinkedHashMap<String, String>> cache, LinkedHashMap<String, String> key) {
    	return cache.get(key);
    }
    
    public void closeCache(Cache<LinkedHashMap<String, String>, LinkedHashMap<String, String>> cache) {
    	if (cache != null) {
    		cache.clear();
    		cache.close();
    	}
    }
}
