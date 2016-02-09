/*
 *
 * Copyright (c) 2015 Caricah <info@caricah.com>.
 *
 * Caricah licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 *  of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under
 *  the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 *  OF ANY  KIND, either express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 *
 *
 *
 */

package com.caricah.iotracah.datastore.ignitecache.internal.impl;

import com.caricah.iotracah.datastore.ignitecache.internal.AbstractHandler;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTRole;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public class RoleHandler extends AbstractHandler<IOTRole> {


    public static final String CONFIG_IGNITECACHE_ROLE_CACHE_NAME = "config.ignitecache.role.cache.name";
    public static final String CONFIG_IGNITECACHE_ROLE_CACHE_NAME_VALUE_DEFAULT = "iotracah_role_cache";


    @Override
    public void configure(Configuration configuration) {


        String cacheName = configuration.getString(CONFIG_IGNITECACHE_ROLE_CACHE_NAME, CONFIG_IGNITECACHE_ROLE_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

    }


    @Override
    protected CacheConfiguration moreConfig(Class<IOTRole> t, CacheConfiguration clCfg) {

        clCfg = clCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        return super.moreConfig(t, clCfg);
    }
}
