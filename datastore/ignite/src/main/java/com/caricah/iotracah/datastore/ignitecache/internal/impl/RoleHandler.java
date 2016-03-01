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

import com.caricah.iotracah.bootstrap.data.models.roles.CacheConfig;
import com.caricah.iotracah.bootstrap.data.models.roles.IotRoleKey;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTRole;
import com.caricah.iotracah.datastore.ignitecache.internal.AbstractHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.sql.DataSource;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public class RoleHandler extends AbstractHandler<IotRoleKey, IOTRole> {


    public static final String CONFIG_IGNITECACHE_ROLE_CACHE_NAME = "config.ignitecache.role.cache.name";
    public static final String CONFIG_IGNITECACHE_ROLE_CACHE_NAME_VALUE_DEFAULT = "iotracah_role_cache";


    @Override
    public void configure(Configuration configuration) {


        String cacheName = configuration.getString(CONFIG_IGNITECACHE_ROLE_CACHE_NAME, CONFIG_IGNITECACHE_ROLE_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

    }

    @Override
    protected CacheConfiguration<IotRoleKey, IOTRole> getCacheConfiguration(boolean persistanceEnabled,  DataSource ds) {
        CacheJdbcPojoStoreFactory<IotRoleKey, IOTRole> factory = null;

        if (persistanceEnabled){
            factory = new CacheJdbcPojoStoreFactory<>();
            factory.setDataSource(ds);
        }

        return CacheConfig.cache(getCacheName(), factory);
    }


    @Override
    public IotRoleKey keyFromModel(IOTRole role) {
        IotRoleKey roleKey = new IotRoleKey();
        roleKey.setPartitionId(role.getPartitionId());
        roleKey.setName(role.getName());
        return roleKey;
    }


    @Override
    public void save(IOTRole item) {

        if(item.getId() == 0){

            item.setId(getIdSequence().incrementAndGet());
        }

        super.save(item);
    }
}
