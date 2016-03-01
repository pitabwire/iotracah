/*
 *
 * Copyright (c) 2016 Caricah <info@caricah.com>.
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

import com.caricah.iotracah.bootstrap.data.models.partition.CacheConfig;
import com.caricah.iotracah.bootstrap.data.models.partition.IotPartition;
import com.caricah.iotracah.bootstrap.data.models.partition.IotPartitionKey;
import com.caricah.iotracah.core.security.DefaultSecurityHandler;
import com.caricah.iotracah.datastore.ignitecache.internal.AbstractHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import rx.Observable;

import javax.sql.DataSource;
import java.util.Objects;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public class PartitionHandler extends AbstractHandler<IotPartitionKey, IotPartition> {


    public static final String CONFIG_IGNITECACHE_PARTITION_CACHE_NAME = "config.ignitecache.partition.cache.name";
    public static final String CONFIG_IGNITECACHE_PARTITION_CACHE_NAME_VALUE_DEFAULT = "iotracah_partition_cache";


    private String defaultPartitionName;

    public String getDefaultPartitionName() {
        return defaultPartitionName;
    }

    public void setDefaultPartitionName(String defaultPartitionName) {
        this.defaultPartitionName = defaultPartitionName;
    }

    @Override
    public void configure(Configuration configuration) {


        String cacheName = configuration.getString(CONFIG_IGNITECACHE_PARTITION_CACHE_NAME, CONFIG_IGNITECACHE_PARTITION_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

        String defaultPartitionName = configuration.getString(DefaultSecurityHandler.CONFIG_SYSTEM_SECURITY_DEFAULT_PARTITION_NAME, DefaultSecurityHandler.CONFIG_SYSTEM_SECURITY_DEFAULT_PARTITION_NAME_VALUE_DEFAULT);
        setDefaultPartitionName(defaultPartitionName);

    }


    @Override
    public void initiate(Class<IotPartition> t, Ignite ignite) {
        super.initiate(t, ignite);

        //Make sure the default partition is available.
        IotPartition partition = new IotPartition();
        partition.setName(getDefaultPartitionName());
        partition.setDescription("Default partition auto created by iotracah for ");
        partition.setIsActive(true);
        partition.setLocked(false);

        IotPartitionKey partitionKey = keyFromModel(partition);

        Observable<IotPartition> partitionObservable = getByKeyWithDefault(partitionKey, null);

        partitionObservable.subscribe(iotPartition -> {

            if (Objects.isNull(iotPartition)) {
                save(partition);
            }
        });

    }

    @Override
    protected CacheConfiguration<IotPartitionKey, IotPartition> getCacheConfiguration(boolean persistanceEnabled, DataSource ds) {

        CacheJdbcPojoStoreFactory<IotPartitionKey, IotPartition> factory = null;

        if (persistanceEnabled){
            factory = new CacheJdbcPojoStoreFactory<>();
            factory.setDataSource(ds);
        }

        return CacheConfig.cache(getCacheName(), factory);
    }


    @Override
    public IotPartitionKey keyFromModel(IotPartition model) {

        IotPartitionKey partitionKey = new IotPartitionKey();
        partitionKey.setName(model.getName());
        return partitionKey;
    }

}
