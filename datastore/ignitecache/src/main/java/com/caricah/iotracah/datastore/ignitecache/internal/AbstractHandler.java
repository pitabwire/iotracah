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

package com.caricah.iotracah.datastore.ignitecache.internal;

import com.caricah.iotracah.core.worker.state.IdKeyComposer;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import rx.Observable;

import javax.cache.Cache.Entry;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public abstract class AbstractHandler<T extends IdKeyComposer> {

    private String cacheName;
    private String excecutorName;
    private IgniteCache<Serializable, T> datastoreCache;
    private IgniteCompute computeGrid;


    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public String getExcecutorName() {
        return excecutorName;
    }

    public void setExcecutorName(String excecutorName) {
        this.excecutorName = excecutorName;
    }

    public IgniteCache<Serializable, T> getDatastoreCache() {
        return datastoreCache;
    }

    public void setDatastoreCache(IgniteCache<Serializable, T> datastoreCache) {
        this.datastoreCache = datastoreCache;
    }

    public IgniteCompute getComputeGrid() {
        return computeGrid;
    }

    public void setComputeGrid(IgniteCompute computeGrid) {
        this.computeGrid = computeGrid;
    }


    public abstract void configure(Configuration configuration);

    public void initiate(Class<T> t, Ignite ignite) {

        CacheConfiguration clCfg = new CacheConfiguration();

        clCfg.setName(getCacheName());
        clCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        clCfg.setCacheMode(CacheMode.PARTITIONED);
        clCfg.setIndexedTypes(String.class, t);
        ignite.createCache(clCfg);
        IgniteCache<Serializable, T> clientIgniteCache = ignite.cache(getCacheName());

        setDatastoreCache(clientIgniteCache);

        ClusterGroup clusterGroup = ignite.cluster().forAttribute("ROLE", getExcecutorName());

        IgniteCompute compute = ignite.compute(clusterGroup).withAsync();
        setComputeGrid(compute);

    }

    public Observable<T> getByKey(Serializable key) {

        return Observable.create(observer -> {
            getComputeGrid().run(() -> {

                try {
                    // do work on separate thread
                    T value = getDatastoreCache().get(key);
                    // callback with value
                    observer.onNext(value);
                    observer.onCompleted();
                } catch (Exception e) {
                    observer.onError(e);
                }

            });
        });

    }


public Observable<T> getByKeyWithDefault(Serializable key, T defaultValue) {

        return Observable.create(observer -> getComputeGrid().run(() -> {

            try {
                // do work on separate thread

                T value = getDatastoreCache().get(key);
                if(null == value){
                    value = defaultValue;
                }

                // callback with value
                observer.onNext(value);
                observer.onCompleted();
            } catch (Exception e) {
                observer.onError(e);
            }

        }));

    }


    public Observable<T> getByQuery(Class<T> t, String query, Object[] params) {

        return Observable.create(observer -> {
            getComputeGrid().run(() -> {

                try {

                    SqlQuery sql = new SqlQuery<Serializable, T>(t, query);
                    sql.setArgs(params);

                    // Find all messages belonging to a client.
                    QueryCursor<Entry<Serializable, T>> queryResult = getDatastoreCache().query(sql);

                    for (Entry<Serializable, T> entry : queryResult) {

                        // callback with value
                        observer.onNext(entry.getValue());
                    }
                    observer.onCompleted();
                } catch (Exception e) {
                    observer.onError(e);
                }

            });
        });

    }


    public void save(T  item) {
        getComputeGrid().run(()-> {
            try {
                getDatastoreCache().put(item.generateIdKey(), item);
            } catch (UnRetriableException e) {

            }
        });
    }

    public void remove(IdKeyComposer item) {

        getComputeGrid().run(()->{
            try {
                getDatastoreCache().remove(item.generateIdKey());
            }catch (UnRetriableException e){
                
            }
        });
    }
}
