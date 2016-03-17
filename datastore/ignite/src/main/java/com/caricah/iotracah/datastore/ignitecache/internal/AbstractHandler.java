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

import com.caricah.iotracah.bootstrap.data.models.client.IotClientKey;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.core.worker.exceptions.DoesNotExistException;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.cache.Cache.Entry;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public abstract class AbstractHandler<K, T> implements Serializable {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String cacheName;
    private IgniteCache<K, T> datastoreCache;

    private IgniteAtomicSequence idSequence;

    private Scheduler scheduler;

    private ExecutorService executorService;

    private Class<T> classType;

    private boolean persistanceEnabled;


    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }


    public boolean isPersistanceEnabled() {
        return persistanceEnabled;
    }

    public void setPersistanceEnabled(boolean persistanceEnabled) {
        this.persistanceEnabled = persistanceEnabled;
    }


    public IgniteCache<K, T> getDatastoreCache() {
        return datastoreCache;
    }

    public void setDatastoreCache(IgniteCache<K, T> datastoreCache) {
        this.datastoreCache = datastoreCache;
    }

    public IgniteAtomicSequence getIdSequence() {
        return idSequence;
    }

    public void setIdSequence(IgniteAtomicSequence idSequence) {
        this.idSequence = idSequence;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler() {
        this.scheduler = Schedulers.from(getExecutorService());
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;

        setScheduler();
    }

    public abstract void configure(Configuration configuration);

    public void initiate(Class<T> t, Ignite ignite) {


        try {
            Context ic = new InitialContext();

            DataSource dataSource;
            if(isPersistanceEnabled())
             dataSource = (DataSource) ic.lookup("jdbc_commonpool");
else
            dataSource = null;

            CacheConfiguration<K,T> clCfg = getCacheConfiguration(isPersistanceEnabled(), dataSource);

            clCfg = extraCacheSettingsConfigure(clCfg);

            ignite.createCache(clCfg);
            IgniteCache<K, T> clientIgniteCache = ignite.cache(getCacheName());

            clientIgniteCache.loadCache(null);

            setDatastoreCache(clientIgniteCache);

            classType = t;

            String nameOfSequence = getCacheName() + "-sequence";
            initializeSequence(nameOfSequence, ignite);

        } catch (NamingException e) {
            log.error(" getFactory : problems obtaining appropriate jdbc context");
            throw new UnRetriableException(e);
        }


    }

    private CacheConfiguration<K,T> extraCacheSettingsConfigure(CacheConfiguration<K,T> clCfg) {

        clCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        clCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        clCfg.setCacheMode(CacheMode.PARTITIONED);

        clCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
        clCfg.setOffHeapMaxMemory(0);

        LruEvictionPolicy lruEvictionPolicy = new LruEvictionPolicy(5170000);
        clCfg.setEvictionPolicy(lruEvictionPolicy);

        clCfg.setSwapEnabled(false);
        clCfg.setRebalanceBatchSize(1024 * 1024);
        clCfg.setRebalanceThrottle(0);
        clCfg.setRebalanceThreadPoolSize(4);

        return clCfg;

    }

    protected abstract CacheConfiguration<K,T> getCacheConfiguration(boolean persistanceEnabled, DataSource ds);

    public void initializeSequence(String nameOfSequence, Ignite ignite) {

        long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        IgniteAtomicSequence idSequence = ignite.atomicSequence(nameOfSequence, currentTime, true);
        setIdSequence(idSequence);
    }

    public Observable<T> getByKey(K key) {

        return Observable.create(observer -> {


                try {
                    // do work on separate thread
                    T actualResult = getDatastoreCache().get(key);

                    if (Objects.nonNull(actualResult)) {
                        observer.onNext(actualResult);
                        observer.onCompleted();
                    } else {
                        observer.onError(new DoesNotExistException(String.format("%s with key [%s] does not exist.", classType, key)));
                    }

                } catch (Exception e) {
                    observer.onError(e);
                }

        });

    }


    public Observable<T> getBySet(Set<K> keys) {

        return Observable.create(observer -> {


                try {

                    keys.forEach(

                            key -> {

                                T item = getDatastoreCache().get(key);

                                    if (Objects.nonNull(item))
                                        observer.onNext(item);


                            }
                    );


                    observer.onCompleted();

                } catch (Exception e) {
                    observer.onError(e);
                }

            });


    }


    public Observable<T> getByKeyWithDefault(K key, T defaultValue) {

        return Observable.create(observer -> {


                try {
                    // do work on separate thread

                    T value = getDatastoreCache().get(key);
                    // callback with value only if not null
                    if (null != value) {
                        observer.onNext(value);
                    } else {
                        observer.onNext(defaultValue);
                    }
                    observer.onCompleted();

                } catch (Exception e) {
                    observer.onError(e);
                }

        });

    }


    public Observable<T> getByQuery(Class<T> t, String query, Object[] params) {

        return Observable.create(observer -> {

                try {

                    SqlQuery sql = new SqlQuery<Serializable, T>(t, query);
                    sql.setArgs(params);

                    // Find all messages belonging to a client.
                    QueryCursor<Entry<K, T>> queryResult = getDatastoreCache().query(sql);

                    for (Entry<K, T> entry : queryResult) {
                        // callback with value
                        observer.onNext(entry.getValue());
                    }


                    observer.onCompleted();
                } catch (Exception e) {
                    observer.onError(e);
                }

        });

    }

    public <L extends Serializable> Observable<L> getByQueryAsValue(Class<L> l, String query, Object[] params) {

        return Observable.create(observer -> {
            try {

                SqlFieldsQuery sql = new SqlFieldsQuery(query);


                // Execute the query and obtain the query result cursor.
                try (QueryCursor<List<?>> queryResult = getDatastoreCache().query(sql.setArgs(params))) {
                    // callback with value

                    for (List entry : queryResult) {
                        // callback with value
                        observer.onNext((L) entry.get(0));
                    }

                }

                observer.onCompleted();
            } catch (Exception e) {
                observer.onError(e);
            }
        });

    }

    public abstract K keyFromModel(T model);

    public void save(T item) {
        save(keyFromModel(item), item);
    }

    protected void save(K key, T item) {


        try {

            getDatastoreCache().put(key, item);

        } catch (Exception e) {
            log.error(" save : issues while saving item ", e);
        }


    }

    public void remove(T item) {
        removeByKey(keyFromModel(item));
    }

    protected void removeByKey(K item) {

        try {
            getDatastoreCache().remove(item);
        } catch (Exception e) {
            log.error(" remove : problem while removing item ", e);
        }

    }


}
