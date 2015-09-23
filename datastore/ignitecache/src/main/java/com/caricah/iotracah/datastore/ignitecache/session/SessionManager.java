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

package com.caricah.iotracah.datastore.ignitecache.session;


import org.apache.commons.configuration.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.DefaultSessionManager;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/23/15
 */
public class SessionManager {

    public static final String CONFIG_IGNITECACHE_SESSION_CACHE_NAME = "config.ignitecache.session.cache.name";
    public static final String CONFIG_IGNITECACHE_SESSION_CACHE_NAME_VALUE_DEFAULT = "iotracah_session_cache";

    public static final String CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME = "config.ignitecache.session.atomic.sequence.name";
    public static final String CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME_VALUE_DEFAULT = "iotracah_session_atomic_sequence";


    private String cacheName;
    private String atomicSequenceName;
    private IgniteCache<Serializable, Session> datastoreCache;
    private IgniteAtomicSequence atomicSequence;


    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public String getAtomicSequenceName() {
        return atomicSequenceName;
    }

    public void setAtomicSequenceName(String atomicSequenceName) {
        this.atomicSequenceName = atomicSequenceName;
    }

    public IgniteCache<Serializable, Session> getDatastoreCache() {
        return datastoreCache;
    }

    public void setDatastoreCache(IgniteCache<Serializable, Session> datastoreCache) {
        this.datastoreCache = datastoreCache;
    }

    public IgniteAtomicSequence getAtomicSequence() {
        return atomicSequence;
    }

    public void setAtomicSequence(IgniteAtomicSequence atomicSequence) {
        this.atomicSequence = atomicSequence;
    }




    public void configure(Configuration configuration){


        String cacheName = configuration.getString(CONFIG_IGNITECACHE_SESSION_CACHE_NAME, CONFIG_IGNITECACHE_SESSION_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

        String atomicSequenceName = configuration.getString(CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME, CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME_VALUE_DEFAULT);
        setAtomicSequenceName(atomicSequenceName);



    }


    public void initiate(Ignite ignite){

        CacheConfiguration clCfg = new CacheConfiguration();

        clCfg.setName(getCacheName());
        clCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        clCfg.setCacheMode(CacheMode.PARTITIONED);
        ignite.createCache(clCfg);
        IgniteCache<Serializable, Session> clientIgniteCache = ignite.cache(getCacheName());
        setDatastoreCache(clientIgniteCache);


        long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        IgniteAtomicSequence atomicSequence = ignite.atomicSequence(getAtomicSequenceName(), currentTime, true);
        setAtomicSequence(atomicSequence);

        //configure the security manager.
        DefaultSecurityManager securityManager = (DefaultSecurityManager) SecurityUtils.getSecurityManager();
        DefaultSessionManager sessionManager = (DefaultSessionManager) securityManager.getSessionManager();

        //Create our sessions DAO
        SessionDAO sessionDAO = new SessionDAO(getDatastoreCache(), getAtomicSequence());
        sessionDAO.init();
        sessionManager.setSessionDAO(sessionDAO);

        sessionManager.setSessionValidationSchedulerEnabled(false);
    }
}
