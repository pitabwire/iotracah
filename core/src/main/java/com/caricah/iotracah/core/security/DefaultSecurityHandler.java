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

package com.caricah.iotracah.core.security;

import com.caricah.iotracah.core.worker.session.SessionDAO;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.security.IOTIniSecurityManagerFactory;
import com.caricah.iotracah.security.realm.IOTAbstractRealm;
import com.caricah.iotracah.security.realm.IOTAccountDatastore;
import com.caricah.iotracah.security.realm.auth.permission.IOTPermissionResolver;
import com.caricah.iotracah.system.ResourceFileUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.pam.ModularRealmAuthenticator;
import org.apache.shiro.authz.ModularRealmAuthorizer;
import org.apache.shiro.config.Ini;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.AuthenticatingRealm;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/7/15
 */
public class DefaultSecurityHandler {

    private static final Logger log = LoggerFactory.getLogger(DefaultSecurityHandler.class);

    public static final String CONFIGURATION_VALUE_DEFAULT_SECURITY_FILE_NAME = "security.ini";

    private final String securityFileName;

    public static final String CONFIG_IGNITECACHE_SESSION_CACHE_NAME = "config.ignitecache.session.cache.name";
    public static final String CONFIG_IGNITECACHE_SESSION_CACHE_NAME_VALUE_DEFAULT = "iotracah_session_cache";

    public static final String CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME = "config.ignitecache.session.atomic.sequence.name";
    public static final String CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME_VALUE_DEFAULT = "iotracah_session_atomic_sequence";


    private String cacheName;
    private String atomicSequenceName;
    private IgniteCache<Serializable, String> datastoreCache;
    private IgniteAtomicSequence atomicSequence;

    private IOTAccountDatastore iotAccountDatastore;

    private IOTIniSecurityManagerFactory iniSecurityManagerFactory;

    public DefaultSecurityHandler(){
        this.securityFileName = CONFIGURATION_VALUE_DEFAULT_SECURITY_FILE_NAME;
    }

    public DefaultSecurityHandler(String securityFileName){
        this.securityFileName = securityFileName;
    }

    public String getSecurityFileName() {
        return securityFileName;
    }


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

    public IgniteCache<Serializable, String> getDatastoreCache() {
        return datastoreCache;
    }

    public void setDatastoreCache(IgniteCache<Serializable, String> datastoreCache) {
        this.datastoreCache = datastoreCache;
    }

    public IgniteAtomicSequence getAtomicSequence() {
        return atomicSequence;
    }

    public void setAtomicSequence(IgniteAtomicSequence atomicSequence) {
        this.atomicSequence = atomicSequence;
    }

    public IOTAccountDatastore getIotAccountDatastore() {
        return iotAccountDatastore;
    }

    public void setIotAccountDatastore(IOTAccountDatastore iotAccountDatastore) {
        this.iotAccountDatastore = iotAccountDatastore;
    }

    public String getSecurityIniPath() throws UnRetriableException{

        File securityFile = new File(getSecurityFileName());

        if(!securityFile.exists()) {

            log.warn( " getSecurityIniPath : Security file not found in the configurations directory. Falling back to the defaults");

            securityFile = ResourceFileUtil.getFileFromResource(getClass(), getSecurityFileName());

                return securityFile.getPath();

        }else {
            return securityFile.getPath();
        }

    }


    public void configure(Configuration configuration){

        String cacheName = configuration.getString(CONFIG_IGNITECACHE_SESSION_CACHE_NAME, CONFIG_IGNITECACHE_SESSION_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

        String atomicSequenceName = configuration.getString(CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME, CONFIG_IGNITECACHE_SESSION_ATOMIC_SEQUENCE_NAME_VALUE_DEFAULT);
        setAtomicSequenceName(atomicSequenceName);

    }


    public void createSecurityManager(String securityFilePath) throws UnRetriableException{


        Ini ini = new Ini();
        ini.loadFromPath(securityFilePath);

        iniSecurityManagerFactory = new IOTIniSecurityManagerFactory(ini, getIotAccountDatastore());

        SecurityManager securityManager = iniSecurityManagerFactory.getInstance();

        if(securityManager instanceof DefaultSecurityManager) {

            //configure the security manager.
            DefaultSecurityManager defaultSecurityManager = (DefaultSecurityManager) securityManager;
            DefaultSessionManager sessionManager = (DefaultSessionManager) defaultSecurityManager.getSessionManager();

            SecurityUtils.setSecurityManager(defaultSecurityManager);



            //Create our sessions DAO
            SessionDAO sessionDAO = new SessionDAO(getDatastoreCache(), getAtomicSequence());
            sessionDAO.init();
            sessionManager.setSessionDAO(sessionDAO);
            sessionManager.setSessionValidationSchedulerEnabled(false);


        }else {
            throw new UnRetriableException("Security manager has to be an instance of the default security manager (DefaultSecurityManager). "+securityManager.getClass().getName()+" was used instead." );
        }
    }




    public void initiate(Ignite ignite){

        CacheConfiguration clCfg = new CacheConfiguration();

        clCfg.setName(getCacheName());
        clCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        clCfg.setCacheMode(CacheMode.PARTITIONED);
        ignite.createCache(clCfg);
        IgniteCache<Serializable, String> clientIgniteCache = ignite.cache(getCacheName());
        setDatastoreCache(clientIgniteCache);


        long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        IgniteAtomicSequence atomicSequence = ignite.atomicSequence(getAtomicSequenceName(), currentTime, true);
        setAtomicSequence(atomicSequence);


    }


}
