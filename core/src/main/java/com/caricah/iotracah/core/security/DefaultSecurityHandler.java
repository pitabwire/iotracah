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

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.IOTIniSecurityManagerFactory;
import com.caricah.iotracah.bootstrap.security.IOTSecurityManager;
import com.caricah.iotracah.bootstrap.security.realm.IOTSecurityDatastore;
import com.caricah.iotracah.bootstrap.system.ResourceFileUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.Ini;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.SessionListener;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/7/15
 */
public class DefaultSecurityHandler {

    private static final Logger log = LoggerFactory.getLogger(DefaultSecurityHandler.class);

    public static final String CONFIGURATION_VALUE_DEFAULT_SECURITY_FILE_NAME = "security.ini";

    public static final String SYSTEM_CONFIG_SECURITY_CONFIG_DIRECTORY = "system.config.security.config.directory";
    public static final String SYSTEM_CONFIG_SECURITY_CONFIG_DIRECTORY_DEFAULT_VALUE = "";

    public static final String CONFIG_SYSTEM_SECURITY_DEFAULT_PARTITION_NAME = "config.system.security.default.partition.name";
    public static final String CONFIG_SYSTEM_SECURITY_DEFAULT_PARTITION_NAME_VALUE_DEFAULT = "default_partition";

    private final String securityFileName;
    private String securityFileDirectory;
    private String defaultPartitionName;


    private IOTSecurityDatastore iotSecurityDatastore;

    private Set<SessionListener> sessionListenerList = new HashSet<>();

    public DefaultSecurityHandler(){
        this.securityFileName = CONFIGURATION_VALUE_DEFAULT_SECURITY_FILE_NAME;
    }

    public DefaultSecurityHandler(String securityFileName){
        this.securityFileName = securityFileName;
    }

    public String getSecurityFileName() {
        return securityFileName;
    }

    public String getSecurityFileDirectory() {
        return securityFileDirectory;
    }

    public void setSecurityFileDirectory(String securityFileDirectory) {
        this.securityFileDirectory = securityFileDirectory;
    }


    public IOTSecurityDatastore getIotSecurityDatastore() {
        return iotSecurityDatastore;
    }

    public void setIotSecurityDatastore(IOTSecurityDatastore iotSecurityDatastore) {
        this.iotSecurityDatastore = iotSecurityDatastore;
    }

    public String getDefaultPartitionName() {
        return defaultPartitionName;
    }

    public void setDefaultPartitionName(String defaultPartitionName) {
        this.defaultPartitionName = defaultPartitionName;
    }

    public Set<SessionListener> getSessionListenerList() {
        return sessionListenerList;
    }

    public String getSecurityIniPath() throws UnRetriableException{

        File securityFile = new File(getSecurityFileDirectory()+File.separator+getSecurityFileName());

        if(!securityFile.exists()) {

            log.warn( " getSecurityIniPath : Security file not found in the configurations directory. Falling back to the defaults");

            securityFile = ResourceFileUtil.getFileFromResource(getClass(), getSecurityFileName());

                return securityFile.getPath();

        }else {
            return securityFile.getPath();
        }

    }


    public void configure(Configuration configuration){



        String securityFileDirectory = System.getProperty("iotracah.default.path.conf", SYSTEM_CONFIG_SECURITY_CONFIG_DIRECTORY_DEFAULT_VALUE);

        securityFileDirectory = configuration.getString(SYSTEM_CONFIG_SECURITY_CONFIG_DIRECTORY, securityFileDirectory);

        setSecurityFileDirectory(securityFileDirectory);


        String defaultPartitionName = configuration.getString(CONFIG_SYSTEM_SECURITY_DEFAULT_PARTITION_NAME, CONFIG_SYSTEM_SECURITY_DEFAULT_PARTITION_NAME_VALUE_DEFAULT);
        setDefaultPartitionName(defaultPartitionName);

    }


    public SecurityManager createSecurityManager(String securityFilePath) throws UnRetriableException{


        Ini ini = new Ini();
        ini.loadFromPath(securityFilePath);

        IOTIniSecurityManagerFactory iniSecurityManagerFactory = new IOTIniSecurityManagerFactory(ini, getIotSecurityDatastore(), getDefaultPartitionName());

        SecurityManager securityManager = iniSecurityManagerFactory.getInstance();

        if(securityManager instanceof IOTSecurityManager) {

            //configure the security manager.
            IOTSecurityManager iotSecurityManager = (IOTSecurityManager) securityManager;
            DefaultSessionManager sessionManager = (DefaultSessionManager) iotSecurityManager.getSessionManager();


            SecurityUtils.setSecurityManager(iotSecurityManager);

            //Assign session dao from the security datastore.
            sessionManager.setSessionDAO(getIotSecurityDatastore());

            sessionManager.setSessionListeners(getSessionListenerList());
            sessionManager.setSessionValidationSchedulerEnabled(true);
            sessionManager.setSessionValidationInterval(1000);

            return securityManager;


        }else {
            throw new UnRetriableException("Security manager has to be an instance of the default security manager (DefaultSecurityManager). "+securityManager.getClass().getName()+" was used instead." );
        }
    }

}
