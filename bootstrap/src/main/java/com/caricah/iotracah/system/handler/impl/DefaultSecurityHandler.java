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

package com.caricah.iotracah.system.handler.impl;

import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.*;
import org.apache.shiro.mgt.SecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/7/15
 */
public class DefaultSecurityHandler {

    private static final Logger log = LoggerFactory.getLogger(DefaultSecurityHandler.class);

    public static final String CONFIGURATION_VALUE_DEFAULT_SECURITY_FILE_NAME = "security.ini";

    private final String securityFileName;

    public DefaultSecurityHandler(){
        this.securityFileName = CONFIGURATION_VALUE_DEFAULT_SECURITY_FILE_NAME;
    }

    public DefaultSecurityHandler(String securityFileName){
        this.securityFileName = securityFileName;
    }

    public String getSecurityFileName() {
        return securityFileName;
    }

    public String getSecurityIniPath() throws UnRetriableException{

        File securityFile = new File(getSecurityFileName());

        if(!securityFile.exists()) {

            log.warn( " getSecurityIniPath : Security file not found in the configurations directory. Falling back to the defaults");

            ClassLoader classLoader = getClass().getClassLoader();

            URL configurationResource = classLoader.getResource(getSecurityFileName());
            if (null != configurationResource) {
                securityFile = new File(configurationResource.getFile());
                return securityFile.getPath();
            }
        }else {
            return securityFile.getPath();
        }

        throw new UnRetriableException("There is no security file located in the system.");


    }


    public void configure(String securityFilePath) throws UnRetriableException{


        Ini ini = new Ini();
        ini.loadFromPath(securityFilePath);

        IniSecurityManagerFactory factory = new IniSecurityManagerFactory(ini);
        SecurityManager securityManager = factory.getInstance();

        if(securityManager instanceof DefaultSecurityManager) {

            SecurityUtils.setSecurityManager(securityManager);

        }else {
            throw new UnRetriableException("Security manager has to be an instance of the default security manager (DefaultSecurityManager). "+securityManager.getClass().getName()+" was used instead." );
        }
    }


}
