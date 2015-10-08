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

package com.caricah.iotracah.security.realm.impl;

import org.apache.shiro.config.Ini;
import org.apache.shiro.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class IOTIniBasedRealm extends IOTTextConfiguredRealm {


    public static final String USERS_SECTION_NAME = "users";
    public static final String ROLES_SECTION_NAME = "roles";

    private static transient final Logger log = LoggerFactory.getLogger(IOTIniBasedRealm.class);

    private Ini ini; //reference added in 1.2 for SHIRO-322

    /**
     * Returns the Ini instance used to configure this realm.  Provided for JavaBeans-style configuration of this
     * realm, particularly useful in Dependency Injection environments.
     *
     * @return the Ini instance which will be inspected to create accounts, groups and permissions for this realm.
     */
    public Ini getIni() {
        return ini;
    }

    /**
     * Sets the Ini instance used to configure this realm.  Provided for JavaBeans-style configuration of this
     * realm, particularly useful in Dependency Injection environments.
     *
     * @param ini the Ini instance which will be inspected to create accounts, groups and permissions for this realm.
     */
    public void setIni(Ini ini) {
        this.ini = ini;
    }

    @Override
    protected void onInit() {
        super.onInit();

        Ini ini = getIni();

        if (CollectionUtils.isEmpty(ini)) {
            String msg = "Ini instance and/or resourcePath resulted in null or empty Ini configuration.  Cannot " +
                    "load account data.";
            throw new IllegalStateException(msg);
        }

        processDefinitions(ini);
    }

    private void processDefinitions(Ini ini) {
        if (CollectionUtils.isEmpty(ini)) {
            log.warn("{} defined, but the ini instance is null or empty.", getClass().getSimpleName());
            return;
        }

        Ini.Section rolesSection = ini.getSection(ROLES_SECTION_NAME);
        if (!CollectionUtils.isEmpty(rolesSection)) {
            log.debug("Discovered the [{}] section.  Processing...", ROLES_SECTION_NAME);
            processRoleDefinitions(rolesSection);
        }

        Ini.Section usersSection = ini.getSection(USERS_SECTION_NAME);
        if (!CollectionUtils.isEmpty(usersSection)) {
            log.debug("Discovered the [{}] section.  Processing...", USERS_SECTION_NAME);
            processUserDefinitions(usersSection);
        } else {
            log.info("{} defined, but there is no [{}] section defined.  This realm will not be populated with any " +
                    "users and it is assumed that they will be populated programatically.  Users must be defined " +
                    "for this Realm instance to be useful.", getClass().getSimpleName(), USERS_SECTION_NAME);
        }
    }
}
