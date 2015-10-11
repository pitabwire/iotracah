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

package com.caricah.iotracah.security;

import com.caricah.iotracah.security.realm.IOTAccountDatastore;
import com.caricah.iotracah.security.realm.auth.permission.IOTPermissionResolver;
import com.caricah.iotracah.security.realm.impl.IOTIniBasedRealm;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.*;
import org.apache.shiro.realm.Realm;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/7/15
 */
public class IOTIniSecurityManagerFactory extends IniSecurityManagerFactory{

    public static final String INI_REALM_NAME = "iotIniRealm";

    private IOTAccountDatastore iotAccountDatastore;
    /**
     * Creates a new instance.  See the {@link #getInstance()} JavaDoc for detailed explanation of how an INI
     * source will be resolved to use to build the instance.
     */

    public IOTIniSecurityManagerFactory(Ini config, IOTAccountDatastore iotAccountDatastore ) {
        super(config);

        setIotAccountDatastore(iotAccountDatastore);
    }


    public IOTAccountDatastore getIotAccountDatastore() {
        return iotAccountDatastore;
    }

    public void setIotAccountDatastore(IOTAccountDatastore iotAccountDatastore) {
        this.iotAccountDatastore = iotAccountDatastore;
    }

    @Override
    protected org.apache.shiro.mgt.SecurityManager createDefaultInstance() {
        return new IOTSecurityManager();
    }

    @Override
    protected Realm createRealm(Ini ini) {
        IOTIniBasedRealm iniBasedRealm = new IOTIniBasedRealm();
        iniBasedRealm.setName(INI_REALM_NAME);
        iniBasedRealm.setIotAccountDatastore(getIotAccountDatastore());
        iniBasedRealm.setIni(ini);
        iniBasedRealm.setPermissionResolver(new IOTPermissionResolver());
        return iniBasedRealm;
    }
}
