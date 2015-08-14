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

package com.caricah.iotracah.core.init;

import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.modules.Eventer;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.system.BaseSystemHandler;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>DatastoresInitializer</code> Handler for initializing base datastore handler
 * plugins.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/11/15
 */
public abstract class DatastoresInitializer extends WorkersInitializer {


    public static final String CORE_CONFIG_LOGGING_DATASTORE_ENGINE_IS_ENABLED = "core.config.logging.datastore.engine.is.enabled";
    public static final boolean CORE_CONFIG_LOGGING_DATASTORE_ENGINE_IS_ENABLED_DEFAULT_VALUE = true;

    private boolean datastoreEngineEnabled;

    public boolean isServerEngineEnabled() {
        return datastoreEngineEnabled;
    }

    public void setServerEngineEnabled(boolean datastoreEngineEnabled) {
        this.datastoreEngineEnabled = datastoreEngineEnabled;
    }

    private List<Datastore> datastoreList = new ArrayList<>();

    public List<Datastore> getDatastoreList() {
        return datastoreList;
    }

    /**
     * <code>startDataStores</code> initiate the database system for IOTracah,
     * During the startup process all sorts of database plugins may be loaded
     * however only one is expected to be allowed to operate.
     *
     * @throws UnRetriableException
     */
    public void startDataStores() throws UnRetriableException {

        for (Datastore datastore : getDatastoreList()) {

            if(validateDatastoreCanBeLoaded(datastore)) {
                //Actually start our datastore guy.
                //
                datastore.initiate();
                break;
            }
        }
    }

    /**
     * Simple method that does validatations to determine if the datastore plugin
     * can be loaded.
     * @param datastore
     * @return
     */
    private boolean validateDatastoreCanBeLoaded(Datastore datastore) {
        //TODO: perform validataion from config files to select database plugin to use.
        if(null != datastore)
        return true;
        else
            return false;
    }


    public void classifyBaseHandler(BaseSystemHandler baseSystemHandler){



        if(baseSystemHandler instanceof Datastore) {

            log.debug(" classifyBaseHandler : found the datastore {}", baseSystemHandler);

            if (isServerEngineEnabled()){

                log.info(" classifyBaseHandler : storing the datastore : {} for use as active plugin", baseSystemHandler);
                datastoreList.add((Datastore) baseSystemHandler);
            }else {
                log.info(" classifyBaseHandler : datastore {} is disabled ", baseSystemHandler);
            }
        }else
        {
            super.classifyBaseHandler(baseSystemHandler);
        }
    }

    /**
     * <code>configure</code> allows the initializer to configure its self
     * Depending on the implementation conditional operation can be allowed
     * So as to make the system instance more specialized.
     * <p>
     * For example: via the configurations the implementation may decide to
     * shutdown backend services and it just works as a server application to receive
     * and route requests to the workers which are in turn connected to the backend/datastore servers...
     *
     * @param configuration
     * @throws UnRetriableException
     */
    @Override
    public void configure(Configuration configuration) throws UnRetriableException {


        boolean configDatastoreEnabled = configuration.getBoolean(CORE_CONFIG_LOGGING_DATASTORE_ENGINE_IS_ENABLED, CORE_CONFIG_LOGGING_DATASTORE_ENGINE_IS_ENABLED_DEFAULT_VALUE);

        setServerEngineEnabled(configDatastoreEnabled);

    }


}
