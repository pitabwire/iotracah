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


    public static final String CORE_CONFIG_ENGINE_DATASTORE_IS_ENABLED = "core.config.engine.datastore.is.enabled";
    public static final boolean CORE_CONFIG_ENGINE_DATASTORE_IS_ENABLED_DEFAULT_VALUE = true;

    public static final String CORE_CONFIG_ENGINE_DATASTORE_CLASS_NAME = "core.config.engine.datastore.class.name";
    public static final String CORE_CONFIG_ENGINE_DATASTORE_CLASS_NAME_DEFAULT_VALUE = "com.caricah.iotracah.datastore.ignitecache.IgniteDatastore";

    public static final String CORE_CONFIG_DATASTORE_PARTITION_BASED_ON_USERNAME = "core.config.datastore.partition.based.on.username";
    public static final boolean CORE_CONFIG_DATASTORE_PARTITION_BASED_ON_USERNAME_DEFAULT_VALUE = false;


    private boolean datastoreEngineEnabled;
    private boolean datastorePartitionBasedOnUsername;

    private String datastoreClassName;

    public boolean isDatastoreEngineEnabled() {
        return datastoreEngineEnabled;
    }

    public void setDatastoreEngineEnabled(boolean datastoreEngineEnabled) {
        this.datastoreEngineEnabled = datastoreEngineEnabled;
    }

    public boolean isDatastorePartitionBasedOnUsername() {
        return datastorePartitionBasedOnUsername;
    }

    public void setDatastorePartitionBasedOnUsername(boolean datastorePartitionBasedOnUsername) {
        this.datastorePartitionBasedOnUsername = datastorePartitionBasedOnUsername;
    }

    public String getDatastoreClassName() {
        return datastoreClassName;
    }

    public void setDatastoreClassName(String datastoreClassName) {
        this.datastoreClassName = datastoreClassName;
    }

    private List<Datastore> datastoreList = new ArrayList<>();

    public List<Datastore> getDatastoreList() {
        return datastoreList;
    }

    private Datastore activeDatastore ;

    public Datastore getActiveDatastore() {
        return activeDatastore;
    }

    public void setActiveDatastore(Datastore activeDatastore) {
        this.activeDatastore = activeDatastore;
    }

    /**
     * <code>startDataStores</code> initiate the database system for IOTracah,
     * During the startup process all sorts of database plugins may be loaded
     * however only one is expected to be allowed to operate.
     *
     * @throws UnRetriableException
     */
    public void startDataStores() throws UnRetriableException {

        log.debug(" startDataStores : Starting the system datastores");

        if(isDatastoreEngineEnabled() && getDatastoreList().isEmpty()) {
            log.warn("List of datastore plugins is empty");
            throw new UnRetriableException(" System expects atleast one datastore plugin to be configured.");
        }

        for (Datastore datastore : getDatastoreList()) {

            if(validateDatastoreCanBeLoaded(datastore)) {

                //Set partitioning scheme.
                datastore.setPartitionBasedOnUsername(isDatastorePartitionBasedOnUsername());

                //Actually start our datastore guy.
                datastore.initiate();

                setActiveDatastore(datastore);
                break;
            }
        }



        //Assign our workers the active datastore.
        getWorkerList().forEach(worker -> worker.setDatastore(getActiveDatastore()));
    }

    /**
     * Simple method that does validatations to determine
     * if the datastore plugin matches whatever is in the config files then it
     * can be loaded.
     * @param datastore
     * @return
     */
    private boolean validateDatastoreCanBeLoaded(Datastore datastore) {
        if(null != datastore ) {

            return datastore.getClass().getName().equals(getDatastoreClassName());
        }
        return false;
    }


    protected void classifyBaseHandler(BaseSystemHandler baseSystemHandler){

        if(baseSystemHandler instanceof Datastore) {

            log.debug(" classifyBaseHandler : found the datastore {}", baseSystemHandler);

            if (isDatastoreEngineEnabled()){

                datastoreList.add((Datastore) baseSystemHandler);
                log.info(" classifyBaseHandler : storing the datastore : {} for use as active plugin", baseSystemHandler);

            }else {

                log.info(" classifyBaseHandler : datastore {} is disabled ", baseSystemHandler);

            }
        }else {
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


        boolean configDatastoreEnabled = configuration.getBoolean(CORE_CONFIG_ENGINE_DATASTORE_IS_ENABLED, CORE_CONFIG_ENGINE_DATASTORE_IS_ENABLED_DEFAULT_VALUE);

        log.debug(" configure : The datastore function is configured to be enabled [{}]", configDatastoreEnabled );

        setDatastoreEngineEnabled(configDatastoreEnabled);


        boolean configDatastorePartitionBasedOnUsername = configuration.getBoolean(CORE_CONFIG_DATASTORE_PARTITION_BASED_ON_USERNAME, CORE_CONFIG_DATASTORE_PARTITION_BASED_ON_USERNAME_DEFAULT_VALUE);

        log.debug(" configure : Datastore partitions based on username : {}", configDatastorePartitionBasedOnUsername );

        setDatastorePartitionBasedOnUsername(configDatastorePartitionBasedOnUsername);



        String configDatastoreClassName = configuration.getString(CORE_CONFIG_ENGINE_DATASTORE_CLASS_NAME, CORE_CONFIG_ENGINE_DATASTORE_CLASS_NAME_DEFAULT_VALUE);

        log.debug(" configure : The datastore class to be loaded by the system is {}", configDatastoreClassName);

        setDatastoreClassName(configDatastoreClassName);

        super.configure(configuration);

    }


}
