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

import com.caricah.iotracah.core.modules.Eventer;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.system.BaseSystemHandler;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>EventsInitializer</code> Handler for initializing base events handler.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/11/15
 */
public abstract class EventersInitializer  extends DatastoresInitializer{



    public static final String CORE_CONFIG_ENGINE_EVENT_IS_ENABLED = "core.config.engine.event.is.enabled";
    public static final boolean CORE_CONFIG_ENGINE_EVENT_IS_ENABLED_DEFAULT_VALUE = true;

    private boolean eventEngineEnabled;

    public boolean isEventEngineEnabled() {
        return eventEngineEnabled;
    }

    public void setEventEngineEnabled(boolean eventEngineEnabled) {
        this.eventEngineEnabled = eventEngineEnabled;
    }

    private List<Eventer> eventerList = new ArrayList<>();

    protected List<Eventer> getEventerList() {
        return eventerList;
    }

    /**
     * <code>startEventers</code> all eventers have to be started before
     * anything else. This allows any part of the system to send events data
     * log or metric or whatever appropriately.
     *
     *
     */
    protected void startEventers() throws UnRetriableException {

        log.debug(" startEventers : Starting all the system eventers");

        for (Eventer eventer : getEventerList()) {
            //Link datastore observable to eventers.
            subscribeObserverToObservables(eventer, getDatastoreList());
            subscribeObserverToObservables(eventer, getWorkerList());
            subscribeObserverToObservables(eventer, getServerList());

            //Actually start our guy.
            eventer.initiate();
        }

    }




    protected void classifyBaseHandler(BaseSystemHandler baseSystemHandler){

        if(baseSystemHandler instanceof Eventer) {

            log.debug(" classifyBaseHandler : found the eventer {}", baseSystemHandler);
            if (isEventEngineEnabled()) {
                log.info(" classifyBaseHandler : storing the eventer : {} for use as active plugin", baseSystemHandler);
                eventerList.add((Eventer) baseSystemHandler);
            } else {
                log.info(" classifyBaseHandler : eventer {} is disabled ", baseSystemHandler);
            }
        }else
            super.classifyBaseHandler(baseSystemHandler);
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


        boolean configEventsEnabled = configuration.getBoolean(CORE_CONFIG_ENGINE_EVENT_IS_ENABLED, CORE_CONFIG_ENGINE_EVENT_IS_ENABLED_DEFAULT_VALUE);

        log.debug(" configure : Eventer function is configured to be enabled [{}]", configEventsEnabled );
        setEventEngineEnabled(configEventsEnabled);

        super.configure(configuration);

    }





}
