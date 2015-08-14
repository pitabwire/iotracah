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

import com.caricah.iotracah.core.messaging.IOTMessage;
import com.caricah.iotracah.core.modules.Server;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.system.BaseSystemHandler;
import com.caricah.iotracah.system.SystemInitializer;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>WorkersInitializer</code> Handler for initializing base worker
 * plugins.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/11/15
 */
public abstract class ServersInitializer implements SystemInitializer {

    public final Logger log = LoggerFactory.getLogger(this.getClass());

    public static final String CORE_CONFIG_LOGGING_SERVER_ENGINE_IS_ENABLED = "core.config.logging.server.engine.is.enabled";
    public static final boolean CORE_CONFIG_LOGGING_SERVER_ENGINE_IS_ENABLED_DEFAULT_VALUE = true;

    private boolean serverEngineEnabled;

    public boolean isServerEngineEnabled() {
        return serverEngineEnabled;
    }

    public void setServerEngineEnabled(boolean serverEngineEnabled) {
        this.serverEngineEnabled = serverEngineEnabled;
    }

    private List<Server> serverList = new ArrayList<>();

    public List<Server> getServerList() {
        return serverList;
    }

    public void startServers() throws UnRetriableException {

        for (Server server : getServerList()) {
            //Actually just start our server guy.
            server.initiate();
        }
    }


    /**
     * <code>classifyBaseHandler</code> separates the base system handler plugins
     * based on their functionality in terms of eventers, datastores, servers or workers.
     *
     * @param baseSystemHandler
     */

    public void classifyBaseHandler(BaseSystemHandler baseSystemHandler){



        if(baseSystemHandler instanceof Server) {
            log.debug(" classifyBaseHandler : found the server {}", baseSystemHandler);


            if (isServerEngineEnabled()){

                log.info(" classifyBaseHandler : storing the server : {} for use as active plugin", baseSystemHandler);
                serverList.add((Server) baseSystemHandler);}else {
                log.info(" classifyBaseHandler : server {} is disabled ", baseSystemHandler);
            }
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


        boolean configWorkerEnabled = configuration.getBoolean(CORE_CONFIG_LOGGING_SERVER_ENGINE_IS_ENABLED, CORE_CONFIG_LOGGING_SERVER_ENGINE_IS_ENABLED_DEFAULT_VALUE);

        setServerEngineEnabled(configWorkerEnabled);

    }


    /**
     * <code>systemInitialize</code> is the entry method to start all the operations within iotracah.
     * This class is expected to be extended by the core plugin only.
     * If there is more than that implementation then the first implementation with no
     * guarantee of selection will be loaded.
     * The system will automatically throw a <code>UnRetriableException</code> if no implementation of this
     * interface is detected.
     *
     * @param baseSystemHandlerList
     * @throws UnRetriableException
     */
    @Override
    public void systemInitialize(List<BaseSystemHandler> baseSystemHandlerList) throws UnRetriableException {

        log.debug(" systemInitialize : performing classifications for base system handlers");
        baseSystemHandlerList.forEach(this::classifyBaseHandler);

    }


    /**
     * <code>subscribeObserverToObservables</code> is the secret weapon for automatic pushing
     * of requests to the appropriate nodes for specialized execution.
     * Worker requests are expected to be handled on worker nodes.
     * Events occur on event nodes.
     *
     * @param subscriber
     * @param observableOnSubscribers
     */
    protected void subscribeObserverToObservables(Subscriber<IOTMessage> subscriber, List observableOnSubscribers) {


        for (Object observableOnSubscriber: observableOnSubscribers){

            Observable<IOTMessage> observable = Observable.create((Observable.OnSubscribe<IOTMessage>) observableOnSubscriber);

            //The schedular obtained allows for processing of data to be sent to the
            //correct excecutor groups.

            //TODO: use a cluster specific executor.
            Scheduler scheduler = Schedulers.computation();
            observable.subscribeOn(scheduler).subscribe(subscriber);

        }



    }


}
