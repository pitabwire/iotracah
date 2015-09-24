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
import com.caricah.iotracah.core.modules.Server;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.modules.base.server.DefaultServerRouter;
import com.caricah.iotracah.core.modules.base.server.ServerRouter;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.system.BaseSystemHandler;
import com.caricah.iotracah.system.SystemInitializer;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.GridSlf4jLogger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * <code>WorkersInitializer</code> Handler for initializing base worker
 * plugins.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/11/15
 */
public abstract class ServersInitializer implements SystemInitializer {

    public final Logger log = LoggerFactory.getLogger(this.getClass());

    public static final String CORE_CONFIG_ENGINE_SERVER_IS_ENABLED = "core.config.engine.server.is.enabled";
    public static final boolean CORE_CONFIG_ENGINE_SERVER_IS_ENABLED_DEFAULT_VALUE = true;

    public static final String CORE_CONFIG_ENGINE_EXCECUTOR_IS_CLUSTER_SEPARATED = "core.config.engine.excecutor.is.cluster.separated";
    public static final boolean CORE_CONFIG_ENGINE_EXCECUTOR_IS_CLUSTER_SEPARATED_DEFAULT_VALUE = false;

    public static final String CORE_CONFIG_DEFAULT_ENGINE_EXCECUTOR_NAME = "core.config.engine.excecutor.default.name";
    public static final String CORE_CONFIG_DEFAULT_ENGINE_EXCECUTOR_NAME_DEFAULT_VALUE = "iotracah-cluster";

    public static final String CORE_CONFIG_ENGINE_EXCECUTOR_SERVER_NAME = "core.config.engine.excecutor.server.name";

    public static final String CORE_CONFIG_ENGINE_EXCECUTOR_WORKER_NAME = "core.config.engine.excecutor.worker.name";

    public static final String CORE_CONFIG_ENGINE_EXCECUTOR_DATASTORE_NAME = "core.config.engine.excecutor.datastore.name";

    public static final String CORE_CONFIG_ENGINE_EXCECUTOR_EVENT_NAME = "core.config.engine.excecutor.event.name";

    public static final String CORE_CONFIG_ENGINE_CLUSTER_DISCOVERY_ADDRESSES = "core.config.engine.cluster.discovery.addresses";

    private boolean serverEngineEnabled;

    private boolean excecutorClusterSeparated;

    private String excecutorDefaultName;

    private String excecutorServerName;

    private String excecutorWorkerName;

    private String excecutorDatastoreName;

    private String excecutorEventerName;

    private String[] discoveryAddresses;

    private List<Subscription> rxSubscriptionList = new ArrayList<>();

    public boolean isServerEngineEnabled() {
        return serverEngineEnabled;
    }

    public void setServerEngineEnabled(boolean serverEngineEnabled) {
        this.serverEngineEnabled = serverEngineEnabled;
    }

    public boolean isExcecutorClusterSeparated() {
        return excecutorClusterSeparated;
    }

    public void setExcecutorClusterSeparated(boolean excecutorClusterSeparated) {
        this.excecutorClusterSeparated = excecutorClusterSeparated;
    }

    public String getExcecutorDefaultName() {
        return excecutorDefaultName;
    }

    public void setExcecutorDefaultName(String excecutorDefaultName) {
        this.excecutorDefaultName = excecutorDefaultName;
    }

    public String getExcecutorServerName() {
        return excecutorServerName;
    }

    public void setExcecutorServerName(String excecutorServerName) {
        this.excecutorServerName = excecutorServerName;
    }

    public String getExcecutorWorkerName() {
        return excecutorWorkerName;
    }

    public void setExcecutorWorkerName(String excecutorWorkerName) {
        this.excecutorWorkerName = excecutorWorkerName;
    }

    public String getExcecutorDatastoreName() {
        return excecutorDatastoreName;
    }

    public void setExcecutorDatastoreName(String excecutorDatastoreName) {
        this.excecutorDatastoreName = excecutorDatastoreName;
    }

    public String getExcecutorEventerName() {
        return excecutorEventerName;
    }

    public void setExcecutorEventerName(String excecutorEventerName) {
        this.excecutorEventerName = excecutorEventerName;
    }

    public String[] getDiscoveryAddresses() {
        return discoveryAddresses;
    }

    public void setDiscoveryAddresses(String[] discoveryAddresses) {
        this.discoveryAddresses = discoveryAddresses;
    }

    private List<Server> serverList = new ArrayList<>();

    public List<Server> getServerList() {
        return serverList;
    }

    public List<Subscription> getRxSubscriptionList() {
        return rxSubscriptionList;
    }


    public void startServers() throws UnRetriableException {

        if (isServerEngineEnabled() && getServerList().isEmpty()) {
            log.warn(" startServers : List of server plugins is empty");
            throw new UnRetriableException(" System expects atleast one server plugin to be configured.");
        }

        log.debug(" startServers : Starting the system servers");

        for (Server server : getServerList()) {

            //Link server observable to .
            subscribeObserverToAnObservable(server, getServerRouter());

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

    protected void classifyBaseHandler(BaseSystemHandler baseSystemHandler) {

        if (baseSystemHandler instanceof Server) {
            log.debug(" classifyBaseHandler : found the server {}", baseSystemHandler);


            if (isServerEngineEnabled()) {

                log.info(" classifyBaseHandler : storing the server : {} for use as active plugin", baseSystemHandler);
                serverList.add((Server) baseSystemHandler);
            } else {
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


        boolean configWorkerEnabled = configuration.getBoolean(CORE_CONFIG_ENGINE_SERVER_IS_ENABLED, CORE_CONFIG_ENGINE_SERVER_IS_ENABLED_DEFAULT_VALUE);

        log.debug(" configure : The server function is configured to be enabled [{}]", configWorkerEnabled);

        setServerEngineEnabled(configWorkerEnabled);

        String excecutorName = configuration.getString(CORE_CONFIG_DEFAULT_ENGINE_EXCECUTOR_NAME, CORE_CONFIG_DEFAULT_ENGINE_EXCECUTOR_NAME_DEFAULT_VALUE);
        setExcecutorDefaultName(excecutorName);


        excecutorName = configuration.getString(CORE_CONFIG_ENGINE_EXCECUTOR_EVENT_NAME, getExcecutorDefaultName());
        setExcecutorEventerName(excecutorName);

        excecutorName = configuration.getString(CORE_CONFIG_ENGINE_EXCECUTOR_DATASTORE_NAME, getExcecutorDefaultName());
        setExcecutorDatastoreName(excecutorName);

        excecutorName = configuration.getString(CORE_CONFIG_ENGINE_EXCECUTOR_WORKER_NAME, getExcecutorDefaultName());
        setExcecutorWorkerName(excecutorName);

        excecutorName = configuration.getString(CORE_CONFIG_ENGINE_EXCECUTOR_SERVER_NAME, getExcecutorDefaultName());
        setExcecutorServerName(excecutorName);

        boolean excecutorClusterSeparated = configuration.getBoolean(CORE_CONFIG_ENGINE_EXCECUTOR_IS_CLUSTER_SEPARATED, CORE_CONFIG_ENGINE_EXCECUTOR_IS_CLUSTER_SEPARATED_DEFAULT_VALUE);
        setExcecutorClusterSeparated(excecutorClusterSeparated);

        String[] discoveryAddresses = configuration.getStringArray(CORE_CONFIG_ENGINE_CLUSTER_DISCOVERY_ADDRESSES);
        setDiscoveryAddresses(discoveryAddresses);
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

        //Initiate ignite.

            synchronized (this) {

                TcpDiscoverySpi spi = new TcpDiscoverySpi();
                TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

                // Set initial IP addresses.
                // Note that you can optionally specify a port or a port range.
                ipFinder.setAddresses(Arrays.asList(getDiscoveryAddresses()));

                spi.setIpFinder(ipFinder);

                IgniteConfiguration cfg = new IgniteConfiguration();
                cfg.setGridName(getExcecutorDefaultName());

                // Override default discovery SPI.
                cfg.setDiscoverySpi(spi);

                GridSlf4jLogger gridLog = new GridSlf4jLogger(LoggerFactory.getLogger("org.apache.ignite")); // Provide correct SLF4J logger here.

                cfg.setGridLogger(gridLog);

                Ignition.start(cfg);

                //Also instantiate the server router.
                IgniteMessaging igniteMessaging = getIgnite().message();
                this.serverRouter = new DefaultServerRouter(igniteMessaging);
            }


        log.debug(" systemInitialize : performing classifications for base system handlers");
        baseSystemHandlerList.forEach(this::classifyBaseHandler);
    }


    /**
     * <code>subscribeObserverToObservables</code> takes in a list of observables and uses
     * it appropriately to push or link data down to the appropriate subscribers.
     *  @param subscriber
     * @param observableOnSubscribers
     */
    protected void subscribeObserverToObservables(Subscriber<IOTMessage> subscriber, List observableOnSubscribers) {


        for( Object observableOnSubscriberObject: observableOnSubscribers){

            Observable.OnSubscribe<IOTMessage> observableOnSubscriber = (Observable.OnSubscribe<IOTMessage>) observableOnSubscriberObject;

            Subscription subscription = subscribeObserverToAnObservable(subscriber, observableOnSubscriber);

            getRxSubscriptionList().add(subscription);
        }
    }
        /**
         * <code>subscribeObserverToObservables</code> is the secret weapon for automatic pushing
         * of requests to the appropriate nodes for specialized execution.
         * Worker requests are expected to be handled on worker nodes.
         * Events occur on event nodes.
         *
         * @param subscriber
         */
    protected Subscription subscribeObserverToAnObservable(Subscriber<IOTMessage> subscriber, Observable.OnSubscribe<IOTMessage> observableOnSubscriber) {

            log.info(" subscribeObserverToAnObservable : {} subscribing to {} for updates.", subscriber, observableOnSubscriber);

            Observable<IOTMessage> observable = Observable.create(observableOnSubscriber);

            //The schedular obtained allows for processing of data to be sent to the
            //correct excecutor groups.

            //Scheduler scheduler = Schedulers.from(getExcecutor(observableOnSubscriber));
            return observable
                    .onBackpressureBuffer()
                    .subscribeOn(Schedulers.io())
                    .subscribe(subscriber);

    }


    private Executor getExcecutor(Object observableOnSubscriber) {

        //Note: since ignite expects a runnable that is serializable we will deffer
        // Work on distributing the load to a cluster separated by functions.
        // And simply use the io scheduler.

        ClusterGroup executionGrp;

        if (isExcecutorClusterSeparated()) {

            if (observableOnSubscriber instanceof Server) {

                // Cluster group for nodes where the attribute 'worker' is defined.
                executionGrp = getIgnite().cluster().forAttribute("ROLE", getExcecutorServerName());

            } else if (observableOnSubscriber instanceof Worker || observableOnSubscriber instanceof ServerRouter) {

                executionGrp = getIgnite().cluster().forAttribute("ROLE", getExcecutorWorkerName());

            } else if (observableOnSubscriber instanceof Eventer) {

                executionGrp = getIgnite().cluster().forAttribute("ROLE", getExcecutorEventerName());
            } else if( observableOnSubscriber instanceof Datastore) {

                executionGrp = getIgnite().cluster().forAttribute("ROLE", getExcecutorDatastoreName());

            }else{
                executionGrp = getIgnite().cluster().forAttribute("ROLE", getExcecutorDefaultName());
            }
        } else {

            executionGrp = getIgnite().cluster().forAttribute("ROLE", getExcecutorDefaultName());
        }

        return getIgnite().executorService(executionGrp);
    }




    public final Ignite getIgnite() {
        return Ignition.ignite(getExcecutorDefaultName());
    }


    private ServerRouter serverRouter;

    public ServerRouter getServerRouter() {

        return serverRouter;
    }
}
