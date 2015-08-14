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
import com.caricah.iotracah.core.modules.Server;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.system.BaseSystemHandler;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>WorkersInitializer</code> Handler for initializing base worker
 * plugins.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/11/15
 */
public abstract class WorkersInitializer extends ServersInitializer {


    public static final String CORE_CONFIG_LOGGING_WORKER_ENGINE_IS_ENABLED = "core.config.logging.worker.engine.is.enabled";
    public static final boolean CORE_CONFIG_LOGGING_WORKER_ENGINE_IS_ENABLED_DEFAULT_VALUE = true;

    private boolean workerEngineEnabled;

    public boolean isServerEngineEnabled() {
        return workerEngineEnabled;
    }

    public void setServerEngineEnabled(boolean workerEngineEnabled) {
        this.workerEngineEnabled = workerEngineEnabled;
    }

    private List<Worker> workerList = new ArrayList<>();

    public List<Worker> getWorkerList() {
        return workerList;
    }


    /**
     * <code>startWorkers</code> Any kind of worker can be loaded on to the system.
     * This can allow for the same request to be processed in two different ways.
     * How that is usefull now is left to the person actually executing such an implementation
     * The design leaves room for creativity upto the core of IOTracah.
     *
     * Beyond multiple workers, normall operations of the system would be expected to be with
     * the default workers only. This way a true mqtt broker is allowed to be built.
     *
     * The workers expect to be pushed data from servers they handle it appropriately producing
     * all the relevant events then they return a response by pushing it to the server layer.
     *
     * @throws UnRetriableException
     */
    public void startWorkers() throws UnRetriableException {

        for (Worker worker : getWorkerList()) {
            //Link worker observable to servers.
            subscribeObserverToObservables(worker, getServerList());

            //Actually start our worker guy.
            worker.initiate();
        }


        //This has to happen at this point because the workers can not be gotten
        //Atleast directly from the servers initializer.
        for(Server server: getServerList()){
                //Link server observable to workers.
                subscribeObserverToObservables(server, getWorkerList());
        }

    }

    public void classifyBaseHandler(BaseSystemHandler baseSystemHandler){

        if(baseSystemHandler instanceof Worker){

            log.debug(" classifyBaseHandler : found the worker {}", baseSystemHandler);

            if(isServerEngineEnabled()){
                log.info(" classifyBaseHandler : storing the worker : {} for use as active plugin", baseSystemHandler);
            workerList.add((Worker) baseSystemHandler);
            } else {
                log.info(" classifyBaseHandler : worker {} is disabled ", baseSystemHandler);
            }
        }
        else{
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


        boolean configWorkerEnabled = configuration.getBoolean(CORE_CONFIG_LOGGING_WORKER_ENGINE_IS_ENABLED, CORE_CONFIG_LOGGING_WORKER_ENGINE_IS_ENABLED_DEFAULT_VALUE);

        setServerEngineEnabled(configWorkerEnabled);

    }


}
