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

package com.caricah.iotracah.system;

import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.commons.configuration.Configuration;

import java.util.List;

/**
 *
 * <code>SystemInitializer</code> is only responsible for starting
 * the base system handlers that are known to have been configured.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/11/15
 */
public interface SystemInitializer {

    /**
     * <code>configure</code> allows the initializer to configure its self
     * Depending on the implementation conditional operation can be allowed
     * So as to make the system instance more specialized.
     *
     * For example: via the configurations the implementation may decide to
     * shutdown backend services and it just works as a server application to receive
     * and route requests to the workers which are in turn connected to the backend/datastore servers...
     *
     * @param configuration
     * @throws UnRetriableException
     */

    void configure(Configuration configuration) throws UnRetriableException;
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
    void systemInitialize(List<BaseSystemHandler> baseSystemHandlerList) throws UnRetriableException;
}
