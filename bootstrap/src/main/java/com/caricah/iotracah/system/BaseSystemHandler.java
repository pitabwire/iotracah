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

import java.io.Serializable;

/**
 *
 * All plugins that perform/interact with client related work are
 * started from this point. <code>BaseSystemHandler</code> plugins
 * provide bindings to configure, initiate and terminate the plugin.
 *
 * The initialization process is systematic and ordered as defined within the implementations in
 * the core module. The order is maintained during the startup stage and
 * reversed during termination.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/8/15
 */
public interface BaseSystemHandler extends Comparable<BaseSystemHandler>, Serializable{

    /**
     * <code>configure</code> allows the base system to configure itself by getting
     * all the settings it requires and storing them internally. The plugin is only expected to
     * pick the settings it has registered on the configuration file for its particular use.
     * @param configuration
     * @throws UnRetriableException
     */
    void configure(Configuration configuration) throws UnRetriableException;

    /**
     * <code>initiate</code> starts the operations of this system handler.
     * All excecution code for the plugins is expected to begin at this point.
     *
     * @throws UnRetriableException
     */
    void initiate() throws UnRetriableException;

    /**
     * <code>terminate</code> halts excecution of this plugin.
     * This provides a clean way to exit /stop operations of this particular plugin.
     */
    void terminate();
}
