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

package com.caricah.iotracah.bootstrap.system.handler;

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import org.apache.commons.configuration.Configuration;

import java.io.File;

/**
 * Standard way that implementations providing configurations
 * Work. The system will supplies whatever configurations it already has and
 * expects the implementation to populate it in an additive way and
 * return the sum total.
 *
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/8/15
 */
public interface ConfigHandler {


    String SYSTEM_CONFIG_CONFIGURATION_FILE_NAME_DEFAULT_VALUE = "iotracah.properties";

    String DEFAULT_CONFIG_DIRECTORY = ".."+ File.separator+"conf";

    /**
     *
     * All system configurations providers are loaded via spi
     * and are given the configurations the system already has.
     * They are further expected to provide their configurations in
     * and additive way. Since the order of loading the configs is not
     * guranteed all the setting keys should be uniquely identified
     * and that task is left to the implementations to enforce.
     *
     * @param configuration
     * @return
     * @throws UnRetriableException
     */

    Configuration populateConfiguration(Configuration configuration) throws UnRetriableException;
}
