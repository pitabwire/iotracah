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

package com.caricah.iotracah.system.handler;

import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.commons.configuration.Configuration;

import java.io.File;

/**
 *
 * The log handler allows the system to configure the logging system.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/9/15
 */
public interface LogHandler {

    String SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE = "system.config.logging.log.config.file";
    String SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE = "log4j.properties";

    String SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY = "system.config.logging.log.config.directory";
    String SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY_DEFAULT_VALUE = "";

    String DEFAULT_CONFIG_DIRECTORY = ".."+ File.separator+"conf";



    /**
     * <code>configure</code> Allows the system to supply configurations
     * from other modules and use these settings to configure the logging system.
     * It is upto the implementation to ensure the settings it expects are supplied
     * in the configuration by populating the necessary config settings.
     *
     * @param configuration
     * @throws UnRetriableException
     */
    void configure(Configuration configuration) throws UnRetriableException;
}
