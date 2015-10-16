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

package com.caricah.iotracah.system.handler.impl;

import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.system.handler.LogHandler;
import com.caricah.iotracah.system.ResourceFileUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/9/15
 */
public class DefaultLogHandler implements LogHandler {


    private static final Logger log = LoggerFactory.getLogger(DefaultLogHandler.class);


    @Override
    public void configure(Configuration configuration) throws UnRetriableException {



            String logsConfigFile = SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE;
            String logsConfigDirectory = System.getProperty("iotracah.default.path.conf");

            if (null != configuration) {
                logsConfigFile = configuration.getString(SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE, SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE);
                logsConfigDirectory = configuration.getString(SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY, logsConfigDirectory);

            }
            String logsConfigFilePath = logsConfigDirectory + File.separator + logsConfigFile;

        log.debug(" configure : path to logging configs is {} .", logsConfigFilePath);

        File logConfigurationFile = new File(logsConfigFilePath);

            if (!logConfigurationFile.exists()) {

                logConfigurationFile = ResourceFileUtil.getFileFromResource(getClass(), logsConfigFile);

            }

        log.debug(" configure : File with logging configs is {} .", logConfigurationFile);


        PropertyConfigurator.configure(logConfigurationFile.getAbsolutePath());



    }
}
