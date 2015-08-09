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

import com.caricah.iotracah.system.handler.ConfigHandler;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.commons.configuration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.*;
import java.util.Locale;

/**
 *
 * The default system implementation for the configuration handler.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/8/15
 */
public class DefaultConfigHandler implements ConfigHandler {


    private static final Logger log = LoggerFactory.getLogger(DefaultConfigHandler.class);

    private final String configurationDirectory;
    private final String configurationFileName;
    public DefaultConfigHandler(){
        this( "" );
    }
    public DefaultConfigHandler(String configurationDirectory){
        this(configurationDirectory,SYSTEM_CONFIG_CONFIGURATION_FILE_NAME_DEFAULT_VALUE);
    }

    public DefaultConfigHandler(String configurationDirectory, String configurationFileName){
        this.configurationDirectory = configurationDirectory;
        this.configurationFileName = configurationFileName;
    }

    public String getConfigurationDirectory() {
        return configurationDirectory;
    }

    public String getConfigurationFileName() {
        return configurationFileName;
    }

    private Path getConfigurationFileInClassPath(String directory) throws IOException {

        PathMatcher matcher =
                FileSystems.getDefault().getPathMatcher(String.format(Locale.US, "glob:*%s", getConfigurationFileName()));

        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(directory));
        for (Path path : directoryStream) {

            if (matcher.matches(path)) {
                return path;
            }
        }
        return null;
    }


    /**
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
    @Override
    public Configuration populateConfiguration(Configuration configuration) throws UnRetriableException {

        try {
            Path configurationFile = getConfigurationFileInClassPath(getConfigurationDirectory());

            if (null == configurationFile) {

                ClassLoader classLoader = getClass().getClassLoader();

                URL configurationResource = classLoader.getResource(getConfigurationFileName());
                if (null != configurationResource) {
                    configurationFile = new File(configurationResource.getFile()).toPath();
                }

            }

            if (null != configurationFile) {


                if(configuration instanceof CompositeConfiguration)
                {
                    ((CompositeConfiguration)configuration).addConfiguration(new PropertiesConfiguration(configurationFile.toFile()));
                    return configuration;
                }else{
                    CompositeConfiguration compositeConfiguration = new CompositeConfiguration();

                    if(null != configuration) {
                        compositeConfiguration.addConfiguration(configuration);
                    }

                    compositeConfiguration.addConfiguration(new PropertiesConfiguration(configurationFile.toFile()));
                    return compositeConfiguration;
                }

            }

        } catch (IOException | ConfigurationException e) {
            log.error(" getConfiguration : ", e);
            throw new UnRetriableException(e);
        }

        throw new UnRetriableException(" There was a problem getting configuration files.");
    }



}
