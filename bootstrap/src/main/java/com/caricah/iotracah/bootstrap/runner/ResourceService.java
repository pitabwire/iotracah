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

package com.caricah.iotracah.bootstrap.runner;

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.system.handler.ConfigHandler;
import com.caricah.iotracah.bootstrap.system.SystemInitializer;
import com.caricah.iotracah.bootstrap.system.BaseSystemHandler;
import com.caricah.iotracah.bootstrap.system.handler.LogHandler;
import org.apache.commons.configuration.Configuration;

import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/8/15
 */
public abstract class ResourceService {


    private Configuration configuration;

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public ServiceLoader<ConfigHandler> getConfigurationSetLoader(){

        return ServiceLoader.load(ConfigHandler.class);
    }

    public ServiceLoader<LogHandler> getLogSetLoader(){

        return ServiceLoader.load(LogHandler.class);
    }

   public List<BaseSystemHandler> getSystemBaseSetLoader(){

        List<BaseSystemHandler> listBaseSystemHandler = new ArrayList<>();
        for (BaseSystemHandler baseSystemHandler: ServiceLoader.load(BaseSystemHandler.class))
            listBaseSystemHandler.add(baseSystemHandler);

       Collections.sort(listBaseSystemHandler);

       return listBaseSystemHandler;
   }


    public List<BaseSystemHandler> getReversedSystemBaseSetLoader() {

        List<BaseSystemHandler> listBaseSystemHandler = getSystemBaseSetLoader();

        Collections.reverse(listBaseSystemHandler);

        return listBaseSystemHandler;
    }


    public SystemInitializer getSystemInitializer() throws UnRetriableException {

        Iterator<SystemInitializer> systemInitializerIterator = ServiceLoader.load(SystemInitializer.class).iterator();

        if(systemInitializerIterator.hasNext())
            return systemInitializerIterator.next();
        else
            throw new UnRetriableException("A plugin supplying the system initializer: SystemInitializer is missing");
    }

    }
