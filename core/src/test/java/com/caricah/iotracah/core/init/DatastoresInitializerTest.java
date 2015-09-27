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

import com.caricah.iotracah.core.DefaultSystemInitializer;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.system.handler.impl.BaseTestClass;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/16/15
 */
public class DatastoresInitializerTest extends BaseTestClass {



    @Test
    public void testIsDatastoreEngineEnabled() throws Exception {

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();

        assertEquals( false, defaultSystemInitializer.isDatastoreEngineEnabled());

        defaultSystemInitializer.setDatastoreEngineEnabled(true);
        assertEquals(true, defaultSystemInitializer.isDatastoreEngineEnabled());


    }

    @Test
    public void testGetDatastoreList() throws Exception {

        Datastore mockDatastore = Mockito.mock(Datastore.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.setDatastoreEngineEnabled(true);
        defaultSystemInitializer.classifyBaseHandler(mockDatastore);

        assertEquals(false, defaultSystemInitializer.getDatastoreList().isEmpty());
    }

    @Test
    public void testGetActiveDatastore() throws Exception {

        Datastore mockDatastore = Mockito.mock(Datastore.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.setActiveDatastore(mockDatastore);

        assertEquals(mockDatastore, defaultSystemInitializer.getActiveDatastore());



    }


    @Test
    public void testStartDataStores() throws Exception {

        Datastore mockDatastore = Mockito.mock(Datastore.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.setDatastoreEngineEnabled(true);
        defaultSystemInitializer.classifyBaseHandler(mockDatastore);

        defaultSystemInitializer.setDatastoreClassName(mockDatastore.getClass().getName());
        defaultSystemInitializer.startDataStores();

        Mockito.verify(mockDatastore, Mockito.times(1)).initiate();

        assertEquals(mockDatastore, defaultSystemInitializer.getActiveDatastore());
    }

    @Test
    public void testClassifyBaseHandler() throws Exception {

        Datastore mockDatastore = Mockito.mock(Datastore.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.setDatastoreEngineEnabled(true);
        defaultSystemInitializer.classifyBaseHandler(mockDatastore);

        assertEquals(1, defaultSystemInitializer.getDatastoreList().size());

    }

    @Test
    public void testConfigure() throws Exception {

        Configuration configuration = Mockito.mock(Configuration.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.configure(configuration);


        Mockito.verify(configuration, Mockito.times(1)).getBoolean(DatastoresInitializer.CORE_CONFIG_ENGINE_DATASTORE_IS_ENABLED, DatastoresInitializer.CORE_CONFIG_ENGINE_DATASTORE_IS_ENABLED_DEFAULT_VALUE);
        Mockito.verify(configuration, Mockito.times(1)).getString(DatastoresInitializer.CORE_CONFIG_ENGINE_DATASTORE_CLASS_NAME, DatastoresInitializer.CORE_CONFIG_ENGINE_DATASTORE_CLASS_NAME_DEFAULT_VALUE);

    }


    @Override
    public void internalSetUp() throws Exception {



    }

    @Override
    public void internalTearDown() throws Exception {

    }
}