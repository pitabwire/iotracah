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
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.init.base.BaseTestClass;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/16/15
 */
public class WorkersInitializerTest extends BaseTestClass {

    @Test
    public void testIsWorkerEngineEnabled() throws Exception {

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();

        assertEquals( false, defaultSystemInitializer.isWorkerEngineEnabled());

        defaultSystemInitializer.setWorkerEngineEnabled(true);
        assertEquals(true, defaultSystemInitializer.isWorkerEngineEnabled());
    }

    @Test
    public void testGetWorkerList() throws Exception {

        Worker mockWorker = Mockito.mock(Worker.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.setWorkerEngineEnabled(true);
        defaultSystemInitializer.classifyBaseHandler(mockWorker);

        assertEquals(false, defaultSystemInitializer.getWorkerList().isEmpty());

    }

    @Test
    public void testStartWorkers() throws Exception {


        Worker mockWorker = Mockito.mock(Worker.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.setWorkerEngineEnabled(true);
        defaultSystemInitializer.classifyBaseHandler(mockWorker);
        defaultSystemInitializer.startWorkers();

        Mockito.verify(mockWorker, Mockito.times(1)).initiate();
    }

    @Test
    public void testClassifyBaseHandler() throws Exception {

        Worker mockWorker = Mockito.mock(Worker.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.setWorkerEngineEnabled(true);
        defaultSystemInitializer.classifyBaseHandler(mockWorker);

        assertEquals(1, defaultSystemInitializer.getWorkerList().size());

    }

    @Test
    public void testConfigure() throws Exception {

        Configuration configuration = Mockito.mock(Configuration.class);

        DefaultSystemInitializer defaultSystemInitializer = new DefaultSystemInitializer();
        defaultSystemInitializer.configure(configuration);

        Mockito.verify(configuration, Mockito.times(1)).getBoolean(WorkersInitializer.CORE_CONFIG_ENGINE_WORKER_IS_ENABLED, WorkersInitializer.CORE_CONFIG_ENGINE_WORKER_IS_ENABLED_DEFAULT_VALUE);

    }

    @Override
    public void internalSetUp() throws Exception {

    }

    @Override
    public void internalTearDown() throws Exception {

    }
}