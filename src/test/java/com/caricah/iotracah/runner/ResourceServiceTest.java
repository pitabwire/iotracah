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

package com.caricah.iotracah.runner;

import com.caricah.iotracah.system.handler.impl.BaseTestClass;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/9/15
 */
public class ResourceServiceTest extends BaseTestClass {


    @Override
    public void internalSetUp() throws Exception {

    }

    @Override
    public void internalTearDown() throws Exception {

    }

    @Test
    public void testGetConfiguration() throws Exception {
        ResourceService resourceService = new ResourceServiceImpl();

        assertNull(resourceService.getConfiguration());


    }

    @Test
    public void testSetConfiguration() throws Exception {

        Configuration configuration = Mockito.mock(Configuration.class);
        ResourceService resourceService = new ResourceServiceImpl();

        resourceService.setConfiguration(configuration);
        assertEquals(configuration, resourceService.getConfiguration());
    }

        @Test
    public void testGetConfigurationSetLoader() throws Exception {

        ResourceService resourceService = new ResourceServiceImpl();

        assertNotNull(resourceService.getConfigurationSetLoader());

    }

    @Test
    public void testGetLogSetLoader() throws Exception {

        ResourceService resourceService = new ResourceServiceImpl();

        assertNotNull(resourceService.getLogSetLoader());
    }

    @Test
    public void testGetSystemBaseSetLoader() throws Exception {

        ResourceService resourceService = new ResourceServiceImpl();

        assertNotNull(resourceService.getSystemBaseSetLoader());
    }




    private class ResourceServiceImpl extends ResourceService{

    }
}