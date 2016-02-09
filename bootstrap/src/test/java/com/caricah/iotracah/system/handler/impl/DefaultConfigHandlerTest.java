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

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.system.handler.impl.DefaultConfigHandler;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;

import org.junit.Test;


import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;



/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/8/15
 */
public class DefaultConfigHandlerTest extends BaseTestClass {

    private String testDirectory = "";
    private String testFileForExcecutionDir = "test.iotracah.properties";
    private String testFileForResourcesPackage = "test1.iotracah.properties";
    private String testFileWithWrongContents = "test2.iotracah.properties";
    private String testFileThatIsNonExistant = "test3.iotracah.properties";
    private CompositeConfiguration systemConfig;


    @Override
    public void internalSetUp() throws Exception {

        systemConfig = new CompositeConfiguration();
        systemConfig.addConfiguration(new SystemConfiguration());

        List<String> goodProps = Arrays.asList("prop1=life", "prop2=life");
        List<String> badProps = Arrays.asList("1st line", "= werwer", "one_in=a_million");


        try {
            Files.write(Paths.get(testFileForExcecutionDir), goodProps, StandardCharsets.UTF_8);
            Files.write(Paths.get(testFileWithWrongContents), badProps, StandardCharsets.UTF_8);


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void internalTearDown() throws Exception {


        Files.delete(Paths.get(testFileForExcecutionDir));
        Files.delete(Paths.get(testFileWithWrongContents));
    }

    @Test
    public void testPopulateConfiguration() throws Exception {

        DefaultConfigHandler configurationFactory = new DefaultConfigHandler();
        Configuration configuration = configurationFactory.populateConfiguration(systemConfig);
        assertNotNull(configuration);

    }

    @Test
    public void testPopulateConfigurationInDirectory() throws Exception {

        DefaultConfigHandler configurationFactory = new DefaultConfigHandler(testDirectory, testFileForExcecutionDir);
        Configuration configuration = configurationFactory.populateConfiguration(systemConfig);

        String property1 = configuration.getString("prop1");
        String property2 = configuration.getString("prop2");
        assertEquals("life", property1);
        assertEquals("life", property2);

    }

    @Test
    public void testPopulateConfigurationWithNullParameter() throws Exception {
        DefaultConfigHandler configurationFactory = new DefaultConfigHandler(testDirectory, testFileForResourcesPackage);
        Configuration configuration = configurationFactory.populateConfiguration(null);
        assertNotNull(configuration);
    }

    @Test
    public void testPopulateConfigurationInClassPath() throws Exception {
        DefaultConfigHandler configurationFactory = new DefaultConfigHandler(testDirectory, testFileForResourcesPackage);
        Configuration configuration = configurationFactory.populateConfiguration(systemConfig);
        assertNotNull(configuration);
    }

    @Test
    public void testPopulateConfigurationMissing() throws Exception {

        exception.expect(UnRetriableException.class);
        DefaultConfigHandler configurationFactory = new DefaultConfigHandler(testDirectory, testFileThatIsNonExistant);
        Configuration configuration = configurationFactory.populateConfiguration(systemConfig);

    }

    @Test
    public void testPopulateConfigurationMissingDirectory() throws UnRetriableException {

        exception.expect(UnRetriableException.class);
        DefaultConfigHandler configurationFactory = new DefaultConfigHandler("world/over");
        Configuration configuration = configurationFactory.populateConfiguration(systemConfig);
    }


    @Test
    public void testPopulateConfigurationFileWithBadContent() throws Exception {

        DefaultConfigHandler configurationFactory = new DefaultConfigHandler(testDirectory, testFileWithWrongContents);
        Configuration configuration = configurationFactory.populateConfiguration(systemConfig);

        String mende = configuration.getString("one_in");
        assertEquals("a_million", mende);
    }

}