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
import com.caricah.iotracah.bootstrap.system.handler.LogHandler;
import com.caricah.iotracah.bootstrap.system.handler.impl.DefaultLogHandler;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;


/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/9/15
 */
public class DefaultLogHandlerTest extends BaseTestClass {


    @Override
    public void internalSetUp() throws Exception {

    }

    @Override
    public void internalTearDown() throws Exception {

    }

    @Test
    public void testConfigure() throws Exception {



        Configuration configuration = Mockito.mock(Configuration.class);

        Mockito.when(configuration.getString(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE, LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE)).thenReturn(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE);
        Mockito.when(configuration.getString(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY, LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE)).thenReturn(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY_DEFAULT_VALUE);

        DefaultLogHandler defaultLogHandler = Mockito.spy(new DefaultLogHandler());
        defaultLogHandler.configure(configuration);

        Mockito.verify(configuration, new Times(1)).getString(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE, LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE);
        Mockito.verify(configuration, new Times(1)).getString(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY, LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY_DEFAULT_VALUE);

    }

    @Test
    public void testConfigureWithNoLoggingFileSetting() throws Exception {

        exception.expect(UnRetriableException.class);
        Configuration configuration = Mockito.mock(Configuration.class);
        Mockito.when(configuration.getString(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE, LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE)).thenReturn("alienLog4j.properties");
        Mockito.when(configuration.getString(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY, LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_FILE_DEFAULT_VALUE)).thenReturn(LogHandler.SYSTEM_CONFIG_LOGGING_LOG_CONFIG_DIRECTORY_DEFAULT_VALUE);

        DefaultLogHandler defaultLogHandler = new DefaultLogHandler();
        defaultLogHandler.configure(configuration);
    }

    @Test
    public void testConfigureWithNullConfigurations() throws Exception {

        DefaultLogHandler defaultLogHandler = new DefaultLogHandler();
        defaultLogHandler.configure(null);
    }


}