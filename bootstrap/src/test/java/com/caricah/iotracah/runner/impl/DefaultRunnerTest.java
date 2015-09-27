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

package com.caricah.iotracah.runner.impl;

import com.caricah.iotracah.system.SystemInitializer;
import com.caricah.iotracah.system.handler.impl.BaseTestClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/9/15
 */
public class DefaultRunnerTest extends BaseTestClass {

    @Test
    public void testInfiniteWait() throws Exception{

        CountDownLatch countDownLatch = Mockito.mock(CountDownLatch.class);
        DefaultRunner defaultRunner = Mockito.spy(new DefaultRunner());
        Mockito.when(defaultRunner.get_latch()).thenReturn(countDownLatch);


        defaultRunner.infiniteWait();


        Mockito.verify(countDownLatch, new Times(1)).await();

    }

    @Test
   public void testStopInfiniteWait() throws Exception{

       CountDownLatch countDownLatch = Mockito.mock(CountDownLatch.class);
       DefaultRunner defaultRunner = Mockito.spy(new DefaultRunner());
       Mockito.when(defaultRunner.get_latch()).thenReturn(countDownLatch);

       defaultRunner.stopInfiniteWait();

       Mockito.verify(countDownLatch, new Times(1)).countDown();

   }


    @Test
    public void testInit() throws Exception {
        DefaultRunner defaultRunner = Mockito.spy(new DefaultRunner());
        defaultRunner.init();


        Mockito.verify(defaultRunner, new Times(1)).getConfigurationSetLoader();
        Mockito.verify(defaultRunner, new Times(1)).getLogSetLoader();
    }

    @Test
    public void testStart() throws Exception {

        CountDownLatch countDownLatch = Mockito.mock(CountDownLatch.class);

        DefaultRunner defaultRunner = Mockito.spy(new DefaultRunner());
        Mockito.when(defaultRunner.get_latch()).thenReturn(countDownLatch);

        Mockito.doNothing().when(defaultRunner).infiniteWait();

        SystemInitializer systemInitializer = Mockito.mock(SystemInitializer.class);
        Mockito.doReturn(systemInitializer).when(defaultRunner).getSystemInitializer();

        defaultRunner.start();

        Mockito.verify(defaultRunner, new Times(1)).getSystemBaseSetLoader();
        Mockito.verify(defaultRunner, new Times(1)).infiniteWait();

    }

    @Test
    public void testTerminate() throws Exception {

        DefaultRunner defaultRunner = Mockito.spy(new DefaultRunner());
        defaultRunner.terminate();

        Mockito.verify(defaultRunner, new Times(1)).getSystemBaseSetLoader();

    }

    @Override
    public void internalSetUp() throws Exception {

    }

    @Override
    public void internalTearDown() throws Exception {

    }
}