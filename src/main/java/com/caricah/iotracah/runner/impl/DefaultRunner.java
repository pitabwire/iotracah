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

import com.caricah.iotracah.system.handler.ConfigHandler;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.runner.ResourceService;
import com.caricah.iotracah.runner.Runner;
import com.caricah.iotracah.system.BaseSystemHandler;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/8/15
 */
public class DefaultRunner extends ResourceService implements Runner {


    private static final Logger log = LoggerFactory.getLogger(DefaultRunner.class);

    //This latch will be used to wait on the system
    private static CountDownLatch _latch = new CountDownLatch(1);




    private void infiniteWait(){

        log.debug(" infiniteWait : application entering an infinite wait state.");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
               terminate();
            }
        });

        try {
            _latch.await();
        } catch (InterruptedException e) {
            log.warn(" infiniteWait : ", e);
        }

    }

    private void stopInfiniteWait(){

        log.debug(" stopInfiniteWait : application leaving the infinite wait state.");

        if(_latch != null) {
            _latch.countDown();
        }
    }



    /**
     * Initializes this instance.
     * <p>
     * This method should be called once the JVM process is created and the
     * <code>Runner</code> instance is created thru its empty public
     * constructor.
     * </p>
     * <p>
     * Apart from set up and allocation of native resources, this method
     * does not start actual operation of <code>Runner</code> (such
     * as starting threads.) as it would impose serious security hazards. The
     * start of operation must be performed in the <code>start()</code>
     * method.
     * </p>
     *
     * @throws UnRetriableException Any exception preventing a successful
     *                              initialization.
     */
    @Override
    public void init() throws UnRetriableException {

        log.debug( " init : initializing system configurations");

        //First load the system settings as the defaults.
        CompositeConfiguration configuration = new CompositeConfiguration();
        configuration.addConfiguration(new SystemConfiguration());

        setConfiguration(configuration);


        for(ConfigHandler configHandler: getConfigurationSetLoader()){

            log.debug(" init : found the configuration handler {} ", configHandler);

                Configuration newConfigs = configHandler.populateConfiguration(getConfiguration());
                setConfiguration(newConfigs);

        }

    }

    /**
     * Starts the operations of our instance. This
     * method is to be invoked by the environment after the init()
     * method has been successfully invoked and possibly the security
     * level of the JVM has been dropped. Implementors of this
     * method are free to start any number of threads, but need to
     * return control after having done that to enable invocation of
     * the stop()-method.
     */
    @Override
    public void start() throws UnRetriableException {

        log.info(" start : Initiating operations of the whole system.");

        for (BaseSystemHandler baseSystemHandler: getSystemBaseSetLoader()){

            log.info(" start : found system handler {} ", baseSystemHandler);

            baseSystemHandler.initialize(getConfiguration());

        }

        infiniteWait();

    }


    /**
     *
     * Stops the operation of this instance and immediately
     * frees any resources allocated by this daemon such as file
     * descriptors or sockets. This method gets called by the container
     * after stop() has been called, before the JVM exits. The Daemon
     * can not be restarted after this method has been called without a
     * new call to the init() method.
     */
    @Override
    public void terminate() {

        log.info(" terminate : Terminating operations system wide.");


        for (BaseSystemHandler baseSystemHandler: getSystemBaseSetLoader()){

            log.info(" terminate : Initiating clean exit for system handler {} ", baseSystemHandler);
            baseSystemHandler.terminate();

        }

        stopInfiniteWait();
    }
}
