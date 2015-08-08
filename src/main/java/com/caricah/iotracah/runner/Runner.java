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

import com.caricah.iotracah.exceptions.UnRetriableException;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/8/15
 */
public interface Runner {


    /**
     * Initializes this instance.
     * <p>
     *   This method should be called once the JVM process is created and the
     *   <code>Runner</code> instance is created thru its empty public
     *   constructor.
     * </p>
     * <p>
     *   Apart from set up and allocation of native resources, this method
     *   does not start actual operation of <code>Runner</code> (such
     *   as starting threads.) as it would impose serious security hazards. The
     *   start of operation must be performed in the <code>start()</code>
     *   method.
     * </p>
     *
     * @exception UnRetriableException Any exception preventing a successful
     *                      initialization.
     */
    void init() throws UnRetriableException;

    /**
     * Starts the operations of our instance. This
     * method is to be invoked by the environment after the init()
     * method has been successfully invoked and possibly the security
     * level of the JVM has been dropped. Implementors of this
     * method are free to start any number of threads, but need to
     * return control after having done that to enable invocation of
     * the stop()-method.
     */
    void start() throws UnRetriableException;

    /**
     *
     * Stops the operation of this instance and immediately
     * frees any resources allocated by this daemon such as file
     * descriptors or sockets. This method gets called by the container
     * after stop() has been called, before the JVM exits. The Daemon
     * can not be restarted after this method has been called without a
     * new call to the init() method.
     */
    void terminate();

}
