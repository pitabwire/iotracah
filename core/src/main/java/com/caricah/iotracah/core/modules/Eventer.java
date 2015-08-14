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

package com.caricah.iotracah.core.modules;

import com.caricah.iotracah.core.messaging.Event;
import com.caricah.iotracah.core.messaging.IOTMessage;
import com.caricah.iotracah.system.BaseSystemHandler;
import rx.Subscriber;

/**
 *
 * <code>Eventer</code> provides the abstraction required to enable
 * Events within the whole system captured. These plugins are the ones
 * loaded before any other plugins are loaded.
 *
 * Eventers Send out messages on system metrics to external services.
 * The metrics sent out include :
 * <ul>
 *     <ol>System status</ol>
 *     <ol>Connected clients</ol>
 *     <ol>Inbound messages</ol>
 *     <ol>Outbound messages</ol>
 *     <ol>Handshake messages</ol>
 *     <ol>Miscellaneous</ol>
 * </ul>
 *
 *
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/10/15
 */
public abstract class Eventer extends Subscriber<IOTMessage> implements BaseSystemHandler {

    @Override
    public void onError(Throwable e) {

    }




    @Override
    public int compareTo(BaseSystemHandler baseSystemHandler) {

        if(null == baseSystemHandler)
            throw new NullPointerException("You can't compare a null object.");

        if(baseSystemHandler instanceof Eventer)
            return 0;
        else
            return 1;
    }
}
