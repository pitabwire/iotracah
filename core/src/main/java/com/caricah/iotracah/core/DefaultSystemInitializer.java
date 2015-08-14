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

package com.caricah.iotracah.core;

import com.caricah.iotracah.core.init.EventersInitializer;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.system.BaseSystemHandler;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.List;

/**
 *
 * <code>DefaultSystemInitializer</code> initializes all the plugins while
 * making sure all the appropriate plugins are initiated in order.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/11/15
 */
public class DefaultSystemInitializer extends EventersInitializer {


    @Override
    public void systemInitialize(List<BaseSystemHandler> baseSystemHandlerList) throws UnRetriableException {
        super.systemInitialize(baseSystemHandlerList);

        //Perform flagging off for system plugins.
        startEventers();
        startDataStores();
        startWorkers();
        startServers();

    }


}
