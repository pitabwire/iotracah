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

import com.caricah.iotracah.core.messaging.IOTMessage;
import com.caricah.iotracah.system.BaseSystemHandler;
import rx.Observable;

/**
 *
 * <code>Datastore</code> provides the abstraction required to hide
 * all the data access.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/10/15
 */
public abstract class Datastore implements Observable.OnSubscribe<IOTMessage>,BaseSystemHandler {


    @Override
    public int compareTo(BaseSystemHandler baseSystemHandler) {

        if(null == baseSystemHandler)
            throw new NullPointerException("You can't compare a null object.");

        if(baseSystemHandler instanceof Datastore)
            return 0;
        else if(baseSystemHandler instanceof Eventer)
            return -1;

        else
            return 1;
    }
}
