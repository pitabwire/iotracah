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

package com.caricah.iotracah.core.modules.base.server;

import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.core.modules.Server;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.lang.IgniteBiPredicate;
import rx.Subscriber;

import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/3/15
 */
public class DefaultServerRouter implements ServerRouter, IgniteBiPredicate<UUID, IOTMessage> {

    private List<Subscriber> subscriberList = new ArrayList<>();

    private  final IgniteMessaging messaging;

    public DefaultServerRouter(IgniteMessaging messaging){
        this.messaging = messaging;
    }

    public IgniteMessaging getMessaging() {
        return messaging;
    }

    @Override
    public void call(Subscriber<? super IOTMessage> subscriber) {

        subscriberList.add(subscriber);
    }

    @Override
    public void route(String cluster, UUID nodeId, IOTMessage message) {
        getMessaging().send(getNodeTopic(cluster, nodeId), message );
    }


    private String getNodeTopic(String cluster, UUID nodeId){
        return String.format("%s-%s", cluster, nodeId);
    }

    /**
     * Predicate body.
     *
     * @param uuid       First parameter.
     * @param IOTMessage Second parameter.
     * @return Return value.
     */
    @Override
    public boolean apply(UUID uuid, IOTMessage IOTMessage) {

        subscriberList.forEach(subscriber -> subscriber.onNext(IOTMessage));
        return true;
    }
}
