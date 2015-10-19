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

package com.caricah.iotracah.core.handlers;


import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.core.worker.state.messages.SubscribeAcknowledgeMessage;
import com.caricah.iotracah.core.worker.state.messages.SubscribeMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class SubscribeHandler extends RequestHandler<SubscribeMessage> {

    public SubscribeHandler(SubscribeMessage message) {
        super(message);
    }

    /**
     * The SUBSCRIBE Packet is sent from the Client to the Server to create one or more
     * Subscriptions. Each Subscription registers a Client’s interest in one or more Topics.
     * The Server sends PUBLISH Packets to the Client in order to forward Application Messages
     * that were published to Topics that match these Subscriptions.
     * The SUBSCRIBE Packet also specifies (for each Subscription) the maximum QoS with which
     * the Server can send Application Messages to the Client
     *
     * @throws RetriableException
     * @throws UnRetriableException
     */
    @Override
    public void handle() throws RetriableException, UnRetriableException {

        log.debug(" handle : begining to handle a subscription {}.", getMessage());

           /**
             * First we obtain the client responsible for this connection.
             */

            List<Integer> grantedQos = new ArrayList<>();


                /**
                 * Before subscribing we should get the current session and validate it.
                 */

                    List<String> topics = new ArrayList<>();
                    getMessage().getTopicFilterList().forEach(topic -> topics.add(topic.getKey()));

                        Observable<Client> permissionObservable = checkPermission(getMessage().getSessionId(),
                                getMessage().getAuthKey(), AuthorityRole.SUBSCRIBE, topics);

                        permissionObservable.subscribe(
                                (client)->{

                                    //We have all the security to proceed.
                            Observable<Map.Entry<String, Integer>> subscribeObservable = getMessenger().subscribe(client.getPartition(), client.getClientId(), getMessage().getTopicFilterList());

                            subscribeObservable.subscribe(
                                    (entry) -> grantedQos.add(entry.getValue()),
                                    this::disconnectDueToError,
                                    () -> {

                                        /**
                                         * Save subscription payload
                                         */
                                        if(getMessage().getProtocal().isNotPersistent()){
                                            client.setProtocalData(getMessage().getReceptionUrl());
                                            getDatastore().saveClient(client);
                                        }

                                        SubscribeAcknowledgeMessage subAckMessage = SubscribeAcknowledgeMessage.from(
                                                getMessage().getMessageId(), grantedQos);
                                        subAckMessage.copyBase(getMessage());
                                        pushToServer(subAckMessage);

                                    });
                        }, this::disconnectDueToError);


    }
}
