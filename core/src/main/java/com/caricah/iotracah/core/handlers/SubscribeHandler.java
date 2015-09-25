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
import com.caricah.iotracah.core.worker.exceptions.ShutdownException;
import com.caricah.iotracah.core.worker.exceptions.TopicSubscriptionException;
import com.caricah.iotracah.core.worker.state.messages.SubscribeAcknowledgeMessage;
import com.caricah.iotracah.core.worker.state.messages.SubscribeMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.shiro.authz.AuthorizationException;
import rx.Observable;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class SubscribeHandler extends RequestHandler {


    private SubscribeMessage message;

    public SubscribeHandler(SubscribeMessage message) {
        this.message = message;
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

        log.debug(" handle : begining to handle a subscription {}.", message);

        try {


            /**
             * Before subscribing we should get the current session and validate it.
             */
            for (Map.Entry<String, Integer> topic : message.getTopicFilterList()) {
                checkPermission(AuthorityRole.SUBSCRIBE, topic.getKey());
            }

            /**
             *
             */

            List<Integer> grantedQos = new ArrayList<>();

            Observable<Client> clientObservable = getClient(message.getPartition(), message.getClientIdentifier());

            clientObservable.subscribe(new Subscriber<Client>() {
                @Override
                public void onCompleted() {

                }

                @Override
                public void onError(Throwable e) {
                    log.error(" handle onError : problems ", e);
                }

                @Override
                public void onNext(Client client) {


                    log.debug(" handle : obtained client {}.", client);


                    Observable<Map.Entry<String, Integer>> subscribeObservable = getMessenger().subscribe(client.getPartition(), client.getClientIdentifier(), message.getTopicFilterList());

                    subscribeObservable.subscribe(
                            new Subscriber<Map.Entry<String, Integer>>() {
                                @Override
                                public void onCompleted() {

                                    SubscribeAcknowledgeMessage subAckMessage = SubscribeAcknowledgeMessage.from(message.getMessageId(), message.isDup(), message.getQos(), false, grantedQos);
                                    subAckMessage.copyBase(message);
                                    pushToServer(subAckMessage);

                                }

                                @Override
                                public void onError(Throwable e) {
                                    log.error(" handle onError : problems", e);
                                }

                                @Override
                                public void onNext(Map.Entry<String, Integer> entry) {

                                    grantedQos.add(entry.getValue());
                                }
                            });
                }
            });

        } catch (AuthorizationException e) {
            getWorker().logError(" handle : System experienced the error ", e);
            throw new ShutdownException(e);
        }

    }
}
