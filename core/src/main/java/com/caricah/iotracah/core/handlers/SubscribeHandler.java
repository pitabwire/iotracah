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


import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.messages.SubscribeAcknowledgeMessage;
import com.caricah.iotracah.bootstrap.data.messages.SubscribeMessage;
import com.caricah.iotracah.bootstrap.data.models.messages.IotMessageKey;
import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.IotSubscriptionFilter;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.core.worker.exceptions.DoesNotExistException;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class SubscribeHandler extends RequestHandler<SubscribeMessage> {

    /**
     * The SUBSCRIBE Packet is sent from the Client to the Server to create one or more
     * Subscriptions. Each SubscriptionFilter registers a Client’s interest in one or more Topics.
     * The Server sends PUBLISH Packets to the Client in order to forward Application Messages
     * that were published to Topics that match these Subscriptions.
     * The SUBSCRIBE Packet also specifies (for each SubscriptionFilter) the maximum QoS with which
     * the Server can send Application Messages to the Client
     *
     * @throws RetriableException
     * @throws UnRetriableException
     */
    @Override
    public void handle(SubscribeMessage subscribeMessage) throws RetriableException, UnRetriableException {

        log.debug(" handle : begining to handle a subscription {}.", subscribeMessage);

        /**
         * First we obtain the client responsible for this connection.
         */

        List<Integer> grantedQos = new ArrayList<>();


        /**
         * Before subscribing we should get the current session and validate it.
         */

        List<String> topics = new ArrayList<>();
        subscribeMessage.getTopicFilterList().forEach(topic -> topics.add(topic.getKey()));

        Observable<IOTClient> permissionObservable = checkPermission(subscribeMessage.getSessionId(),
                subscribeMessage.getAuthKey(), AuthorityRole.SUBSCRIBE, topics);

        permissionObservable.subscribe(
                (iotSession) -> {
                    //We have all the security to proceed.
                    Observable<Map.Entry<String, Integer>> subscribeObservable = getMessenger().subscribe(iotSession, subscribeMessage.getTopicFilterList());

                    subscribeObservable.subscribe(
                            entry -> grantedQos.add(entry.getValue()),
                            throwable1 -> disconnectDueToError(throwable1, subscribeMessage),
                            () -> {

                                /**
                                 * Save subscription payload
                                 */
                                if (subscribeMessage.getProtocol().isNotPersistent()) {
                                    iotSession.setProtocolData(subscribeMessage.getReceptionUrl());
                                    iotSession.touch();
                                }

                                SubscribeAcknowledgeMessage subAckMessage = SubscribeAcknowledgeMessage.from(
                                        subscribeMessage.getMessageId(), grantedQos);
                                subAckMessage.copyTransmissionData(subscribeMessage);
                                pushToServer(subAckMessage);

                                log.debug(" handle : successfully subscribed topics {} with result {}.", subscribeMessage.getTopicFilterList(), grantedQos );


                                /**
                                 * Queue retained messages to our subscriber.
                                 */

                                if (grantedQos.size() > 0) {

                                    int count = 0;

                                    for (Map.Entry<String, Integer> entry : subscribeMessage.getTopicFilterList())
                                        if (grantedQos.get(count++) != 0x80) {

                                            log.debug(" handle : checking if topic filter {} has retained messages ", entry );

                                            Observable<IotSubscriptionFilter> subscriptionFilterObservable = getDatastore().getSubscriptionFilterTree(iotSession.getPartitionId(), entry.getKey());
                                            subscriptionFilterObservable.toBlocking().forEach(
                                                    subscriptionFilter -> {

                                                        try {

                                                            log.debug(" handle : checking for retained messages for filter {}", subscriptionFilter);

                                                            Observable<PublishMessage> retainedMessageObservable = getDatastore().getRetainedMessage(subscriptionFilter);
                                                            retainedMessageObservable.subscribe(retainedMessage -> {

                                                                log.debug(" handle : we got to release a retained message {}. ", retainedMessage);

                                                                PublishMessage publishMessage = iotSession.copyTransmissionData(retainedMessage);

                                                                if (publishMessage.getQos() > 0) {
                                                                    publishMessage.setIsRelease(false);
                                                                    //Save the message as we proceed.
                                                                    publishMessage.setMessageId(PublishMessage.ID_TO_FORCE_GENERATION_ON_SAVE);
                                                                    Map.Entry<Long, IotMessageKey> messageIdentity = getDatastore().saveMessage(publishMessage).toBlocking().single();
                                                                    publishMessage.setMessageId(messageIdentity.getValue().getMessageId());
                                                                }


                                                                try {

                                                                    getWorker().getHandler(PublishOutHandler.class).handle(publishMessage);

                                                                } catch (RetriableException | UnRetriableException e) {
                                                                    log.error(" handle : problems publishing ", e);
                                                                }


                                                            }, throwable -> {
                                                                if(throwable instanceof DoesNotExistException) {
                                                                    //
                                                                }else{
                                                                    log.error(" handle: problems getting retained message", throwable);
                                                                }
                                                            });

                                                        } catch (UnRetriableException e) {
                                                            log.error("  handle : problems obtaining subscription filter key", e);
                                                        }
                                                    }
                                            );

                                        }
                                }
                            });
                }, throwable2 -> disconnectDueToError(throwable2, subscribeMessage));


    }
}
