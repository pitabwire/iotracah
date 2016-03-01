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

package com.caricah.iotracah.core.worker.state;


import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.models.client.IotClientKey;
import com.caricah.iotracah.bootstrap.data.models.messages.IotMessageKey;
import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.IotSubscriptionFilter;
import com.caricah.iotracah.bootstrap.data.models.subscriptions.IotSubscription;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.core.handlers.PublishOutHandler;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.modules.Worker;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Implementation class that handles subscribing, unsubscribing and publishing of messages
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/30/15
 */
public class Messenger {

    private static final Logger log = LoggerFactory.getLogger(Messenger.class);

    private Datastore datastore;

    private Worker worker;

    public Datastore getDatastore() {
        return datastore;
    }

    public void setDatastore(Datastore datastore) {
        this.datastore = datastore;
    }

    public Worker getWorker() {
        return worker;
    }

    public void setWorker(Worker worker) {
        this.worker = worker;
    }

    public Observable<Map.Entry<String, Integer>> subscribe(IOTClient iotClient, List<Map.Entry<String, Integer>> topicFilterQosList) {

        return Observable.create(observer -> {

            ListIterator<Map.Entry<String, Integer>> listIterator = topicFilterQosList.listIterator();


            while(listIterator.hasNext()){

                Map.Entry<String, Integer> topicFilterQos = listIterator.next();


                    /**
                     *
                     * Allowed return codes:
                     *  0x00 - Success - Maximum QoS 0
                     *  0x01 - Success - Maximum QoS 1
                     *  0x02 - Success - Maximum QoS 2
                     *  0x80 - Failure
                     */


                    Observable<IotSubscriptionFilter> subscriptionFilterObservable
                            = getDatastore().getOrCreateSubscriptionFilter(
                            iotClient.getPartitionId(),
                            topicFilterQos.getKey());

                    subscriptionFilterObservable.subscribe(
                            subscriptionFilter -> {


                                IotSubscription subscription = new IotSubscription();
                                subscription.setPartitionId(iotClient.getPartitionId());
                                subscription.setClientId(iotClient.getSessionId());
                                subscription.setSubscriptionFilterId(subscriptionFilter.getId());


                                Observable<IotSubscription> subscriptionObservable = getDatastore().getSubscriptionWithDefault(subscription);

                                subscriptionObservable.subscribe(
                                        iotSubscription -> {

                                            int qos = null == topicFilterQos.getValue() ? 0 : topicFilterQos.getValue();

                                            if(qos > subscription.getQos()) {
                                                subscription.setQos(qos);
                                                getDatastore().saveSubscription(subscription);
                                            }

                                            log.debug(" subscribe : successfully saved a subscription {}", subscription);

                                            observer.onNext(topicFilterQos);

                                            if (!listIterator.hasNext()){
                                                observer.onCompleted();
                                            }


                                        },throwable -> {

                                            log.warn(" subscribe : funny things happening during subscription", throwable);

                                            topicFilterQos.setValue(0x80);
                                            observer.onNext(topicFilterQos);

                                            if (!listIterator.hasNext()){
                                                observer.onCompleted();
                                            }
                                        }
                                );




                            }, throwable -> {


                                log.error(" subscribe : problems during subscription", throwable);

                                topicFilterQos.setValue(0x80);
                                observer.onNext(topicFilterQos);


                                if (!listIterator.hasNext()){
                                    observer.onCompleted();
                                }


                            });
                }

        });

    }

    public void unSubscribe(IotSubscription subscription) {

        // and delete it from our db
        getDatastore().removeSubscription(subscription);

        //TODO: Completely remove the subscriptionFilter if it has no subscribers.


    }

    public void publish(String partitionId, PublishMessage publishMessage) throws RetriableException {

        getWorker().getExecutorService().submit(()->{
           publishMessage.setPartitionId(partitionId);
            publish(publishMessage);
        });
    }

    private void publish(PublishMessage publishMessage) throws RetriableException {

        log.debug(" publish : new message {} to publish from {} in partition {}", publishMessage, publishMessage.getSessionId(), publishMessage.getPartitionId());

        //Obtain a list of all the subscribed clients who will receive a message.
        Observable<IotSubscriptionFilter> subscriptionFilterObservable = getDatastore().getMatchingSubscriptionFilter(publishMessage.getPartitionId(), publishMessage.getTopic());

        subscriptionFilterObservable.subscribe(
                subscriptionFilter -> {

                    try {

                        Observable<IotSubscription> subscriptionObservable
                                = getDatastore().getSubscriptions(subscriptionFilter,  publishMessage.getQos());
                        subscriptionObservable.subscribeOn(getWorker().getScheduler()).distinct().subscribe(
                                subscription -> {

                                    IotClientKey clientKey = new IotClientKey();
                                    clientKey.setSessionId(subscription.getClientId());
                                    Observable<IOTClient> clientObservable = getDatastore().getSession(clientKey);

                                    clientObservable.subscribe(iotSession -> {

                                        try {
                                            log.debug(" publish : found subscription {} for message {} in partition {}", iotSession, publishMessage, publishMessage.getPartitionId());


                                            final PublishMessage clonePublishMessage = iotSession.copyTransmissionData(publishMessage.cloneMessage());

                                            if (clonePublishMessage.getQos() > MqttQoS.AT_MOST_ONCE.value()) {

                                                try {
                                                    //Save the message as we proceed.
                                                    Map.Entry<Long, IotMessageKey> messageIdentity = getDatastore().saveMessage(clonePublishMessage).toBlocking().single();

                                                    log.debug(" publish : new generated message id is {}", messageIdentity);

                                                    clonePublishMessage.setMessageId(messageIdentity.getValue().getMessageId());

                                                } catch (Exception e) {
                                                    log.error(" publish : error details ", e);
                                                }
                                            }


                                            if (iotSession.getIsActive()) {
                                                //Actually push out the message.
                                                //This message should be released to the connected client

                                                getWorker().getHandler(PublishOutHandler.class).handle(clonePublishMessage);

                                            }
                                        } catch (RetriableException | UnRetriableException e) {
                                            log.error(" publish : problems releasing stored messages", e);
                                        }

                                    });


                                }, throwable -> log.error(" process : database problems", throwable));


                    } catch (UnRetriableException e) {
                        e.printStackTrace();
                    }

                }, throwable -> log.error(" publish : database problems", throwable), ()->{

                    //Store the retained message.

                    if (publishMessage.getIsRetain()) {


                        log.debug(" publish : Message {} on partition {} should be retained", publishMessage, publishMessage.getPartitionId());


                        /**
                         * 3.3.1.3 RETAIN
                         * Position: byte 1, bit 0.
                         * If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a Server, the Server MUST store the Application Message and its QoS, so that it can be delivered to future subscribers whose subscriptions match its topic name [MQTT-3.3.1-5]. When a new subscription is established, the last retained message, if any, on each matching topic name MUST be sent to the subscriber [MQTT-3.3.1-6]. If the Server receives a QoS 0 message with the RETAIN flag set to 1 it MUST discard any message previously retained for that topic. It SHOULD store the new QoS 0 message as the new retained message for that topic, but MAY choose to discard it at any time - if this happens there will be no retained message for that topic [MQTT-3.3.1-7]. See Section 4.1 for more information on storing state.
                         * When sending a PUBLISH Packet to a Client the Server MUST set the RETAIN flag to 1 if a message is sent as a result of a new subscription being made by a Client [MQTT-3.3.1-8]. It MUST set the RETAIN flag to 0 when a PUBLISH Packet is sent to a Client because it matches an established subscription regardless of how the flag was set in the message it received [MQTT-3.3.1-9].
                         * A PUBLISH Packet with a RETAIN flag set to 1 and a payload containing zero bytes will be processed as normal by the Server and sent to Clients with a subscription matching the topic name. Additionally any existing retained message with the same topic name MUST be removed and any future subscribers for the topic will not receive a retained message [MQTT-3.3.1-10]. “As normal” means that the RETAIN flag is not set in the message received by existing Clients. A zero byte retained message MUST NOT be stored as a retained message on the Server [MQTT-3.3.1-11].
                         * If the RETAIN flag is 0, in a PUBLISH Packet sent by a Client to a Server, the Server MUST NOT store the message and MUST NOT remove or replace any existing retained message [MQTT-3.3.1-12].
                         * Non normative comment
                         * Retained messages are useful where publishers send state messages on an irregular basis. A new subscriber will receive the most recent state.
                         *
                         */


                        //Get or Create a new subscription filter just in case it does not already exist.

                        Observable<IotSubscriptionFilter> retainedSubscriptionFilterObservable = getDatastore()
                                .getOrCreateSubscriptionFilter(publishMessage.getPartitionId(), publishMessage.getTopic());

                        retainedSubscriptionFilterObservable.subscribe(subscriptionFilter -> {

                            log.debug(" publish : Message {} will be retained on subscription filter {}", publishMessage, subscriptionFilter);

                            try {

                                if (((byte[]) publishMessage.getPayload()).length > 0) {

                                    //Save the retain message.
                                    getDatastore().saveRetainedMessage(subscriptionFilter, publishMessage.getQos(), publishMessage.getPayload());

                                } else {
                                    getDatastore().removeRetainedMessage(subscriptionFilter);
                                }
                            } catch (UnRetriableException e) {
                                log.warn(" publish : problems ", e);
                            }
                        });


                    }

                });

    }


}
