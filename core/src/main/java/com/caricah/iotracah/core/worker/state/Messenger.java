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


import com.caricah.iotracah.core.handlers.PublishOutHandler;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.models.ClSubscription;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.SubscriptionFilter;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public Observable<Map.Entry<String, Integer>> subscribe(Client client, List<Map.Entry<String, Integer>> topicFilterQosList) {

        return Observable.create(observer -> {

            for (Map.Entry<String, Integer> topicFilterQos : topicFilterQosList) {


                /**
                 *
                 * Allowed return codes:
                 *  0x00 - Success - Maximum QoS 0
                 *  0x01 - Success - Maximum QoS 1
                 *  0x02 - Success - Maximum QoS 2
                 *  0x80 - Failure
                 */

                SubscriptionFilter newSubscriptionFilter = new SubscriptionFilter();
                newSubscriptionFilter.setTopicFilter(topicFilterQos.getKey());

                int qos = null == topicFilterQos.getValue() ? 0 : topicFilterQos.getValue();

                newSubscriptionFilter.setQos(qos);
                newSubscriptionFilter.setPartition(client.getPartition());


                Observable<SubscriptionFilter> subscriptionObservable
                        = getDatastore().getSubscriptionFilter(
                        newSubscriptionFilter.getPartition(),
                        newSubscriptionFilter.getQos(),
                        newSubscriptionFilter.getTopicFilter());

                subscriptionObservable.singleOrDefault(newSubscriptionFilter).subscribe(
                        subscriptionFilter -> {

                            try {

                                if (null == subscriptionFilter.getId()) {
                                    //This is a new clSubscription filter we need to save it.

                                    subscriptionFilter.setId(datastore.nextSubscriptionFilterId());
                                    getDatastore().saveSubscriptionFilter(subscriptionFilter);
                                }

                                ClSubscription clSubscription = new ClSubscription();
                                clSubscription.setId(datastore.nextSubscriptionId());
                                clSubscription.setPartition(client.getPartition());
                                clSubscription.setClientId(client.getClientId());
                                clSubscription.setTopicFilterKey(  subscriptionFilter.getId());
                                getDatastore().saveSubscription(clSubscription);

                                observer.onNext(topicFilterQos);

                            } catch (Exception e) {
                                topicFilterQos.setValue(0x80);
                                observer.onNext(topicFilterQos);
                            }

                        });
            }


            observer.onCompleted();

        });

    }

    /**
     * getTopicBreakDown is a utility method expected to return
     * the topic and all its associated lower level wildcards that match it.
     *
     * @param topic
     * @return
     */
    private Set<String> getTopicBreakDown(String topic) {

        Set<String> topicBreakDownSet = new HashSet<>();
        topicBreakDownSet.add(topic);

        String[] topicLevels = topic.split(Constant.PATH_SEPARATOR);
        String activeMultiLevelTopicFilter = "";
        for (int i = 0; i < topicLevels.length; i++) {


            if (topicLevels.length > i + 1) {
                activeMultiLevelTopicFilter += topicLevels[i] + Constant.PATH_SEPARATOR;
                topicBreakDownSet.add(activeMultiLevelTopicFilter + Constant.MULTI_LEVEL_WILDCARD);
            }

            String activeSingleLevelTopicFilter = "";

            for (int j = 0; j < topicLevels.length; j++) {
                if (j == i) {
                    activeSingleLevelTopicFilter += Constant.SINGLE_LEVEL_WILDCARD;
                } else {
                    activeSingleLevelTopicFilter += topicLevels[j];
                }

                if (topicLevels.length > j + 1) {
                    activeSingleLevelTopicFilter += Constant.PATH_SEPARATOR;
                }
            }

            topicBreakDownSet.add(activeSingleLevelTopicFilter);

        }
        return topicBreakDownSet;
    }


    public void unSubscribe(ClSubscription clSubscription) {


        getDatastore().removeSubscription(clSubscription);

        //TODO: Completely remove the subscriptionFilter if it has no subscribers.


    }

    public void publish(String partition, PublishMessage publishMessage) throws RetriableException {

        log.debug(" publish : new message {} to publish from {} in partition {}", publishMessage, publishMessage.getClientId(), partition);

        Set<String> topicBreakDown = getTopicBreakDown(publishMessage.getTopic());


        //Obtain a list of all the subscribed clients who will receive a message.

                        Observable<ClSubscription> subscriptionObservable
                                = getDatastore().getSubscription(partition, publishMessage.getQos(), topicBreakDown);
                        subscriptionObservable.subscribe(
                                subscription -> {

                                    log.debug(" publish onNext : obtained a subscription {} to send message to", subscription);

                                    Observable<Client> clientObservable = getDatastore().getClient(partition, subscription.getClientId());

                                    clientObservable.subscribe(client -> {

                                        PublishMessage clonePublishMessage = publishMessage.cloneMessage();
                                        clonePublishMessage = client.copyTransmissionData(clonePublishMessage);


                                        if (clonePublishMessage.getQos() > 0) {
                                            //Save the message as we proceed.
                                            getDatastore().saveMessage(clonePublishMessage);
                                        }

                                        //Actually push out the message.
                                        //This message should be released to the connected client
                                        PublishOutHandler handler = new PublishOutHandler(clonePublishMessage, client.getProtocalData());
                                        handler.setWorker(getWorker());

                                        try {
                                            handler.handle();
                                        } catch (RetriableException | UnRetriableException e) {
                                            log.error(" process : problems releasing stored messages", e);
                                        }
                                    });


                                }, throwable -> log.error(" process : database problems", throwable));




    }





}
