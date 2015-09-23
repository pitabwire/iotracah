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
import com.caricah.iotracah.core.worker.exceptions.TopicSubscriptionException;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.Subscription;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import rx.Observable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Implementation class that handles subscribing, unsubscribing and publishing of messages
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/30/15
 */
public class Messenger {

    private Datastore datastore;

    private Worker worker;

    private ExecutorService executorService;

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

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public Observable<Map.Entry<String, Integer>> subscribe(String partition, String clientIdentifier, List<Map.Entry<String, Integer>> topicFilterQosList) {

        return Observable.create(observer -> {
            getExecutorService().submit(() -> {

                for (Map.Entry<String, Integer> topicFilterQos : topicFilterQosList) {



                    /**
                     *
                     * Allowed return codes:
                     *  0x00 - Success - Maximum QoS 0
                     *  0x01 - Success - Maximum QoS 1
                     *  0x02 - Success - Maximum QoS 2
                     *  0x80 - Failure
                     */

                    Subscription newSubscription = new Subscription();
                    newSubscription.setTopicFilter(topicFilterQos.getKey());
                    newSubscription.setQos(topicFilterQos.getValue());
                    newSubscription.setPartition(partition);

                    String partionQosTopicFilter = newSubscription.getPartitionQosTopicFilter();

                    Observable<Subscription> subscriptionObservable = getDatastore().getSubscription(
                            newSubscription.getPartition(), partionQosTopicFilter, newSubscription);

                    subscriptionObservable.subscribe(
                            subscription -> {


                                try {
                                    if (null == subscription.getSubscriptions()) {
                                        subscription.setSubscriptions(new HashSet<>());
                                    }

                                    //TODO: clean by futher rationalizing the dataset.
                                    subscription.getSubscriptions().add(clientIdentifier);
                                    getDatastore().saveSubscription(subscription);

                                    Observable<Client> clientObservable = getDatastore().getClient(partition, clientIdentifier);

                                    final Subscription finalSubscription = subscription;
                                    clientObservable.subscribe(client -> {
                                        client.getPartiotionQosTopicFilters().add(finalSubscription.getPartitionQosTopicFilter());
                                        getDatastore().saveClient(client);

                                    });

                                    observer.onNext(topicFilterQos);

                                } catch (Exception e) {
                                    topicFilterQos.setValue(3);
                                    observer.onNext(topicFilterQos);
                                }

                            });
                }


                observer.onCompleted();

            });
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


    public void unSubscribe(String partition, String clientIdentifier, String partitionQosTopicFilter) throws RetriableException {

        Observable<Subscription> subscriptionObservable = getDatastore().getSubscription(partition, partitionQosTopicFilter);

        subscriptionObservable.subscribe(subscription -> {

            try {
                if (null != subscription) {
                    subscription.getSubscriptions().remove(clientIdentifier);


                    //Completely remove the subscription if it has no subscribers.
                    if (subscription.getSubscriptions().isEmpty()) {
                        getDatastore().removeSubscription(subscription);
                    } else {
                        getDatastore().saveSubscription(subscription);
                    }
                }

                Observable<Client> clientObservable = getDatastore().getClient(partition, clientIdentifier);

                clientObservable.subscribe(client -> {
                    client.getPartiotionQosTopicFilters().remove(partitionQosTopicFilter);
                    getDatastore().saveClient(client);
                });

            } catch (Exception e) {

                getWorker().logError(" unSubscribe : issues unsubscribing : " + clientIdentifier + " from : " + partitionQosTopicFilter, e);

            }

        });

    }

    public void publish(PublishMessage publishMessage) throws RetriableException {


        Set<String> topicBreakDown = getTopicBreakDown(publishMessage.getTopic());

        Observable<PublishMessage> distributePublishMessageObservable = getDatastore().distributePublish(topicBreakDown, publishMessage);

        distributePublishMessageObservable.subscribe(distributePublishMessage -> {

            //This message should be released to the connected client
            PublishOutHandler handler = new PublishOutHandler(distributePublishMessage);
            handler.setWorker(getWorker());

            try {
                handler.handle();
            } catch (RetriableException | UnRetriableException e) {
                getWorker().logError(" process : problems releasing stored messages", e);
            }

        });


    }


}
