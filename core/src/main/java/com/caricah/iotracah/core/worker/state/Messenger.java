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
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.Subscription;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

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

    private static final Logger log = LoggerFactory.getLogger(Messenger.class);

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

                    int qos = null == topicFilterQos.getValue()? 0: topicFilterQos.getValue();

                    newSubscription.setQos(qos);
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

    }

    /**
     * getTopicBreakDown is a utility method expected to return
     * the topic and all its associated lower level wildcards that match it.
     *
     * @param partition
     * @param qos
     * @param topic
     * @return
     */
    private Set<String> getTopicBreakDown(String partition, int qos, String topic) {

        Set<String> topicBreakDownSet = new HashSet<>();
        addTopicBreakDownBasedOnQos(partition, qos, topic, topicBreakDownSet);

        String[] topicLevels = topic.split(Constant.PATH_SEPARATOR);
        String activeMultiLevelTopicFilter = "";
        for (int i = 0; i < topicLevels.length; i++) {


            if (topicLevels.length > i + 1) {
                activeMultiLevelTopicFilter += topicLevels[i] + Constant.PATH_SEPARATOR;
                addTopicBreakDownBasedOnQos(partition, qos, activeMultiLevelTopicFilter + Constant.MULTI_LEVEL_WILDCARD, topicBreakDownSet);
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

            addTopicBreakDownBasedOnQos(partition, qos, activeSingleLevelTopicFilter, topicBreakDownSet);

        }
        return topicBreakDownSet;
    }

    private void addTopicBreakDownBasedOnQos(String partition, int qos, String topicPart, Set<String> topicBreakDownSet){

       String qos2topic = Subscription.getPartitionQosTopicFilter(partition, 2, topicPart );
       topicBreakDownSet.add(qos2topic);


        if(qos < 2 ) {

            String qos1topic = Subscription.getPartitionQosTopicFilter(partition, 1, topicPart);
            topicBreakDownSet.add(qos1topic);

            if(qos < 1) {
                String qos0topic = Subscription.getPartitionQosTopicFilter(partition, 0, topicPart);
                topicBreakDownSet.add(qos0topic);
            }
        }
    }

    public void unSubscribe(String partition, String clientIdentifier, String partitionQosTopicFilter) {

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

                log.error(" unSubscribe : issues unsubscribing : " + clientIdentifier + " from : " + partitionQosTopicFilter, e);

            }

        });

    }

    public void publish(PublishMessage publishMessage) throws RetriableException {

        log.debug(" publish : new message {} to publish from {} in partition {}", publishMessage, publishMessage.getClientIdentifier(), publishMessage.getPartition());

        Set<String> topicBreakDown = getTopicBreakDown(publishMessage.getPartition(), publishMessage.getQos(), publishMessage.getTopic());

        //Obtain a list of all the subscribed clients who will receive a message.
        Observable<String> distributePublishObservable = getDatastore().distributePublish(topicBreakDown, publishMessage);

        distributePublishObservable.subscribe(


                new Subscriber<String>() {
                    @Override
                    public void onCompleted() {
                        log.info(" publish onCompleted : publish of {} complete", publishMessage );
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.error(" publish onError : problems publishing a message  ", e);
                    }

                    @Override
                    public void onNext(String clientIdentifier) {


                        log.debug(" publish onNext : obtained a client {} to send message to", clientIdentifier);


                        Observable<Client> clientObservable = getDatastore().getClient(publishMessage.getPartition(), clientIdentifier);

                        clientObservable.subscribe(client -> {

                            PublishMessage clonePublishMessage = publishMessage.cloneMessage();
                            clonePublishMessage = client.copyTransmissionData(clonePublishMessage);


                            if (clonePublishMessage.getQos() > 0 ) {
                                //Save the message as we proceed.
                                getDatastore().saveMessage(clonePublishMessage);
                            }

                            //Actually push out the message.
                            //This message should be released to the connected client
                            PublishOutHandler handler = new PublishOutHandler(clonePublishMessage);
                            handler.setWorker(getWorker());

                            try {
                                handler.handle();
                            } catch (RetriableException | UnRetriableException e) {
                                log.error(" process : problems releasing stored messages", e);
                            }
                        });

                    }
                }


                );


    }


}
