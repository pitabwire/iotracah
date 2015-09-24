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

package com.caricah.iotracah.datastore.ignitecache;

import com.caricah.iotracah.core.init.ServersInitializer;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.messages.WillMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.Subscription;
import com.caricah.iotracah.datastore.ignitecache.internal.impl.ClientHandler;
import com.caricah.iotracah.datastore.ignitecache.internal.impl.MessageHandler;
import com.caricah.iotracah.datastore.ignitecache.internal.impl.SubscriptionHandler;
import com.caricah.iotracah.datastore.ignitecache.internal.impl.WillHandler;
import com.caricah.iotracah.datastore.ignitecache.session.SessionManager;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import rx.Observable;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Set;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/15/15
 */
public class IgniteDatastore extends Datastore{

    private String excecutorDefaultName;

    private String datastoreExcecutorName;

    private IgniteAtomicSequence clientIdSequence;

    private final ClientHandler clientHandler = new ClientHandler();

    private final WillHandler willHandler = new WillHandler();

    private final SubscriptionHandler subscriptionHandler = new SubscriptionHandler();

    private final MessageHandler messageHandler = new MessageHandler();

    private final SessionManager sessionManager = new SessionManager();


    public String getExcecutorDefaultName() {
        return excecutorDefaultName;
    }

    public void setExcecutorDefaultName(String excecutorDefaultName) {
        this.excecutorDefaultName = excecutorDefaultName;
    }

    public String getDatastoreExcecutorName() {
        return datastoreExcecutorName;
    }

    public void setDatastoreExcecutorName(String datastoreExcecutorName) {
        this.datastoreExcecutorName = datastoreExcecutorName;
    }

    public IgniteAtomicSequence getClientIdSequence() {
        return clientIdSequence;
    }

    public void setClientIdSequence(IgniteAtomicSequence clientIdSequence) {
        this.clientIdSequence = clientIdSequence;
    }

    /**
     * <code>configure</code> allows the base system to configure itself by getting
     * all the settings it requires and storing them internally. The plugin is only expected to
     * pick the settings it has registered on the configuration file for its particular use.
     *
     * @param configuration
     * @throws UnRetriableException
     */
    @Override
    public void configure(Configuration configuration) throws UnRetriableException {


        String excecutorDefaultName = configuration.getString(ServersInitializer.CORE_CONFIG_DEFAULT_ENGINE_EXCECUTOR_NAME, ServersInitializer.CORE_CONFIG_DEFAULT_ENGINE_EXCECUTOR_NAME_DEFAULT_VALUE);
        setExcecutorDefaultName(excecutorDefaultName);

        String datastoreExcecutorName = configuration.getString(ServersInitializer.CORE_CONFIG_ENGINE_EXCECUTOR_DATASTORE_NAME, getExcecutorDefaultName());
        setDatastoreExcecutorName(datastoreExcecutorName);

        clientHandler.setExcecutorName(getDatastoreExcecutorName());
        clientHandler.configure(configuration);

        subscriptionHandler.setExcecutorName(getDatastoreExcecutorName());
        subscriptionHandler.configure(configuration);

        messageHandler.setExcecutorName(getDatastoreExcecutorName());
        messageHandler.configure(configuration);

        willHandler.setExcecutorName(getDatastoreExcecutorName());
        willHandler.configure(configuration);

        sessionManager.configure(configuration);
    }

    /**
     * <code>initiate</code> starts the operations of this system handler.
     * All excecution code for the plugins is expected to begin at this point.
     *
     * @throws UnRetriableException
     */
    @Override
    public void initiate() throws UnRetriableException {

        Ignite ignite = Ignition.ignite(getExcecutorDefaultName());

        long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        String nameOfSequenceForClientId = "iotracah-sequence-client-id";
        IgniteAtomicSequence seq = ignite.atomicSequence(nameOfSequenceForClientId, currentTime, true);
        setClientIdSequence(seq);

        sessionManager.initiate(ignite);
        clientHandler.initiate(Client.class, ignite);
        subscriptionHandler.initiate(Subscription.class, ignite);
        messageHandler.initiate(PublishMessage.class, ignite);
        willHandler.initiate(WillMessage.class, ignite);



    }

    /**
     * <code>terminate</code> halts excecution of this plugin.
     * This provides a clean way to exit /stop operations of this particular plugin.
     */
    @Override
    public void terminate() {

    }

    @Override
    public Observable<Client> getClient(String partition, String clientIdentifier) {

        String query = "partition = ? and clientIdentifier = ?";
        Object[] params = {partition, clientIdentifier};

       return clientHandler.getByQuery(Client.class, query, params );
    }

    @Override
    public void saveClient(Client client) {
        clientHandler.save(client);
    }

    @Override
    public void removeClient(Client client) {
        clientHandler.remove(client);
    }

    @Override
    public Observable<WillMessage> getWill(String partition, String clientIdentifier) {

        String query = "partition = ? and clientIdentifier = ?";
        Object[] params = {partition, clientIdentifier};

        return willHandler.getByQuery(WillMessage.class, query, params);
    }

    @Override
    public void saveWill(WillMessage will) {
        willHandler.save(will);
    }

    @Override
    public void removeWill(WillMessage will) {
        willHandler.remove(will);
    }

    @Override
    public Observable<Subscription> getSubscription(String partition, String partitionQosTopicFilter, Subscription defaultSubscription) {
       return subscriptionHandler.getByKeyWithDefault(partitionQosTopicFilter, defaultSubscription);
    }

    @Override
    public void saveSubscription(Subscription subscription) {
        subscriptionHandler.save(subscription);
    }

    @Override
    public void removeSubscription(Subscription subscription) {
        subscriptionHandler.remove(subscription);
    }

    @Override
    public Observable<PublishMessage> distributePublish(Set<String> topicBreakDown, PublishMessage publishMessage) {

        return Observable.create(observer -> {
            subscriptionHandler.getComputeGrid().run(() -> {

                try {
                    for (String topicFilter : topicBreakDown) {

                        Observable<Subscription> subscriptionObservable = getSubscription(publishMessage.getPartition(), topicFilter);
                        subscriptionObservable.subscribe(
                                subscription -> {

                                    for (String clientIdentifier : subscription.getSubscriptions()) {

                                        Observable<Client> clientObservable = getClient(publishMessage.getPartition(), clientIdentifier);

                                        clientObservable.subscribe(client -> {

                                            PublishMessage clonePublishMessage = publishMessage.cloneMessage();
                                            clonePublishMessage = client.copyTransmissionData(clonePublishMessage);


                                            if (clonePublishMessage.getQos() == 1 || clonePublishMessage.getQos() == 2) {
                                                //Save the message as we proceed.
                                                saveMessage(clonePublishMessage);
                                            }

                                            // callback with value
                                            observer.onNext(clonePublishMessage);
                                        });
                                    }
                                }
                        );


                    }

                    observer.onCompleted();
                } catch (Exception e) {
                    observer.onError(e);
                }
            });
        });

    }

    @Override
    public Observable<PublishMessage> getActiveMessages(Client client) {

        String query = "partition = ? and clientIdentifier = ?";
        Object[] params = {client.getPartition(), client.getClientIdentifier()};

        return messageHandler.getByQuery(PublishMessage.class, query, params);
    }

    @Override
    public Observable<PublishMessage> getMessage(String partition, String clientIdentifier, long messageId, boolean isInbound) {

        String query = "partition = ? and clientIdentifier = ? and messageId = ? and inBound = ?";
        Object[] params = {partition, clientIdentifier, messageId, isInbound};

        return messageHandler.getByQuery(PublishMessage.class, query, params);
    }

    @Override
    public Observable<Long> saveMessage(PublishMessage publishMessage) {

        messageHandler.save(publishMessage);

        return Observable.create(observer -> {
            // callback with value
            observer.onNext(publishMessage.getMessageId());
            observer.onCompleted();


        });

    }

    @Override
    public void removeMessage(PublishMessage publishMessage) {
        messageHandler.remove(publishMessage);
    }

    @Override
    public String nextClientId() {
        long nextSequence = getClientIdSequence().incrementAndGet();
        return String.format("iotracah-cl-id-%d", nextSequence);
    }


}
