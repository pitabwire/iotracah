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

import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.messages.RetainedMessage;
import com.caricah.iotracah.core.worker.state.messages.WillMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.Subscription;
import com.caricah.iotracah.core.worker.state.models.SubscriptionFilter;
import com.caricah.iotracah.datastore.ignitecache.internal.impl.*;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.security.realm.state.IOTAccount;
import com.caricah.iotracah.security.realm.state.IOTRole;
import org.apache.commons.configuration.Configuration;
import rx.Observable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/15/15
 */
public class IgniteDatastore extends Datastore {



    private final ClientHandler clientHandler = new ClientHandler();

    private final WillHandler willHandler = new WillHandler();

    private final SubscriptionFilterHandler subscriptionFilterHandler = new SubscriptionFilterHandler();

    private final SubscriptionHandler subscriptionHandler = new SubscriptionHandler();

    private final MessageHandler messageHandler = new MessageHandler();

    private final RetainedMessageHandler retainedMessageHandler = new RetainedMessageHandler();

    private final AccountHandler accountHandler = new AccountHandler();

    private final RoleHandler roleHandler = new RoleHandler();




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

        clientHandler.configure(configuration);

        subscriptionFilterHandler.configure(configuration);

        subscriptionHandler.configure(configuration);

        messageHandler.configure(configuration);

        retainedMessageHandler.configure(configuration);

        willHandler.configure(configuration);

        accountHandler.configure(configuration);

        roleHandler.configure(configuration);
    }

    /**
     * <code>initiate</code> starts the operations of this system handler.
     * All excecution code for the plugins is expected to begin at this point.
     *
     * @throws UnRetriableException
     */
    @Override
    public void initiate() throws UnRetriableException {


        clientHandler.initiate(Client.class, getIgnite());
        subscriptionFilterHandler.initiate(SubscriptionFilter.class, getIgnite());
        retainedMessageHandler.initiate(RetainedMessage.class, getIgnite());
        subscriptionHandler.initiate(Subscription.class, getIgnite());
        messageHandler.initiate(PublishMessage.class, getIgnite());
        willHandler.initiate(WillMessage.class, getIgnite());
        accountHandler.initiate(IOTAccount.class, getIgnite());
        roleHandler.initiate(IOTRole.class, getIgnite());

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

        return clientHandler.getByKey(Client.createIdKey(partition, clientIdentifier));
    }

    @Override
    public Observable<Client> getClient(String partition, String clientIdentifier, Client defaultClient) {

        return clientHandler.getByKeyWithDefault(Client.createIdKey(partition, clientIdentifier), defaultClient);
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
    public Observable<WillMessage> getWill(Serializable willKey) {

        return willHandler.getByKey(willKey);
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
    public Observable<SubscriptionFilter> getMatchingSubscriptionFilter(String partition, String topic) {
        return subscriptionFilterHandler.matchTopicFilterTree(partition, getTopicNavigationRoute(topic));
    }

    @Override
    public Observable<SubscriptionFilter> getOrCreateSubscriptionFilter(String partition, String topic) {

        return Observable.create(observer -> {

            List<String> topicNavigationRoute = getTopicNavigationRoute(topic);

            SubscriptionFilter subscriptionFilter =  subscriptionFilterHandler
                    .getByKeyWithDefault(SubscriptionFilter
                            .quickCheckIdKey(partition, topicNavigationRoute), null).toBlocking().single();

                    if(Objects.isNull(subscriptionFilter)){

                                 subscriptionFilter = subscriptionFilterHandler.createTree(
                            partition, topicNavigationRoute).toBlocking().single();
                    }


                observer.onNext(subscriptionFilter);
                observer.onCompleted();

        });


    }

    @Override
    public Observable<SubscriptionFilter> getSubscriptionFilter(String partition, String topic) {
        return subscriptionFilterHandler.getTopicFilterTree(partition, getTopicNavigationRoute(topic));
    }

    @Override
    public void removeSubscriptionFilter(SubscriptionFilter subscriptionFilter) {
        subscriptionFilterHandler.remove(subscriptionFilter);
    }


    @Override
    public Observable<Subscription> getSubscriptions(Client client) {
        String query = "partition = ? and clientId = ?";
        Object[] params = {client.getPartition(), client.getClientId()};
        return subscriptionHandler.getByQuery(Subscription.class, query, params);

    }

    @Override
    public Observable<Subscription> getSubscriptions(String partition, String topicFilterKey, int qos) {

        String query = "partition = ? and topicFilterKey = ? and qos >= ?";
        Object[] params = {partition, topicFilterKey, qos };
        return subscriptionHandler.getByQuery(Subscription.class, query, params);

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
    public Observable<PublishMessage> getMessages(Client client) {

        String query = "partition = ? and clientId = ?";
        Object[] params = {client.getPartition(), client.getClientId()};

        return messageHandler.getByQuery(PublishMessage.class, query, params);
    }

    @Override
    public Observable<PublishMessage> getMessage(String partition, String clientIdentifier, long messageId, boolean isInbound) {

        String query = "partition = ? and clientId = ? and messageId = ? and inBound = ?";
        Object[] params = {partition, clientIdentifier, messageId, isInbound};

        return messageHandler.getByQuery(PublishMessage.class, query, params);
    }

    @Override
    public Observable<Long> saveMessage(PublishMessage publishMessage) {

       return messageHandler.saveWithIdCheck(publishMessage);

    }

    @Override
    public void removeMessage(PublishMessage publishMessage) {
        messageHandler.remove(publishMessage);
    }

    @Override
    public Observable<RetainedMessage> getRetainedMessage(String partition, String topicFilterId) {
        return retainedMessageHandler.getByKey(RetainedMessage.createKey(partition, topicFilterId));
    }

    @Override
    public void saveRetainedMessage(RetainedMessage retainedMessage) {
        retainedMessageHandler.save(retainedMessage);
    }

    @Override
    public void removeRetainedMessage(RetainedMessage retainedMessage) {
        retainedMessageHandler.remove(retainedMessage);
    }

    @Override
    public String nextClientId() {
        long nextSequence = clientHandler.nextId();
        return String.format("iotracah-cl-id-%d", nextSequence);
    }


    @Override
    public IOTAccount getIOTAccount(String partition, String username) {

        String cacheKey = IOTAccount.createCacheKey(partition, username);

        return accountHandler.getByKeyWithDefault(cacheKey, null).toBlocking().single();
    }

    @Override
    public void saveIOTAccount(IOTAccount account) {

        accountHandler.save(account);

    }

    @Override
    public IOTRole getIOTRole(String partition, String rolename) {

        String cacheKey = IOTRole.createCacheKey(partition, rolename);

        return roleHandler.getByKeyWithDefault(cacheKey, null).toBlocking().single();
    }

    @Override
    public void saveIOTRole(IOTRole iotRole) {
        roleHandler.save(iotRole);
    }


}
