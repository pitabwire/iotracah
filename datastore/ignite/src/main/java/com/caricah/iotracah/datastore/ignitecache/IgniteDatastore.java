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
import com.caricah.iotracah.core.worker.state.messages.WillMessage;
import com.caricah.iotracah.core.worker.state.models.ClSubscription;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.SubscriptionFilter;
import com.caricah.iotracah.datastore.ignitecache.internal.impl.*;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.security.realm.state.IOTAccount;
import com.caricah.iotracah.security.realm.state.IOTRole;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.IgniteAtomicSequence;
import rx.Observable;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/15/15
 */
public class IgniteDatastore extends Datastore {

    private IgniteAtomicSequence clientIdSequence;

    private IgniteAtomicSequence subscriptionIdSequence;

    private IgniteAtomicSequence subscriptionFilterIdSequence;

    private final ClientHandler clientHandler = new ClientHandler();

    private final WillHandler willHandler = new WillHandler();

    private final SubscriptionFilterHandler subscriptionFilterHandler = new SubscriptionFilterHandler();

    private final SubscriptionHandler subscriptionHandler = new SubscriptionHandler();

    private final MessageHandler messageHandler = new MessageHandler();

    private final AccountHandler accountHandler = new AccountHandler();

    private final RoleHandler roleHandler = new RoleHandler();

    public IgniteAtomicSequence getClientIdSequence() {
        return clientIdSequence;
    }

    public void setClientIdSequence(IgniteAtomicSequence clientIdSequence) {
        this.clientIdSequence = clientIdSequence;
    }

    public IgniteAtomicSequence getSubscriptionIdSequence() {
        return subscriptionIdSequence;
    }

    public void setSubscriptionIdSequence(IgniteAtomicSequence subscriptionIdSequence) {
        this.subscriptionIdSequence = subscriptionIdSequence;
    }

    public IgniteAtomicSequence getSubscriptionFilterIdSequence() {
        return subscriptionFilterIdSequence;
    }

    public void setSubscriptionFilterIdSequence(IgniteAtomicSequence subscriptionFilterIdSequence) {
        this.subscriptionFilterIdSequence = subscriptionFilterIdSequence;
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

        clientHandler.configure(configuration);

        subscriptionFilterHandler.configure(configuration);

        subscriptionHandler.configure(configuration);

        messageHandler.configure(configuration);

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
        subscriptionHandler.initiate(ClSubscription.class, getIgnite());
        messageHandler.initiate(PublishMessage.class, getIgnite());
        willHandler.initiate(WillMessage.class, getIgnite());
        accountHandler.initiate(IOTAccount.class, getIgnite());
        roleHandler.initiate(IOTRole.class, getIgnite());


        long currentTime = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        String nameOfSequenceForClientId = "iotracah-sequence-client-id";
        IgniteAtomicSequence seq = getIgnite().atomicSequence(nameOfSequenceForClientId, currentTime, true);
        setClientIdSequence(seq);


        String nameOfSequenceForSubscriptionId = "iotracah-sequence-subscription-id";
        IgniteAtomicSequence seq2 = getIgnite().atomicSequence(nameOfSequenceForSubscriptionId, currentTime, true);
        setSubscriptionIdSequence(seq2);

        String nameOfSequenceForSubscriptionFilterId = "iotracah-sequence-subscription-filter-id";
        IgniteAtomicSequence seq3 = getIgnite().atomicSequence(nameOfSequenceForSubscriptionFilterId, currentTime, true);
        setSubscriptionFilterIdSequence(seq3);


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

        String query = "partition = ? and clientId = ?";
        Object[] params = {partition, clientIdentifier};

        return clientHandler.getByQuery(Client.class, query, params);
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

        String query = "partition = ? and clientId = ?";
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
    public Observable<SubscriptionFilter> getSubscriptionFilter(String partition, int qos, String topicFilter) {

        String query = "partition = ? and qos = ? and topicFilter = ?";
        Object[] params = {partition, qos, topicFilter};
        return subscriptionFilterHandler.getByQuery(SubscriptionFilter.class, query, params);

    }




    @Override
    public void saveSubscriptionFilter(SubscriptionFilter subscriptionFilter) {
        subscriptionFilterHandler.save(subscriptionFilter);
    }

    @Override
    public void removeSubscriptionFilter(SubscriptionFilter subscriptionFilter) {
        subscriptionFilterHandler.remove(subscriptionFilter);
    }


    @Override
    public Observable<ClSubscription> getSubscription(Client client) {
        String query = "partition = ? and clientId = ?";
        Object[] params = {client.getPartition(), client.getClientId()};
        return subscriptionHandler.getByQuery(ClSubscription.class, query, params);

    }

    @Override
    public Observable<ClSubscription> getSubscription(Client client, Collection<String> topicFilterList) {

        return Observable.create(observer -> {

            Observable<List> subscriptionFilterListObservable = subscriptionFilterHandler.getAsList(client.getPartition(), 0, topicFilterList);

            subscriptionFilterListObservable.subscribe(

                    subscriptionFilterList -> {

//                        Observable<ClSubscription> subscriptionObservable = subscriptionHandler.getSubscription(client.getPartition(), subscriptionFilterList);
//                        subscriptionObservable.subscribe(observer::onNext, observer::onError, observer::onCompleted);


                    }, observer::onError
            );

        });

    }

    @Override
    public Observable<ClSubscription> getSubscription(String partition, int qos, Collection<String> topicFilterList) {


        return Observable.create(observer ->
                     {

                log.debug(" getSubscription : processing request to :  ");
                log.debug(" getSubscription : ************************************ ***********************************************");
                log.debug(" getSubscription : **** get subscriptions for {} {} {} ********* ", partition, qos, topicFilterList);
                log.debug(" getSubscription : ************************************************************************************ ");

                Observable<List> subscriptionFilterListObservable = subscriptionFilterHandler.getAsList(partition, qos, topicFilterList);

                subscriptionFilterListObservable.subscribe(

                        subscriptionFilterList -> {

                            Object[] array = subscriptionFilterList.stream().map(o1 -> ((List) o1).get(0)).filter(id -> null != id).toArray(Object[]::new);

                            if(null ==array || array.length <=0){
                                observer.onCompleted();
                            }else {


                                Observable<ClSubscription> subscriptionObservable = subscriptionHandler.getSubscription(partition, array);
                                subscriptionObservable.subscribe(observer::onNext, observer::onError, observer::onCompleted);
                            }
                        }, observer::onError);}

        );

    }

    @Override
    public void saveSubscription(ClSubscription clSubscription) {

        log.debug(" saveSubscription : -----------------------------------------------------------");
        log.debug(" saveSubscription : --------------- Obtained {} -----------", clSubscription);
        log.debug(" saveSubscription : -----------------------------------------------------------");


        subscriptionHandler.save(clSubscription);
    }

    @Override
    public void removeSubscription(ClSubscription clSubscription) {
        subscriptionHandler.remove(clSubscription);
    }


    @Override
    public Observable<PublishMessage> getActiveMessages(Client client) {

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

    @Override
    public String nextSubscriptionFilterId() {
        return "" + getSubscriptionFilterIdSequence().incrementAndGet();
    }

    @Override
    public String nextSubscriptionId() {
        return "" + getSubscriptionIdSequence().incrementAndGet();
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
