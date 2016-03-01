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

import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.models.client.IotClientKey;
import com.caricah.iotracah.bootstrap.data.models.messages.IotMessageKey;
import com.caricah.iotracah.bootstrap.data.models.partition.IotPartition;
import com.caricah.iotracah.bootstrap.data.models.retained.IotMessageRetained;
import com.caricah.iotracah.bootstrap.data.models.roles.IotRoleKey;
import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.IotSubscriptionFilter;
import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.IotSubscriptionFilterKey;
import com.caricah.iotracah.bootstrap.data.models.subscriptions.IotSubscription;
import com.caricah.iotracah.bootstrap.data.models.subscriptions.IotSubscriptionKey;
import com.caricah.iotracah.bootstrap.data.models.users.IotAccountKey;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTAccount;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTRole;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.worker.exceptions.DoesNotExistException;
import com.caricah.iotracah.datastore.IotDataSource;
import com.caricah.iotracah.datastore.ignitecache.internal.impl.*;
import org.apache.commons.configuration.Configuration;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import rx.Observable;

import javax.naming.NamingException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/15/15
 */
public class IgniteDatastore extends Datastore {

    public static final String CONFIG_IGNITECACHE_PERSITENCE_ENABLED = "config.ignitecache.persistence.enabled";
    public static final boolean CONFIG_IGNITECACHE_PERSITENCE_ENABLED_VALUE_DEFAULT = false;
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DRIVER_NAME = "config.ignitecache.persistence.driver.name";
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DRIVER_NAME_VALUE_DEFAULT = "org.h2.Driver";
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DB_URL = "config.ignitecache.persistence.db.url";
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DB_URL_VALUE_DEFAULT = "jdbc:h2:~/iotracah/iotracah.db";
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DB_USERNAME = "config.ignitecache.persistence.db.username";
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DB_USERNAME_VALUE_DEFAULT = "iotracah";
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DB_PASSWORD = "config.ignitecache.persistence.db.password";
    public static final String CONFIG_IGNITECACHE_PERSITENCE_DB_PASSWORD_VALUE_DEFAULT = "!0tr@c@h";



    private final SubscriptionFilterHandler subscriptionFilterHandler = new SubscriptionFilterHandler();

    private final SubscriptionHandler subscriptionHandler = new SubscriptionHandler();

    private final MessageHandler messageHandler = new MessageHandler();

    private final RetainedMessageHandler retainedMessageHandler = new RetainedMessageHandler();

    private final AccountHandler accountHandler = new AccountHandler();

    private final RoleHandler roleHandler = new RoleHandler();

    private final ClientHandler clientHandler = new ClientHandler();

    private final PartitionHandler partitionHandler = new PartitionHandler();


    private boolean persistanceEnabled;

    public boolean isPersistanceEnabled() {
        return persistanceEnabled;
    }

    public void setPersistanceEnabled(boolean persistanceEnabled) {
        this.persistanceEnabled = persistanceEnabled;
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

        try {


            String driver = configuration.getString(CONFIG_IGNITECACHE_PERSITENCE_DRIVER_NAME, CONFIG_IGNITECACHE_PERSITENCE_DRIVER_NAME_VALUE_DEFAULT);
            String url = configuration.getString(CONFIG_IGNITECACHE_PERSITENCE_DB_URL, CONFIG_IGNITECACHE_PERSITENCE_DB_URL_VALUE_DEFAULT);
            String username = configuration.getString(CONFIG_IGNITECACHE_PERSITENCE_DB_USERNAME, CONFIG_IGNITECACHE_PERSITENCE_DB_USERNAME_VALUE_DEFAULT);
            String password = configuration.getString(CONFIG_IGNITECACHE_PERSITENCE_DB_PASSWORD, CONFIG_IGNITECACHE_PERSITENCE_DB_PASSWORD_VALUE_DEFAULT);

            IotDataSource.getInstance().setupDatasource(driver, url, username, password);

            boolean persistanceIsEnabled = configuration.getBoolean(CONFIG_IGNITECACHE_PERSITENCE_ENABLED, CONFIG_IGNITECACHE_PERSITENCE_ENABLED_VALUE_DEFAULT);
            setPersistanceEnabled(persistanceIsEnabled);

            partitionHandler.configure(configuration);

            subscriptionFilterHandler.configure(configuration);

            subscriptionHandler.configure(configuration);

            messageHandler.configure(configuration);

            retainedMessageHandler.configure(configuration);

            accountHandler.configure(configuration);

            roleHandler.configure(configuration);

            clientHandler.configure(configuration);

        } catch (NamingException e) {
            throw new UnRetriableException(e);
        }
    }

    /**
     * <code>initiate</code> starts the operations of this system handler.
     * All excecution code for the plugins is expected to begin at this point.
     *
     * @throws UnRetriableException
     */
    @Override
    public void initiate() throws UnRetriableException {

        partitionHandler.setPersistanceEnabled(isPersistanceEnabled());
        partitionHandler.initiate(IotPartition.class, getIgnite());
        partitionHandler.setExecutorService(getExecutorService());

        subscriptionFilterHandler.setPersistanceEnabled(isPersistanceEnabled());
        subscriptionFilterHandler.initiate(IotSubscriptionFilter.class, getIgnite());
        subscriptionFilterHandler.setExecutorService(getExecutorService());

        retainedMessageHandler.setPersistanceEnabled(isPersistanceEnabled());
        retainedMessageHandler.initiate(IotMessageRetained.class, getIgnite());
        retainedMessageHandler.setExecutorService(getExecutorService());

        subscriptionHandler.setPersistanceEnabled(isPersistanceEnabled());
        subscriptionHandler.initiate(IotSubscription.class, getIgnite());
        subscriptionHandler.setExecutorService(getExecutorService());

        messageHandler.setPersistanceEnabled(isPersistanceEnabled());
        messageHandler.initiate(PublishMessage.class, getIgnite());
        messageHandler.setExecutorService(getExecutorService());

        accountHandler.setPersistanceEnabled(isPersistanceEnabled());
        accountHandler.initiate(IOTAccount.class, getIgnite());
        accountHandler.setExecutorService(getExecutorService());

        roleHandler.setPersistanceEnabled(isPersistanceEnabled());
        roleHandler.initiate(IOTRole.class, getIgnite());
        roleHandler.setExecutorService(getExecutorService());

        clientHandler.setPersistanceEnabled(isPersistanceEnabled());
        clientHandler.initiate(IOTClient.class, getIgnite());
        clientHandler.setExecutorService(getExecutorService());

    }

    /**
     * <code>terminate</code> halts excecution of this plugin.
     * This provides a clean way to exit /stop operations of this particular plugin.
     */
    @Override
    public void terminate() {

    }


    @Override
    public Observable<PublishMessage> getWill(IOTClient client) {

        return Observable.create(observer -> {

            IotMessageKey messageKey = new IotMessageKey();
            messageKey.setPartitionId(client.getPartitionId());
            messageKey.setClientId(client.getSessionId());
            messageKey.setMessageId(PublishMessage.ID_TO_SHOW_IS_WILL);

                        Observable<PublishMessage> messageObservable = messageHandler.getByKey(messageKey);

                        messageObservable.subscribe(observer::onNext, observer::onError, observer::onCompleted);

        });
    }

    @Override
    public void saveWill(IOTClient client, PublishMessage publishMessage) {

        publishMessage.setPartitionId(client.getPartitionId());
        publishMessage.setClientId(client.getSessionId());
        publishMessage.setMessageId(PublishMessage.ID_TO_SHOW_IS_WILL);

        messageHandler.getByKey(messageHandler.keyFromModel(publishMessage)).subscribe(
                publishMessage1 -> {

                    publishMessage1.setTopic(publishMessage.getTopic());
                    publishMessage1.setPayload(publishMessage.getPayload());
                    publishMessage1.setQos(publishMessage.getQos());
                    messageHandler.save(publishMessage1);

                }, throwable -> {

                    messageHandler.save(publishMessage);
                }
        );


    }

    @Override
    public void removeWill(IOTClient client) {


                    IotMessageKey messageKey = new IotMessageKey();
                    messageKey.setPartitionId(client.getPartitionId());
                    messageKey.setClientId(client.getSessionId());
                    messageKey.setMessageId(PublishMessage.ID_TO_SHOW_IS_WILL);

        messageHandler.getByKey(messageKey).subscribe(messageHandler::remove, throwable -> {}, ()->{});
    }

    @Override
    public Observable<IotSubscriptionFilter> getMatchingSubscriptionFilter(String partition, String topic) {
        return subscriptionFilterHandler.matchTopicFilterTree(partition, getTopicNavigationRoute(topic));
    }

    @Override
    public Observable<IotSubscriptionFilter> getOrCreateSubscriptionFilter(String partition, String topic) {

        return Observable.create(observer -> {

            List<String> topicNavigationRoute = getTopicNavigationRoute(topic);


            IotSubscriptionFilterKey filterKey = subscriptionFilterHandler.keyFromList(partition, topicNavigationRoute);


            Observable<IotSubscriptionFilter> filterObservable = subscriptionFilterHandler
                    .getByKeyWithDefault(filterKey, null);

            filterObservable.subscribe(

                    subscriptionFilter -> {

                        if (Objects.isNull(subscriptionFilter)) {

                            subscriptionFilterHandler.createTree(
                                    partition, topicNavigationRoute).single()
                                    .subscribe(observer::onNext, observer::onError, observer::onCompleted);
                        } else {
                            observer.onNext(subscriptionFilter);
                        }

                    }, observer::onError, observer::onCompleted);

        });


    }

    @Override
    public Observable<IotSubscriptionFilter> getSubscriptionFilterTree(String partitionId, String topicName) {

      return  subscriptionFilterHandler.getTopicFilterTree(partitionId, getTopicNavigationRoute(topicName) );
    }


    @Override
    public void removeSubscriptionFilter(IotSubscriptionFilter subscriptionFilter) {

        subscriptionFilterHandler.remove(subscriptionFilter);
    }


    @Override
    public Observable<IotSubscription> getSubscriptions(IOTClient iotClient) {
        String query = "clientId = ?";
        Object[] params = {iotClient.getSessionId()};
        return subscriptionHandler.getByQuery(IotSubscription.class, query, params);

    }

    @Override
    public Observable<IotSubscription> getSubscriptions(IotSubscriptionFilter subscriptionFilter, int qos) {

        String query = "partitionId = ? and subscriptionFilterId = ? and qos >= ?";
        Object[] params = {subscriptionFilter.getPartitionId(), subscriptionFilter.getId(), qos};
        return subscriptionHandler.getByQuery(IotSubscription.class, query, params);

    }

    @Override
    public Observable<IotSubscription> getSubscriptionWithDefault(IotSubscription subscription) {


        return Observable.create(observable ->{

            IotSubscriptionKey subscriptionKey = subscriptionHandler.keyFromModel(subscription);

            subscriptionHandler.getByKey(subscriptionKey).subscribe(observable::onNext, throwable -> {

                if(throwable instanceof DoesNotExistException){

                    subscription.setId(subscriptionHandler.getIdSequence().incrementAndGet());
                    observable.onNext(subscription);
                    observable.onCompleted();

                }else{
                    observable.onError(throwable);
                }

            }, observable::onCompleted);

        });


        }

    @Override
    public void saveSubscription(IotSubscription subscription) {
        subscriptionHandler.save(subscription);
    }

    @Override
    public void removeSubscription(IotSubscription subscription) {
        subscriptionHandler.remove(subscription);
    }


    @Override
    public Observable<PublishMessage> getMessages(IOTClient iotClient) {

        String query = "clientId = ?";
        Object[] params = {iotClient.getSessionId()};

        return messageHandler.getByQuery(PublishMessage.class, query, params);
    }

    @Override
    public Observable<PublishMessage> getMessage(IOTClient iotClient, long messageId, boolean isInbound) {

        String query = "clientId = ? and messageId = ? and isInbound = ?";
        Object[] params = {iotClient.getSessionId(), messageId, isInbound};

        return messageHandler.getByQuery(PublishMessage.class, query, params);
    }

    @Override
    public Observable<Map.Entry<Long, IotMessageKey>> saveMessage(PublishMessage publishMessage) {

        return messageHandler.saveWithIdCheck(publishMessage);

    }

    @Override
    public void removeMessage(PublishMessage publishMessage) {
        messageHandler.remove(publishMessage);
    }

    @Override
    public Observable<PublishMessage> getRetainedMessage(IotSubscriptionFilter subscriptionFilter) {

        return Observable.create(observer -> {
            Observable<IotMessageRetained> subscriptionFilterRetainedMessageObservable = retainedMessageHandler.getRetainedMessagesByFilter(subscriptionFilter);

            subscriptionFilterRetainedMessageObservable.subscribe(

                    iotMessageRetained -> {

                        ByteBuffer byteBuffer = ByteBuffer.wrap((byte[]) iotMessageRetained.getPayload());

                        PublishMessage publishMessage = PublishMessage.from(PublishMessage.ID_TO_FORCE_GENERATION_ON_SAVE, false, iotMessageRetained.getQos(), false, subscriptionFilter.getName(), byteBuffer, false);

                        observer.onNext(publishMessage);

                    }, observer::onError, observer::onCompleted);


        });
    }

    @Override
    public void saveRetainedMessage(IotSubscriptionFilter subscriptionFilter, int qos, Object payload) {


        Observable<IotMessageRetained> subscriptionFilterRetainedMessageObservable = retainedMessageHandler.getRetainedMessagesByFilter(subscriptionFilter);

        subscriptionFilterRetainedMessageObservable.subscribe(

                iotMessageRetained -> {

                    iotMessageRetained.setQos(qos);
                    iotMessageRetained.setPayload(payload);
                    retainedMessageHandler.save(iotMessageRetained);




                }, throwable -> {

                    if(throwable instanceof  DoesNotExistException){

                        IotMessageRetained iotMessageRetained = new IotMessageRetained();
                        iotMessageRetained.setIsActive(true);
                        iotMessageRetained.setPartitionId(subscriptionFilter.getPartitionId());
                        iotMessageRetained.setSubscriptionFilterId(subscriptionFilter.getId());
                        iotMessageRetained.setQos(qos);
                        iotMessageRetained.setPayload(payload);
                        iotMessageRetained.setId(retainedMessageHandler.getIdSequence().incrementAndGet());
                        retainedMessageHandler.save(iotMessageRetained);
                    }
                }

        );


    }

    @Override
    public void removeRetainedMessage(IotSubscriptionFilter iotSubscriptionFilter) {


        Observable<IotMessageRetained> subscriptionFilterRetainedMessageObservable = retainedMessageHandler.getRetainedMessagesByFilter(iotSubscriptionFilter);

        subscriptionFilterRetainedMessageObservable.subscribe(retainedMessageHandler::remove, throwable -> {}, ()->{});


    }


    @Override
    public IOTAccount getIOTAccount(String partition, String username) {

        IotAccountKey userKey = new IotAccountKey();
        userKey.setPartitionId(partition);
        userKey.setUsername(username);

        return accountHandler.getByKeyWithDefault(userKey, null).toBlocking().single();
    }

    @Override
    public void saveIOTAccount(IOTAccount account) {

        accountHandler.save(account);

    }

    @Override
    public IOTRole getIOTRole(String partition, String rolename) {

        IotRoleKey roleKey = new IotRoleKey();
        roleKey.setPartitionId(partition);
        roleKey.setName(rolename);

        return roleHandler.getByKeyWithDefault(roleKey, null).toBlocking().single();
    }

    @Override
    public void saveIOTRole(IOTRole iotRole) {
        roleHandler.save(iotRole);
    }


    @Override
    public Serializable create(Session session) {

        IOTClient client = (IOTClient) session;
        clientHandler.save(client);

        return client.getId();
    }

    @Override
    public Session readSession(Serializable clientId) throws UnknownSessionException {

        return clientHandler.getByKeyWithDefault((IotClientKey) clientId, null).toBlocking().single();

    }

    @Override
    public void update(Session session) throws UnknownSessionException {
        clientHandler.save((IOTClient) session);
    }

    @Override
    public void delete(Session session) {
        IOTClient client = (IOTClient) session;
        clientHandler.remove(client);
    }

    @Override
    public Collection<Session> getActiveSessions() {

        try {

            String query = "isActive = ? AND expiryTimestamp < ? LIMIT ?";

            Object[] params = {true, Timestamp.from(Instant.now()), 100};

            Observable<IOTClient> clientObservable = clientHandler.getByQuery(IOTClient.class, query, params);

            Set<Session> activeSessions = new HashSet<>();
            clientObservable.toBlocking().forEach(activeSessions::add);

            if(!activeSessions.isEmpty()) {
                log.debug(" getActiveSessions : found {} expired sessions", activeSessions.size());
            }

            return activeSessions;
        } catch (Exception e) {
            log.error(" getActiveSessions : problems with active sessions collector ", e);
            return Collections.emptySet();
        }

    }
}
