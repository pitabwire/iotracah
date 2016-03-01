/*
 *
 * Copyright (c) 2016 Caricah <info@caricah.com>.
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

package com.caricah.iotracah.core.worker;

import com.caricah.iotracah.bootstrap.data.messages.*;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.bootstrap.data.models.subscriptions.IotSubscription;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.worker.exceptions.ShutdownException;
import com.caricah.iotracah.core.worker.state.Constant;
import com.caricah.iotracah.core.worker.state.SessionResetManager;
import com.mashape.unirest.http.Unirest;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.shiro.session.Session;
import rx.Observable;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/15/15
 */
public class DumbWorker extends Worker {


    private HashMap<String, Set<Serializable>> subscriptions = new HashMap<>();
    private IgniteCache<String, Set<String>> igniteSubscriptions = null;


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

        boolean configAnnoymousLoginEnabled = configuration.getBoolean(CORE_CONFIG_WORKER_ANNONYMOUS_LOGIN_ENABLED, CORE_CONFIG_WORKER_ANNONYMOUS_LOGIN_ENABLED_DEFAULT_VALUE);

        log.debug(" configure : Anonnymous login is configured to be enabled [{}]", configAnnoymousLoginEnabled);

        setAnnonymousLoginEnabled(configAnnoymousLoginEnabled);


        String configAnnoymousLoginUsername = configuration.getString(CORE_CONFIG_WORKER_ANNONYMOUS_LOGIN_USERNAME, CORE_CONFIG_ENGINE_WORKER_ANNONYMOUS_LOGIN_USERNAME_DEFAULT_VALUE);
        log.debug(" configure : Anonnymous login username is configured to be [{}]", configAnnoymousLoginUsername);
        setAnnonymousLoginUsername(configAnnoymousLoginUsername);


        String configAnnoymousLoginPassword = configuration.getString(CORE_CONFIG_WORKER_ANNONYMOUS_LOGIN_PASSWORD, CORE_CONFIG_ENGINE_WORKER_ANNONYMOUS_LOGIN_PASSWORD_DEFAULT_VALUE);
        log.debug(" configure : Anonnymous login password is configured to be [{}]", configAnnoymousLoginPassword);
        setAnnonymousLoginPassword(configAnnoymousLoginPassword);


        int keepaliveInSeconds = configuration.getInt(CORE_CONFIG_WORKER_CLIENT_KEEP_ALIVE_IN_SECONDS, CORE_CONFIG_WORKER_CLIENT_KEEP_ALIVE_IN_SECONDS_DEFAULT_VALUE);
        log.debug(" configure : Keep alive maximum is configured to be [{}]", keepaliveInSeconds);
        setKeepAliveInSeconds(keepaliveInSeconds);

    }

    /**
     * <code>initiate</code> starts the operations of this system handler.
     * All excecution code for the plugins is expected to begin at this point.
     *
     * @throws UnRetriableException
     */
    @Override
    public void initiate() throws UnRetriableException {

        //Initiate the session reset manager.
        SessionResetManager sessionResetManager = new SessionResetManager();
        sessionResetManager.setWorker(this);
        sessionResetManager.setDatastore(this.getDatastore());
        setSessionResetManager(sessionResetManager);


        String igniteCacheName = "dumbTester";

        CacheConfiguration clCfg = new CacheConfiguration();

        clCfg.setName(igniteCacheName);
        clCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        clCfg.setCacheMode(CacheMode.PARTITIONED);
        clCfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);

        LruEvictionPolicy lruEvictionPolicy = new LruEvictionPolicy(5170000);
        clCfg.setEvictionPolicy(lruEvictionPolicy);

        clCfg.setSwapEnabled(true);
        igniteSubscriptions = getIgnite().createCache(clCfg);

        //Initiate unirest properties.
        Unirest.setTimeouts(5000, 5000);


    }

    /**
     * <code>terminate</code> halts excecution of this plugin.
     * This provides a clean way to exit /stop operations of this particular plugin.
     */
    @Override
    public void terminate() {
        //Shutdown unirest.
        try {
            Unirest.shutdown();
        } catch (IOException e) {
            log.warn(" terminate : problem closing unirest", e);
        }
    }

    /**
     * Provides the Observer with a new item to observe.
     * <p>
     * The {@link com.caricah.iotracah.core.modules.Server} may call this method 0 or more times.
     * <p>
     * The {@code Observable} will not call this method again after it calls either {@link #onCompleted} or
     * {@link #onError}.
     *
     * @param iotMessage the item emitted by the Observable
     */
    @Override
    public void onNext(IOTMessage iotMessage) {



        getExecutorService().submit(()->{
        log.info(" onNext : received {}", iotMessage);
        try {


            IOTMessage response = null;


            switch (iotMessage.getMessageType()) {
                case ConnectMessage.MESSAGE_TYPE:
                    ConnectMessage connectMessage = (ConnectMessage) iotMessage;
                    response = ConnectAcknowledgeMessage.from(connectMessage.isDup(), connectMessage.getQos(), connectMessage.isRetain(), connectMessage.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_ACCEPTED);

                    break;
                case SubscribeMessage.MESSAGE_TYPE:


                    SubscribeMessage subscribeMessage = (SubscribeMessage) iotMessage;

                    List<Integer> grantedQos = new ArrayList<>();
                    subscribeMessage.getTopicFilterList().forEach(topic ->
                    {

                        String topicKey = quickCheckIdKey("", Arrays.asList(topic.getKey().split(Constant.PATH_SEPARATOR)));

                        Set<String> channelIds = igniteSubscriptions.get(topicKey);

                        if (Objects.isNull(channelIds)) {
                            channelIds = new HashSet<>();
                        }

                        channelIds.add(subscribeMessage.getConnectionId());
                        igniteSubscriptions.put(topicKey, channelIds);

                        grantedQos.add(topic.getValue());

                    });

                    response = SubscribeAcknowledgeMessage.from(
                            subscribeMessage.getMessageId(), grantedQos);


                    break;
                case UnSubscribeMessage.MESSAGE_TYPE:
                    UnSubscribeMessage unSubscribeMessage = (UnSubscribeMessage) iotMessage;
                    response = UnSubscribeAcknowledgeMessage.from(unSubscribeMessage.getMessageId());

                    break;
                case Ping.MESSAGE_TYPE:
                    response = iotMessage;
                    break;
                case PublishMessage.MESSAGE_TYPE:


                    PublishMessage publishMessage = (PublishMessage) iotMessage;

                    Set<String> matchingTopics = getMatchingSubscriptions("", publishMessage.getTopic());

                        Map<String, Set<String>> channelIdMap = igniteSubscriptions.getAll(matchingTopics);

                        channelIdMap.values().forEach(channelIds -> {
                            if (Objects.nonNull(channelIds)) {

                                channelIds.forEach(id -> {

                                    PublishMessage clonePublishMessage = publishMessage.cloneMessage();
                                    clonePublishMessage.copyTransmissionData(iotMessage);
                                    clonePublishMessage.setConnectionId(id);
                                    pushToServer(clonePublishMessage);
                                });

                            }
                        });


                    if (MqttQoS.AT_MOST_ONCE.value() == publishMessage.getQos()) {

                        break;

                    } else if (MqttQoS.AT_LEAST_ONCE.value() == publishMessage.getQos()) {

                        response = AcknowledgeMessage.from(
                                publishMessage.getMessageId());
                        break;


                    }


                case PublishReceivedMessage.MESSAGE_TYPE:
                case ReleaseMessage.MESSAGE_TYPE:
                case CompleteMessage.MESSAGE_TYPE:
                case DisconnectMessage.MESSAGE_TYPE:
                case AcknowledgeMessage.MESSAGE_TYPE:
                default:
                    DisconnectMessage disconnectMessage = DisconnectMessage.from(true);
                    disconnectMessage.copyTransmissionData(iotMessage);

                    throw new ShutdownException(disconnectMessage);

            }

            if (Objects.nonNull(response)) {

                response.copyTransmissionData(iotMessage);
                pushToServer(response);
            }


        } catch (ShutdownException e) {

            IOTMessage response = e.getResponse();
            if (Objects.nonNull(response)) {
                pushToServer(response);
            }


        } catch (Exception e) {
            log.error(" onNext : Serious error that requires attention ", e);
        }

        });
    }

    private Set<String> getMatchingSubscriptions(String partition, String topic) {

        Set<String> topicFilterKeys = new HashSet<>();

        ListIterator<String> pathIterator = Arrays.asList(topic.split(Constant.PATH_SEPARATOR)).listIterator();

        List<String> growingTitles = new ArrayList<>();

        while (pathIterator.hasNext()) {

            String name = pathIterator.next();


            List<String> slWildCardList = new ArrayList<>(growingTitles);

            if (pathIterator.hasNext()) {
                //We deal with wildcard.
                slWildCardList.add(Constant.SINGLE_LEVEL_WILDCARD);
                topicFilterKeys.add(quickCheckIdKey(partition, slWildCardList));

            } else {
                //we deal with full topic
                slWildCardList.add(name);
            }

            List<String> reverseSlWildCardList = new ArrayList<>(slWildCardList);

            growingTitles.add(name);

            int sizeOfTopic = slWildCardList.size();
            if (sizeOfTopic > 1) {
                sizeOfTopic -= 1;

                for (int i = 0; i < sizeOfTopic; i++) {

                    if (i >= 0) {
                        slWildCardList.set(i, Constant.MULTI_LEVEL_WILDCARD);
                        reverseSlWildCardList.set(sizeOfTopic - i, Constant.MULTI_LEVEL_WILDCARD);

                        topicFilterKeys.add(quickCheckIdKey(partition, slWildCardList));
                        topicFilterKeys.add(quickCheckIdKey(partition, reverseSlWildCardList));

                    }
                }
            }


        }

        topicFilterKeys.add(quickCheckIdKey(partition, growingTitles));


        return topicFilterKeys;

    }

    private static String quickCheckIdKey(String partition, List<String> nameParts){

        return getPartitionAsInitialParentId(partition) +":"+ String.join(":", nameParts);
    }

    private static String getPartitionAsInitialParentId(String partition) {
        return "p[" + partition + "]";
    }


    @Override
    public void onStart(Session session) {

    }

    @Override
    public void onStop(Session session) {
        postSessionCleanUp((IOTClient) session, false);
    }

    @Override
    public void onExpiration(Session session) {

        log.debug(" onExpiration : -----------------------------------------------------");
        log.debug(" onExpiration : -------  We have an expired session {} -------", session);
        log.debug(" onExpiration : -----------------------------------------------------");

        postSessionCleanUp((IOTClient) session, true);
    }


    private void postSessionCleanUp(IOTClient iotClient, boolean isExpiry) {



        if (isExpiry) {
            log.debug(" postSessionCleanUp : ---------------- We are to publish a will man for {}", iotClient);
            publishWill(iotClient);
        }


        //Notify the server to remove this client from further sending in requests.
        DisconnectMessage disconnectMessage = DisconnectMessage.from(false);
        disconnectMessage = iotClient.copyTransmissionData(disconnectMessage);
        pushToServer(disconnectMessage);


        // Unsubscribe all

        if (iotClient.getIsCleanSession()) {
            Observable<IotSubscription> subscriptionObservable = getDatastore().getSubscriptions(iotClient);

            subscriptionObservable.subscribe(
                    subscription ->
                            getMessenger().unSubscribe(subscription)

                    , throwable -> log.error(" postSessionCleanUp : problems while unsubscribing", throwable)

                    , () -> {

                        Observable<PublishMessage> publishMessageObservable = getDatastore().getMessages(iotClient);
                        publishMessageObservable.subscribe(
                                getDatastore()::removeMessage,
                                throwable -> {
                                    log.error(" postSessionCleanUp : problems while unsubscribing", throwable);
                                    // any way still delete it from our db
                                },
                                () -> {
                                    // and delete it from our db
                                });

                    }
            );
        }

    }

}
