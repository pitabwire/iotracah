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

package com.caricah.iotracah.core.worker;

import com.caricah.iotracah.core.handlers.*;
import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.core.worker.exceptions.ShutdownException;
import com.caricah.iotracah.core.worker.state.messages.*;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.worker.state.SessionResetManager;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.Subscription;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.security.IOTSecurityManager;
import com.caricah.iotracah.security.realm.auth.IdConstruct;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.PrincipalCollection;
import rx.Observable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/15/15
 */
public class DefaultWorker extends Worker {


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


        log.info(" onNext : received {}", iotMessage);
        RequestHandler requestHandler = getHandlerForMessage(iotMessage);
        try {

            if (null != requestHandler) {
                requestHandler.handle();
            } else {

                throw new ShutdownException("Unknown messages being propergated");
            }
        } catch (ShutdownException e) {

            IOTMessage response = e.getResponse();
            if (null != response) {
                pushToServer(response);
            }

            try {
                DisconnectMessage disconnectMessage = DisconnectMessage.from(true);
                disconnectMessage.copyBase(iotMessage);

                DisconnectHandler disconnectHandler = new DisconnectHandler(disconnectMessage);
                disconnectHandler.setWorker(this);
                disconnectHandler.handle();
            } catch (RetriableException | UnRetriableException finalEx) {
                log.error(" onNext : Problems disconnecting.", finalEx);
            }

        } catch (Exception e) {
            log.error(" onNext : Serious error that requires attention ", e);
        }

    }

    private RequestHandler getHandlerForMessage(IOTMessage iotMessage) {

        RequestHandler requestHandler;

        switch (iotMessage.getMessageType()) {
            case ConnectMessage.MESSAGE_TYPE:

                ConnectMessage connectMessage = (ConnectMessage) iotMessage;
                if(connectMessage.isAnnonymousSession() && isAnnonymousLoginEnabled()){
                    connectMessage.setUserName(getAnnonymousLoginUsername());
                    connectMessage.setPassword(getAnnonymousLoginPassword());
                }

                if(connectMessage.getKeepAliveTime() <= 0){
                    connectMessage.setKeepAliveTime(getKeepAliveInSeconds());
                }

                requestHandler = new ConnectionHandler(connectMessage);

                break;
            case SubscribeMessage.MESSAGE_TYPE:
                requestHandler = new SubscribeHandler((SubscribeMessage) iotMessage);
                break;
            case UnSubscribeMessage.MESSAGE_TYPE:
                requestHandler = new UnSubscribeHandler((UnSubscribeMessage) iotMessage);
                break;
            case Ping.MESSAGE_TYPE:
                requestHandler = new PingRequestHandler((Ping) iotMessage);
                break;
            case PublishMessage.MESSAGE_TYPE:
                requestHandler = new PublishInHandler((PublishMessage) iotMessage);

                break;
            case PublishReceivedMessage.MESSAGE_TYPE:
                requestHandler = new PublishReceivedHandler((PublishReceivedMessage) iotMessage);

                break;
            case ReleaseMessage.MESSAGE_TYPE:
                requestHandler = new PublishReleaseHandler((ReleaseMessage) iotMessage);
                break;
            case CompleteMessage.MESSAGE_TYPE:
                requestHandler = new PublishCompleteHandler((CompleteMessage) iotMessage);
                break;
            case DisconnectMessage.MESSAGE_TYPE:
                requestHandler = new DisconnectHandler((DisconnectMessage) iotMessage);
                break;
            case AcknowledgeMessage.MESSAGE_TYPE:
                requestHandler = new PublishAcknowledgeHandler((AcknowledgeMessage) iotMessage);
                break;
            default:
                return null;
        }

        requestHandler.setWorker(this);


        return requestHandler;
    }


    @Override
    public void onStart(Session session) {

    }

    @Override
    public void onStop(Session session) {
        postSessionCleanUp(session, false);
    }

    @Override
    public void onExpiration(Session session) {


        log.debug(" onExpiration : -----------------------------------------------------");
        log.debug(" onExpiration : -------  We have an expired session {} -------", session);
        log.debug(" onExpiration : -----------------------------------------------------");


        postSessionCleanUp(session, true);
    }



    private void postSessionCleanUp(Session session, boolean isExpiry){

        PrincipalCollection principales = (PrincipalCollection) session.getAttribute(IOTSecurityManager.SESSION_PRINCIPLES_KEY);
        IdConstruct construct = (IdConstruct) principales.getPrimaryPrincipal();

        String partition = construct.getPartition();
        String session_client_id = construct.getClientId();

        Observable<Client> clientObservable = getDatastore().getClient(partition, session_client_id);

        clientObservable.subscribe(client -> {

            if(isExpiry){
                log.debug(" postSessionCleanUp : ---------------- We are to publish a will man for {}", client);
                publishWill(client);
            }


            //Notify the server to remove this client from further sending in requests.
            DisconnectMessage disconnectMessage = DisconnectMessage.from(false);
            disconnectMessage = client.copyTransmissionData(disconnectMessage);
            pushToServer(disconnectMessage);


            // Unsubscribe all

            if(client.isCleanSession()) {
                Observable<Subscription> subscriptionObservable = getDatastore().getSubscriptions(client);

                subscriptionObservable.subscribe(
                        subscription ->
                                getMessenger().unSubscribe(subscription)

                        , throwable -> log.error(" postSessionCleanUp : problems while unsubscribing", throwable)

                        , () -> {

                            Observable<PublishMessage> publishMessageObservable = getDatastore().getMessages(client);
                            publishMessageObservable.subscribe(
                                    getDatastore()::removeMessage,
                                    throwable ->{
                                        log.error(" postSessionCleanUp : problems while unsubscribing", throwable);
                                        // any way still delete it from our db
                                        getDatastore().removeClient(client);
                                    },
                                    () -> {
                                        // and delete it from our db
                                        getDatastore().removeClient(client);
                                    });

                        }
                );
            }else{
                //Mark the client as inactive
                client.setActive(false);
                client.setSessionId(null);
                getDatastore().saveClient(client);

            }



        }, throwable -> log.error(" postSessionCleanUp : problems obtaining user for session {}", session));


    }

}
