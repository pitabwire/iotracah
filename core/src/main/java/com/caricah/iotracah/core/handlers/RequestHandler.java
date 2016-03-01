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

package com.caricah.iotracah.core.handlers;


import com.caricah.iotracah.bootstrap.data.messages.base.Protocol;
import com.caricah.iotracah.bootstrap.data.models.client.IotClientKey;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.bootstrap.data.messages.DisconnectMessage;
import com.caricah.iotracah.bootstrap.security.realm.auth.permission.IOTPermission;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.core.worker.state.Messenger;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public abstract class RequestHandler<T extends IOTMessage> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final String SESSION_AUTH_KEY = "auth_key";

    private Worker worker;

    public Worker getWorker() {
        return worker;
    }

    public void setWorker(Worker worker) {
        this.worker = worker;
    }

    public Datastore getDatastore() {
        return getWorker().getDatastore();
    }

    public Messenger getMessenger() {
        return getWorker().getMessenger();
    }

    private IOTPermission getPermission(String partition, String username, String clientId, AuthorityRole role, String topic) {
        String permissionString = new StringJoiner("")
                .add(role.name())
                .add(":").add(topic).toString();

        return new IOTPermission(partition, username, clientId, permissionString);
    }

    public Observable<IOTClient> checkPermission(String sessionId, String authKey, AuthorityRole role, String... topicList) {
        return checkPermission(sessionId, authKey, role, Arrays.asList(topicList));

    }

    public Observable<IOTClient> checkPermission(String sessionId, String authKey, AuthorityRole role, List<String> topicList) {

        return Observable.create(observable -> {

            IotClientKey clientKey = new IotClientKey();
            clientKey.setSessionId(sessionId);

            Subject subject = new Subject.Builder().sessionId(clientKey).buildSubject();

            final IOTClient session = (IOTClient) subject.getSession(false);

            if (session != null && subject.isAuthenticated()) {


                try {

                    if (!AuthorityRole.CONNECT.equals(role)) {


                        if(Protocol.fromString(session.getProtocol()).isNotPersistent()) {

                            String session_auth_key = session.getAuthKey();

                            /**
                             * Make sure for non persistent connections the authKey matches
                             * the stored authKey. Otherwise fail the request.
                             */
                            if (!StringUtils.isEmpty(session_auth_key)) {
                                if (!session_auth_key.equals(authKey))
                                    throw new UnauthenticatedException("Client fails auth key assertion.");

                            }
                        }

                        List<Permission> permissions = topicList
                                .stream()
                                .map(topic ->
                                        getPermission( session.getPartitionId(), session.getUsername(),
                                                session.getClientIdentification(), role, topic))
                                .collect(Collectors.toList());

                        subject.checkPermissions(permissions);
                    }

                    //Update session last accessed time.
                    session.touch();

                    observable.onNext(session);
                    observable.onCompleted();

                } catch (AuthorizationException e) {
                    //Notify failure to authorize user.
                    observable.onError(e);
                }

            } else {
                observable.onError(new AuthenticationException("Client must be authenticated {Try connecting first} found : " + session));
            }

        });

    }


    public void disconnectDueToError(Throwable e, T message) {
        log.warn(" disconnectDueToError : System experienced the error ", e);

        //Notify the server to remove this client from further sending in requests.
        DisconnectMessage disconnectMessage = DisconnectMessage.from(true);
        disconnectMessage.copyTransmissionData(message);

        try {
            getWorker().getHandler(DisconnectHandler.class).handle(disconnectMessage);
        } catch (RetriableException | UnRetriableException ex) {
            log.error(" disconnectDueToError : issues disconnecting.", ex);
        }


    }

    /**
     * Wrapper method to assist in pushing/routing requests to the client.
     * It provides convenience to access the active worker and push
     * out the available messages to the connected client.
     *
     * @param iotMessage
     */
    public void pushToServer(IOTMessage iotMessage) {


        getWorker().pushToServer(iotMessage);

    }

    public abstract void handle(T message) throws RetriableException, UnRetriableException;
}
