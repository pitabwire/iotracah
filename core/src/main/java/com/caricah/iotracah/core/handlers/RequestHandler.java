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


import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.worker.state.messages.DisconnectMessage;
import com.caricah.iotracah.security.IOTSecurityManager;
import com.caricah.iotracah.security.realm.auth.IdConstruct;
import com.caricah.iotracah.security.realm.auth.permission.IOTPermission;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.worker.state.Messenger;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.PrincipalCollection;
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

    public Datastore getDatastore(){
        return getWorker().getDatastore();
    }

    public Messenger getMessenger(){
        return  getWorker().getMessenger();
    }

    private IOTPermission getPermission( String partition, String username, String clientId, AuthorityRole role, String topic) {
        String permissionString = new StringJoiner("")
                .add(role.name())
                .add(":").add(topic).toString();

        return new IOTPermission(partition, username, clientId, permissionString);
    }

    public Observable<Client> checkPermission(Serializable sessionId, String authKey, AuthorityRole role, String ... topicList) {
        return checkPermission(sessionId, authKey, role, Arrays.asList(topicList));

    }

    public Observable<Client> checkPermission(Serializable sessionId, String authKey, AuthorityRole role, List<String>  topicList) {

        return Observable.create(observable ->{

            Subject subject = new Subject.Builder().sessionId(sessionId).buildSubject();

            final Session session = subject.getSession(false);

            if (session != null && subject.isAuthenticated() ) {


                try{


                PrincipalCollection principales = (PrincipalCollection) session.getAttribute(IOTSecurityManager.SESSION_PRINCIPLES_KEY);
                IdConstruct construct = (IdConstruct) principales.getPrimaryPrincipal();

                    String partition = construct.getPartition();
                    String username = construct.getUsername();
                    String session_client_id = construct.getClientId();


                    String session_auth_key = (String) session.getAttribute(SESSION_AUTH_KEY);



                /**
                 * Make sure for non persistent connections the authKey matches
                 * the stored authKey. Otherwise fail the request.
                 */
                if(!StringUtils.isEmpty(session_auth_key)){
                    if(!session_auth_key.equals(authKey))
                        throw new UnauthenticatedException("Client fails auth key assertion.");

                }

                    if(AuthorityRole.CONNECT.equals(role)){
                        //No need to check for this permission.
                    }else {

                        List<Permission> permissions = topicList
                                .stream()
                                .map(topic ->
                                        getPermission(
                                                username, partition,
                                                session_client_id,
                                                role, topic))
                                .collect(Collectors.toList());


                        subject.checkPermissions(permissions);
                    }

                    //Update session last accessed time.
                    session.touch();

                    Observable<Client> clientObservable = getDatastore().getClient(partition, session_client_id);

                    clientObservable.subscribe(observable::onNext, observable::onError, observable::onCompleted);


                }catch (AuthorizationException e) {
                    //Notify failure to authorize user.
                    observable.onError(e);
                }

            }else{
                observable.onError(new AuthenticationException("Client must be authenticated {Try connecting first} found : "+ session));
            }

        });

    }


    public void disconnectDueToError(Throwable e, T message){
        log.warn(" disconnectDueToError : System experienced the error ", e);

        //Notify the server to remove this client from further sending in requests.
        DisconnectMessage disconnectMessage = DisconnectMessage.from(true);
        disconnectMessage.copyBase(message);

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
     * @param iotMessage
     */
    public void pushToServer(IOTMessage iotMessage){


        getWorker().pushToServer(iotMessage);

    }

    public abstract void handle(T message) throws RetriableException, UnRetriableException;
}
