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


import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.bootstrap.data.messages.DisconnectMessage;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import org.apache.shiro.subject.Subject;
import rx.Observable;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class DisconnectHandler extends RequestHandler<DisconnectMessage> {


    @Override
    public void handle(DisconnectMessage disconnectMessage) throws RetriableException, UnRetriableException {


        /**
         * Before disconnecting we should get the current session and close it
         * then close the network connection.
         */

        Observable<IOTClient> permissionObservable = checkPermission(disconnectMessage.getSessionId(),
                disconnectMessage.getAuthKey(), AuthorityRole.CONNECT);

        permissionObservable.subscribe(
                (iotSession) -> {

                    if (disconnectMessage.isDirtyDisconnect()) {
                          getWorker().publishWill(iotSession);
                    }

                    logOutSession(iotSession.getId());

                }, (throwable -> {
                    log.warn(" handle : attempting to disconnect errorfull person.", throwable);
                }));


    }

    private void logOutSession(Serializable sessionId) {
       try {

           Subject subject = new Subject.Builder().sessionId(sessionId).buildSubject();
           subject.logout();

       }catch (Exception e){
           log.error(" logOutSession : problems during disconnection ", e);
       }

    }



}
