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


import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.core.worker.state.messages.DisconnectMessage;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.messages.WillMessage;
import com.caricah.iotracah.core.worker.state.models.Subscription;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import rx.Observable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class DisconnectHandler extends RequestHandler<DisconnectMessage> {


    public DisconnectHandler(DisconnectMessage message) {
        super(message);
    }

    @Override
    public void handle() throws RetriableException, UnRetriableException {


        /**
         * Before disconnecting we should get the current session and close it
         * then close the network connection.
         */

        Observable<Client> permissionObservable = checkPermission(getMessage().getSessionId(),
                getMessage().getAuthKey(), AuthorityRole.CONNECT);

        permissionObservable.subscribe(
                (client) -> {

                    if (getMessage().isCleanDisconnect()) {
                        cleanDisconnect(client);
                    } else {
                        dirtyDisconnect(client);
                    }

                }, (throwable -> {
                    log.warn(" handle : attempting to disconnect a disconnected person.");
                }));


    }


    public void cleanDisconnect(Client client) {

        // Unsubscribe all
        //Notify the server to remove this client from further sending in requests.
        DisconnectMessage disconnectMessage = DisconnectMessage.from(true);
        disconnectMessage = client.copyTransmissionData(disconnectMessage);
        pushToServer(disconnectMessage);

            Observable<Subscription> subscriptionObservable = getDatastore().getSubscriptions(client);

            subscriptionObservable.subscribe(
                    subscription -> {
                        getMessenger().unSubscribe(subscription);

                    }, throwable -> {
                        // and delete it from our db
                        getDatastore().removeClient(client);

                    }, () -> {
                        // and delete it from our db
                        getDatastore().removeClient(client);

                    }
            );

    }


    public void dirtyDisconnect(Client client) {

        log.debug(" dirtyDisconnect : client : " + client.getClientId() + " may have lost connectivity.");

        //Mark the client as inactive
        //Publish will before handling other
        client.setActive(false);

        getDatastore().saveClient(client);

        Observable<WillMessage> willMessageObservable = client.getWill(getDatastore());

        willMessageObservable.subscribe(
                willMessage -> {

                    if (null != willMessage && null != willMessage.getPayload()) {

                        PublishMessage willPublishMessage = willMessage.toPublishMessage();

                        willPublishMessage.copyBase(willMessage);

                        try {
                            client.internalPublishMessage(getMessenger(), willPublishMessage);
                        } catch (RetriableException e) {
                            log.error(" dirtyDisconnect : experienced issues publishing will.", e);
                        }
                    }

                }
        );


        if (client.isCleanSession()) {

            cleanDisconnect(client);

        }
    }

}
