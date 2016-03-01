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


import com.caricah.iotracah.bootstrap.data.messages.UnSubscribeAcknowledgeMessage;
import com.caricah.iotracah.bootstrap.data.messages.UnSubscribeMessage;
import com.caricah.iotracah.bootstrap.data.models.subscriptions.IotSubscription;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.core.security.AuthorityRole;
import rx.Observable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class UnSubscribeHandler extends RequestHandler<UnSubscribeMessage> {

    @Override
    public void handle(UnSubscribeMessage unSubscribeMessage) throws RetriableException, UnRetriableException {


        /**
         * If a Server receives an UNSUBSCRIBE packet that contains multiple Topic
         * Filters it MUST handle that packet as if it had received a sequence of
         * multiple UNSUBSCRIBE packets, except that it sends just one UNSUBACK response
         */


        /**
         * Before unsubscribing we should get the current session and validate it.
         */

        Observable<IOTClient> permittedObservable = checkPermission(unSubscribeMessage.getSessionId(),
                unSubscribeMessage.getAuthKey(), AuthorityRole.SUBSCRIBE,
                unSubscribeMessage.getTopicFilterList());

        permittedObservable.subscribe(iotSession -> {

            Observable<IotSubscription> subscriptionObservable = getDatastore().getSubscriptions(iotSession);

            subscriptionObservable.subscribe(
                    subscription ->

                        getMessenger().unSubscribe(subscription)

                    ,
                    throwable ->
                            log.error(" handle : problems unsubscribing ", throwable)


            );


            UnSubscribeAcknowledgeMessage unSubscribeAcknowledgeMessage = UnSubscribeAcknowledgeMessage.from(unSubscribeMessage.getMessageId());
            unSubscribeAcknowledgeMessage.copyTransmissionData(unSubscribeMessage);
            pushToServer(unSubscribeAcknowledgeMessage);


        }, throwable1 -> disconnectDueToError(throwable1, unSubscribeMessage ));

    }


}
