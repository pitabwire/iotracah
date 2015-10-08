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
import com.caricah.iotracah.core.worker.exceptions.ShutdownException;
import com.caricah.iotracah.core.worker.state.messages.UnSubscribeAcknowledgeMessage;
import com.caricah.iotracah.core.worker.state.messages.UnSubscribeMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.worker.state.models.Subscription;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.shiro.authz.AuthorizationException;
import rx.Observable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class UnSubscribeHandler extends RequestHandler {

    private UnSubscribeMessage message;

    public UnSubscribeHandler(UnSubscribeMessage message) {
        this.message = message;
    }

    @Override
    public void handle() throws RetriableException, UnRetriableException {


        /**
         * If a Server receives an UNSUBSCRIBE packet that contains multiple Topic
         * Filters it MUST handle that packet as if it had received a sequence of
         * multiple UNSUBSCRIBE packets, except that it sends just one UNSUBACK response
         */


        /**
         * Before unsubscribing we should get the current session and validate it.
         */

        Observable<Client> permittedObservable = checkPermission(message.getSessionId(),
                message.getAuthKey(), AuthorityRole.SUBSCRIBE,
                message.getTopicFilterList());

        permittedObservable.subscribe(isPermitted -> {

            for (String topic : message.getTopicFilterList()) {
                String partitionQosTopicFilter = Subscription.getPartitionQosTopicFilter(message.getPartition(), -1, topic);

                getMessenger().unSubscribe(message.getPartition(), message.getClientIdentifier(), partitionQosTopicFilter);
            }

            UnSubscribeAcknowledgeMessage unSubscribeAcknowledgeMessage = UnSubscribeAcknowledgeMessage.from(message.getMessageId(), false, message.getQos(), false);
            unSubscribeAcknowledgeMessage.copyBase(message);
            pushToServer(unSubscribeAcknowledgeMessage);


        }, this::disconnectDueToError);

    }


}
