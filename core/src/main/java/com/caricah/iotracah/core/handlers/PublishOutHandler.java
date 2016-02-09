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


import com.caricah.iotracah.core.handlers.protocal.http.OnPushSuccessListener;
import com.caricah.iotracah.core.handlers.protocal.http.PushHandler;
import com.caricah.iotracah.bootstrap.data.messages.AcknowledgeMessage;
import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class PublishOutHandler extends RequestHandler<PublishMessage> implements OnPushSuccessListener {



    @Override
    public void handle(PublishMessage publishMessage) throws RetriableException, UnRetriableException {

        log.debug(" handle : outbound message {} being processed", publishMessage);

        if (publishMessage.getProtocol().isPersistent()) {

            //We need to generate a publish message to start this conversation.
            pushToServer(publishMessage);

        } else {
            switch (publishMessage.getProtocol()) {

                case HTTP:
                    PushHandler httpPushHandler = new PushHandler();
                    httpPushHandler.pushToUrl(publishMessage, this);
                    break;
                default:
                    log.error(" handle : outbound message {} using none implemented protocal");
            }
        }
    }

    @Override
    public void success(AcknowledgeMessage acknowledgeMessage) {

        try {
            getWorker().getHandler(PublishAcknowledgeHandler.class).handle(acknowledgeMessage);
        } catch (RetriableException | UnRetriableException e) {
            log.warn(" httpPushToUrl completed : problem closing connection. ");
        }
    }
}
