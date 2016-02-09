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

package com.caricah.iotracah.core.worker.state;

import com.caricah.iotracah.bootstrap.security.realm.state.IOTSession;
import com.caricah.iotracah.core.handlers.PublishOutHandler;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.messages.PublishReceivedMessage;
import com.caricah.iotracah.bootstrap.data.messages.ReleaseMessage;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 7/30/15
 */
public class SessionResetManager {

    private static final Logger log = LoggerFactory.getLogger(SessionResetManager.class);

    private Worker worker;

    private Datastore datastore;

    public Worker getWorker() {
        return worker;
    }

    public void setWorker(Worker worker) {
        this.worker = worker;
    }

    public Datastore getDatastore() {
        return datastore;
    }

    public void setDatastore(Datastore datastore) {
        this.datastore = datastore;
    }

    public void process(IOTSession iotSession) {

        log.debug(" process : Resetting a session for client {} ", iotSession);


        Observable<PublishMessage> publishMessageObservable = getDatastore().getMessages(iotSession);

        publishMessageObservable.subscribe(publishMessage -> {

            publishMessage = iotSession.copyTransmissionData(publishMessage);
            //Update current session id for message.

                    if (publishMessage.isInBound()) {

                        //We need to generate a PUBREC message to acknowledge message received.
                        if (publishMessage.getQos() == MqttQoS.EXACTLY_ONCE.value()) {


                            PublishReceivedMessage publishReceivedMessage = PublishReceivedMessage.from(publishMessage.getMessageId());
                            publishReceivedMessage = iotSession.copyTransmissionData(publishReceivedMessage);
                            getWorker().pushToServer(publishReceivedMessage);


                        }

                    } else {

                        if (publishMessage.getQos() == MqttQoS.EXACTLY_ONCE.value() && publishMessage.isReleased()) {

                            //We need to generate a PUBREL message to allow transmission of qos 2 message.
                            ReleaseMessage releaseMessage = ReleaseMessage.from(publishMessage.getMessageId(), true);
                            releaseMessage = iotSession.copyTransmissionData(releaseMessage);
                            getWorker().pushToServer(releaseMessage);


                        } else {

                            //This message should be released to the client

                            try {
                                getWorker().getHandler(PublishOutHandler.class).handle(publishMessage);
                            } catch (RetriableException | UnRetriableException e) {
                                log.error(" process : problems releasing stored messages", e);
                            }

                        }
                    }

        });


    }


}
