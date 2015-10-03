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

import com.caricah.iotracah.core.handlers.PublishOutHandler;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.worker.state.messages.AcknowledgeMessage;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.messages.ReleaseMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

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

    public void process(Client client) {

        Observable<PublishMessage> publishMessageObservable = getDatastore().getActiveMessages(client);

        publishMessageObservable.subscribe(new Subscriber<PublishMessage>() {
            @Override
            public void onCompleted() {

            }

            @Override
            public void onError(Throwable e) {

            }

            @Override
            public void onNext(PublishMessage publishMessage) {

                if (publishMessage.isInBound()) {

                    //We need to generate a PUBREC message to acknowledge message received.
                    if (publishMessage.getQos() == MqttQoS.EXACTLY_ONCE.value()) {


//                        MqttFixedHeader recFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, publishMessage.isDup(), MqttQoS.EXACTLY_ONCE, publishMessage.isRetain(), 0);
//                        MqttMessageIdVariableHeader msgIdVariableHeader = MqttMessageIdVariableHeader.from(publishMessage.getMessageId());
//                        MqttMessage message = MqttMessageFactory.newMessage(recFixedHeader, msgIdVariableHeader, null);
//                        getChannel().write(message);

                        AcknowledgeMessage acknowledgeMessage = AcknowledgeMessage.from(publishMessage.getMessageId(), publishMessage.isDup(), publishMessage.getQos(), publishMessage.isRetain(), !publishMessage.isInBound());
                        acknowledgeMessage.copyBase(publishMessage);
                        getWorker().pushToServer(acknowledgeMessage);



                    }

                } else {

                    if (publishMessage.getQos() == MqttQoS.EXACTLY_ONCE.value() && publishMessage.isReleased()) {

                        //We need to generate a PUBREL message to allow transmission of qos 2 message.
//                        MqttFixedHeader recFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, publishMessage.isDup(), MqttQoS.AT_LEAST_ONCE, publishMessage.isRetain(), 0);
//                        MqttMessageIdVariableHeader msgIdVariableHeader = MqttMessageIdVariableHeader.from(publishMessage.getMessageId());
//                        MqttMessage message = MqttMessageFactory.newMessage(recFixedHeader, msgIdVariableHeader, null);
//                        getChannel().write(message);

                        ReleaseMessage releaseMessage = ReleaseMessage.from(publishMessage.getMessageId(), publishMessage.isDup(), MqttQoS.AT_LEAST_ONCE.value(), publishMessage.isRetain(), publishMessage.isInBound());
                        releaseMessage.copyBase(publishMessage);
                        getWorker().pushToServer(releaseMessage);




                    } else {

                        //This message should be released to the client
                        PublishOutHandler handler = new PublishOutHandler(publishMessage, client.getProtocalData());
                        handler.setWorker(getWorker());

                        try {
                            handler.handle();
                        } catch (RetriableException | UnRetriableException e) {
                            log.error(" process : problems releasing stored messages", e);
                        }

                    }
                }


            }
        });


    }



}
