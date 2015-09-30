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

package com.caricah.iotracah.server.httpserver.transform;

import com.caricah.iotracah.core.worker.state.messages.ConnectMessage;
import com.caricah.iotracah.core.worker.state.messages.DisconnectMessage;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.messages.SubscribeMessage;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.server.httpserver.transform.json.Connect;
import com.caricah.iotracah.server.httpserver.transform.json.Disconnect;
import com.caricah.iotracah.server.httpserver.transform.json.Publish;
import com.caricah.iotracah.server.httpserver.transform.json.Request;
import com.caricah.iotracah.server.transform.MqttIOTTransformer;
import com.google.gson.Gson;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/23/15
 */
public class HttpIOTTransformerImpl implements MqttIOTTransformer<FullHttpMessage> {

    private final Gson gson = new Gson();

    @Override
    public IOTMessage toIOTMessage(FullHttpMessage serverMessage) {


        if (serverMessage instanceof FullHttpRequest) {

            FullHttpRequest request = (FullHttpRequest) serverMessage;
            final String content = request.content().toString(CharsetUtil.UTF_8);


            final String path = request.uri().toUpperCase();

            switch (path) {


                case "/PUBLISH":

                    Publish pubContent = gson.fromJson(content, Publish.class);

                    PublishMessage publishMessage = PublishMessage.from(pubContent.getMessageId(), pubContent.isDup(), pubContent.getQos(),
                            pubContent.isRetain(), pubContent.getTopic(), pubContent.getPayload(), true);

                    publishMessage.setClientIdentifier(pubContent.getClientId());
                    publishMessage.setPartition(pubContent.getPartition());
                    return publishMessage;

                case "/CONNECT":

                    Connect conContent = gson.fromJson(content, Connect.class);

                    ConnectMessage connectMessage = ConnectMessage.from(
                            conContent.isDup(), conContent.getQos(), conContent.isRetain(),
                            "MQTT", 4, false, conContent.getClientId(),
                            conContent.getUsername(), conContent.getPassword(),
                            0, "");

                    SubscribeMessage subscribeMessage = SubscribeMessage.from(1, conContent.isDup(), conContent.getQos(), conContent.isRetain() );
                    subscribeMessage.getTopicFilterList().addAll(conContent.getTopicQosList());
                    subscribeMessage.setReceptionUrl(conContent.getRecipientUrl());

                    connectMessage.setPayload(subscribeMessage);

                    return connectMessage;


                case "/DISCONNECT":
                    Disconnect disconContent = gson.fromJson(content, Disconnect.class);

                    DisconnectMessage disconMessage = DisconnectMessage.from(true);
                    disconMessage.setClientIdentifier(disconContent.getClientId());
                    disconMessage.setPartition(disconContent.getPartition());

                    return disconMessage;

                default:
                    return null;
            }


        }

        return null;
    }


}
