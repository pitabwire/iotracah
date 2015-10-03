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

import com.caricah.iotracah.core.worker.state.messages.*;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.server.transform.MqttIOTTransformer;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/23/15
 */
public class HttpIOTTransformerImpl implements MqttIOTTransformer<FullHttpMessage> {

    private static final Logger log = LoggerFactory.getLogger(HttpIOTTransformerImpl.class);

    @Override
    public IOTMessage toIOTMessage(FullHttpMessage serverMessage) {


        if (serverMessage instanceof FullHttpRequest) {

            FullHttpRequest request = (FullHttpRequest) serverMessage;
            final String content = request.content().toString(CharsetUtil.UTF_8);
            final JSONObject json = new JSONObject(content);

            log.debug(" toIOTMessage : received content {} ", content);


            final String path = request.uri().toUpperCase();

            switch (path) {

                case "/CONNECT":

                   
                    return ConnectMessage.from(
                            false, 1, false,
                            "MQTT", 4, false, json.getString("clientId"),
                            json.getString("username"), json.getString("password"),
                                    0, "");

                case "/PUBLISH":

                    ByteBuffer byteBuffer = ByteBuffer.wrap(json.getString("payload").getBytes());

                            PublishMessage publishMessage = PublishMessage.from(json.getLong("messageId"), json.getBoolean("dup"), json.getInt("qos"),
                                    json.getBoolean("retain"), json.getString("topic"), byteBuffer, true);

                    publishMessage.setClientIdentifier(json.getString("clientId"));
                    publishMessage.setPartition(json.getString("partition"));
                    publishMessage.setAuthKey(json.getString("authKey"));
                    return publishMessage;



                case "/SUBSCRIBE":

                   
                    SubscribeMessage subscribeMessage = SubscribeMessage.from(1, false, 1, false);

                    JSONArray jsonTopicQosList = json.getJSONArray("topicQosList");
                    for(int i=0; i< jsonTopicQosList.length(); i++) {
                        JSONObject topicQos = jsonTopicQosList.getJSONObject(i);

                        String topic = (String) topicQos.keys().next();
                        int qos = topicQos.getInt(topic);

                        Map.Entry<String, Integer> entry =
                                new AbstractMap.SimpleEntry<>(topic, qos);
                        subscribeMessage.getTopicFilterList().add(entry);
                    }
                    subscribeMessage.setReceptionUrl(json.getString("recipientUrl"));
                    subscribeMessage.setClientIdentifier(json.getString("clientId"));
                    subscribeMessage.setPartition(json.getString("partition"));
                    subscribeMessage.setAuthKey(json.getString("authKey"));


                    return subscribeMessage;

                case "/UNSUBSCRIBE":

                    List<String> topicList = new ArrayList<>();
                    JSONArray jsonTopicList = json.getJSONArray("topicQosList");
                    for(int i=0; i< jsonTopicList.length(); i++) {
                        String topic = jsonTopicList.getString(i);

                        topicList.add(topic);
                    }

                    UnSubscribeMessage unSubscribeMessage = UnSubscribeMessage.from(1, false, 1, false, topicList);
                    unSubscribeMessage.setClientIdentifier(json.getString("clientId"));
                    unSubscribeMessage.setPartition(json.getString("partition"));
                    unSubscribeMessage.setAuthKey(json.getString("authKey"));

                case "/DISCONNECT":
                    
                    DisconnectMessage disconMessage = DisconnectMessage.from(true);
                    disconMessage.setClientIdentifier(json.getString("clientId"));
                    disconMessage.setPartition(json.getString("partition"));
                    disconMessage.setAuthKey(json.getString("authKey"));

                    return disconMessage;

                default:
                    return null;
            }


        }

        return null;
    }


}
