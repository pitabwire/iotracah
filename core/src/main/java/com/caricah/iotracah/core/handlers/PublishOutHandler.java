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


import com.caricah.iotracah.core.handlers.protocal.http.PushHandler;
import com.caricah.iotracah.core.worker.state.messages.AcknowledgeMessage;
import com.caricah.iotracah.core.worker.state.messages.PublishMessage;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.body.MultipartBody;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class PublishOutHandler extends RequestHandler<PublishMessage> {

    private String protocalData;

    public PublishOutHandler(PublishMessage message, String protocalData) {
        super(message);
        this.protocalData = protocalData;
    }

    @Override
    public void handle() throws RetriableException, UnRetriableException {

        log.debug(" handle : outbound message {} being processed", getMessage());

        if (getMessage().getProtocal().isPersistent()) {

            //We need to generate a publish message to start this conversation.
            pushToServer(getMessage());

        } else {
            switch (getMessage().getProtocal()) {

                case HTTP:
                    PushHandler httpPushHandler = new PushHandler();
                    httpPushHandler.pushToUrl(protocalData, getMessage());
                    break;
                default:
                    log.error(" handle : outbound message {} using none implemented protocal");
            }
        }
    }


}
